#!/usr/bin/env python3
"""
Mega download helper with folder file-selection support.

Normal flow (no -s flag or file link):
    add_mega_download → fetch node → download whole node → onDownloadComplete

Selection flow (-s flag + folder link):
    add_mega_download → fetch root node → collect_mega_files() → show inline UI
    → wait for user confirmation → download each selected node sequentially
    → onDownloadComplete

The MegaAppListener now supports two modes:
  • Single-file mode  — original behaviour, onTransferFinish calls onDownloadComplete
  • Multi-file mode   — onTransferFinish just signals the event; the main loop
                        calls onDownloadComplete after all files finish
"""
from secrets import token_hex
from asyncio import Event
from os import path as ospath
from aiofiles.os import makedirs
from mega import MegaApi, MegaListener, MegaRequest, MegaTransfer, MegaError

from bot import (
    LOGGER,
    config_dict,
    download_dict_lock,
    download_dict,
    non_queued_dl,
    queue_dict_lock,
)
from bot.helper.telegram_helper.message_utils import sendMessage, sendStatusMessage
from bot.helper.ext_utils.bot_utils import (
    get_mega_link_type,
    async_to_sync,
    sync_to_async,
    get_readable_file_size,
)
from bot.helper.mirror_utils.status_utils.mega_download_status import MegaDownloadStatus
from bot.helper.mirror_utils.status_utils.queue_status import QueueStatus
from bot.helper.ext_utils.task_manager import (
    is_queued,
    limit_checker,
    stop_duplicate_check,
)

# ─── Shared selection cache ───────────────────────────────────────────────────
# Populated by add_mega_download, consumed by mega_select.py callbacks.
# Key: listener.uid (message ID).
mega_select_cache: dict = {}


# ─── File-tree collector ──────────────────────────────────────────────────────

def collect_mega_files(api: MegaApi, folder_api, parent_node, prefix: str, files: list):
    """
    Recursively walk a Mega folder node tree and append file entries to `files`.

    For public-folder links `folder_api` is set; nodes must be authorised via
    folder_api.authorizeNode() before they can be downloaded via the main `api`.
    For private/account links `folder_api` is None and `api` is used directly.

    Each entry in `files` is:
        {
            'name':     filename (no path),
            'rel_path': relative path inside the root folder,
            'size':     int (bytes),
            'node':     MegaNode authorised for download,
            'selected': True,
        }
    """
    list_api = folder_api if folder_api is not None else api
    children = list_api.getChildren(parent_node)
    if children is None:
        return
    for i in range(children.size()):
        child      = children.get(i)
        child_name = child.getName()
        rel_path   = f"{prefix}/{child_name}" if prefix else child_name

        if child.isFile():
            # Authorise node for download through the main api (public folders only)
            dl_node = folder_api.authorizeNode(child) if folder_api is not None else child
            files.append({
                'name':     child_name,
                'rel_path': rel_path,
                'size':     list_api.getSize(child),
                'node':     dl_node,
                'selected': True,
            })
        else:
            # Recurse into sub-folder using the same list_api perspective
            collect_mega_files(api, folder_api, child, rel_path, files)


# ─── MegaAppListener ─────────────────────────────────────────────────────────

class MegaAppListener(MegaListener):
    _NO_EVENT_ON = (MegaRequest.TYPE_LOGIN, MegaRequest.TYPE_FETCH_NODES)
    NO_ERROR = "no error"

    def __init__(self, continue_event: Event, listener):
        self.continue_event   = continue_event
        self.node             = None
        self.public_node      = None
        self.listener         = listener
        self.is_cancelled     = False
        self.error            = None

        self.__bytes_transferred = 0
        self.__speed             = 0
        self.__name              = ""

        # Multi-file mode state
        self.__multi_file_mode  = False
        self.__cumulative_bytes = 0   # bytes confirmed across already-finished files

        super().__init__()

    # ── Public helpers for multi-file mode ──

    def set_current_file(self, filename: str):
        """Tell the listener which filename to expect for the next transfer."""
        self.__name = filename

    def enable_multi_file_mode(self):
        """Switch listener to multi-file sequential mode."""
        self.__multi_file_mode  = True
        self.__bytes_transferred = 0
        self.__cumulative_bytes  = 0

    def advance_cumulative(self, file_size: int):
        """Called after each file finishes; adds its size to the running total."""
        self.__cumulative_bytes += file_size

    # ── Properties consumed by MegaDownloadStatus ──

    @property
    def speed(self):
        return self.__speed

    @property
    def downloaded_bytes(self):
        return self.__bytes_transferred

    # ── MegaListener callbacks ──

    def onRequestFinish(self, api, request, error):
        if str(error).lower() != "no error":
            self.error = error.copy()
            LOGGER.error(f"Mega onRequestFinishError: {self.error}")
            self.continue_event.set()
            return
        request_type = request.getType()
        if request_type == MegaRequest.TYPE_LOGIN:
            api.fetchNodes()
        elif request_type == MegaRequest.TYPE_GET_PUBLIC_NODE:
            self.public_node = request.getPublicMegaNode()
            self.__name      = self.public_node.getName()
        elif request_type == MegaRequest.TYPE_FETCH_NODES:
            LOGGER.info("Fetching Root Node.")
            self.node   = api.getRootNode()
            self.__name = self.node.getName()
            LOGGER.info(f"Node Name: {self.node.getName()}")
        if (
            request_type not in self._NO_EVENT_ON
            or self.node and "cloud drive" not in self.__name.lower()
        ):
            self.continue_event.set()

    def onRequestTemporaryError(self, api, request, error: MegaError):
        LOGGER.error(f"Mega Request error in {error}")
        if not self.is_cancelled:
            self.is_cancelled = True
            async_to_sync(
                self.listener.onDownloadError, f"RequestTempError: {error.toString()}"
            )
        self.error = error.toString()
        self.continue_event.set()

    def onTransferUpdate(self, api: MegaApi, transfer: MegaTransfer):
        if self.is_cancelled:
            api.cancelTransfer(transfer, None)
            self.continue_event.set()
            return
        self.__speed = transfer.getSpeed()
        if self.__multi_file_mode:
            # Show cumulative progress across all files
            self.__bytes_transferred = (
                self.__cumulative_bytes + transfer.getTransferredBytes()
            )
        else:
            self.__bytes_transferred = transfer.getTransferredBytes()

    def onTransferFinish(self, api: MegaApi, transfer: MegaTransfer, error):
        try:
            if self.is_cancelled:
                self.continue_event.set()
            elif transfer.isFinished():
                if self.__multi_file_mode:
                    # Signal the per-file await in add_mega_download's loop.
                    # onDownloadComplete is called by the loop after all files finish.
                    self.continue_event.set()
                elif transfer.isFolderTransfer() or transfer.getFileName() == self.__name:
                    async_to_sync(self.listener.onDownloadComplete)
                    self.continue_event.set()
        except Exception as e:
            LOGGER.error(e)

    def onTransferTemporaryError(self, api, transfer, error):
        filen  = transfer.getFileName()
        state  = transfer.getState()
        errStr = error.toString()
        LOGGER.error(f"Mega download error in file {transfer} {filen}: {error}")

        # States 1 (queued) and 4 (retrying) — let the SDK handle internally
        if state in [1, 4]:
            return

        self.error = errStr
        if not self.is_cancelled:
            self.is_cancelled = True
            if not self.__multi_file_mode:
                # Single-file: call error handler immediately
                async_to_sync(
                    self.listener.onDownloadError,
                    f"TransferTempError: {errStr} ({filen})",
                )
            # Multi-file: the download loop checks is_cancelled and calls onDownloadError
        self.continue_event.set()

    async def cancel_download(self):
        self.is_cancelled = True
        await self.listener.onDownloadError("Download Canceled by user")


# ─── AsyncExecutor ────────────────────────────────────────────────────────────

class AsyncExecutor:
    """Bridges Mega SDK's sync-callback model into asyncio."""

    def __init__(self):
        self.continue_event = Event()

    async def do(self, function, args):
        """Call *function(*args)* in a thread, then await the SDK callback event."""
        self.continue_event.clear()
        await sync_to_async(function, *args)
        await self.continue_event.wait()


# ─── Main entry point ─────────────────────────────────────────────────────────

async def add_mega_download(mega_link: str, path: str, listener, name: str):
    MEGA_EMAIL    = config_dict["MEGA_EMAIL"]
    MEGA_PASSWORD = config_dict["MEGA_PASSWORD"]

    executor     = AsyncExecutor()
    api          = MegaApi(None, None, None, "WZML-X")
    folder_api   = None
    mega_listener = MegaAppListener(executor.continue_event, listener)
    api.addListener(mega_listener)

    # ── Login (optional) ──
    if MEGA_EMAIL and MEGA_PASSWORD:
        await executor.do(api.login, (MEGA_EMAIL, MEGA_PASSWORD))

    # ── Fetch root node ──
    if get_mega_link_type(mega_link) == "file":
        await executor.do(api.getPublicNode, (mega_link,))
        node = mega_listener.public_node
    else:
        folder_api = MegaApi(None, None, None, "WZML-X")
        folder_api.addListener(mega_listener)
        await executor.do(folder_api.loginToFolder, (mega_link,))
        node = await sync_to_async(folder_api.authorizeNode, mega_listener.node)

    if mega_listener.error is not None:
        await sendMessage(listener.message, str(mega_listener.error))
        await executor.do(api.logout, ())
        if folder_api:
            await executor.do(folder_api.logout, ())
        return

    name = name or node.getName()

    # ── Duplicate check ──
    msg, button = await stop_duplicate_check(name, listener)
    if msg:
        await sendMessage(listener.message, msg, button)
        await executor.do(api.logout, ())
        if folder_api:
            await executor.do(folder_api.logout, ())
        return

    # ════════════════════════════════════════════════════════════════════════
    # FOLDER FILE SELECTION FLOW
    # Triggered when: -s flag is set AND the link is a folder (folder_api set)
    # ════════════════════════════════════════════════════════════════════════
    if listener.select and folder_api is not None:

        # ── Collect file tree ──
        files: list = []
        await sync_to_async(
            collect_mega_files, api, folder_api, mega_listener.node, "", files
        )

        if not files:
            await sendMessage(listener.message, "❌ No files found in this Mega folder.")
            await executor.do(api.logout, ())
            await executor.do(folder_api.logout, ())
            return

        # ── Populate cache + show UI ──
        sel_event = Event()
        mega_select_cache[listener.uid] = {
            'files':        files,
            'event':        sel_event,
            'is_cancelled': False,
            'folder_name':  name,
            'user_id':      listener.message.from_user.id,
        }

        def prepare_mega_file_list(files):
            file_list = []
            seen_dirs = set()
            for idx, f in enumerate(files):
                rel_path = f['rel_path']
                parts = rel_path.split('/')
                for i in range(len(parts) - 1):
                    dir_path = "/".join(parts[:i])
                    if dir_path:
                        dir_path += "/"
                    dir_name = parts[i]
                    full_dir_path = f"{dir_path}{dir_name}"
                    if full_dir_path not in seen_dirs:
                        seen_dirs.add(full_dir_path)
                        file_list.append({
                            "name": dir_name,
                            "path": dir_path,
                            "size": 0,
                            "id": f"dir_{full_dir_path}",
                            "is_dir": True,
                            "selected": True
                        })
                
                file_path = "/".join(parts[:-1])
                if file_path:
                    file_path += "/"
                file_list.append({
                    "name": f['name'],
                    "path": file_path,
                    "size": f['size'],
                    "id": str(idx),
                    "is_dir": False,
                    "selected": f.get('selected', True)
                })
            return file_list

        from web.mega_selection_store import MegaSelectionStore
        file_list = prepare_mega_file_list(files)
        MegaSelectionStore().save_data(listener.uid, file_list)
        all_ids = [str(idx) for idx, _ in enumerate(files)]
        MegaSelectionStore().save_selection(listener.uid, all_ids)

        from bot.modules.mega_select import mega_selection_buttons
        msg_text = "Your Mega download paused. Choose files from selection web link then press Done Selecting button."
        buttons = mega_selection_buttons(listener.uid)
        await sendMessage(listener.message, msg_text, buttons)

        # ── Wait for user action (Done / Cancel) ──
        await sel_event.wait()

        cache = mega_select_cache.pop(listener.uid, {})
        if cache.get('is_cancelled'):
            await listener.onDownloadError("Mega file selection cancelled by user")
            await executor.do(api.logout, ())
            await executor.do(folder_api.logout, ())
            return

        selected_files = [f for f in cache.get('files', []) if f['selected']]
        if not selected_files:
            await sendMessage(listener.message, "❌ No files selected. Download cancelled.")
            await executor.do(api.logout, ())
            await executor.do(folder_api.logout, ())
            return

        # ── Size limit check on selected total ──
        total_size = sum(f['size'] for f in selected_files)
        if limit_exceeded := await limit_checker(total_size, listener, isMega=True):
            await sendMessage(listener.message, limit_exceeded)
            await executor.do(api.logout, ())
            await executor.do(folder_api.logout, ())
            return

        # ── Queue check ──
        gid = token_hex(5)
        added_to_queue, queue_event = await is_queued(listener.uid)
        if added_to_queue:
            LOGGER.info(f"[MEGA SELECT] Added to queue: {name}")
            async with download_dict_lock:
                download_dict[listener.uid] = QueueStatus(
                    name, total_size, gid, listener, "Dl"
                )
            await listener.onDownloadStart()
            await sendStatusMessage(listener.message)
            await queue_event.wait()
            async with download_dict_lock:
                if listener.uid not in download_dict:
                    await executor.do(api.logout, ())
                    await executor.do(folder_api.logout, ())
                    return

        # ── Register status + start ──
        async with download_dict_lock:
            download_dict[listener.uid] = MegaDownloadStatus(
                name, total_size, gid, mega_listener,
                listener.message, listener.upload_details,
            )
        async with queue_dict_lock:
            non_queued_dl.add(listener.uid)

        if not added_to_queue:
            await listener.onDownloadStart()
        await sendStatusMessage(listener.message)
        LOGGER.info(
            f"[MEGA SELECT] Downloading {len(selected_files)} selected file(s) "
            f"from '{name}' — {get_readable_file_size(total_size)}"
        )

        # ── Switch listener to multi-file mode ──
        mega_listener.enable_multi_file_mode()

        # ── Sequential per-file download ──
        root_dest = ospath.join(path, name)
        await makedirs(root_dest, exist_ok=True)

        for f in selected_files:
            if mega_listener.is_cancelled:
                break

            rel      = f['rel_path']
            filename = rel.rsplit('/', 1)[-1] if '/' in rel else rel
            sub_rel  = rel.rsplit('/', 1)[0]  if '/' in rel else ''
            dest_dir = ospath.join(root_dest, sub_rel) if sub_rel else root_dest

            await makedirs(dest_dir, exist_ok=True)

            LOGGER.info(f"[MEGA SELECT] Downloading: {rel}")
            mega_listener.set_current_file(filename)

            await executor.do(
                api.startDownload,
                (f['node'], dest_dir + '/', filename, None, False, None),
            )

            if not mega_listener.is_cancelled:
                # Advance cumulative counter so overall progress bar is correct
                mega_listener.advance_cumulative(f['size'])

        # ── Finalise ──
        if not mega_listener.is_cancelled:
            await listener.onDownloadComplete()
        else:
            err = (
                f"TransferTempError: {mega_listener.error}"
                if mega_listener.error
                else "Download cancelled by user"
            )
            await listener.onDownloadError(err)

        await executor.do(api.logout, ())
        await executor.do(folder_api.logout, ())
        return

    # ════════════════════════════════════════════════════════════════════════
    # NORMAL (non-selection) DOWNLOAD FLOW — unchanged from original
    # ════════════════════════════════════════════════════════════════════════
    gid  = token_hex(5)
    size = api.getSize(node)

    if limit_exceeded := await limit_checker(size, listener, isMega=True):
        await sendMessage(listener.message, limit_exceeded)
        return

    added_to_queue, event = await is_queued(listener.uid)
    if added_to_queue:
        LOGGER.info(f"Added to Queue/Download: {name}")
        async with download_dict_lock:
            download_dict[listener.uid] = QueueStatus(name, size, gid, listener, "Dl")
        await listener.onDownloadStart()
        await sendStatusMessage(listener.message)
        await event.wait()
        async with download_dict_lock:
            if listener.uid not in download_dict:
                await executor.do(api.logout, ())
                if folder_api is not None:
                    await executor.do(folder_api.logout, ())
                return
        from_queue = True
        LOGGER.info(f"Start Queued Download from Mega: {name}")
    else:
        from_queue = False

    async with download_dict_lock:
        download_dict[listener.uid] = MegaDownloadStatus(
            name, size, gid, mega_listener, listener.message, listener.upload_details
        )
    async with queue_dict_lock:
        non_queued_dl.add(listener.uid)

    if from_queue:
        LOGGER.info(f"Start Queued Download from Mega: {name}")
    else:
        await listener.onDownloadStart()
        await sendStatusMessage(listener.message)
        LOGGER.info(f"Download from Mega: {name}")

    await makedirs(path, exist_ok=True)
    await executor.do(api.startDownload, (node, path, name, None, False, None))
    await executor.do(api.logout, ())
    if folder_api is not None:
        await executor.do(folder_api.logout, ())
