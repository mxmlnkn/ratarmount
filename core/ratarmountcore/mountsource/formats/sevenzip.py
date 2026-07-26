"""Custom 7z MountSource with real pack offsets for random access.

Unlike LibarchiveMountSource, this backend parses the 7z header once, stores
pack-stream offsets in the SQLite index, and opens members without re-scanning
the archive from the beginning.

Open support:
  - Store (Copy) members: true random access via StenciledFile
  - LZMA / LZMA2 / Deflate / BZip2: streaming folder decode with a chunk cache
    so large/solid members do not require holding the full unpacked folder in RAM
  - Encrypted folders: not supported (falls back via factory to py7zr)
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import io
import logging
import os
import stat
import tempfile
import threading
from pathlib import Path
from typing import IO, Iterator, Optional, Union, cast  # noqa: I001 — Union used for passwords

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.sevenzip import (
    SevenZipArchiveInfo,
    SevenZipError,
    SevenZipFileEntry,
    StreamingFolderDecoder,
    create_folder_decoder,
    decompress_folder,
    parse_7z_archive,
    prepare_folder_packed,
)
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.StenciledFile import RawStenciledFile, StenciledFile
from ratarmountcore.utils import RatarmountError, overrides

logger = logging.getLogger(__name__)

# Below this unpacked folder size, decompress the whole folder at once (cheaper for tiny archives).
_DEFAULT_SMALL_FOLDER_THRESHOLD = 4 * 1024 * 1024
_DEFAULT_CHUNK_SIZE = 1024 * 1024
_DEFAULT_MAX_CACHED_CHUNKS = 64
_READ_CHUNK_SIZE = 1024 * 1024
_NESTED_SPOOL_THRESHOLD = 16 * 1024 * 1024
_NESTED_SPOOL_MAGIC = b"7z\xbc\xaf\x27\x1c"
_ORPHAN_TEMP_SPOOLS_PRUNED = False
_NESTED_SPOOL_GLOBAL_LOCK = threading.Lock()
_NESTED_SPOOL_MIN_FILES = 50


def nested_spool_cache_dir() -> Path:
    override = os.environ.get("RATARMOUNT_NESTED_7Z_CACHE")
    if override:
        return Path(override)
    xdg = os.environ.get("XDG_CACHE_HOME")
    if xdg:
        return Path(xdg) / "ratarmount" / "nested-7z"
    return Path.home() / ".cache" / "ratarmount" / "nested-7z"


def nested_spool_cache_key(tar_file_name: str, archive_size: int) -> str:
    label = tar_file_name or "nested.7z"
    return hashlib.sha256(f"{label}:{archive_size}".encode()).hexdigest()


def validate_nested_spool_file(path: Path | str, expected_size: int) -> bool:
    spool_path = Path(path)
    try:
        if spool_path.stat().st_size != expected_size:
            return False
        with spool_path.open("rb") as handle:
            if handle.read(len(_NESTED_SPOOL_MAGIC)) != _NESTED_SPOOL_MAGIC:
                return False
            handle.seek(0)
            archive = parse_7z_archive(handle)
        if not archive.files:
            return False
        if expected_size > 100 * 1024 * 1024 and len(archive.files) < _NESTED_SPOOL_MIN_FILES:
            return False
        return True
    except OSError:
        return False
    except Exception:
        return False


def prune_orphan_temp_spools() -> int:
    """Remove legacy incomplete mkstemp spools from the system temp directory."""
    removed = 0
    for path in Path(tempfile.gettempdir()).glob("ratarmount-7z-*.7z"):
        with contextlib.suppress(OSError):
            path.unlink()
            removed += 1
    return removed


def read_bytes_at(file: IO[bytes], lock: threading.Lock, offset: int, size: int) -> bytes:
    """Read exactly *size* bytes at *offset*, tolerating short ``read()`` returns."""
    if size <= 0:
        return b""
    with lock:
        file.seek(offset)
        parts: list[bytes] = []
        remaining = size
        while remaining > 0:
            chunk = file.read(min(remaining, _READ_CHUNK_SIZE))
            if not chunk:
                got = size - remaining
                raise SevenZipError(
                    f"Short read while reading 7z data at offset {offset} "
                    f"(wanted {size} bytes, got {got})"
                )
            parts.append(chunk)
            remaining -= len(chunk)
    return b"".join(parts)


class SevenZipMemberFile(io.RawIOBase):
    """Seekable view of a fully-decompressed 7z member (small-folder path)."""

    def __init__(self, data: bytes):
        super().__init__()
        self._buffer = io.BytesIO(data)

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)

    @overrides(io.RawIOBase)
    def readinto(self, b) -> int:  # type: ignore[override]
        return self._buffer.readinto(b)  # type: ignore[arg-type]

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        return self._buffer.seek(offset, whence)

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self._buffer.tell()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self._buffer.close()
        super().close()


class SevenZipStreamingMemberFile(io.RawIOBase):
    """Seekable member view backed by a shared StreamingFolderDecoder.

    Positions are relative to the member; the decoder addresses absolute offsets
    inside the folder's unpacked stream (member_start + pos).
    """

    def __init__(self, decoder: StreamingFolderDecoder, member_start: int, member_size: int, lock: threading.Lock):
        super().__init__()
        self._decoder = decoder
        self._member_start = member_start
        self._member_size = member_size
        self._lock = lock
        self._pos = 0
        self._closed = False

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return not self._closed

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("read of closed file")
        if self._pos >= self._member_size:
            return b""
        if size is None or size < 0:
            size = self._member_size - self._pos
        size = min(size, self._member_size - self._pos)
        with self._lock:
            data = self._decoder.read_range(self._member_start + self._pos, size)
        self._pos += len(data)
        return data

    @overrides(io.RawIOBase)
    def readinto(self, b) -> int:  # type: ignore[override]
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            new_pos = offset
        elif whence == io.SEEK_CUR:
            new_pos = self._pos + offset
        elif whence == io.SEEK_END:
            new_pos = self._member_size + offset
        else:
            raise ValueError(f"invalid whence: {whence}")
        if new_pos < 0:
            raise ValueError("negative seek position")
        self._pos = min(new_pos, self._member_size)
        return self._pos

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self._pos

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self._closed = True
        super().close()


class SevenZipMountSource(SQLiteIndexMountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes], Path], **options) -> None:
        if isinstance(fileOrPath, Path):
            fileOrPath = str(fileOrPath)

        self.isFileObject = not isinstance(fileOrPath, str)
        self.fileObject: IO[bytes] = open(fileOrPath, "rb") if isinstance(fileOrPath, str) else fileOrPath
        self.fileObjectLock = threading.Lock()
        self._archive: Optional[SevenZipArchiveInfo] = None
        self.passwords: list[Union[str, bytes]] = list(options.get("passwords") or [])
        self._password: Optional[Union[str, bytes]] = None

        # Small-folder full-cache (legacy path for tiny archives).
        self._folderCache: dict[int, bytes] = {}
        self._folderCacheLock = threading.Lock()
        self._folderCacheMax = int(options.get("sevenZipFolderCacheMax", 4))

        # Streaming decoders for large folders (shared across member opens).
        self._streamDecoders: dict[int, StreamingFolderDecoder] = {}
        self._streamDecoderLock = threading.Lock()
        self._streamDecoderMax = int(options.get("sevenZipStreamDecoderMax", 4))
        self._chunkSize = int(options.get("sevenZipChunkSize", _DEFAULT_CHUNK_SIZE))
        self._maxCachedChunks = int(options.get("sevenZipMaxCachedChunks", _DEFAULT_MAX_CACHED_CHUNKS))
        self._smallFolderThreshold = int(
            options.get("sevenZipSmallFolderThreshold", _DEFAULT_SMALL_FOLDER_THRESHOLD)
        )
        self._packedStreamCache: dict[int, bytes] = {}
        self._packedCacheLock = threading.Lock()
        self._spool_path: str | None = None
        self._spool_cached = False
        self._tarFileName = str(options.get("tarFileName") or "nested.7z")

        try:
            self._maybe_spool_nested_archive()
            self._archive = parse_7z_archive(self.fileObject)
            self._password = self._resolve_password()
            self._entry_by_offsets: dict[tuple[int, int], SevenZipFileEntry] = {}
            for entry in self._archive.files:
                if entry.is_dir or entry.is_empty_stream:
                    continue
                self._entry_by_offsets[(entry.pack_offset, entry.unpack_offset)] = entry
        except SevenZipError:
            if not self.isFileObject:
                self.fileObject.close()
            raise
        self.blockSize = 512
        with contextlib.suppress(Exception):
            self.blockSize = os.fstat(self.fileObject.fileno()).st_blksize

        indexOptions = {
            "archiveFilePath": fileOrPath if isinstance(fileOrPath, str) else None,
            "backendName": "SevenZipMountSource",
            **{k: v for k, v in options.items() if k not in ("indexFilePath",)},
        }
        if "indexFilePath" in options:
            indexOptions["indexFilePath"] = options["indexFilePath"]

        super().__init__(**indexOptions)
        self._finalize_index(self._create_index)

    def _resolve_password(self) -> Optional[Union[str, bytes]]:
        """Pick a working password for encrypted folders, or None if unencrypted."""
        assert self._archive is not None
        encrypted_folders = [f for f in self._archive.folders if f.is_encrypted()]
        if not encrypted_folders:
            return None

        # Verify codec support up front.
        for folder in encrypted_folders:
            if not folder.is_supported_for_open(allow_encrypted=True):
                raise SevenZipError(
                    f"Unsupported encrypted 7z coder chain: {[c.method.hex() for c in folder.coders]}"
                )

        candidates: list[Optional[Union[str, bytes]]] = [None, *self.passwords]
        # Find a non-empty data entry to trial-decrypt.
        trial_entry = next((e for e in self._archive.files if e.folder_index is not None and e.size > 0), None)
        if trial_entry is None:
            # Hierarchy-only archive; accept first password if any.
            return self.passwords[0] if self.passwords else None

        folder = self._archive.folders[trial_entry.folder_index]  # type: ignore[index]
        packed = self._read_packed(trial_entry)
        last_error: Optional[Exception] = None
        for password in candidates:
            if password is None:
                continue
            try:
                content_folder, content_packed = prepare_folder_packed(folder, packed, password=password)
                # Ensure the content codec can actually produce bytes.
                if content_folder.get_unpack_size() > 0:
                    sample = decompress_folder(content_folder, content_packed)
                    if len(sample) != content_folder.get_unpack_size():
                        continue
                return password
            except Exception as exception:  # noqa: BLE001 — trial passwords may fail loudly
                last_error = exception
                continue

        if self.passwords:
            raise SevenZipError(
                f"Could not decrypt 7z archive with the provided password(s): {last_error}"
            ) from last_error
        raise SevenZipError(
            "7z archive contents are encrypted; pass passwords=[...] or use --password"
        )

    def _create_index(self) -> None:
        assert self._archive is not None
        rows = []
        for index, entry in enumerate(self._archive.files):
            rows.append(self._convert_to_row(entry, index))
        self.index.set_file_infos(rows)

    def _nested_stream_size(self) -> int:
        with self.fileObjectLock:
            self.fileObject.seek(0, io.SEEK_END)
            archive_size = self.fileObject.tell()
            self.fileObject.seek(0)
        return archive_size

    @contextlib.contextmanager
    def _nested_spool_lock(self, lock_path: Path) -> Iterator[None]:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def _write_nested_spool(self, archive_size: int, partial_path: Path) -> None:
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        with partial_path.open("wb") as out:
            with self.fileObjectLock:
                self.fileObject.seek(0)
                remaining = archive_size
                while remaining > 0:
                    chunk = self.fileObject.read(min(remaining, _READ_CHUNK_SIZE))
                    if not chunk:
                        raise SevenZipError(
                            "Short read while spooling nested 7z archive "
                            f"(got {archive_size - remaining} of {archive_size} bytes)"
                        )
                    out.write(chunk)
                    remaining -= len(chunk)
            out.flush()
            os.fsync(out.fileno())
        if partial_path.stat().st_size != archive_size:
            raise SevenZipError(
                f"Nested 7z spool size mismatch: wrote {partial_path.stat().st_size}, "
                f"expected {archive_size}"
            )
        if not validate_nested_spool_file(partial_path, archive_size):
            raise SevenZipError(
                f"Nested 7z spool failed validation for {partial_path} ({archive_size} bytes)"
            )

    def _use_nested_spool_file(self, path: Path) -> None:
        self._spool_path = str(path)
        self._spool_cached = True
        self.fileObject = path.open("rb")

    def _maybe_spool_nested_archive(self) -> None:
        """Copy nested archive streams to a validated on-disk cache for reliable seeks."""
        global _ORPHAN_TEMP_SPOOLS_PRUNED  # noqa: PLW0603 — one-time temp cleanup
        if not self.isFileObject:
            return
        if not _ORPHAN_TEMP_SPOOLS_PRUNED:
            removed = prune_orphan_temp_spools()
            if removed:
                logger.info("Removed %d legacy incomplete nested 7z temp spool(s)", removed)
            _ORPHAN_TEMP_SPOOLS_PRUNED = True

        archive_size = self._nested_stream_size()
        if archive_size <= _NESTED_SPOOL_THRESHOLD:
            return

        cache_dir = nested_spool_cache_dir()
        cache_key = nested_spool_cache_key(self._tarFileName, archive_size)
        cache_path = cache_dir / f"{cache_key}.7z"
        lock_path = cache_dir / f"{cache_key}.lock"
        partial_path = cache_path.with_suffix(".7z.partial")

        if validate_nested_spool_file(cache_path, archive_size):
            logger.info(
                "Reusing cached nested 7z spool %s (%d bytes, %s)",
                cache_path,
                archive_size,
                self._tarFileName,
            )
            self._use_nested_spool_file(cache_path)
            return

        with self._nested_spool_lock(lock_path):
            if validate_nested_spool_file(cache_path, archive_size):
                logger.info(
                    "Reusing cached nested 7z spool %s (%d bytes, %s)",
                    cache_path,
                    archive_size,
                    self._tarFileName,
                )
                self._use_nested_spool_file(cache_path)
                return

            for stale in (cache_path, partial_path):
                if stale.exists() and not validate_nested_spool_file(stale, archive_size):
                    with contextlib.suppress(OSError):
                        stale.unlink()

            logger.info(
                "Spooling nested 7z archive %s (%d bytes) to %s",
                self._tarFileName,
                archive_size,
                cache_path,
            )
            try:
                with _NESTED_SPOOL_GLOBAL_LOCK:
                    self._write_nested_spool(archive_size, partial_path)
                partial_path.replace(cache_path)
            except Exception:
                with contextlib.suppress(OSError):
                    partial_path.unlink()
                raise

        self._use_nested_spool_file(cache_path)

    def _read_at(self, offset: int, size: int) -> bytes:
        return read_bytes_at(self.fileObject, self.fileObjectLock, offset, size)

    def _representative_entry_for_folder(self, folder_index: int) -> SevenZipFileEntry:
        assert self._archive is not None
        for entry in self._archive.files:
            if entry.folder_index == folder_index and not entry.is_dir and not entry.is_empty_stream:
                return entry
        raise SevenZipError(f"No data entry found for 7z folder {folder_index}")

    def _read_packed_for_folder(self, folder_index: int) -> bytes:
        with self._packedCacheLock:
            cached = self._packedStreamCache.get(folder_index)
            if cached is not None:
                return cached

        entry = self._representative_entry_for_folder(folder_index)
        packed = self._read_at(entry.pack_offset, entry.pack_size)
        with self._packedCacheLock:
            self._packedStreamCache[folder_index] = packed
        return packed

    def _read_packed(self, entry: SevenZipFileEntry) -> bytes:
        if entry.folder_index is None:
            return self._read_at(entry.pack_offset, entry.pack_size)
        return self._read_packed_for_folder(entry.folder_index)

    def _read_member_bytes(self, entry: SevenZipFileEntry) -> bytes:
        """Read full member contents (used for symlink targets at index time)."""
        if entry.size == 0 or entry.folder_index is None:
            return b""
        folder = self._archive.folders[entry.folder_index]  # type: ignore[index]
        if folder.is_copy_only() and not folder.is_encrypted():
            return self._read_at(entry.pack_offset + entry.unpack_offset, entry.size)
        # Prefer streaming decoder so large solid folders are not fully loaded for a symlink.
        decoder = self._get_stream_decoder(entry)
        with self._streamDecoderLock:
            return decoder.read_range(entry.unpack_offset, entry.size)
    def _convert_to_row(self, entry: SevenZipFileEntry, entry_index: int) -> tuple:
        path, name = SQLiteIndex.normpath(self.transform(entry.path)).rsplit("/", 1)
        mode = entry.mode
        if entry.is_dir and not stat.S_ISDIR(mode):
            mode = (mode & 0o777) | stat.S_IFDIR
        elif not entry.is_dir and not stat.S_ISLNK(mode) and not stat.S_ISREG(mode):
            mode = (mode & 0o777) | stat.S_IFREG

        linkname = ""
        size = entry.size
        if stat.S_ISLNK(mode):
            try:
                linkname = self._read_member_bytes(entry).decode("utf-8", errors="surrogateescape")
            except Exception as exception:
                logger.warning("Failed to read symlink target for %s: %s", entry.path, exception)
            size = 0

        if entry.folder_index is not None:
            header_offset = entry.pack_offset
        else:
            header_offset = (1 << 62) + entry_index
        data_offset = entry.unpack_offset

        # fmt: off
        return (
            path,            # 0  path
            name,            # 1  name
            header_offset,   # 2  offsetheader (pack offset)
            data_offset,     # 3  offset (unpack offset in folder)
            size,            # 4  size
            entry.mtime,     # 5  mtime
            mode,            # 6  mode
            0,               # 7  type
            linkname,        # 8  linkname
            0,               # 9  uid
            0,               # 10 gid
            False,           # 11 isTar
            False,           # 12 isSparse
            False,           # 13 isGenerated
            0,               # 14 recursion depth
        )
        # fmt: on

    def _find_entry(self, fileInfo: FileInfo) -> SevenZipFileEntry:
        assert self._archive is not None
        user_data = SQLiteIndex.get_index_userdata(fileInfo.userdata)
        pack_offset = user_data.offsetheader
        unpack_offset = user_data.offset

        entry = self._entry_by_offsets.get((pack_offset, unpack_offset))
        if entry is not None:
            if entry.size == fileInfo.size or (stat.S_ISLNK(fileInfo.mode) and fileInfo.size == 0):
                return entry

        for entry in self._archive.files:
            if entry.is_dir or entry.is_empty_stream:
                continue
            # Symlinks are stored with size 0 in the index but may have pack location.
            if entry.pack_offset == pack_offset and entry.unpack_offset == unpack_offset:
                if entry.size == fileInfo.size or (stat.S_ISLNK(fileInfo.mode) and fileInfo.size == 0):
                    return entry

        raise RatarmountError(
            f"Could not locate 7z member for pack_offset={pack_offset} unpack_offset={unpack_offset}"
        )

    def _get_folder_bytes(self, entry: SevenZipFileEntry) -> bytes:
        """Full-folder decompress path for small folders."""
        assert self._archive is not None
        if entry.folder_index is None:
            raise RatarmountError("Entry has no folder")

        with self._folderCacheLock:
            cached = self._folderCache.get(entry.folder_index)
            if cached is not None:
                return cached

        folder = self._archive.folders[entry.folder_index]
        packed = self._read_packed(entry)
        data = decompress_folder(folder, packed, password=self._password)

        with self._folderCacheLock:
            if len(self._folderCache) >= self._folderCacheMax:
                self._folderCache.pop(next(iter(self._folderCache)))
            self._folderCache[entry.folder_index] = data
        return data

    def _get_stream_decoder(self, entry: SevenZipFileEntry) -> StreamingFolderDecoder:
        assert self._archive is not None
        if entry.folder_index is None:
            raise RatarmountError("Entry has no folder")

        with self._streamDecoderLock:
            decoder = self._streamDecoders.get(entry.folder_index)
            if decoder is not None:
                return decoder

        folder = self._archive.folders[entry.folder_index]
        packed = self._read_packed(entry)
        content_folder, content_packed = prepare_folder_packed(folder, packed, password=self._password)
        decoder = create_folder_decoder(
            content_folder,
            content_packed,
            chunk_size=self._chunkSize,
            max_cached_chunks=self._maxCachedChunks,
        )

        with self._streamDecoderLock:
            if len(self._streamDecoders) >= self._streamDecoderMax:
                self._streamDecoders.pop(next(iter(self._streamDecoders)))
            self._streamDecoders[entry.folder_index] = decoder
            return self._streamDecoders[entry.folder_index]

    def _open_store(self, entry: SevenZipFileEntry, buffering: int) -> IO[bytes]:
        offset = entry.pack_offset + entry.unpack_offset
        size = entry.size
        if buffering == 0:
            return cast(IO[bytes], RawStenciledFile([(self.fileObject, offset, size)], self.fileObjectLock))
        return cast(
            IO[bytes],
            StenciledFile(
                [(self.fileObject, offset, size)],
                self.fileObjectLock,
                bufferSize=self.blockSize if buffering == -1 else buffering,
            ),
        )

    def _open_compressed(self, entry: SevenZipFileEntry) -> IO[bytes]:
        assert self._archive is not None
        assert entry.folder_index is not None
        folder = self._archive.folders[entry.folder_index]
        folder_size = folder.get_unpack_size()

        # Small folders: full decompress is simpler and avoids stream overhead.
        if folder_size <= self._smallFolderThreshold:
            folder_data = self._get_folder_bytes(entry)
            end = entry.unpack_offset + entry.size
            if end > len(folder_data):
                raise SevenZipError(
                    f"Member slice [{entry.unpack_offset}:{end}] exceeds folder size {len(folder_data)}"
                )
            return cast(IO[bytes], SevenZipMemberFile(folder_data[entry.unpack_offset : end]))

        decoder = self._get_stream_decoder(entry)
        return cast(
            IO[bytes],
            SevenZipStreamingMemberFile(decoder, entry.unpack_offset, entry.size, self._streamDecoderLock),
        )

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering: int = -1) -> IO[bytes]:
        if stat.S_ISDIR(fileInfo.mode):
            raise RatarmountError("Cannot open directory as file")
        if stat.S_ISLNK(fileInfo.mode):
            raise RatarmountError("Cannot read contents of symbolic link!")
        if fileInfo.size == 0:
            return cast(IO[bytes], io.BytesIO(b""))

        entry = self._find_entry(fileInfo)
        assert entry.folder_index is not None
        folder = self._archive.folders[entry.folder_index]  # type: ignore[index]

        allow_encrypted = (not folder.is_encrypted()) or (self._password is not None)
        if not folder.is_supported_for_open(allow_encrypted=allow_encrypted):
            raise SevenZipError(
                f"Unsupported 7z folder codecs for {entry.path!r}: "
                f"{[m.hex() for m in folder.methods()]}"
            )

        if folder.is_copy_only() and not folder.is_encrypted():
            return self._open_store(entry, buffering)
        return self._open_compressed(entry)

    @overrides(SQLiteIndexMountSource)
    def close(self) -> None:
        super().close()
        with self._folderCacheLock:
            self._folderCache.clear()
        with self._packedCacheLock:
            self._packedStreamCache.clear()
        with self._streamDecoderLock:
            self._streamDecoders.clear()
        if getattr(self, "fileObject", None) is not None:
            if self._spool_path or not self.isFileObject:
                self.fileObject.close()
                self.fileObject = None  # type: ignore[assignment]
        if self._spool_path and not self._spool_cached:
            with contextlib.suppress(OSError):
                os.unlink(self._spool_path)
        self._spool_path = None
        self._spool_cached = False
