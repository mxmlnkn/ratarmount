"""Pure-Python 7z archive header parser and member location helpers.

This module intentionally avoids third-party 7z libraries so ratarmount can
build indexes with real pack-stream offsets for random access. Decompression
of common codecs (Copy, LZMA, LZMA2) uses the stdlib ``lzma`` module.
"""

from __future__ import annotations

import dataclasses
import hashlib
import io
import lzma
import struct
import zlib
from collections.abc import Sequence
from typing import BinaryIO, Optional, Union

from .utils import RatarmountError

try:
    from Cryptodome.Cipher import AES as _AES
except ImportError:  # pragma: no cover
    try:
        from Crypto.Cipher import AES as _AES  # type: ignore
    except ImportError:
        _AES = None  # type: ignore

MAGIC_7Z = b"7z\xbc\xaf'\x1c"
SIGNATURE_HEADER_SIZE = 32

# Property IDs from the 7z format specification.
PROP_END = 0x00
PROP_HEADER = 0x01
PROP_ARCHIVE_PROPERTIES = 0x02
PROP_ADDITIONAL_STREAMS_INFO = 0x03
PROP_MAIN_STREAMS_INFO = 0x04
PROP_FILES_INFO = 0x05
PROP_PACK_INFO = 0x06
PROP_UNPACK_INFO = 0x07
PROP_SUBSTREAMS_INFO = 0x08
PROP_SIZE = 0x09
PROP_CRC = 0x0A
PROP_FOLDER = 0x0B
PROP_CODERS_UNPACK_SIZE = 0x0C
PROP_NUM_UNPACK_STREAM = 0x0D
PROP_EMPTY_STREAM = 0x0E
PROP_EMPTY_FILE = 0x0F
PROP_ANTI = 0x10
PROP_NAME = 0x11
PROP_CREATION_TIME = 0x12
PROP_LAST_ACCESS_TIME = 0x13
PROP_LAST_WRITE_TIME = 0x14
PROP_ATTRIBUTES = 0x15
PROP_COMMENT = 0x16
PROP_ENCODED_HEADER = 0x17
PROP_START_POS = 0x18
PROP_DUMMY = 0x19

# Compression method IDs.
METHOD_COPY = b"\x00"
METHOD_LZMA = b"\x03\x01\x01"
METHOD_LZMA2 = b"\x21"
METHOD_BCJ = b"\x03\x03\x01\x03"
METHOD_BCJ2 = b"\x03\x03\x01\x1b"
METHOD_DELTA = b"\x03"
METHOD_AES = b"\x06\xf1\x07\x01"
METHOD_BZIP2 = b"\x04\x02\x02"
METHOD_DEFLATE = b"\x04\x01\x08"
METHOD_PPMD = b"\x03\x04\x01"

# Windows FILETIME epoch (1601-01-01) vs Unix epoch (1970-01-01).
_FILETIME_UNIX_DELTA = 116444736000000000
_WINDOWS_DIRECTORY_ATTR = 0x10
_WINDOWS_UNIX_ATTR_MASK = 0xFFFF0000


class SevenZipError(RatarmountError):
    """Raised when a 7z archive cannot be parsed or a codec is unsupported."""


def _crc32(data: bytes, value: int = 0) -> int:
    return zlib.crc32(data, value) & 0xFFFFFFFF


def _read_exact(file: BinaryIO, size: int) -> bytes:
    data = file.read(size)
    if len(data) != size:
        raise SevenZipError(f"Unexpected end of 7z data (wanted {size} bytes, got {len(data)})")
    return data


def _read_byte(file: BinaryIO) -> int:
    return _read_exact(file, 1)[0]


def _read_uint64(file: BinaryIO) -> int:
    """Read a variable-length UINT64 as defined by the 7z format."""
    first = _read_byte(file)
    if first == 0xFF:
        return struct.unpack("<Q", _read_exact(file, 8))[0]

    mask = 0x80
    extra = 0
    while mask and (first & mask):
        extra += 1
        mask >>= 1

    if extra == 0:
        return first

    value_bytes = _read_exact(file, extra)
    value = int.from_bytes(value_bytes, "little")
    high_bits = first & (mask - 1) if mask else 0
    # When all 7 high bits of first byte are set (extra==7), the first byte is only a length marker.
    if extra < 7:
        value |= high_bits << (extra * 8)
    return value


def _read_bools(file: BinaryIO, count: int, check_all: bool = False) -> list[bool]:
    if check_all:
        all_defined = _read_byte(file)
        if all_defined != 0:
            return [True] * count
    result: list[bool] = []
    bit = 0
    byte = 0
    for _ in range(count):
        if bit == 0:
            byte = _read_byte(file)
            bit = 0x80
        result.append(bool(byte & bit))
        bit >>= 1
    return result


def _read_utf16z(file: BinaryIO) -> str:
    chars: list[bytes] = []
    while True:
        pair = _read_exact(file, 2)
        if pair == b"\x00\x00":
            break
        chars.append(pair)
    return b"".join(chars).decode("utf-16-le")


def _filetime_to_unix(filetime: int) -> float:
    if filetime <= 0:
        return 0.0
    # Convert 100-ns intervals to seconds.
    return (filetime - _FILETIME_UNIX_DELTA) / 10_000_000


@dataclasses.dataclass
class Coder:
    method: bytes
    num_in_streams: int = 1
    num_out_streams: int = 1
    properties: Optional[bytes] = None


@dataclasses.dataclass
class Folder:
    coders: list[Coder] = dataclasses.field(default_factory=list)
    bind_pairs: list[tuple[int, int]] = dataclasses.field(default_factory=list)
    packed_indices: list[int] = dataclasses.field(default_factory=list)
    unpack_sizes: list[int] = dataclasses.field(default_factory=list)
    has_crc: bool = False
    crc: int = 0

    def total_in_streams(self) -> int:
        return sum(c.num_in_streams for c in self.coders)

    def total_out_streams(self) -> int:
        return sum(c.num_out_streams for c in self.coders)

    def get_unpack_size(self) -> int:
        if not self.unpack_sizes:
            return 0
        # Primary output is the unbound out-stream; for simple folders this is the last size.
        return self.unpack_sizes[-1]

    def methods(self) -> tuple[bytes, ...]:
        return tuple(c.method for c in self.coders)

    def is_copy_only(self) -> bool:
        return len(self.coders) == 1 and self.coders[0].method == METHOD_COPY

    def is_encrypted(self) -> bool:
        return any(c.method == METHOD_AES for c in self.coders)

    def is_supported_for_open(self, *, allow_encrypted: bool = False) -> bool:
        """Return whether this folder can be opened with the current decoder set."""
        if not self.coders:
            return False
        coders = list(self.coders)
        if coders and coders[0].method == METHOD_AES:
            if not allow_encrypted:
                return False
            coders = coders[1:]
            if not coders:
                # AES-only folder: treat decrypted bytes as Copy.
                return True
        if len(coders) == 1 and coders[0].method in (
            METHOD_COPY,
            METHOD_LZMA,
            METHOD_LZMA2,
            METHOD_BZIP2,
            METHOD_DEFLATE,
        ):
            return True
        # Simple BCJ + LZMA(2) chains are common for executables; not yet supported.
        return False

    def content_coder(self) -> Optional[Coder]:
        """Return the non-AES content coder, if this is a supported simple chain."""
        coders = list(self.coders)
        if coders and coders[0].method == METHOD_AES:
            coders = coders[1:]
        if len(coders) == 1:
            return coders[0]
        return None

    def content_unpack_size(self) -> int:
        return self.get_unpack_size()


@dataclasses.dataclass
class PackInfo:
    pack_pos: int = 0
    pack_sizes: list[int] = dataclasses.field(default_factory=list)
    crcs: list[Optional[int]] = dataclasses.field(default_factory=list)

    @property
    def pack_positions(self) -> list[int]:
        positions = [0]
        for size in self.pack_sizes:
            positions.append(positions[-1] + size)
        return positions[:-1]


@dataclasses.dataclass
class SubstreamsInfo:
    num_unpack_streams: list[int] = dataclasses.field(default_factory=list)
    unpack_sizes: list[int] = dataclasses.field(default_factory=list)
    digests: list[Optional[int]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class StreamsInfo:
    pack_info: Optional[PackInfo] = None
    folders: list[Folder] = dataclasses.field(default_factory=list)
    substreams: Optional[SubstreamsInfo] = None


@dataclasses.dataclass
class SevenZipFileEntry:
    """One logical file entry from FilesInfo, linked to pack/folder location when present."""

    path: str
    size: int = 0
    mtime: float = 0.0
    mode: int = 0
    is_dir: bool = False
    is_empty_stream: bool = False
    is_empty_file: bool = False
    is_anti: bool = False
    # Location inside the archive (only for non-empty streams).
    folder_index: Optional[int] = None
    # Byte offset of this file's data inside the folder's unpacked stream.
    unpack_offset: int = 0
    # Absolute byte offset of the folder's first packed stream in the archive file.
    pack_offset: int = 0
    pack_size: int = 0
    pack_stream_index: int = 0
    methods: tuple[bytes, ...] = ()
    has_crc: bool = False
    crc: int = 0


@dataclasses.dataclass
class SevenZipArchiveInfo:
    after_header: int  # First byte after SignatureHeader (always 32 for normal archives).
    pack_pos_base: int  # after_header + pack_info.pack_pos
    folders: list[Folder]
    pack_info: Optional[PackInfo]
    files: list[SevenZipFileEntry]
    solid: bool = False


def _parse_pack_info(file: BinaryIO) -> PackInfo:
    info = PackInfo()
    info.pack_pos = _read_uint64(file)
    num_streams = _read_uint64(file)
    if num_streams > 1_000_000:
        raise SevenZipError(f"Unreasonable number of pack streams: {num_streams}")

    prop = _read_byte(file)
    if prop == PROP_SIZE:
        info.pack_sizes = [_read_uint64(file) for _ in range(num_streams)]
        prop = _read_byte(file)
        if prop == PROP_CRC:
            defined = _read_bools(file, num_streams, check_all=True)
            info.crcs = []
            for is_defined in defined:
                info.crcs.append(struct.unpack("<I", _read_exact(file, 4))[0] if is_defined else None)
            prop = _read_byte(file)
        else:
            info.crcs = [None] * num_streams
    else:
        info.pack_sizes = [0] * num_streams
        info.crcs = [None] * num_streams

    if prop != PROP_END:
        raise SevenZipError(f"Expected END after PackInfo, got 0x{prop:02x}")
    return info


def _parse_folder(file: BinaryIO) -> Folder:
    folder = Folder()
    num_coders = _read_uint64(file)
    if num_coders == 0 or num_coders > 64:
        raise SevenZipError(f"Unreasonable number of coders in folder: {num_coders}")

    total_in = 0
    total_out = 0
    for _ in range(num_coders):
        flags = _read_byte(file)
        method_size = flags & 0x0F
        is_complex = bool(flags & 0x10)
        has_attrs = bool(flags & 0x20)
        if flags & 0x80:
            raise SevenZipError("Unsupported coder flag bit 0x80")
        method = _read_exact(file, method_size) if method_size else METHOD_COPY
        if is_complex:
            num_in = _read_uint64(file)
            num_out = _read_uint64(file)
        else:
            num_in = 1
            num_out = 1
        properties = None
        if has_attrs:
            prop_size = _read_uint64(file)
            properties = _read_exact(file, prop_size)
        folder.coders.append(Coder(method=method, num_in_streams=num_in, num_out_streams=num_out, properties=properties))
        total_in += num_in
        total_out += num_out

    num_bind_pairs = total_out - 1
    folder.bind_pairs = [(_read_uint64(file), _read_uint64(file)) for _ in range(num_bind_pairs)]
    num_packed = total_in - num_bind_pairs
    if num_packed == 1:
        used = {pair[0] for pair in folder.bind_pairs}
        for i in range(total_in):
            if i not in used:
                folder.packed_indices.append(i)
                break
    else:
        folder.packed_indices = [_read_uint64(file) for _ in range(num_packed)]
    return folder


def _parse_unpack_info(file: BinaryIO) -> list[Folder]:
    prop = _read_byte(file)
    if prop != PROP_FOLDER:
        raise SevenZipError(f"Expected FOLDER property, got 0x{prop:02x}")
    num_folders = _read_uint64(file)
    if num_folders > 1_000_000:
        raise SevenZipError(f"Unreasonable number of folders: {num_folders}")
    external = _read_byte(file)
    if external != 0:
        raise SevenZipError("External folder data is not supported")
    folders = [_parse_folder(file) for _ in range(num_folders)]

    prop = _read_byte(file)
    if prop != PROP_CODERS_UNPACK_SIZE:
        raise SevenZipError(f"Expected CODERS_UNPACK_SIZE, got 0x{prop:02x}")
    for folder in folders:
        for coder in folder.coders:
            for _ in range(coder.num_out_streams):
                folder.unpack_sizes.append(_read_uint64(file))

    prop = _read_byte(file)
    if prop == PROP_CRC:
        defined = _read_bools(file, num_folders, check_all=True)
        for i, folder in enumerate(folders):
            if defined[i]:
                folder.has_crc = True
                folder.crc = struct.unpack("<I", _read_exact(file, 4))[0]
        prop = _read_byte(file)

    if prop != PROP_END:
        raise SevenZipError(f"Expected END after UnpackInfo, got 0x{prop:02x}")
    return folders


def _parse_substreams_info(file: BinaryIO, folders: Sequence[Folder]) -> SubstreamsInfo:
    info = SubstreamsInfo()
    num_folders = len(folders)
    prop = _read_byte(file)

    if prop == PROP_NUM_UNPACK_STREAM:
        info.num_unpack_streams = [_read_uint64(file) for _ in range(num_folders)]
        prop = _read_byte(file)
    else:
        info.num_unpack_streams = [1] * num_folders

    if prop == PROP_SIZE:
        for folder_index, num_streams in enumerate(info.num_unpack_streams):
            sizes: list[int] = []
            total = 0
            for _ in range(num_streams - 1):
                size = _read_uint64(file)
                sizes.append(size)
                total += size
            sizes.append(folders[folder_index].get_unpack_size() - total)
            info.unpack_sizes.extend(sizes)
        prop = _read_byte(file)
    else:
        for folder_index, num_streams in enumerate(info.num_unpack_streams):
            if num_streams == 1:
                info.unpack_sizes.append(folders[folder_index].get_unpack_size())
            else:
                raise SevenZipError("Missing Substreams SIZE property for multi-stream folder")

    # Count digests that must be present in the CRC block.
    num_digests = 0
    for i, num_streams in enumerate(info.num_unpack_streams):
        if num_streams != 1 or not folders[i].has_crc:
            num_digests += num_streams

    if prop == PROP_CRC:
        defined = _read_bools(file, num_digests, check_all=True)
        crcs = [struct.unpack("<I", _read_exact(file, 4))[0] if d else None for d in defined]
        digest_index = 0
        for i, num_streams in enumerate(info.num_unpack_streams):
            if num_streams == 1 and folders[i].has_crc:
                info.digests.append(folders[i].crc)
            else:
                for _ in range(num_streams):
                    info.digests.append(crcs[digest_index])
                    digest_index += 1
        prop = _read_byte(file)
    else:
        info.digests = [None] * sum(info.num_unpack_streams)

    if prop != PROP_END:
        raise SevenZipError(f"Expected END after SubstreamsInfo, got 0x{prop:02x}")
    return info


def _parse_streams_info(file: BinaryIO) -> StreamsInfo:
    streams = StreamsInfo()
    prop = _read_byte(file)
    if prop == PROP_PACK_INFO:
        streams.pack_info = _parse_pack_info(file)
        prop = _read_byte(file)
    if prop == PROP_UNPACK_INFO:
        streams.folders = _parse_unpack_info(file)
        prop = _read_byte(file)
    if prop == PROP_SUBSTREAMS_INFO:
        streams.substreams = _parse_substreams_info(file, streams.folders)
        prop = _read_byte(file)
    elif streams.folders:
        # Default: one stream per folder with folder unpack size.
        streams.substreams = SubstreamsInfo(
            num_unpack_streams=[1] * len(streams.folders),
            unpack_sizes=[folder.get_unpack_size() for folder in streams.folders],
            digests=[folder.crc if folder.has_crc else None for folder in streams.folders],
        )
    if prop != PROP_END:
        raise SevenZipError(f"Expected END after StreamsInfo, got 0x{prop:02x}")
    return streams


def _parse_files_info(file: BinaryIO) -> tuple[list[dict], list[bool], list[bool], list[bool]]:
    num_files = _read_uint64(file)
    if num_files > 10_000_000:
        raise SevenZipError(f"Unreasonable number of files: {num_files}")

    files: list[dict] = [{"emptystream": False} for _ in range(num_files)]
    empty_files: list[bool] = []
    anti_files: list[bool] = []

    while True:
        prop = _read_byte(file)
        if prop == PROP_END:
            break
        size = _read_uint64(file)
        payload = io.BytesIO(_read_exact(file, size))

        if prop == PROP_DUMMY:
            continue
        if prop == PROP_EMPTY_STREAM:
            defined = _read_bools(payload, num_files, check_all=False)
            for i, is_empty in enumerate(defined):
                files[i]["emptystream"] = is_empty
            num_empty = sum(1 for x in defined if x)
            empty_files = [False] * num_empty
            anti_files = [False] * num_empty
        elif prop == PROP_EMPTY_FILE:
            empty_files = _read_bools(payload, sum(1 for f in files if f["emptystream"]), check_all=False)
        elif prop == PROP_ANTI:
            anti_files = _read_bools(payload, sum(1 for f in files if f["emptystream"]), check_all=False)
        elif prop == PROP_NAME:
            external = _read_byte(payload)
            if external != 0:
                raise SevenZipError("External file names are not supported")
            for f in files:
                f["filename"] = _read_utf16z(payload).replace("\\", "/")
        elif prop in (PROP_CREATION_TIME, PROP_LAST_ACCESS_TIME, PROP_LAST_WRITE_TIME):
            key = {
                PROP_CREATION_TIME: "ctime",
                PROP_LAST_ACCESS_TIME: "atime",
                PROP_LAST_WRITE_TIME: "mtime",
            }[prop]
            defined = _read_bools(payload, num_files, check_all=True)
            external = _read_byte(payload)
            if external != 0:
                raise SevenZipError("External timestamps are not supported")
            for i, is_defined in enumerate(defined):
                if is_defined:
                    files[i][key] = struct.unpack("<Q", _read_exact(payload, 8))[0]
        elif prop == PROP_ATTRIBUTES:
            defined = _read_bools(payload, num_files, check_all=True)
            external = _read_byte(payload)
            if external != 0:
                raise SevenZipError("External attributes are not supported")
            for i, is_defined in enumerate(defined):
                if is_defined:
                    files[i]["attributes"] = struct.unpack("<I", _read_exact(payload, 4))[0]
        elif prop == PROP_START_POS:
            defined = _read_bools(payload, num_files, check_all=True)
            external = _read_byte(payload)
            if external != 0:
                raise SevenZipError("External start positions are not supported")
            for i, is_defined in enumerate(defined):
                if is_defined:
                    files[i]["startpos"] = struct.unpack("<Q", _read_exact(payload, 8))[0]
        else:
            # Skip unknown property payload (already consumed into BytesIO).
            pass

    return files, empty_files, anti_files, [f.get("emptystream", False) for f in files]


def _attributes_to_mode(attributes: Optional[int], is_dir: bool) -> int:
    if attributes is not None and (attributes & _WINDOWS_UNIX_ATTR_MASK):
        # High 16 bits often store Unix mode when archive created on Unix.
        unix_mode = (attributes >> 16) & 0o7777
        if unix_mode:
            file_type = 0o040000 if is_dir else 0o100000
            # Detect symlink: S_IFLNK is 0o120000
            if (attributes >> 16) & 0o170000 == 0o120000:
                return ((attributes >> 16) & 0o777) | 0o120000
            return unix_mode | file_type
    if is_dir:
        return 0o755 | 0o040000
    return 0o644 | 0o100000


def _is_directory_entry(filename: str, attributes: Optional[int], is_empty_stream: bool, is_empty_file: bool) -> bool:
    if attributes is not None and (attributes & _WINDOWS_DIRECTORY_ATTR):
        return True
    if filename.endswith("/"):
        return True
    # Empty stream that is not an empty file is typically a directory.
    if is_empty_stream and not is_empty_file:
        return True
    return False


def _build_file_entries(
    raw_files: list[dict],
    empty_files: list[bool],
    anti_files: list[bool],
    streams: Optional[StreamsInfo],
    after_header: int,
) -> tuple[list[SevenZipFileEntry], bool]:
    if streams is None or streams.pack_info is None:
        # Archive with only empty files / directories.
        entries: list[SevenZipFileEntry] = []
        empty_index = 0
        for raw in raw_files:
            is_empty_stream = bool(raw.get("emptystream"))
            is_empty_file = False
            is_anti = False
            if is_empty_stream:
                is_empty_file = empty_files[empty_index] if empty_index < len(empty_files) else False
                is_anti = anti_files[empty_index] if empty_index < len(anti_files) else False
                empty_index += 1
            name = raw.get("filename", "")
            attrs = raw.get("attributes")
            is_dir = _is_directory_entry(name, attrs, is_empty_stream, is_empty_file)
            entries.append(
                SevenZipFileEntry(
                    path=name.rstrip("/"),
                    size=0,
                    mtime=_filetime_to_unix(raw.get("mtime", 0) or 0),
                    mode=_attributes_to_mode(attrs, is_dir),
                    is_dir=is_dir,
                    is_empty_stream=is_empty_stream,
                    is_empty_file=is_empty_file,
                    is_anti=is_anti,
                )
            )
        return entries, False

    pack_info = streams.pack_info
    folders = streams.folders
    substreams = streams.substreams
    assert substreams is not None

    # Map each non-empty stream index to folder + offsets.
    stream_map: list[tuple[int, int, int, int]] = []  # folder_idx, unpack_offset, size, pack_stream_start_index
    pack_stream_cursor = 0
    unpack_size_cursor = 0
    solid = False

    for folder_index, folder in enumerate(folders):
        num_streams = substreams.num_unpack_streams[folder_index]
        if num_streams > 1:
            solid = True
        unpack_offset = 0
        folder_pack_streams = len(folder.packed_indices) if folder.packed_indices else 1
        for _ in range(num_streams):
            size = substreams.unpack_sizes[unpack_size_cursor]
            stream_map.append((folder_index, unpack_offset, size, pack_stream_cursor))
            unpack_offset += size
            unpack_size_cursor += 1
        pack_stream_cursor += folder_pack_streams

    pack_base = after_header + pack_info.pack_pos
    pack_positions = pack_info.pack_positions

    entries = []
    empty_index = 0
    stream_index = 0

    for raw in raw_files:
        is_empty_stream = bool(raw.get("emptystream"))
        is_empty_file = False
        is_anti = False
        if is_empty_stream:
            is_empty_file = empty_files[empty_index] if empty_index < len(empty_files) else False
            is_anti = anti_files[empty_index] if empty_index < len(anti_files) else False
            empty_index += 1

        name = raw.get("filename", "")
        attrs = raw.get("attributes")
        is_dir = _is_directory_entry(name, attrs, is_empty_stream, is_empty_file)
        mtime = _filetime_to_unix(raw.get("mtime", 0) or 0)

        if is_empty_stream or is_dir:
            entries.append(
                SevenZipFileEntry(
                    path=name.rstrip("/"),
                    size=0,
                    mtime=mtime,
                    mode=_attributes_to_mode(attrs, is_dir or (is_empty_stream and not is_empty_file)),
                    is_dir=is_dir or (is_empty_stream and not is_empty_file),
                    is_empty_stream=is_empty_stream,
                    is_empty_file=is_empty_file,
                    is_anti=is_anti,
                )
            )
            continue

        if stream_index >= len(stream_map):
            raise SevenZipError("More non-empty files than unpack streams")

        folder_index, unpack_offset, size, pack_stream_index = stream_map[stream_index]
        stream_index += 1
        folder = folders[folder_index]
        folder_pack_count = len(folder.packed_indices) if folder.packed_indices else 1
        pack_offset = pack_base + pack_positions[pack_stream_index]
        pack_size = sum(pack_info.pack_sizes[pack_stream_index : pack_stream_index + folder_pack_count])
        digest = substreams.digests[stream_index - 1] if stream_index - 1 < len(substreams.digests) else None

        entries.append(
            SevenZipFileEntry(
                path=name.rstrip("/"),
                size=size,
                mtime=mtime,
                mode=_attributes_to_mode(attrs, False),
                is_dir=False,
                folder_index=folder_index,
                unpack_offset=unpack_offset,
                pack_offset=pack_offset,
                pack_size=pack_size,
                pack_stream_index=pack_stream_index,
                methods=folder.methods(),
                has_crc=digest is not None,
                crc=digest or 0,
            )
        )

    return entries, solid


def _decode_folder_to_bytes(file: BinaryIO, folder: Folder, pack_offset: int, pack_size: int) -> bytes:
    file.seek(pack_offset)
    packed = _read_exact(file, pack_size)
    return decompress_folder(folder, packed)


def calculate_7z_key(password: bytes, cycles: int, salt: bytes) -> bytes:
    """Derive the 32-byte AES key used by 7-Zip (SHA-256, password as UTF-16LE bytes)."""
    if cycles > 0x3F:
        raise SevenZipError(f"Invalid AES cycle count: {cycles}")
    if cycles == 0x3F:
        return (salt + password + bytes(32))[:32]

    # Optimized batched SHA-256 rounds (same algorithm as 7-Zip / py7zr).
    cat_cycle = 6
    if cycles > cat_cycle:
        rounds = 1 << cat_cycle
        stages = 1 << (cycles - cat_cycle)
    else:
        rounds = 1 << cycles
        stages = 1
    digest = hashlib.sha256()
    salt_password = salt + password
    counter = 0
    for _ in range(stages):
        digest.update(
            b"".join(salt_password + (counter + i).to_bytes(8, "little") for i in range(rounds))
        )
        counter += rounds
    return digest.digest()[:32]


@dataclasses.dataclass
class AesProperties:
    cycles: int
    salt: bytes
    iv: bytes  # always 16 bytes (zero-padded)


def parse_aes_properties(properties: Optional[bytes]) -> AesProperties:
    if not properties or len(properties) < 1:
        raise SevenZipError("Missing 7z AES properties")
    first = properties[0]
    cycles = first & 0x3F
    if first & 0xC0 == 0:
        raise SevenZipError("Invalid 7z AES properties (no salt/iv flags)")
    salt_size = (first >> 7) & 1
    iv_size = (first >> 6) & 1
    if len(properties) < 2:
        raise SevenZipError("Truncated 7z AES properties")
    second = properties[1]
    salt_size += second >> 4
    iv_size += second & 0x0F
    expected = 2 + salt_size + iv_size
    if len(properties) < expected:
        raise SevenZipError(f"Truncated 7z AES properties (need {expected}, got {len(properties)})")
    salt = properties[2 : 2 + salt_size]
    iv = properties[2 + salt_size : 2 + salt_size + iv_size]
    if len(iv) < 16:
        iv = iv + bytes(16 - len(iv))
    return AesProperties(cycles=cycles, salt=salt, iv=iv)


def aes_decrypt_7z(packed: bytes, properties: Optional[bytes], password: Union[str, bytes]) -> bytes:
    """Decrypt a 7z AES-256-CBC packed stream."""
    if _AES is None:
        raise SevenZipError(
            "Encrypted 7z support requires the 'pycryptodomex' package (Cryptodome). "
            "Install it with: pip install pycryptodomex"
        )
    if isinstance(password, str):
        password_bytes = password.encode("utf-16le")
    else:
        # Callers may pass utf-8 bytes of the password string; convert like py7zr/CLI.
        try:
            password_bytes = password.decode("utf-8").encode("utf-16le")
        except UnicodeDecodeError:
            password_bytes = password  # already opaque; try as-is

    props = parse_aes_properties(properties)
    key = calculate_7z_key(password_bytes, props.cycles, props.salt)
    cipher = _AES.new(key, _AES.MODE_CBC, props.iv)

    # Ciphertext must be a multiple of the AES block size. 7z pads packed data accordingly.
    if len(packed) == 0:
        return b""
    if len(packed) % 16 != 0:
        packed = packed + bytes(16 - (len(packed) % 16))
    return cipher.decrypt(packed)


def prepare_folder_packed(
    folder: Folder,
    packed: bytes,
    password: Optional[Union[str, bytes]] = None,
) -> tuple[Folder, bytes]:
    """Strip a leading AES coder (if any) and return (content_folder, content_packed).

    For unencrypted folders this is a no-op. For AES+compressor chains the packed
    bytes are decrypted and a synthetic single-coder folder is returned so the
    existing streaming decoder can run unchanged.
    """
    if not folder.coders:
        raise SevenZipError("Folder has no coders")

    if folder.coders[0].method != METHOD_AES:
        return folder, packed

    if password is None:
        raise SevenZipError("Password required for encrypted 7z folder")

    decrypted = aes_decrypt_7z(packed, folder.coders[0].properties, password)

    # Intermediate size is the AES coder's unpack size when present.
    if folder.unpack_sizes:
        intermediate_size = folder.unpack_sizes[0]
        if intermediate_size <= len(decrypted):
            decrypted = decrypted[:intermediate_size]

    rest = folder.coders[1:]
    if not rest:
        # AES-only: expose as Copy over decrypted bytes.
        content = Folder(
            coders=[Coder(method=METHOD_COPY)],
            unpack_sizes=[len(decrypted)],
            has_crc=folder.has_crc,
            crc=folder.crc,
        )
        return content, decrypted

    if len(rest) != 1:
        raise SevenZipError(f"Unsupported encrypted coder chain: {[c.method.hex() for c in folder.coders]}")

    content_coder = rest[0]
    if content_coder.method not in (METHOD_COPY, METHOD_LZMA, METHOD_LZMA2, METHOD_BZIP2, METHOD_DEFLATE):
        raise SevenZipError(f"Unsupported codec after AES: {content_coder.method.hex()}")

    # Remaining unpack sizes belong to the content coder(s).
    content_unpack = folder.unpack_sizes[1:] if len(folder.unpack_sizes) > 1 else [folder.get_unpack_size()]
    content = Folder(
        coders=[
            Coder(
                method=content_coder.method,
                num_in_streams=content_coder.num_in_streams,
                num_out_streams=content_coder.num_out_streams,
                properties=content_coder.properties,
            )
        ],
        unpack_sizes=content_unpack if content_unpack else [folder.get_unpack_size()],
        has_crc=folder.has_crc,
        crc=folder.crc,
    )
    return content, decrypted


def _lzma_filter_from_coder(coder: Coder) -> dict:
    """Build a stdlib lzma filter dict from a 7z coder (uses CPython private helper)."""
    if coder.method == METHOD_LZMA2:
        filter_id = lzma.FILTER_LZMA2
    elif coder.method == METHOD_LZMA:
        filter_id = lzma.FILTER_LZMA1
    else:
        raise SevenZipError(f"Not an LZMA coder: {coder.method.hex()}")

    properties = coder.properties
    if properties is not None:
        # CPython exposes this to map 7z/XZ filter property blobs to filter dicts.
        decode = getattr(lzma, "_decode_filter_properties", None)
        if decode is None:
            raise SevenZipError("This Python build lacks lzma._decode_filter_properties")
        return decode(filter_id, properties)
    return {"id": filter_id}


def _lzma_decompress_raw(packed: bytes, filters: list[dict], unpack_size: int) -> bytes:
    decompressor = lzma.LZMADecompressor(format=lzma.FORMAT_RAW, filters=filters)
    # Some RAW streams do not signal EOS; request exactly unpack_size when possible.
    try:
        out = decompressor.decompress(packed, max_length=unpack_size)
    except TypeError:
        out = decompressor.decompress(packed)
    if len(out) < unpack_size and not decompressor.eof:
        # Feed remaining / finish.
        try:
            out += decompressor.decompress(b"", max_length=unpack_size - len(out))
        except Exception:
            pass
    if len(out) > unpack_size:
        out = out[:unpack_size]
    if len(out) < unpack_size:
        raise SevenZipError(f"LZMA decompressed {len(out)} bytes but expected {unpack_size}")
    return out


@dataclasses.dataclass(frozen=True)
class Lzma2ChunkIndex:
    """One LZMA2 chunk in a folder's packed stream."""

    index: int
    packed_offset: int
    packed_size: int
    unpacked_offset: int
    unpacked_size: int
    control: int
    is_lzma: bool
    independent: bool


def index_lzma2_chunks(packed: bytes) -> list[Lzma2ChunkIndex]:
    """Walk an LZMA2 packed stream and record chunk boundaries without decompressing."""
    pos = 0
    unpacked_pos = 0
    chunks: list[Lzma2ChunkIndex] = []
    need_dict_reset = True
    chunk_index = 0

    while pos < len(packed):
        chunk_start = pos
        control = packed[pos]
        pos += 1
        if control == 0:
            break

        dict_reset = control >= 0xE0 or control == 0x01
        if not dict_reset and need_dict_reset:
            raise SevenZipError(f"LZMA2 stream missing dictionary reset at offset {chunk_start}")
        if dict_reset:
            need_dict_reset = False

        if control >= 0x80:
            if pos + 4 > len(packed):
                raise SevenZipError("Truncated LZMA2 chunk header")
            unpacked_size = ((control & 0x1F) << 16) + (packed[pos] << 8) + packed[pos + 1] + 1
            pos += 2
            compressed_size = (packed[pos] << 8) + packed[pos + 1] + 1
            pos += 2
            if control >= 0xC0:
                pos += 1
            if pos + compressed_size > len(packed):
                raise SevenZipError("Truncated LZMA2 compressed data")
            pos += compressed_size
            independent = control >= 0xE0 or control == 0x01
            chunks.append(
                Lzma2ChunkIndex(
                    index=chunk_index,
                    packed_offset=chunk_start,
                    packed_size=pos - chunk_start,
                    unpacked_offset=unpacked_pos,
                    unpacked_size=unpacked_size,
                    control=control,
                    is_lzma=True,
                    independent=independent,
                )
            )
            unpacked_pos += unpacked_size
        elif control in (1, 2):
            if pos + 2 > len(packed):
                raise SevenZipError("Truncated LZMA2 uncompressed chunk header")
            copy_size = (packed[pos] << 8) + packed[pos + 1] + 1
            pos += 2
            if pos + copy_size > len(packed):
                raise SevenZipError("Truncated LZMA2 uncompressed data")
            pos += copy_size
            chunks.append(
                Lzma2ChunkIndex(
                    index=chunk_index,
                    packed_offset=chunk_start,
                    packed_size=pos - chunk_start,
                    unpacked_offset=unpacked_pos,
                    unpacked_size=copy_size,
                    control=control,
                    is_lzma=False,
                    independent=control == 1,
                )
            )
            unpacked_pos += copy_size
        else:
            raise SevenZipError(f"Invalid LZMA2 control byte 0x{control:02x} at offset {chunk_start}")

        chunk_index += 1

    return chunks


def _lzma2_props_byte_to_filter(props_byte: int) -> dict:
    lc = props_byte % 9
    remainder = props_byte // 9
    lp = remainder % 5
    pb = remainder // 5
    return {"id": lzma.FILTER_LZMA2, "lc": lc, "lp": lp, "pb": pb}


def _lzma2_chunk_filter(chunk: Lzma2ChunkIndex, packed: bytes, default_filter: dict) -> dict:
    if chunk.is_lzma and chunk.control >= 0xC0:
        props_offset = chunk.packed_offset + 5
        if props_offset >= len(packed):
            raise SevenZipError("Truncated LZMA2 properties byte")
        return _lzma2_props_byte_to_filter(packed[props_offset])
    return default_filter


def _lzma2_decompress_stream(packed_stream: bytes, filters: list[dict], max_length: int) -> bytes:
    decompressor = lzma.LZMADecompressor(format=lzma.FORMAT_RAW, filters=filters)
    try:
        out = decompressor.decompress(packed_stream, max_length=max_length)
    except TypeError:
        out = decompressor.decompress(packed_stream)
    if len(out) > max_length:
        out = out[:max_length]
    return out


class Lzma2RandomAccessDecoder:
    """Serve random unpacked ranges from an LZMA2 folder using chunk indexing.

    Chunks with a dictionary reset (control >= 0xE0 or copy-with-reset) decode
    independently. Other chunks decode from the nearest prior reset chunk forward,
    which matches LZMA2 semantics and avoids full-folder replay on random reads.
    """

    def __init__(
        self,
        folder: Folder,
        packed: bytes,
        *,
        max_cached_chunks: int = 128,
    ):
        if not folder.coders or folder.coders[0].method != METHOD_LZMA2:
            raise SevenZipError("Lzma2RandomAccessDecoder requires an LZMA2 folder")
        if max_cached_chunks < 1:
            raise ValueError("max_cached_chunks must be >= 1")
        self.folder = folder
        self.packed = packed
        self.default_filter = _lzma_filter_from_coder(folder.coders[0])
        self.chunks = index_lzma2_chunks(packed)
        self.unpack_size = folder.get_unpack_size()
        self.max_cached_chunks = max_cached_chunks
        self._decoded: dict[int, bytes] = {}
        self._chunk_order: list[int] = []

    @staticmethod
    def _chain_start(chunks: Sequence[Lzma2ChunkIndex], chunk_index: int) -> int:
        while chunk_index > 0:
            if chunks[chunk_index].independent:
                return chunk_index
            chunk_index -= 1
        return 0

    def _touch_chunk(self, chunk_index: int) -> None:
        if chunk_index in self._chunk_order:
            self._chunk_order.remove(chunk_index)
        self._chunk_order.append(chunk_index)
        while len(self._chunk_order) > self.max_cached_chunks:
            old = self._chunk_order.pop(0)
            self._decoded.pop(old, None)

    def _store_chunk(self, chunk_index: int, data: bytes) -> None:
        self._decoded[chunk_index] = data
        self._touch_chunk(chunk_index)

    def _decode_copy_chunk(self, chunk: Lzma2ChunkIndex) -> bytes:
        data_start = chunk.packed_offset + 3
        return self.packed[data_start : data_start + chunk.unpacked_size]

    def _decode_chunk(self, chunk_index: int) -> bytes:
        cached = self._decoded.get(chunk_index)
        if cached is not None:
            self._touch_chunk(chunk_index)
            return cached

        chunk = self.chunks[chunk_index]
        if not chunk.is_lzma:
            data = self._decode_copy_chunk(chunk)
            self._store_chunk(chunk_index, data)
            return data

        if chunk.independent:
            packed_slice = self.packed[chunk.packed_offset : chunk.packed_offset + chunk.packed_size]
            filt = _lzma2_chunk_filter(chunk, self.packed, self.default_filter)
            data = _lzma2_decompress_stream(packed_slice + b"\x00", [filt], chunk.unpacked_size)
            self._store_chunk(chunk_index, data)
            return data

        chain_start = self._chain_start(self.chunks, chunk_index)
        chain_packed = b"".join(
            self.packed[item.packed_offset : item.packed_offset + item.packed_size]
            for item in self.chunks[chain_start : chunk_index + 1]
        )
        start_chunk = self.chunks[chain_start]
        # Use the folder-level LZMA2 filter for multi-chunk chains. Per-chunk
        # property bytes are interpreted by the LZMA2 stream itself; applying
        # _lzma2_chunk_filter on the chain start corrupts long dependent chains.
        expected = chunk.unpacked_offset + chunk.unpacked_size - start_chunk.unpacked_offset
        chain_out = _lzma2_decompress_stream(chain_packed + b"\x00", [self.default_filter], expected)
        offset = 0
        for item in self.chunks[chain_start : chunk_index + 1]:
            piece = chain_out[offset : offset + item.unpacked_size]
            if len(piece) != item.unpacked_size:
                raise SevenZipError(
                    f"LZMA2 chain decode short read for chunk {item.index}: "
                    f"got {len(piece)} expected {item.unpacked_size}"
                )
            self._store_chunk(item.index, piece)
            offset += item.unpacked_size
        return self._decoded[chunk_index]

    def _chunk_for_offset(self, unpacked_offset: int) -> int:
        for chunk in self.chunks:
            if chunk.unpacked_offset <= unpacked_offset < chunk.unpacked_offset + chunk.unpacked_size:
                return chunk.index
        raise SevenZipError(f"Unpack offset {unpacked_offset} outside LZMA2 stream")

    def read_range(self, start: int, length: int) -> bytes:
        if length <= 0 or start >= self.unpack_size:
            return b""
        start = max(0, start)
        end = min(self.unpack_size, start + length)
        first = self._chunk_for_offset(start)
        last = self._chunk_for_offset(end - 1 if end > start else start)
        for chunk_index in range(first, last + 1):
            self._decode_chunk(chunk_index)

        parts: list[bytes] = []
        offset = start
        while offset < end:
            chunk = self.chunks[self._chunk_for_offset(offset)]
            data = self._decoded[chunk.index]
            local = offset - chunk.unpacked_offset
            take = min(end - offset, len(data) - local)
            parts.append(data[local : local + take])
            offset += take
        return b"".join(parts)

    @property
    def cached_chunk_count(self) -> int:
        return len(self._decoded)

    @property
    def decoded_through(self) -> int:
        if not self._decoded:
            return 0
        return max(
            chunk.unpacked_offset + len(data)
            for chunk in self.chunks
            if (data := self._decoded.get(chunk.index)) is not None
        )


def create_folder_decoder(
    folder: Folder,
    packed: bytes,
    *,
    chunk_size: int = 1024 * 1024,
    max_cached_chunks: int = 64,
) -> Union["Lzma2RandomAccessDecoder", "StreamingFolderDecoder"]:
    """Return the best random-access decoder for *folder*'s primary codec."""
    if folder.coders and folder.coders[0].method == METHOD_LZMA2:
        return Lzma2RandomAccessDecoder(folder, packed, max_cached_chunks=max(max_cached_chunks, 64))
    return StreamingFolderDecoder(
        folder,
        packed,
        chunk_size=chunk_size,
        max_cached_chunks=max_cached_chunks,
    )


class StreamingFolderDecoder:
    """Incrementally decompress a 7z folder and serve random ranges via a chunk cache.

    Designed so that:
      - A pread near the start of a large member does not decompress the whole folder.
      - Solid multi-file folders share one progressive decode cursor + chunk cache.
      - Memory is bounded by ``max_cached_chunks * chunk_size`` (plus a small pending buffer).

    Backward seeks that miss the chunk cache restart decompression from the beginning
    of the packed stream (same complexity class as libarchive, but subsequent hits in
    the chunk cache are O(1)).
    """

    def __init__(
        self,
        folder: Folder,
        packed: bytes,
        *,
        chunk_size: int = 1024 * 1024,
        max_cached_chunks: int = 64,
    ):
        if chunk_size < 4096:
            raise ValueError("chunk_size must be >= 4096")
        if max_cached_chunks < 1:
            raise ValueError("max_cached_chunks must be >= 1")
        self.folder = folder
        self.packed = packed
        self.chunk_size = chunk_size
        self.max_cached_chunks = max_cached_chunks
        self.unpack_size = folder.get_unpack_size()
        self._chunks: dict[int, bytes] = {}
        self._chunk_order: list[int] = []  # LRU: oldest at front
        # Progressive decode: _unpacked_pos is total bytes produced (including pending).
        self._decoder: Optional[object] = None
        self._packed_pos = 0
        self._unpacked_pos = 0
        self._pending = bytearray()
        self._finished = False
        self._method = folder.coders[0].method if folder.coders else METHOD_COPY
        # Bytes decoded so far that have been fully committed to chunks or pending.
        self._pending_base = 0  # unpacked offset of pending[0]

    def _touch_chunk(self, index: int) -> None:
        if index in self._chunk_order:
            self._chunk_order.remove(index)
        self._chunk_order.append(index)
        while len(self._chunk_order) > self.max_cached_chunks:
            old = self._chunk_order.pop(0)
            self._chunks.pop(old, None)

    def _store_chunk(self, index: int, data: bytes) -> None:
        if not data:
            return
        if index not in self._chunks:
            self._chunks[index] = data
        self._touch_chunk(index)

    def _make_decoder(self):
        method = self._method
        if method in (METHOD_LZMA, METHOD_LZMA2):
            filters = [_lzma_filter_from_coder(self.folder.coders[0])]
            return lzma.LZMADecompressor(format=lzma.FORMAT_RAW, filters=filters)
        if method == METHOD_DEFLATE:
            return zlib.decompressobj(wbits=-15)
        if method == METHOD_BZIP2:
            import bz2

            return bz2.BZ2Decompressor()
        if method == METHOD_COPY:
            return None
        raise SevenZipError(f"Unsupported method for streaming: {method.hex()}")

    def _reset_decoder(self) -> None:
        self._decoder = self._make_decoder()
        self._packed_pos = 0
        self._unpacked_pos = 0
        self._pending = bytearray()
        self._pending_base = 0
        self._finished = False

    def _flush_pending_chunks(self) -> None:
        while len(self._pending) >= self.chunk_size:
            index = self._pending_base // self.chunk_size
            data = bytes(self._pending[: self.chunk_size])
            del self._pending[: self.chunk_size]
            self._pending_base += self.chunk_size
            self._store_chunk(index, data)

    def _materialize_pending_partial(self) -> None:
        """Store a short final/partial chunk so readers can slice it."""
        if not self._pending:
            return
        index = self._pending_base // self.chunk_size
        self._store_chunk(index, bytes(self._pending))

    def _emit(self, data: bytes) -> None:
        if not data:
            return
        # Cap at declared unpack size.
        remaining = self.unpack_size - self._unpacked_pos
        if remaining <= 0:
            self._finished = True
            return
        if len(data) > remaining:
            data = data[:remaining]
        self._pending.extend(data)
        self._unpacked_pos += len(data)
        self._flush_pending_chunks()
        if self._unpacked_pos >= self.unpack_size:
            self._finished = True
            self._materialize_pending_partial()

    def _decode_more(self, want_unpacked_through: int) -> None:
        """Advance progressive decode until unpacked_pos >= want or stream ends."""
        want_unpacked_through = min(want_unpacked_through, self.unpack_size)
        if self._finished or self._unpacked_pos >= want_unpacked_through:
            return
        if self._decoder is None and self._method != METHOD_COPY:
            self._reset_decoder()

        if self._method == METHOD_COPY:
            target = min(want_unpacked_through, self.unpack_size, len(self.packed))
            if self._unpacked_pos < target:
                self._emit(self.packed[self._unpacked_pos : target])
            return

        while self._unpacked_pos < want_unpacked_through and not self._finished:
            remaining_out = want_unpacked_through - self._unpacked_pos
            max_length = max(remaining_out, self.chunk_size)

            if self._method in (METHOD_LZMA, METHOD_LZMA2):
                assert self._decoder is not None
                dec = self._decoder
                if not getattr(dec, "needs_input", True):
                    try:
                        out = dec.decompress(b"", max_length=max_length)
                    except EOFError:
                        out = b""
                        self._finished = True
                    if out:
                        self._emit(out)
                    else:
                        self._finished = True
                    continue

                if self._packed_pos >= len(self.packed):
                    try:
                        out = dec.decompress(b"", max_length=max_length)
                    except EOFError:
                        out = b""
                        self._finished = True
                    if out:
                        self._emit(out)
                    else:
                        self._finished = True
                    continue

                feed = self.packed[self._packed_pos : self._packed_pos + 65536]
                try:
                    out = dec.decompress(feed, max_length=max_length)
                except EOFError:
                    out = b""
                    self._finished = True
                if getattr(dec, "eof", False):
                    unused = getattr(dec, "unused_data", b"") or b""
                    self._packed_pos += len(feed) - len(unused)
                    self._finished = True
                else:
                    # Input fully consumed whether or not more output is pending.
                    self._packed_pos += len(feed)
                if out:
                    self._emit(out)
                elif self._packed_pos >= len(self.packed) and getattr(dec, "needs_input", True):
                    self._finished = True

            elif self._method == METHOD_DEFLATE:
                assert self._decoder is not None
                dec = self._decoder
                if self._packed_pos >= len(self.packed):
                    try:
                        out = dec.flush()  # type: ignore[attr-defined]
                    except Exception:
                        out = b""
                    if out:
                        self._emit(out)
                    self._finished = True
                    continue
                feed = self.packed[self._packed_pos : self._packed_pos + 65536]
                # zlib supports max_length on decompress in 3.0+.
                try:
                    out = dec.decompress(feed, max_length)  # type: ignore[call-arg]
                except TypeError:
                    out = dec.decompress(feed)  # type: ignore[attr-defined]
                unused = getattr(dec, "unused_data", b"") or b""
                if unused:
                    self._packed_pos += len(feed) - len(unused)
                    self._finished = True
                else:
                    self._packed_pos += len(feed)
                if out:
                    self._emit(out)
                elif self._packed_pos >= len(self.packed):
                    self._finished = True

            elif self._method == METHOD_BZIP2:
                assert self._decoder is not None
                dec = self._decoder
                if self._packed_pos >= len(self.packed):
                    self._finished = True
                    continue
                feed = self.packed[self._packed_pos : self._packed_pos + 65536]
                try:
                    out = dec.decompress(feed)  # type: ignore[attr-defined]
                except EOFError:
                    out = b""
                    self._finished = True
                self._packed_pos += len(feed)
                if out:
                    self._emit(out)
                if getattr(dec, "eof", False):
                    self._finished = True
            else:
                raise SevenZipError(f"Unsupported streaming method {self._method.hex()}")

        # If we stopped mid-chunk, keep pending; materialize when finished or on demand.
        if self._finished:
            self._materialize_pending_partial()

    def read_range(self, start: int, length: int) -> bytes:
        """Return ``length`` bytes starting at unpacked folder offset ``start``."""
        if length <= 0 or start >= self.unpack_size:
            return b""
        start = max(0, start)
        end = min(self.unpack_size, start + length)

        if self._method == METHOD_COPY:
            return self.packed[start:end]

        first_chunk = start // self.chunk_size
        last_chunk = (end - 1) // self.chunk_size
        need_through = min(self.unpack_size, (last_chunk + 1) * self.chunk_size)

        missing = [i for i in range(first_chunk, last_chunk + 1) if i not in self._chunks]
        if missing:
            first_missing_start = missing[0] * self.chunk_size
            # Restart if the progressive cursor is already past a missing region.
            if self._unpacked_pos > first_missing_start:
                self._reset_decoder()
            self._decode_more(need_through)
            # Materialize partial pending if it covers a requested chunk.
            if self._pending:
                partial_index = self._pending_base // self.chunk_size
                if first_chunk <= partial_index <= last_chunk:
                    self._materialize_pending_partial()

        parts: list[bytes] = []
        offset = start
        while offset < end:
            index = offset // self.chunk_size
            chunk = self._chunks.get(index)
            if chunk is None:
                self._reset_decoder()
                self._decode_more(min(self.unpack_size, (index + 1) * self.chunk_size))
                if self._pending and self._pending_base // self.chunk_size == index:
                    self._materialize_pending_partial()
                chunk = self._chunks.get(index)
                if chunk is None:
                    raise SevenZipError(
                        f"Failed to materialize folder chunk {index} "
                        f"(unpacked_pos={self._unpacked_pos}, want={offset})"
                    )
            else:
                self._touch_chunk(index)
            chunk_start = index * self.chunk_size
            local = offset - chunk_start
            take = min(end - offset, len(chunk) - local)
            if take <= 0:
                raise SevenZipError(f"Invalid chunk slice at {index}: local={local} len={len(chunk)}")
            parts.append(chunk[local : local + take])
            offset += take
        return b"".join(parts)

    @property
    def cached_chunk_count(self) -> int:
        return len(self._chunks)

    @property
    def decoded_through(self) -> int:
        return self._unpacked_pos


def decompress_folder(
    folder: Folder,
    packed: bytes,
    password: Optional[Union[str, bytes]] = None,
) -> bytes:
    """Decompress a folder's packed bytes to the primary unpack stream."""
    content_folder, content_packed = prepare_folder_packed(folder, packed, password=password)
    if not content_folder.coders:
        raise SevenZipError("Folder has no coders")
    if len(content_folder.coders) != 1:
        raise SevenZipError(f"Unsupported multi-coder folder: {[c.method.hex() for c in content_folder.coders]}")

    coder = content_folder.coders[0]
    method = coder.method
    unpack_size = content_folder.get_unpack_size()

    if method == METHOD_COPY:
        if len(content_packed) < unpack_size:
            raise SevenZipError("Copy-coded data shorter than unpack size")
        return content_packed[:unpack_size]

    if method in (METHOD_LZMA2, METHOD_LZMA):
        filters = [_lzma_filter_from_coder(coder)]
        return _lzma_decompress_raw(content_packed, filters, unpack_size)

    if method == METHOD_DEFLATE:
        # Raw deflate stream.
        return zlib.decompress(content_packed, wbits=-15)[:unpack_size]

    if method == METHOD_BZIP2:
        import bz2

        return bz2.decompress(content_packed)[:unpack_size]

    raise SevenZipError(f"Unsupported 7z compression method: {method.hex()}")


def _parse_header_buffer(buffer: BinaryIO, archive_file: BinaryIO, after_header: int) -> tuple[Optional[StreamsInfo], list[dict], list[bool], list[bool]]:
    prop = buffer.read(1)
    if not prop:
        return None, [], [], []
    prop_id = prop[0]

    if prop_id == PROP_HEADER:
        return _parse_unpacked_header(buffer)

    if prop_id != PROP_ENCODED_HEADER:
        raise SevenZipError(f"Unknown header property 0x{prop_id:02x}")

    # Encoded header: PackInfo + UnpackInfo (no substreams).
    streams = StreamsInfo()
    next_prop = _read_byte(buffer)
    if next_prop != PROP_PACK_INFO:
        raise SevenZipError("Encoded header missing PackInfo")
    streams.pack_info = _parse_pack_info(buffer)
    next_prop = _read_byte(buffer)
    if next_prop != PROP_UNPACK_INFO:
        raise SevenZipError("Encoded header missing UnpackInfo")
    streams.folders = _parse_unpack_info(buffer)
    next_prop = _read_byte(buffer)
    if next_prop != PROP_END:
        raise SevenZipError(f"Expected END in encoded header streams, got 0x{next_prop:02x}")

    assert streams.pack_info is not None
    if not streams.folders:
        raise SevenZipError("Encoded header has no folders")

    # Decompress each folder of the encoded header and concatenate.
    pack_base = after_header + streams.pack_info.pack_pos
    positions = streams.pack_info.pack_positions
    decoded = bytearray()
    pack_index = 0
    for folder in streams.folders:
        folder_pack_count = len(folder.packed_indices) if folder.packed_indices else 1
        pack_offset = pack_base + positions[pack_index]
        pack_size = sum(streams.pack_info.pack_sizes[pack_index : pack_index + folder_pack_count])
        decoded.extend(_decode_folder_to_bytes(archive_file, folder, pack_offset, pack_size))
        pack_index += folder_pack_count

    decoded_buf = io.BytesIO(decoded)
    header_byte = _read_byte(decoded_buf)
    if header_byte != PROP_HEADER:
        raise SevenZipError(f"Decoded header does not start with HEADER, got 0x{header_byte:02x}")
    return _parse_unpacked_header(decoded_buf)


def _parse_unpacked_header(buffer: BinaryIO) -> tuple[Optional[StreamsInfo], list[dict], list[bool], list[bool]]:
    streams: Optional[StreamsInfo] = None
    raw_files: list[dict] = []
    empty_files: list[bool] = []
    anti_files: list[bool] = []

    prop = _read_byte(buffer)
    if prop == PROP_MAIN_STREAMS_INFO:
        streams = _parse_streams_info(buffer)
        prop = _read_byte(buffer)
    if prop == PROP_FILES_INFO:
        raw_files, empty_files, anti_files, _ = _parse_files_info(buffer)
        prop = _read_byte(buffer)
    if prop != PROP_END:
        raise SevenZipError(f"Expected END at end of Header, got 0x{prop:02x}")
    return streams, raw_files, empty_files, anti_files


def parse_7z_archive(file: BinaryIO) -> SevenZipArchiveInfo:
    """Parse a 7z archive from a seekable binary file object."""
    if not hasattr(file, "seek") or not hasattr(file, "read"):
        raise SevenZipError("7z parser requires a seekable binary file object")

    file.seek(0)
    magic = _read_exact(file, 6)
    if magic != MAGIC_7Z:
        raise SevenZipError(f"Not a 7z archive (bad magic: {magic!r})")

    major = _read_byte(file)
    minor = _read_byte(file)
    if major != 0:
        raise SevenZipError(f"Unsupported 7z major version: {major}.{minor}")

    start_header_crc = struct.unpack("<I", _read_exact(file, 4))[0]
    next_header_offset = struct.unpack("<Q", _read_exact(file, 8))[0]
    next_header_size = struct.unpack("<Q", _read_exact(file, 8))[0]
    next_header_crc = struct.unpack("<I", _read_exact(file, 4))[0]

    start_data = struct.pack("<QQI", next_header_offset, next_header_size, next_header_crc)
    if _crc32(start_data) != start_header_crc:
        raise SevenZipError("Invalid 7z StartHeader CRC")

    after_header = SIGNATURE_HEADER_SIZE
    if next_header_size == 0:
        return SevenZipArchiveInfo(after_header=after_header, pack_pos_base=after_header, folders=[], pack_info=None, files=[])

    file.seek(after_header + next_header_offset)
    header_data = _read_exact(file, next_header_size)
    if _crc32(header_data) != next_header_crc:
        raise SevenZipError("Invalid 7z NextHeader CRC")

    streams, raw_files, empty_files, anti_files = _parse_header_buffer(io.BytesIO(header_data), file, after_header)
    files, solid = _build_file_entries(raw_files, empty_files, anti_files, streams, after_header)

    pack_info = streams.pack_info if streams else None
    folders = streams.folders if streams else []
    pack_pos_base = after_header + (pack_info.pack_pos if pack_info else 0)

    return SevenZipArchiveInfo(
        after_header=after_header,
        pack_pos_base=pack_pos_base,
        folders=folders,
        pack_info=pack_info,
        files=files,
        solid=solid,
    )


def is_7z_file(file: BinaryIO) -> bool:
    pos = file.tell()
    try:
        return file.read(6) == MAGIC_7Z
    finally:
        file.seek(pos)


def method_names(methods: Sequence[bytes]) -> str:
    names = {
        METHOD_COPY: "Copy",
        METHOD_LZMA: "LZMA",
        METHOD_LZMA2: "LZMA2",
        METHOD_BZIP2: "BZip2",
        METHOD_DEFLATE: "Deflate",
        METHOD_AES: "AES",
        METHOD_PPMD: "PPMd",
        METHOD_BCJ: "BCJ",
        METHOD_DELTA: "Delta",
    }
    return "+".join(names.get(m, m.hex()) for m in methods)
