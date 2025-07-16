"""
Contains some quick format checks. Should not try to import large dependencies.
If the dependency is necessary, the check function should be moved into the respective MountSource implementation.
It basically is something like libmagic (Linux 'file' command), but only for the types we actually need.
There seem to be no nice, simple, widespread, and maintained Python package for this.

https://github.com/h2non/filetype.py                    MIT  ~700 stars    Last Release: 2022-11-02
    Small, dependency-free, fast Python package to infer binary file types checking the magic numbers signature
https://github.com/riad-azz/py-file-type                MIT  1 star        Last Release: 2023-04-22
     Wrapper for python-magic that includes necessary bin files.
https://github.com/enchant97/python-file-type-guesser   MIT  1 star        Last Release: 2021-12-20
    Guess a file's type from its content and extension.
    This is intended to be a pure Python implementation of something like python-magic.
https://github.com/dveselov/python-libmagic             MIT  ~30 stars     Last Release: 2016-07-07
    Python bindings to libmagic

The situation for file type detection Python packages is abysmal.
And I already skipped as many other low-effort packages on PyPI.

See:
 - https://en.wikipedia.org/wiki/List_of_file_signatures
 - https://en.wikipedia.org/wiki/List_of_archive_formats
"""

import dataclasses
import enum
import struct
import tarfile
import zipfile
from typing import IO, Callable, Optional, Union


class FileFormatID(enum.Enum):
    # fmt: off
    # Archive Formats (bundles more than one file)
    # "Normal" Archive formats with compression
    SEVEN_ZIP        = 0x101
    RAR              = 0x102
    ZIP              = 0x103
    SQLAR            = 0x104
    CAB              = 0x105
    AIX_SMALL        = 0x106
    AR               = 0x107
    XAR              = 0x108
    CPIO             = 0x109
    # "TAR"-like compression formats without compression
    TAR              = 0x201
    ASAR             = 0x202
    # Read-Only File systems
    SQUASHFS         = 0x301
    ISO9660          = 0x302
    # Fully-fledged file systems
    FAT              = 0x401
    EXT4             = 0x402
    # Other archive  formats
    RATARMOUNT_INDEX = 0x901
    WARC             = 0x902

    # Compression formats (compresses a single file / stream)
    BZIP2            = 0x1001
    GZIP             = 0x1002
    XZ               = 0x1003
    ZSTANDARD        = 0x1004
    ZLIB             = 0x1005
    LZ4              = 0x1006
    LZMA             = 0x1007

    GRZIP            = 0x1021
    LRZIP            = 0x1022
    LZIP             = 0x1023
    LZOP             = 0x1024
    RPM              = 0x1025
    UU               = 0x1026
    Z                = 0x1027
    # fmt: on


FID = FileFormatID


def is_tar(fileobj: IO[bytes], encoding: str = tarfile.ENCODING) -> bool:
    try:
        # Will only open uncompressed TAR files.
        with tarfile.open(fileobj=fileobj, mode='r:', encoding=encoding):
            return True
    except (tarfile.ReadError, tarfile.CompressionError):
        pass
    return False


def find_asar_header(fileobj: IO[bytes]) -> tuple[int, int, int]:
    """Return triple (start of header JSON, size of header JSON, start of file objects)"""
    # https://github.com/electron/asar/issues/128
    # https://github.com/electron/asar
    # > UInt32: header_size | String: header | Bytes: file1 | ...
    # > The header_size and header are serialized with Pickle class
    # > The header is a JSON string, and the header_size is the size of header's Pickle object.
    # Numbers are in little endian. Pickling writes the data, prepends a uint32 before and pads to 4 B offsets.
    # Because header_size is always 4 bytes, the implicit "magic" / redundant bytes of this file format are:
    # \x04\x00\x00\x00
    # Furthermore because pickling "header" already prepends the length of "header", the actual "header_size"
    # is completely redundant and can also be used as some kind of integrity check...
    # I don't understand where the fourth 32-bit number comes from. For some reason, the header seems to be
    # pickled twice, contrary to the ReadMe!?
    # https://github.com/electron/asar/issues/16
    # https://github.com/electron/asar/issues/226
    # https://knifecoat.com/Posts/ASAR+Format+Spec
    # -> Nice rant about the format as I have to concur about the usage of pickle-js.
    ASAR_MAGIC_SIZE = 4 * 4
    sizeOfPickledSize, sizeOfPickledPickledPickledHeader, sizeOfPickledPickledHeader, sizeOfPickledHeader = (
        struct.unpack('<LLLL', fileobj.read(ASAR_MAGIC_SIZE))
    )
    if sizeOfPickledSize != 4:
        raise ValueError("First magic bytes quadruplet does not match SQLAR!")
    if sizeOfPickledPickledPickledHeader != sizeOfPickledPickledHeader + 4:
        raise ValueError("Second magic bytes quadruplet does not match SQLAR!")
    padding = (4 - sizeOfPickledHeader % 4) % 4
    if sizeOfPickledPickledHeader != sizeOfPickledHeader + padding + 4:
        raise ValueError("Third magic bytes quadruplet does not match SQLAR!")

    dataOffset = ASAR_MAGIC_SIZE + sizeOfPickledHeader + padding

    # It would be nice if we could check that dataOffset < file size, but we cannot get that (cheaply)
    # in case of compressed files or for large headers and because "seek(offset)" will always return that
    # offset even if it is > file size, so we would actually have to read the data.

    return ASAR_MAGIC_SIZE, sizeOfPickledHeader, dataOffset


def is_asar(fileobj: IO[bytes]) -> bool:
    offset = fileobj.tell()
    try:
        # Reading the header and checking it to be correct JSON is to expensive for large archives.
        # The unnecessary pickling already should introduce enough redundancy for checks and detection.
        find_asar_header(fileobj)
        return True
    except Exception:
        pass
    finally:
        fileobj.seek(offset)
    return False


def is_squashfs(fileobj: IO[bytes]) -> bool:
    offset = fileobj.tell()
    try:
        # https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock
        magicBytes = fileobj.read(4)
        if magicBytes != b"hsqs":
            return False

        _inodeCount, _modificationTime, blockSize, _fragmentCount = struct.unpack('<IIII', fileobj.read(4 * 4))
        compressor, blockSizeLog2, _flags, _idCount, major, minor = struct.unpack('<HHHHHH', fileobj.read(6 * 2))
        # root_inode, bytes_used, id_table, xattr_table, inode_table, dir_table, frag_table, export_table =
        # struct.unpack('<QQQQQQQQ', fileobj.read(8 * 8))

        # The size of a data block in bytes. Must be a power of two between 4096 (4k) and 1048576 (1 MiB).
        # log2 4096 = 12, log2 1024*1024 = 20
        if blockSizeLog2 < 12 or blockSizeLog2 > 20 or 2**blockSizeLog2 != blockSize:
            return False

        if major != 4 or minor != 0:
            return False

        # Compressions: 0:None, 1:GZIP, 2:LZMA, 3:LZO, 4:XZ, 5:LZ4, 6:ZSTD
        if compressor > 6:
            return False

    finally:
        fileobj.seek(offset)

    return True


def find_squashfs_offset(fileobj: IO[bytes], maxSkip=1024 * 1024) -> int:
    """
    Looks for the SquashFS superblock, which can be at something other than offset 0 for AppImage files.
    """
    # https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock
    if is_squashfs(fileobj):
        return 0

    oldOffset = fileobj.tell()
    try:
        magic = b"hsqs"
        data = fileobj.read(maxSkip + len(magic))
        magicOffset = 0
        while True:
            magicOffset = data.find(magic, magicOffset + 1)
            if magicOffset < 0 or magicOffset >= len(data):
                break
            fileobj.seek(magicOffset)
            if is_squashfs(fileobj):
                return magicOffset
    finally:
        fileobj.seek(oldOffset)

    return -1


def _is_cpio(fileobj: IO[bytes]) -> bool:
    # http://justsolve.archiveteam.org/wiki/Cpio
    # https://github.com/libarchive/libarchive/blob/6110e9c82d8ba830c3440f36b990483ceaaea52c/libarchive/
    #   archive_read_support_format_cpio.c#L272
    firstBytes = fileobj.read(5)
    return firstBytes == b'07070' or firstBytes[:2] in [b'\x71\xc7', b'\xc7\x71']


def _is_iso9660(fileobj: IO[bytes]) -> bool:
    # https://www.iso.org/obp/ui/#iso:std:iso:9660:ed-1:v1:en
    # http://www.brankin.com/main/technotes/Notes_ISO9660.htm
    # https://en.wikipedia.org/wiki/ISO_9660
    # https://en.wikipedia.org/wiki/Optical_disc_image
    offset = 32 * 1024 + 1
    udfOffset = 38 * 1024 + 1
    buffer = fileobj.read(max(offset, udfOffset) + 4)
    return buffer[offset : offset + 5] == b'CD001' or buffer[udfOffset : udfOffset + 4] == b'NSR0'


def _check_zlib_header(fileobj: IO[bytes]) -> bool:
    header = fileobj.read(2)
    cmf = header[0]
    if cmf & 0xF != 8:
        return False
    if cmf >> 4 > 7:
        return False
    flags = header[1]
    if ((cmf << 8) + flags) % 31 != 0:
        return False
    usesDictionary = ((flags >> 5) & 1) != 0
    return not usesDictionary


def _is_bzip2(fileobj: IO[bytes]) -> bool:
    return fileobj.read(4)[:3] == b'BZh' and fileobj.read(6) == (0x314159265359).to_bytes(6, 'big')


def _check_lz4_header(fileobj: IO[bytes]) -> bool:
    SKIPPABLE_FRAME_MAGIC = 0x184D2A50
    (magic,) = struct.unpack('<L', fileobj.read(4))

    # https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md#skippable-frames
    while magic & 0xFFFF_FFF0 == SKIPPABLE_FRAME_MAGIC:
        (frame_size,) = struct.unpack('<L', fileobj.read(4))
        fileobj.seek(fileobj.tell() + frame_size)
        (magic,) = struct.unpack('<L', fileobj.read(4))

    # https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md#general-structure-of-lz4-frame-format
    return magic == 0x184D2204


def _check_zstandard_header(fileobj: IO[bytes]) -> bool:
    SKIPPABLE_FRAME_MAGIC = 0x184D2A50
    (magic,) = struct.unpack('<L', fileobj.read(4))

    # https://github.com/facebook/zstd/blob/dev/doc/zstd_compression_format.md#skippable-frames
    while magic & 0xFFFF_FFF0 == SKIPPABLE_FRAME_MAGIC:
        (frame_size,) = struct.unpack('<L', fileobj.read(4))
        fileobj.seek(fileobj.tell() + frame_size)
        (magic,) = struct.unpack('<L', fileobj.read(4))

    return magic == 0xFD2FB528


@dataclasses.dataclass
class FileFormatInfo:
    # Extensions without the initial '.'
    extensions: list[str]
    # If the first bytes of a format are constant, then they should be stated here.
    # This is used to build a lookup table for faster format detection as a first stage to thin down
    # the formats to actually call checkHeader for.
    # Some formats like ZIP (only has magic footer) and ISO9660 don't have a magic footer!
    magicBytes: Optional[bytes]
    # File format checkers should err on the side of false positives because else the file
    # will be rejected without actually trying to open it!
    # For simplicity, assume that all of these checkHeader will read from the current offset and be
    # at an arbitrary offset after the check. Normally, the offset should be larger than before in order
    # to mostly work with non-seekable files (although how is the caller supposed to parse the format after
    # an opaque check that skips an arbitrary amount of header?).
    checkHeader: Optional[Callable[[IO[bytes]], bool]] = None


ARCHIVE_FORMATS: dict[FileFormatID, FileFormatInfo] = {
    # "Normal" Archive formats with compression
    FID.SEVEN_ZIP: FileFormatInfo(['7z', '7zip'], b'7z\xbc\xaf\x27\x1c'),
    FID.RAR: FileFormatInfo(['rar'], b'Rar!\x1a\x07'),
    # is_zipfile might yields some false positives, we want it to err on the positive side.
    # See: https://bugs.python.org/issue42096
    FID.ZIP: FileFormatInfo(['zip'], None, zipfile.is_zipfile),
    # SQLAR can be encrypted, for which it will not have any magic bytes! The first 16 B are the salt.
    # lambda x: x.read(16) == b'SQLite format 3\x00'
    FID.SQLAR: FileFormatInfo(['sqlar'], None, None),
    # https://download.microsoft.com/download/4/d/a/4da14f27-b4ef-4170-a6e6-5b1ef85b1baa/[ms-cab].pdf
    FID.CAB: FileFormatInfo(['cab'], b'MSCF'),
    # https://www.ibm.com/docs/en/aix/7.2.0?topic=formats-ar-file-format-small
    FID.AIX_SMALL: FileFormatInfo(['ar'], b'<aiaff>\n'),
    # https://www.ibm.com/docs/en/aix/7.2.0?topic=formats-ar-file-format-big#ar_big
    # https://en.wikipedia.org/wiki/Ar_(Unix)
    FID.AR: FileFormatInfo(['a', 'ar', 'lib'], b'!<arch>\n'),
    FID.XAR: FileFormatInfo(['xar'], b'xar!'),
    FID.CPIO: FileFormatInfo(['cpio'], None, _is_cpio),
    # "TAR"-like compression formats without compression
    FID.TAR: FileFormatInfo(['tar'], None, is_tar),
    FID.ASAR: FileFormatInfo(['asar'], b'\x04\x00\x00\x00', is_asar),
    # Read-Only File systems
    FID.SQUASHFS: FileFormatInfo(['squashfs', 'AppImage', 'snap'], None, lambda x: find_squashfs_offset(x) >= 0),
    FID.ISO9660: FileFormatInfo(['iso'], None, _is_iso9660),
    # Fully-fledged file systems
    FID.FAT: FileFormatInfo(['fat', 'img', 'dd', 'fat12', 'fat16', 'fat32', 'raw'], None),
    FID.EXT4: FileFormatInfo(['ext4', 'img', 'dd', 'raw'], None),
    # Other archive formats
    FID.RATARMOUNT_INDEX: FileFormatInfo(['index.sqlite'], b'SQLite format 3\x00'),
    # https://www.iso.org/standard/68004.html
    # https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/#file-and-record-model
    FID.WARC: FileFormatInfo(['warc'], b'WARC/1.'),
}

COMPRESSION_FORMATS: dict[FileFormatID, FileFormatInfo] = {
    FID.BZIP2: FileFormatInfo(['bz2', 'bzip2'], b'BZh', _is_bzip2),
    FID.GZIP: FileFormatInfo(['gz', 'gzip'], b'\x1f\x8b'),
    FID.XZ: FileFormatInfo(['xz'], b"\xfd7zXZ\x00"),
    FID.ZSTANDARD: FileFormatInfo(['zst', 'zstd', 'pzstd'], None, _check_zstandard_header),
    FID.ZLIB: FileFormatInfo(['zz', 'zlib'], None, _check_zlib_header),
    # https://github.com/libarchive/libarchive/blob/6110e9c82d8ba830c3440f36b990483ceaaea52c/libarchive/
    # archive_read_support_filter_grzip.c#L46
    # It is almost impossible to find the original sources or a specification:
    # https://t2sde.org/packages/grzip
    # ftp://ftp.ac-grenoble.fr/ge/compression/grzip-0.3.0.tar.bz2
    FID.GRZIP: FileFormatInfo(['grz', 'grzip'], b'GRZipII\x00\x02\x04:)'),
    # https://github.com/ckolivas/lrzip/blob/master/doc/magic.header.txt
    FID.LRZIP: FileFormatInfo(['lrz', 'lrzip'], b'LRZI\x00'),
    # https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md#general-structure-of-lz4-frame-format
    FID.LZ4: FileFormatInfo(['lz4'], None, _check_lz4_header),
    # https://www.ietf.org/archive/id/draft-diaz-lzip-09.txt
    FID.LZIP: FileFormatInfo(['lz', 'lzip'], b'LZIP\x01'),
    # https://github.com/jljusten/LZMA-SDK/blob/master/DOC/lzma-specification.txt
    # https://github.com/frizb/FirmwareReverseEngineering/blob/master/IdentifyingCompressionAlgorithms.md#lzma
    FID.LZMA: FileFormatInfo(['lzma'], b'\x5d\x00\x00'),
    FID.LZOP: FileFormatInfo(['lzo', 'lzop'], b'\x89\x4c\x5a\x4f\x00\x0d\x0a\x1a\x0a'),
    # https://refspecs.linuxbase.org/LSB_4.1.0/LSB-Core-generic/LSB-Core-generic/pkgformat.html
    FID.RPM: FileFormatInfo(['rpm'], b'\xed\xab\xee\xdb'),
    # https://en.wikipedia.org/wiki/Uuencoding
    FID.UU: FileFormatInfo(['uu'], b'begin '),
    # https://github.com/file/file/blob/master/magic/Magdir/compress
    FID.Z: FileFormatInfo(['Z'], b'\x1f\x9d'),
}


FILE_FORMATS = {**ARCHIVE_FORMATS, **COMPRESSION_FORMATS}

# Check that all defined format IDs have FileFormatInfo, so that we can assume FORMATS[FID] to not fail.
for _formatInfo in FileFormatID:
    assert _formatInfo in FILE_FORMATS, f"Missing file format information for: {_formatInfo}"


# Maps the first 2 bytes to possible candidates. 2 B was chosen because most formats have
# at least 2 B of constant magic and if the table was implemented in contiguous memory, it would be nicely
# addressable by a 16-bit index (the 2 B). Statistically, this cuts down the number of expensive checkHeader
# calls by 65536.
_MAGIC_BYTES_TO_FORMATS: dict[bytes, list[FileFormatID]] = {}
_FORMATS_WITHOUT_MAGIC_BYTES: list[FileFormatID] = []


def recompute_cached_magic_bytes():
    """Should be called when injecting new file formats from outsfide."""
    for fid, info in FILE_FORMATS.items():
        if info.magicBytes is None or len(info.magicBytes) < 2:
            _FORMATS_WITHOUT_MAGIC_BYTES.append(fid)
        else:
            firstTwoBytes = info.magicBytes[:2]
            if firstTwoBytes not in _MAGIC_BYTES_TO_FORMATS:
                _MAGIC_BYTES_TO_FORMATS[firstTwoBytes] = []
            _MAGIC_BYTES_TO_FORMATS[firstTwoBytes].append(fid)


recompute_cached_magic_bytes()


def might_be_format(fileobj: IO[bytes], fid: Union[FileFormatID, FileFormatInfo]) -> bool:
    formatInfo = fid if isinstance(fid, FileFormatInfo) else FILE_FORMATS[fid]
    oldOffset = fileobj.tell()
    try:
        if formatInfo.magicBytes and fileobj.read(len(formatInfo.magicBytes)) != formatInfo.magicBytes:
            return False
        if formatInfo.checkHeader:
            fileobj.seek(oldOffset)
            return formatInfo.checkHeader(fileobj)
    finally:
        fileobj.seek(oldOffset)

    # No magic bytes and no header check, reject it because there seems to be the necessary module to read
    # that format missing anyway.
    return bool(formatInfo.magicBytes or formatInfo.checkHeader)


def detect_formats(fileobj: IO[bytes]) -> set[FileFormatID]:
    oldOffset = fileobj.tell()
    try:
        firstTwoBytes = fileobj.read(2)
        # I don't think there is any file format that can be recognized if the file is smaller than 2 B.
        if len(firstTwoBytes) < 2:
            return set()

        formatsToTest = _MAGIC_BYTES_TO_FORMATS.get(firstTwoBytes, [])
    finally:
        fileobj.seek(oldOffset)

    return {fid for fid in formatsToTest + _FORMATS_WITHOUT_MAGIC_BYTES if might_be_format(fileobj, fid)}


def replace_format_check(fid: FileFormatID, checkHeader: Optional[Callable[[IO[bytes]], bool]] = None):
    for formats in [ARCHIVE_FORMATS, COMPRESSION_FORMATS, FILE_FORMATS]:
        if fid in formats:
            info = dataclasses.asdict(formats[fid])
            info['checkHeader'] = checkHeader
            formats[fid] = FileFormatInfo(**info)
