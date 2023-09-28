#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import io
import json
import os
import stat
import struct
import tarfile
from timeit import default_timer as timer

from typing import Any, Dict, IO, Iterable, List, Optional, Tuple, Union

from .compressions import zipfile
from .MountSource import FileInfo, MountSource
from .SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from .utils import InvalidIndexError, overrides


# Only really 8 (Deflate), 12 (bzip2), 14 (LZMA), and maybe 93 (Zstandard) are common.
# 1-6 are not even recommended by the APPNOTE anymore.
COMPRESSION_METHODS = {
    0: "The file is stored (no compression)",
    1: "The file is Shrunk",
    2: "The file is Reduced with compression factor 1",
    3: "The file is Reduced with compression factor 2",
    4: "The file is Reduced with compression factor 3",
    5: "The file is Reduced with compression factor 4",
    6: "The file is Imploded",
    7: "Reserved for Tokenizing compression algorithm",
    8: "Deflate",  # Most common. Should be supported!
    9: "Enhanced Deflating using Deflate64(tm)",
    10: "PKWARE Data Compression Library Imploding (old IBM TERSE)",
    11: "Reserved by PKWARE",
    12: "BZIP2",  # Next most common.
    13: "Reserved by PKWARE",
    14: "LZMA",  # Also in use.
    15: "Reserved by PKWARE",
    16: "IBM z/OS CMPSC Compression",
    17: "Reserved by PKWARE",
    18: "File is compressed using IBM TERSE (new)",
    19: "IBM LZ77 z Architecture",
    20: "deprecated (use method 93 for zstd)",
    93: "Zstandard (zstd) Compression",  # Modern compression
    94: "MP3 Compression",
    95: "XZ Compression",  # I think that LZMA is used instead of this?
    96: "JPEG variant",
    97: "WavPack compressed data",
    98: "PPMd version I, Rev 1",
    99: "AE-x encryption marker (see APPENDIX E)",
}

EXTRA_FIELD_IDS = {
    0x0001: "Zip64 extended information extra field",
    0x0007: "AV Info",
    0x0008: "Reserved for extended language encoding data (PFS) (see APPENDIX D)",
    0x0009: "OS/2",
    0x000A: "NTFS",
    0x000C: "OpenVMS",
    0x000D: "UNIX",
    0x000E: "Reserved for file stream and fork descriptors",
    0x000F: "Patch Descriptor",
    0x0014: "PKCS#7 Store for X.509 Certificates",
    0x0015: "X.509 Certificate ID and Signature for individual file",
    0x0016: "X.509 Certificate ID for Central Directory",
    0x0017: "Strong Encryption Header",
    0x0018: "Record Management Controls",
    0x0019: "PKCS#7 Encryption Recipient Certificate List",
    0x0020: "Reserved for Timestamp record",
    0x0021: "Policy Decryption Key Record",
    0x0022: "Smartcrypt Key Provider Record",
    0x0023: "Smartcrypt Policy Key Data Record",
    0x0065: "IBM S/390 (Z390), AS/400 (I400) attributes - uncompressed",
    0x0066: "Reserved for IBM S/390 (Z390), AS/400 (I400) attributes - compressed",
    0x4690: "POSZIP 4690 (reserved)",
    0x07C8: "Macintosh",
    0x1986: "Pixar USD header ID",
    0x2605: "ZipIt Macintosh",
    0x2705: "ZipIt Macintosh 1.3.5+",
    0x2805: "ZipIt Macintosh 1.3.5+",
    0x334D: "Info-ZIP Macintosh",
    0x4154: "Tandem",
    0x4341: "Acorn/SparkFS",
    0x4453: "Windows NT security descriptor (binary ACL)",
    0x4704: "VM/CMS",
    0x470F: "MVS",
    0x4854: "THEOS (old?)",
    0x4B46: "FWKCS MD5 (see below)",
    0x4C41: "OS/2 access control list (text ACL)",
    0x4D49: "Info-ZIP OpenVMS",
    0x4D63: "Macintosh Smartzip (??)",
    0x4F4C: "Xceed original location extra field",
    0x5356: "AOS/VS (ACL)",
    0x5455: "extended timestamp",
    0x554E: "Xceed unicode extra field",
    0x5855: "Info-ZIP UNIX (original, also OS/2, NT, etc)",
    0x6375: "Info-ZIP Unicode Comment Extra Field",
    0x6542: "BeOS/BeBox",
    0x6854: "THEOS",
    0x7075: "Info-ZIP Unicode Path Extra Field",
    0x7441: "AtheOS/Syllable",
    0x756E: "ASi UNIX",
    0x7855: "Info-ZIP UNIX (new)",
    0x7875: "Info-ZIP UNIX (newer UID/GID)",
    0xA11E: "Data Stream Alignment (Apache Commons-Compress)",
    0xA220: "Microsoft Open Packaging Growth Hint",
    0xCAFE: "Java JAR file Extra Field Header ID",
    0xD935: "Android ZIP Alignment Extra Field",
    0xE57A: "Korean ZIP code page info",
    0xFD4A: "SMS/QDOS",
    0x9901: "AE-x encryption structure (see APPENDIX E)",
    0x9902: "unknown",
}


class LocalFileHeader:
    """
    This class can be constructed from a file object. During construction it reads the ZIP local file header
    from the current position and advances the position to the file data.
    Currently, this class is only indirectly used by ZipMountSource to get the file data offset.
    """

    # https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
    #
    # > 4.3.7  Local file header:
    # >
    # >    local file header signature     4 bytes  (0x04034b50)
    # >    version needed to extract       2 bytes
    # >    general purpose bit flag        2 bytes
    # >    compression method              2 bytes
    # >    last mod file time              2 bytes
    # >    last mod file date              2 bytes
    # >    crc-32                          4 bytes
    # >    compressed size                 4 bytes
    # >    uncompressed size               4 bytes
    # >    file name length                2 bytes
    # >    extra field length              2 bytes
    # >
    # >    file name (variable size)
    # >    extra field (variable size)
    # >
    # > 4.3.8  File data
    # >
    # >    Immediately following the local header for a file
    # >    SHOULD be placed the compressed or stored data for the file.
    # >    If the file is encrypted, the encryption header for the file
    # >    SHOULD be placed after the local header and before the file
    # >    data. The series of [local file header][encryption header]
    # >    [file data][data descriptor] repeats for each file in the
    # >    .ZIP archive.
    # >
    # >    Zero-byte files, directories, and other file types that
    # >    contain no content MUST NOT include file data.
    #
    # For more details, see "4.4 Explanation of fields"

    FixedLocalFileHeader = struct.Struct('<LHHHHHLLLHH')

    def __init__(self, fileObject: IO[bytes]):
        result = LocalFileHeader.FixedLocalFileHeader.unpack(fileObject.read(LocalFileHeader.FixedLocalFileHeader.size))
        assert len(result) == 11

        (
            self.signature,
            self.extractVersion,
            self.flags,
            self.compression,
            self.modificationTime,
            self.modificationDate,
            self.crc32,
            self.compressedSize,
            self.uncompressedSize,
            self.fileNameLength,
            self.extraFieldLength,
        ) = result

        self.fileName = fileObject.read(self.fileNameLength)
        self.extraField = fileObject.read(self.extraFieldLength)

        assert self.signature == 0x04034B50

        self.encryptionHeader = fileObject.read(12) if self.isEncrypted() else None

    def isEncrypted(self):
        return (self.flags & 1) != 0

    def _printExtraFields(self):
        print("Extra Field:")
        extraFields = io.BytesIO(self.extraField)
        while extraFields.tell() < len(self.extraField):
            extraId, size = struct.unpack('<HH', extraFields.read(4))
            extraData = extraFields.read(size)
            print(f"    ID: {EXTRA_FIELD_IDS.get(extraId, hex(extraId))}, size: {size}, contents: {extraData}")


class ZipMountSource(MountSource):
    def __init__(
        self,
        # fmt: off
        fileOrPath             : Union[str, IO[bytes]],
        writeIndex             : bool                = False,
        clearIndexCache        : bool                = False,
        indexFilePath          : Optional[str]       = None,
        indexFolders           : Optional[List[str]] = None,
        encoding               : str                 = tarfile.ENCODING,
        verifyModificationTime : bool                = False,
        printDebug             : int                 = 0,
        **options
        # fmt: on
    ) -> None:
        self.rawFileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        self.fileObject = zipfile.ZipFile(fileOrPath, 'r')
        self.archiveFilePath = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding = encoding
        self.verifyModificationTime = verifyModificationTime
        self.printDebug = printDebug
        self.options = options

        ZipMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = {info.header_offset: info for info in self.fileObject.infolist()}

        self.index = SQLiteIndex(
            indexFilePath,
            indexFolders=indexFolders,
            archiveFilePath=self.archiveFilePath,
            encoding=self.encoding,
            checkMetadata=self._checkMetadata,
            printDebug=self.printDebug,
            preferMemory=len(self.files) < options.get("indexMinimumFileCount", 1000),
        )

        if clearIndexCache:
            self.index.clearIndexes()

        isFileObject = not isinstance(fileOrPath, str)

        self.index.openExisting()
        if self.index.indexIsLoaded():
            # self._loadOrStoreCompressionOffsets()  # load
            self.index.reloadIndexReadOnly()
        else:
            # Open new database when we didn't find an existing one.
            # Simply open in memory without an error even if writeIndex is True but when not indication
            # for a index file location has been given.
            if writeIndex and (indexFilePath or not isFileObject):
                self.index.openWritable()
            else:
                self.index.openInMemory()

            self._createIndex()
            # self._loadOrStoreCompressionOffsets()  # store
            if self.index.indexIsLoaded():
                self._storeMetadata()
                self.index.reloadIndexReadOnly()

    def _storeMetadata(self) -> None:
        argumentsToSave = ['encoding']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.storeMetadata(argumentsMetadata, self.archiveFilePath)

    def _findDataOffset(self, headerOffset: int):
        self.rawFileObject.seek(headerOffset)
        LocalFileHeader(self.rawFileObject)
        return self.rawFileObject.tell()

    def _convertToRow(self, info: "zipfile.ZipInfo") -> Tuple:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        mtime = datetime.datetime(*info.date_time, tzinfo=datetime.timezone.utc).timestamp() if info.date_time else 0

        # According to section 4.5.7 in the .ZIP file format specification, links are supported:
        # https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
        # The Python zipfile module has no API for links: https://bugs.python.org/issue45286
        # However, the file mode exposes whether it's a link and the file mode is shown by ZipInfo.__repr__.
        # For that, it uses the OS-dependent external_attr member. See also the ZIP specification on that:
        # > 4.4.15 external file attributes: (4 bytes)
        # >   The mapping of the external attributes is host-system dependent (see 'version made by').
        # >   For MS-DOS, the low order byte is the MS-DOS directory attribute byte.
        # >   If input came from standard input, this field is set to zero.

        # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
        linkname = ""
        if stat.S_ISLNK(info.external_attr >> 16):
            linkname = self.fileObject.read(info).decode()
            mode = 0o555 | stat.S_IFLNK

        path, name = SQLiteIndex.normpath(info.filename).rsplit("/", 1)

        # Currently, this is unused. The index only is used for getting metadata. (The data offset
        # is already determined and written out in order to possibly speed up reading of encrypted
        # files by implementing the decryption ourselves.)
        # The data offset is deprecated again! Collecting it can add a huge overhead for large zip files
        # because we have to seek to every position and read a few bytes from it. Furthermore, it is useless
        # by itself anyway. We don't even store yet how the data is compressed or encrypted, so we would
        # have to read the local header again anyway!
        # dataOffset = self._findDataOffset(info.header_offset)
        dataOffset = 0

        # fmt: off
        fileInfo : Tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            info.header_offset,  # 2  : header offset
            dataOffset        ,  # 3  : data offset
            info.file_size    ,  # 4  : file size
            mtime             ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            linkname          ,  # 8  : linkname
            0                 ,  # 9  : user ID
            0                 ,  # 10 : group ID
            False             ,  # 11 : is TAR (unused?)
            False             ,  # 12 : is sparse
        )
        # fmt: on

        return fileInfo

    def _createIndex(self) -> None:
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensureIntermediaryTables()

        fileInfos = []
        for info in self.fileObject.infolist():
            fileInfos.append(self._convertToRow(info))
        self.index.setFileInfos(fileInfos)

        # Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if self.printDebug >= 2:
            print("Resorting files by path ...")

        self.index.finalize()

        t1 = timer()
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} took {t1 - t0:.2f}s")

    @staticmethod
    def _cleanPath(path):
        result = os.path.normpath(path) + ('/' if path.endswith('/') else '')
        while result.startswith('../'):
            result = result[3:]
        return result

    @staticmethod
    def _findPassword(fileobj: "zipfile.ZipFile", passwords):
        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.infolist()
        if not files:
            for password in passwords:
                fileobj.setpassword(password)
                files = fileobj.infolist()
                if files:
                    return password

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if not file.is_dir() and file.file_size > 0]
        if not files:
            return None

        for password in [None] + passwords:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except Exception:
                pass

        raise RuntimeError("Could not find a matching password!")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.index.close()
        self.fileObject.close()

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.index.getFileInfo(path, fileVersion=fileVersion)

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self.index.listDir(path)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        fileVersions = self.index.fileVersions(path)
        return len(fileVersions) if isinstance(fileVersions, dict) else 0

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        assert fileInfo.userdata
        extendedFileInfo = fileInfo.userdata[-1]
        assert isinstance(extendedFileInfo, SQLiteIndexedTarUserData)
        info = self.files[extendedFileInfo.offsetheader]
        assert isinstance(info, zipfile.ZipInfo)
        # CPython's zipfile module does handle multiple file objects being opened and reading from the
        # same underlying file object concurrently by using a _SharedFile class that even includes a lock.
        # Very nice!
        # https://github.com/python/cpython/blob/a87c46eab3c306b1c5b8a072b7b30ac2c50651c0/Lib/zipfile/__init__.py#L1569
        return self.fileObject.open(info, 'r')  # https://github.com/pauldmccarthy/indexed_gzip/issues/85

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)

    def _checkMetadata(self, metadata: Dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""

        if 'tarstats' in metadata:
            if not self.archiveFilePath:
                raise InvalidIndexError("Archive contains file stats but cannot stat real archive!")

            storedStats = json.loads(metadata['tarstats'])
            archiveStats = os.stat(self.archiveFilePath)

            if hasattr(archiveStats, "st_size") and 'st_size' in storedStats:
                if archiveStats.st_size < storedStats['st_size']:
                    raise InvalidIndexError(
                        f"Archive for this SQLite index has shrunk in size from "
                        f"{storedStats['st_size']} to {archiveStats.st_size}"
                    )

            # Only happens very rarely, e.g., for more recent files with the same size.
            if (
                self.verifyModificationTime
                and hasattr(archiveStats, "st_mtime")
                and 'st_mtime' in storedStats
                and archiveStats.st_mtime != storedStats['st_mtime']
            ):
                raise InvalidIndexError(
                    f"The modification date for the archive file {storedStats['st_mtime']} "
                    f"to this SQLite index has changed ({str(archiveStats.st_mtime)})",
                )

        # Check arguments used to create the found index.
        # These are only warnings and not forcing a rebuild by default.
        # TODO: Add --force options?
        if 'arguments' in metadata:
            indexArgs = json.loads(metadata['arguments'])
            argumentsToCheck = ['encoding']
            differingArgs = []
            for arg in argumentsToCheck:
                if arg in indexArgs and hasattr(self, arg) and indexArgs[arg] != getattr(self, arg):
                    differingArgs.append((arg, indexArgs[arg], getattr(self, arg)))
            if differingArgs:
                print("[Warning] The arguments used for creating the found index differ from the arguments ")
                print("[Warning] given for mounting the archive now. In order to apply these changes, ")
                print("[Warning] recreate the index using the --recreate-index option!")
                for arg, oldState, newState in differingArgs:
                    print(f"[Warning] {arg}: index: {oldState}, current: {newState}")
