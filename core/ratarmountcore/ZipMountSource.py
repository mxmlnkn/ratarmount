#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import json
import os
import re
import stat
import tarfile
import traceback
from timeit import default_timer as timer

from typing import Any, Dict, IO, Iterable, List, Optional, Tuple, Union

from .compressions import zipfile
from .MountSource import FileInfo, MountSource
from .SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from .utils import InvalidIndexError, overrides


class ZipMountSource(MountSource):
    def __init__(
        self,
        # fmt: off
        fileOrPath             : Union[str, IO[bytes]],
        writeIndex             : bool                      = False,
        clearIndexCache        : bool                      = False,
        indexFilePath          : Optional[str]             = None,
        indexFolders           : Optional[List[str]]       = None,
        encoding               : str                       = tarfile.ENCODING,
        verifyModificationTime : bool                      = False,
        printDebug             : int                       = 0,
        indexMinimumFileCount  : int                       = 1000,
        transform              : Optional[Tuple[str, str]] = None,
        **options
        # fmt: on
    ) -> None:
        # fmt: off
        self.fileObject             = zipfile.ZipFile(fileOrPath, 'r')
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.printDebug             = printDebug
        self.options                = options
        self.transformPattern       = transform
        # fmt: on

        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )

        ZipMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = {info.header_offset: info for info in self.fileObject.infolist()}

        self.index = SQLiteIndex(
            indexFilePath,
            indexFolders=indexFolders,
            archiveFilePath=self.archiveFilePath,
            encoding=self.encoding,
            checkMetadata=self._checkMetadata,
            printDebug=self.printDebug,
            indexMinimumFileCount=indexMinimumFileCount,
            backendName='ZipMountSource',
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
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.storeMetadata(argumentsMetadata, self.archiveFilePath)

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

        path, name = SQLiteIndex.normpath(self.transform(info.filename)).rsplit("/", 1)

        # Currently, this is unused. The index only is used for getting metadata. (The data offset
        # is already determined and written out in order to possibly speed up reading of encrypted
        # files by implementing the decryption ourselves.)
        # The data offset is deprecated again! Collecting it can add a huge overhead for large zip files
        # because we have to seek to every position and read a few bytes from it. Furthermore, it is useless
        # by itself anyway. We don't even store yet how the data is compressed or encrypted, so we would
        # have to read the local header again anyway!
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

    @overrides(MountSource)
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

    def _tryToOpenFirstFile(self):
        # Get first row that has the regular file bit set in mode (stat.S_IFREG == 32768 == 1<<15).
        result = self.index.getConnection().execute(
            f"""SELECT path,name {SQLiteIndex.FROM_REGULAR_FILES} ORDER BY "offsetheader" ASC LIMIT 1;"""
        )
        if not result:
            return
        firstFile = result.fetchone()
        if not firstFile:
            return

        if self.printDebug >= 2:
            print(
                "[Info] The index contains no backend name. Therefore, will try to open the first file as "
                "an integrity check."
            )
        try:
            fileInfo = self.getFileInfo(firstFile[0] + '/' + firstFile[1])
            if not fileInfo:
                return

            with self.open(fileInfo) as file:
                file.read(1)
        except Exception as exception:
            if self.printDebug >= 2:
                print("[Info] Trying to open the first file raised an exception:", exception)
            if self.printDebug >= 3:
                traceback.print_exc()
            raise InvalidIndexError("Integrity check of opening the first file failed.") from exception

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

        if 'arguments' in metadata:
            SQLiteIndex.checkMetadataArguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self._tryToOpenFirstFile()
