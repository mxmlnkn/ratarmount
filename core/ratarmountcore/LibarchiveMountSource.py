#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import json
import os
import re
import stat
import tarfile

from ctypes import create_string_buffer
from timeit import default_timer as timer
from typing import Any, Dict, IO, Iterable, List, Optional, Tuple, Union

from .MountSource import FileInfo, MountSource
from .SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from .utils import InvalidIndexError, overrides

try:
    # Use the FFI directly because the higher-level interface does not work sufficiently for our use case.
    import libarchive.ffi as laffi
except ImportError:
    pass


class ArchiveEntry:
    def __init__(self, archive=None, encoding: str = 'utf-8'):
        self._archive = archive
        self._entry = laffi.entry_new()
        self.encoding = encoding

    def __del__(self):
        laffi.entry_free(self._entry)

    def getTimeByName(self, name):
        if not getattr(laffi, f'entry_{name}_is_set')(self._entry):
            return None
        seconds = getattr(laffi, f'entry_{name}')(self._entry)
        nseconds = getattr(laffi, f'entry_{name}_nsec')(self._entry)
        return float(seconds) + float(nseconds) / 1e9 if nseconds else int(seconds)

    def getTime(self):
        result = self.getTimeByName('mtime')
        if result is None:
            result = self.getTimeByName('ctime')
        if result is None:
            result = self.getTimeByName('birthtime')
        return result if result else 0

    def path(self):
        path = laffi.entry_pathname_w(self._entry)
        if not path:
            path = laffi.entry_pathname(self._entry)
            if path is not None:
                try:
                    path = path.decode(self.encoding)
                except UnicodeError:
                    pass
        return path

    def filetype(self):
        return laffi.entry_filetype(self._entry)

    def isDirectory(self):
        return self.filetype() & 0o170000 == 0o040000

    def isSymbolicLink(self):
        return self.filetype() & 0o170000 == 0o120000

    def linkname(self):
        path = ""
        if self.isSymbolicLink():
            path = laffi.entry_symlink_w(self._entry)
            if not path:
                path = laffi.entry_symlink(self._entry)
        else:
            path = laffi.entry_hardlink_w(self._entry)
            if not path:
                path = laffi.entry_hardlink(self._entry)

        if isinstance(path, bytes):
            try:
                path = path.decode(self.encoding)
            except UnicodeError:
                pass

        return path

    def size(self):
        return laffi.entry_size(self._entry) if laffi.entry_size_is_set(self._entry) else 0

    def convertToRow(self, entryCount: int, transform: Optional[Tuple[str, str]] = None) -> Tuple:
        mode = 0o555 | (stat.S_IFDIR if self.isDirectory() else stat.S_IFREG)
        linkname = self.linkname()
        if linkname:
            mode = 0o555 | stat.S_IFLNK
        else:
            linkname = ""

        path = self.path()
        path, name = SQLiteIndex.normpath(transform(path) if transform else path).rsplit("/", 1)

        # Currently, this is unused. The index only is used for getting metadata. (The data offset
        # is already determined and written out in order to possibly speed up reading of encrypted
        # files by implementing the decryption ourselves.)
        # The data offset is deprecated again! Collecting it can add a huge overhead for large zip files
        # because we have to seek to every position and read a few bytes from it. Furthermore, it is useless
        # by itself anyway. We don't even store yet how the data is compressed or encrypted, so we would
        # have to read the local header again anyway!
        dataOffset = 0

        # The header offset needs to be set to some unique value in order to account for entries with identical paths.
        # We probably are not able to query the actual offset in the TAR via libarchive.
        # This makes the created index incompatible with the SQLiteIndexedTar backend.
        # For that reason more consistency checks were added as well as the backendName key in the index metadata.
        headerOffset = entryCount

        # fmt: off
        fileInfo : Tuple = (
            path          ,  # 0  : path
            name          ,  # 1  : file name
            headerOffset  ,  # 2  : header offset
            dataOffset    ,  # 3  : data offset
            self.size()   ,  # 4  : file size
            self.getTime(),  # 5  : modification time
            mode          ,  # 6  : file mode / permissions
            0             ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            linkname      ,  # 8  : linkname
            0             ,  # 9  : user ID
            0             ,  # 10 : group ID
            False         ,  # 11 : is TAR (unused?)
            False         ,  # 12 : is sparse
        )
        # fmt: on

        return fileInfo


class IterableArchive:
    """A class that opens an archive with libarchive and can iterate over the entries."""

    def __init__(self, fileDescriptorOrPath: Union[str, int], encoding='utf-8'):
        self.fileDescriptorOrPath = fileDescriptorOrPath
        self.encoding = encoding
        self.password = None

        self._archive = laffi.read_new()

        try:
            if self.password is not None:
                laffi.read_add_password(self._archive, self.password)
        except AttributeError:
            raise NotImplementedError(
                f"This libarchive library version ({laffi.version_number()}) at "
                f"{laffi.libarchive_path}) does not support encryption!"
            )

        laffi.get_read_filter_function('all')(self._archive)
        laffi.get_read_format_function('all')(self._archive)

        fdOrPath = self.fileDescriptorOrPath
        if isinstance(fdOrPath, str):
            laffi.read_open_filename_w(self._archive, fdOrPath, os.stat(fdOrPath).st_blksize)
        elif isinstance(fdOrPath, int):
            laffi.read_open_fd(self._archive, fdOrPath, os.fstat(fdOrPath).st_blksize)
        else:
            # TODO add support for file objects using:
            # ffi( 'read_open',
            #     [c_archive_p, c_void_p, OPEN_CALLBACK, READ_CALLBACK, CLOSE_CALLBACK],
            #     c_int, check_int)
            raise ValueError("Libarchive backend currently only works with file path or descriptor.")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def __del__(self):
        laffi.read_free(self._archive)

    def __iter__(self):
        while True:
            entry = ArchiveEntry(self._archive, self.encoding)
            if laffi.read_next_header2(self._archive, entry._entry) == laffi.ARCHIVE_EOF:
                return
            yield entry


class LibarchiveFile(io.RawIOBase):
    def __init__(self, fileDescriptorOrPath, entryOffset, fileSize):
        io.RawIOBase.__init__(self)
        self.fileDescriptorOrPath = fileDescriptorOrPath
        self.fileSize = fileSize
        self.entryOffset = entryOffset
        self.archive = None
        self.entry = None
        self.fileobj = None
        self.reopen()

    def reopen(self):
        if self.fileobj:
            self.fileobj.close()
            self.fileobj = None

        self.archive = IterableArchive(self.fileDescriptorOrPath)
        self.entry = None
        entryCount = 0
        for entry in self.archive:
            if entryCount == self.entryOffset:
                print("Open entry:", entry.path())
                # self.entry = entry  # TODO implement reopenable, seek-back variant for large files
                buffer = create_string_buffer(self.fileSize)
                readSize = laffi.read_data(self.archive._archive, buffer, self.fileSize)
                if readSize != self.fileSize:
                    raise RuntimeError(
                        f"Read {readSize} bytes but expected {self.fileSize} for entry {self.entryOffset}!"
                    )
                self.fileobj = io.BytesIO(buffer)
                break
            entryCount += 1

        if self.entry is None and self.fileobj is None:
            raise ValueError(f"Failed to find archive entry {self.entryOffset}.")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        if self.fileobj:
            self.fileobj.close()
            self.fileobj = None
        if self.entry:
            del self.entry
            self.entry = None
        if self.archive:
            del self.archive
            self.archive = None

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        return self.fileobj.read(size)

    def _skip(self, size: int) -> None:
        BLKSIZE = 128 * 1024
        while size > 0:
            data = self.fileobj.read(min(size, BLKSIZE))
            if not data:
                break
            size -= len(data)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        here = self.tell()
        if whence == io.SEEK_CUR:
            offset += here
        elif whence == io.SEEK_END:
            offset += self.fileSize

        if offset >= here:
            self._skip(offset - here)
            return self.tell()

        self.fileobj.close()
        self.fileobj = self.reopen()
        self._skip(offset)
        return self.tell()

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.fileobj.tell()


# The implementation is similar to ZipMountSource and SQLiteIndexedTarUserData.
class LibarchiveMountSource(MountSource):
    def __init__(
        self,
        # fmt: off
        fileDescriptorOrPath   : Union[str, int],
        writeIndex             : bool                      = False,
        clearIndexCache        : bool                      = False,
        indexFilePath          : Optional[str]             = None,
        indexFolders           : Optional[List[str]]       = None,
        encoding               : str                       = tarfile.ENCODING,
        verifyModificationTime : bool                      = False,
        printDebug             : int                       = 0,
        transform              : Optional[Tuple[str, str]] = None,
        indexMinimumFileCount  : int                       = 0,
        **options
        # fmt: on
    ) -> None:
        # fmt: off
        self.archiveFilePath        = fileDescriptorOrPath if isinstance(fileDescriptorOrPath, str) else None
        self.fileDescriptorOrPath   = fileDescriptorOrPath
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

        # TODO
        # self._findPassword(options.get("passwords", []))

        # Force indexes in memory because:
        #  - They are incompatible with SQLiteIndexedTar and relevant consistency checks do not exist in older versions.
        #    Older versions would simply show the wrong folder hierarchy and return Input/Output error on file access.
        #  - Seeking to a file takes on average half as time much as creating the index. I.e., the overhead for
        #    creating the index feels relatively insignificant assuming that more than 2 files are accessed.
        indexFilePath = ':memory:'
        self.index = SQLiteIndex(
            indexFilePath,
            indexFolders=indexFolders,
            archiveFilePath=self.archiveFilePath,
            encoding=self.encoding,
            checkMetadata=self._checkMetadata,
            printDebug=self.printDebug,
            indexMinimumFileCount=indexMinimumFileCount,
            backendName='LibarchiveMountSource',
        )

        if clearIndexCache:
            self.index.clearIndexes()

        isFileObject = False  # Not supported yet

        self.index.openExisting()
        if self.index.indexIsLoaded():
            metadata = dict(self.index.getConnection().execute('SELECT * FROM metadata;'))
            if 'backend' not in metadata or metadata['backend'] != 'libarchive':
                self.__exit__(None, None, None)
                raise InvalidIndexError("The found index was not created by the libarchive backend.")

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
            if self.index.indexIsLoaded():
                self._storeMetadata()
                self.index.reloadIndexReadOnly()

    def _createIndex(self) -> None:
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensureIntermediaryTables()

        fileInfos = []
        entryCount = 0  # consecutive number to identify entries even if they have the same name
        with IterableArchive(self.fileDescriptorOrPath) as archive:
            for entry in archive:
                fileInfos.append(entry.convertToRow(entryCount, self.transform))
                print(fileInfos[-1])
                entryCount += 1
            if len(fileInfos) > 1000:
                self.index.setFileInfos(fileInfos)
                fileInfos = []
        if fileInfos:
            self.index.setFileInfos(fileInfos)

        # Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if self.printDebug >= 2:
            print("Resorting files by path ...")

        self.index.finalize()

        t1 = timer()
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} took {t1 - t0:.2f}s")

    def _storeMetadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.storeMetadata(argumentsMetadata, self.archiveFilePath)
        self.index.storeMetadataKeyValue('backend', 'libarchive')

    def __enter__(self):
        return self

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.index.close()
        self.fileObject.close()

    def __del__(self):
        # TODO check that all objects are really closed to avoid memory leaks
        pass

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
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)
        return LibarchiveFile(self.fileDescriptorOrPath, tarFileInfo.offsetheader, fileInfo.size)

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
            argumentsToCheck = ['encoding', 'transformPattern']
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
