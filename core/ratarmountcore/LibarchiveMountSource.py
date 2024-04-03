#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# libarchive.ffi produces many false positives on my local system and there seems to be nothing else works.
# pylint: disable=no-member

import ctypes
import io
import json
import os
import re
import stat
import tarfile

from timeit import default_timer as timer
from typing import Any, Callable, Dict, IO, Iterable, List, Optional, Tuple, Union

from .compressions import LIBARCHIVE_FILTER_FORMATS
from .MountSource import FileInfo, MountSource
from .SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from .utils import InvalidIndexError, overrides

try:
    # Use the FFI directly because the higher-level interface does not work sufficiently for our use case.
    import libarchive.ffi as laffi
    from libarchive.exception import ArchiveError
except ImportError:
    pass


class ArchiveEntry:
    def __init__(self, archive, entryIndex: int, encoding: str = 'utf-8'):
        self._archive = archive  # Store for lifetime
        self._entry = laffi.entry_new()
        self.eof = laffi.read_next_header2(self._archive, self._entry) == laffi.ARCHIVE_EOF
        self.encoding = encoding
        self.entryIndex = entryIndex
        self._fileInfoRow: Optional[Tuple] = None

        if self.eof:
            return

    def __del__(self):
        laffi.entry_free(self._entry)

    def formatName(self):
        return laffi.format_name(self._archive)

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

    def path(self) -> str:
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

    def convertToRow(self, entryCount: int, transform: Callable[[str], str], path: Optional[str] = None) -> Tuple:
        # The data logic may only be evaluated once because determining the size may require reading the whole file!
        if self._fileInfoRow is not None:
            return self._fileInfoRow

        if laffi.entry_size_is_set(self._entry):
            size = laffi.entry_size(self._entry)
        else:
            buffer = ctypes.create_string_buffer(1024 * 1024)
            size = 0
            while True:
                readSize = laffi.read_data(self._archive, buffer, len(buffer))
                if not readSize:
                    break
                size += readSize

        mode = 0o555 | (stat.S_IFDIR if self.isDirectory() else stat.S_IFREG)
        linkname = self.linkname()
        if linkname:
            mode = 0o555 | stat.S_IFLNK
        else:
            linkname = ""

        if not path:
            path = self.path()
        path, name = SQLiteIndex.normpath(transform(path)).rsplit("/", 1)

        # Currently, this is unused. Only the headerOffset should be used.
        dataOffset = 0

        # The header offset needs to be set to some unique value in order to account for entries with identical paths.
        # We probably are not able to query the actual offset in the TAR via libarchive.
        # This makes the created index incompatible with the SQLiteIndexedTar backend.
        # For that reason more consistency checks were added as well as the backendName key in the index metadata.
        headerOffset = entryCount

        # fmt: off
        self._fileInfoRow = (
            path          ,  # 0  : path
            name          ,  # 1  : file name
            headerOffset  ,  # 2  : header offset
            dataOffset    ,  # 3  : data offset
            size          ,  # 4  : file size
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

        return self._fileInfoRow


class IterableArchive:
    """A class that opens an archive with libarchive and can iterate over the entries."""

    # Read formats list supported by python-libarchive-c:
    #   '7zip', 'all', 'ar', 'cab', 'cpio', 'empty', 'iso9660', 'lha', 'mtree',
    #   'rar', 'raw', 'tar', 'xar', 'zip', 'warc'
    # We especially do not want mtree because it has not magic bytes and basically matches anything with newlines,
    # including some test text files. 'raw', 'empty', and 'all' are special input formats.
    # It seems that 'raw' cannot be combined in an or-manner. It disables all archive format recognitions and only
    # applies the filters.
    # https://github.com/libarchive/libarchive/wiki/FormatRaw#caveat-dont-mix-_raw_-with-other-handlers
    # > aveat: Don't mix _raw_ with other handlers
    # > If you are using the raw handler, you should generally not enable any other handler
    ENABLED_FORMATS = ['7zip', 'ar', 'cab', 'cpio', 'iso9660', 'lha', 'rar', 'rar5', 'tar', 'xar', 'warc', 'zip']

    def __init__(
        self,
        file: Union[str, IO[bytes]],
        encoding='utf-8',
        passwords: Optional[List[str]] = None,
        bufferSize=1024 * 1024,
        printDebug: int = 0,
    ):
        self.encoding = encoding
        self.printDebug = printDebug
        self.bufferSize = bufferSize
        self._file = file
        self._entryIndex = 0  # A consecutive number to identify entries even if they have the same name.
        self._buffer = None
        self._readCallback = None
        self._seekCallback = None
        self._entry: Optional[ArchiveEntry] = None
        self._eof = False

        self._archive = laffi.read_new()
        try:
            self._setPasswords(passwords if passwords else [])
            self._tryToOpen(allowArchives=True)
        except ArchiveError as exception:
            if self.printDebug >= 2:
                print(f"[Info] Was not able to open {self._file} as given archive. Try to undo compressions next.")
                print(f"[Info] Exception: {exception}")
            try:
                laffi.read_free(self._archive)
                self._archive = laffi.read_new()

                self._setPasswords(passwords if passwords else [])
                self._tryToOpen(allowArchives=False)
            except ArchiveError as exception2:
                raise exception2 from exception

    def filterNames(self):
        allNames = [laffi.filter_name(self._archive, i) for i in range(laffi.filter_count(self._archive))]
        return [name for name in allNames if name != b'none']

    def _tryToOpen(self, allowArchives: bool):
        laffi.get_read_filter_function('all')(self._archive)

        for formatToEnable in IterableArchive.ENABLED_FORMATS if allowArchives else ['raw']:
            laffi.get_read_format_function(formatToEnable)(self._archive)

        if isinstance(self._file, str):
            laffi.read_open_filename_w(self._archive, self._file, os.stat(self._file).st_blksize)
        elif isinstance(self._file, int):
            laffi.read_open_fd(self._archive, self._file, os.fstat(self._file).st_blksize)
        elif hasattr(self._file, 'readinto'):
            self._openWithFileObject()
        else:
            raise ValueError(
                "Libarchive backend currently only works with file path, descriptor, or a file object, "
                f"but got: {self._file}"
            )

        if not allowArchives and not self.filterNames():
            raise ArchiveError("When not looking for archives, there must be at least one filter!")

    def _setPasswords(self, passwords: List[str]):
        try:
            for password in passwords:
                laffi.read_add_passphrase(self._archive, password.encode() if isinstance(password, str) else password)
        except AttributeError as exception:
            raise NotImplementedError(
                f"This libarchive library version ({laffi.version_number()}) at "
                f"{laffi.libarchive_path}) does not support encryption!"
            ) from exception

    def _openWithFileObject(self):
        if not hasattr(self._file, 'readinto'):
            return
        self._buffer = ctypes.create_string_buffer(self.bufferSize)

        def readFromFileObject(_archive, _context, pointerToBufferPointer):
            try:
                size = self._file.readinto(self._buffer)
            except (NotImplementedError, AttributeError):
                result = self._file.read(self.bufferSize)
                size = len(result)
                self._buffer = ctypes.c_char_p(result)
            pointerToBufferPointer = ctypes.cast(pointerToBufferPointer, ctypes.POINTER(ctypes.c_void_p))
            pointerToBufferPointer[0] = ctypes.cast(self._buffer, ctypes.c_void_p)
            return size

        if hasattr(self._file, 'seekable') and self._file.seekable() and hasattr(self._file, 'tell'):

            def seekFileObject(_archive, _context, offset, whence):
                self._file.seek(offset, whence)
                return self._file.tell()

            self._file.seek(0)
            self._seekCallback = laffi.SEEK_CALLBACK(seekFileObject)
            # Needs to be assigned to self, or else, ctypes lifetime issues result in a segfault!
            laffi.read_set_seek_callback(self._archive, self._seekCallback)

        # Needs to be assigned to self, or else, ctypes lifetime issues result in a segfault!
        self._readCallback = laffi.READ_CALLBACK(readFromFileObject)
        laffi.read_open(self._archive, None, laffi.NO_OPEN_CB, self._readCallback, laffi.NO_CLOSE_CB)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def __del__(self):
        laffi.read_free(self._archive)

    def nextEntry(self) -> Optional[ArchiveEntry]:
        if self._eof:
            return None

        self._entry = ArchiveEntry(self._archive, entryIndex=self._entryIndex, encoding=self.encoding)
        if self._entryIndex == 0 and self.printDebug >= 2:
            formatName = laffi.format_name(self._archive)
            if isinstance(formatName, bytes):
                formatName = formatName.decode()
            # We need to try and read the first entry before format_name returns anything other than 'none'.
            print(
                f"[Info] Successfully opened type '{formatName}' with libarchive. "
                f"Using filters: {self.filterNames()}"
            )

        self._entryIndex += 1
        if self._entry.eof:
            self._eof = True
            return None

        return self._entry

    def entryIndex(self):
        return self._entryIndex

    def readData(self, buffer, size):
        return laffi.read_data(self._archive, buffer, size)


class IterableArchiveCache:
    def __init__(self, cacheSize: int = 5):
        self.cacheSize = cacheSize
        self._cache: List[IterableArchive] = []

    def insert(self, archive: IterableArchive):
        self._cache.append(archive)
        while len(self._cache) > self.cacheSize:
            self._cache.pop(0)

    def take(self, entryIndex: int) -> Optional[IterableArchive]:
        qualified = [i for i in range(len(self._cache)) if self._cache[i].entryIndex() < entryIndex]
        if not qualified:
            return None
        # Find the archive with the closest (highest) index not larger or equal than the requested one
        result = max(qualified, key=lambda i: self._cache[i].entryIndex())
        return self._cache.pop(result)


class LibarchiveFile(io.RawIOBase):
    def __init__(
        self,
        file,
        entryIndex,
        fileSize,
        passwords: Optional[List[str]] = None,
        printDebug: int = 0,
        archiveCache: Optional[IterableArchiveCache] = None,
    ):
        io.RawIOBase.__init__(self)
        self.file = file
        self.fileSize = fileSize
        self.entryIndex = entryIndex
        self.passwords = passwords
        self.printDebug = printDebug
        self._archiveCache = archiveCache
        self._archive = None
        self._entry = None
        self._bufferSizeMax = 1024 * 1024
        self._buffer = None
        self._bufferOffset = 0  # Offset in self.entry from which the buffer was read
        self._bufferIO = None

        self._open()

    def _open(self):
        self.close()

        self._archive = self._archiveCache.take(self.entryIndex)
        if self._archive is None:
            self._archive = IterableArchive(self.file, passwords=self.passwords, printDebug=self.printDebug)

        while True:
            self._entry = self._archive.nextEntry()
            if self._entry is None:
                break
            if self._entry.entryIndex == self.entryIndex:
                self._refillBuffer()
                break

        if self._entry is None and self._bufferIO is None:
            raise ValueError(f"Failed to find archive entry {self.entryIndex}.")

    def _refillBuffer(self):
        bufferedSize = len(self._buffer) if self._buffer else 0
        if self._bufferOffset + bufferedSize >= self.fileSize:
            return

        self._bufferOffset += bufferedSize
        sizeToRead = min(self._bufferSizeMax, max(0, self.fileSize - self._bufferOffset))

        # Reallocate buffer if we need a different size. This is necessary because I don't see an API to
        # specify a size to io,BytesIO to make it work on a subset of the buffer without copying.
        if sizeToRead >= self._bufferSizeMax:
            if self._buffer is None or len(self._buffer) < self._bufferSizeMax:
                self._buffer = ctypes.create_string_buffer(self._bufferSizeMax)
        elif self._buffer is None or len(self._buffer) != sizeToRead:
            self._buffer = ctypes.create_string_buffer(sizeToRead)

        readSize = self._archive.readData(self._buffer, sizeToRead)
        if readSize != sizeToRead:
            raise RuntimeError(f"Read {readSize} bytes but expected {self.fileSize} for entry {self.entryIndex}!")
        self._bufferIO = io.BytesIO(self._buffer)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        if self._archiveCache and self._archive:
            self._archiveCache.insert(self._archive)
        if self._bufferIO is not None:
            self._bufferIO.close()
            self._bufferIO = None
            self._bufferOffset = 0
        self._buffer = None
        self._entry = None
        self._archive = None

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return self._bufferIO is not None and self._bufferIO.readable()

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    def read1(self, size: int = -1) -> bytes:
        if not self._bufferIO:
            raise RuntimeError("Closed file cannot be read from!")
        result = self._bufferIO.read(size)
        if result:
            return result
        self._refillBuffer()
        return self._bufferIO.read(size)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        result = bytearray()
        while size < 0 or len(result) < size:
            readData = self.read1(size if size < 0 else size - len(result))
            if not readData:
                break
            result.extend(readData)
        return bytes(result)

    def _skip(self, size: int) -> None:
        if not self._bufferIO:
            raise RuntimeError("Closed file cannot be read from!")
        while size > 0:
            data = self.read1(size)
            if not data:
                break
            size -= len(data)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if not self._bufferIO:
            raise RuntimeError("Closed file cannot be seeked!")

        here = self.tell()
        if whence == io.SEEK_CUR:
            offset += here
        elif whence == io.SEEK_END:
            offset += self.fileSize

        if offset >= here:
            self._skip(offset - here)
            return self.tell()

        bufferedSize = 0
        if self._buffer:
            bufferedSize = len(self._buffer)
        if offset >= self._bufferOffset and offset < self._bufferOffset + bufferedSize:
            self._bufferIO.seek(offset - self._bufferOffset)
        else:
            self._open()
            self._skip(max(0, offset))
        return self.tell()

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        result = self._bufferOffset
        if self._bufferIO:
            result += self._bufferIO.tell()
        return result


# The implementation is similar to ZipMountSource and SQLiteIndexedTarUserData.
class LibarchiveMountSource(MountSource):
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
        transform              : Optional[Tuple[str, str]] = None,
        indexMinimumFileCount  : int                       = 0,
        tarFileName            : Optional[str]             = None,
        **options
        # fmt: on
    ) -> None:
        # fmt: off
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.fileOrPath             = fileOrPath
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.printDebug             = printDebug
        self.options                = options
        self.transformPattern       = transform
        self.passwords              = options.get("passwords", [])
        self.tarFileName            = tarFileName
        self._archiveCache          = IterableArchiveCache()
        # fmt: on

        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )

        # Determine an archive file name to show for debug output and as file name inside the mount point for
        # simple non-TAR gzip/bzip2 stream-compressed files.
        if tarFileName:
            self.tarFileName = tarFileName
        elif isinstance(fileOrPath, str):
            self.tarFileName = fileOrPath
        else:
            self.tarFileName = '<file object>'

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
        with IterableArchive(self.fileOrPath, passwords=self.passwords, printDebug=self.printDebug) as archive:
            while True:
                entry = archive.nextEntry()
                if entry is None:
                    break

                entryPath = None
                if entry.entryIndex == 0 and self.tarFileName and entry.formatName() == b'raw':
                    libarchiveSuffixes = [s for _, info in LIBARCHIVE_FILTER_FORMATS.items() for s in info.suffixes]
                    fname = os.path.basename(self.tarFileName)
                    for suffix in ['gz', 'bz2', 'bzip2', 'gzip', 'xz', 'zst', 'zstd'] + libarchiveSuffixes:
                        suffix = '.' + suffix
                        if fname.lower().endswith(suffix.lower()) and len(fname) > len(suffix):
                            fname = fname[: -len(suffix)]
                            break
                    entryPath = fname

                fileInfos.append(entry.convertToRow(entry.entryIndex, self.transform, path=entryPath))
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
    def open(self, fileInfo: FileInfo):
        assert fileInfo.userdata
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)
        return LibarchiveFile(
            self.fileOrPath,
            tarFileInfo.offsetheader,
            fileSize=fileInfo.size,
            passwords=self.passwords,
            printDebug=self.printDebug,
            archiveCache=self._archiveCache,
        )

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
