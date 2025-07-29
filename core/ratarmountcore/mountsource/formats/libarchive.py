# libarchive.ffi produces many false positives on my local system and there seems to be nothing else works.
# pylint: disable=no-member

import contextlib
import ctypes
import io
import json
import logging
import os
import re
import stat
import sys
import tarfile
from collections.abc import Sequence
from timeit import default_timer as timer
from typing import IO, Any, Callable, Optional, Union, cast

from ratarmountcore.compressions import COMPRESSION_BACKENDS
from ratarmountcore.formats import FILE_FORMATS
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from ratarmountcore.utils import InvalidIndexError, overrides

try:
    # Use the FFI directly because the higher-level interface does not work sufficiently for our use case.
    import libarchive.ffi as laffi
    from libarchive.exception import ArchiveError
except (ImportError, AttributeError):
    pass

try:
    import py7zr  # pylint: disable=unused-import
except ImportError:
    py7zr = None  # type: ignore


logger = logging.getLogger(__name__)


class ArchiveEntry:
    def __init__(self, archive, entry_index: int, encoding: str = 'utf-8'):
        self._archive = archive  # Store for lifetime
        self._entry = laffi.entry_new()
        self.eof = laffi.read_next_header2(self._archive, self._entry) == laffi.ARCHIVE_EOF
        self.encoding = encoding
        self.entry_index = entry_index
        self._fileInfoRow: Optional[tuple] = None

    def __del__(self):
        laffi.entry_free(self._entry)

    def format_name(self):
        return laffi.format_name(self._archive)

    def get_time_by_name(self, name):
        if not getattr(laffi, f'entry_{name}_is_set')(self._entry):
            return None
        seconds = getattr(laffi, f'entry_{name}')(self._entry)
        nseconds = getattr(laffi, f'entry_{name}_nsec')(self._entry)
        return float(seconds) + float(nseconds) / 1e9 if nseconds else int(seconds)

    def get_time(self):
        result = self.get_time_by_name('mtime')
        if result is None:
            result = self.get_time_by_name('ctime')
        if result is None:
            result = self.get_time_by_name('birthtime')
        return result or 0

    def path(self) -> str:
        path = laffi.entry_pathname_w(self._entry)
        if not path:
            path = laffi.entry_pathname(self._entry)
            if path is not None:
                with contextlib.suppress(UnicodeError):
                    path = path.decode(self.encoding)
        return path

    def mode(self):
        return laffi.entry_mode(self._entry)

    def filetype(self):
        return laffi.entry_filetype(self._entry)

    def is_directory(self):
        return self.filetype() & 0o170000 == 0o040000

    def is_symbolic_link(self):
        return self.filetype() & 0o170000 == 0o120000

    def linkname(self):
        path = ""
        if self.is_symbolic_link():
            path = laffi.entry_symlink_w(self._entry)
            if not path:
                path = laffi.entry_symlink(self._entry)
        else:
            path = laffi.entry_hardlink_w(self._entry)
            if not path:
                path = laffi.entry_hardlink(self._entry)

        if isinstance(path, bytes):
            with contextlib.suppress(UnicodeError):
                path = path.decode(self.encoding)

        return path

    def convert_to_row(self, entryCount: int, transform: Callable[[str], str], path: Optional[str] = None) -> tuple:
        # The data logic may only be evaluated once because determining the size may require reading the whole file!
        if self._fileInfoRow is not None:
            return self._fileInfoRow

        if laffi.entry_size_is_set(self._entry):
            size = laffi.entry_size(self._entry)
        else:
            buffer = ctypes.create_string_buffer(1024 * 1024)
            size = 0
            while readSize := laffi.read_data(self._archive, buffer, len(buffer)):
                size += readSize

        mode = self.mode() & 0o777
        linkname = self.linkname()
        if linkname:
            mode = mode | stat.S_IFLNK
        else:
            linkname = ""
            mode = mode | (stat.S_IFDIR if self.is_directory() else stat.S_IFREG)

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
            self.get_time(),  # 5  : modification time
            mode          ,  # 6  : file mode / permissions
            0             ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            linkname      ,  # 8  : linkname
            0             ,  # 9  : user ID
            0             ,  # 10 : group ID
            False         ,  # 11 : is TAR (unused?)
            False         ,  # 12 : is sparse
            False         ,  # 13 : is generated (parent folder)
            0             ,  # 14 : recursion depth
        )
        # fmt: on

        return self._fileInfoRow


class IterableArchive:
    """A class that opens an archive with libarchive and can iterate over the entries."""

    # Read formats list supported by python-libarchive-c:
    #   '7zip', 'all', 'ar', 'cab', 'cpio', 'empty', 'iso9660', 'lha', 'mtree',
    #   'rar', 'raw', 'tar', 'xar', 'zip', 'warc'
    # We especially do not want mtree because it has no magic bytes and basically matches anything with newlines,
    # including some test text files. 'raw', 'empty', and 'all' are special input formats.
    # It seems that 'raw' cannot be combined in an or-manner. It disables all archive format recognitions and only
    # applies the filters.
    # https://github.com/libarchive/libarchive/wiki/FormatRaw#caveat-dont-mix-_raw_-with-other-handlers
    # > aveat: Don't mix _raw_ with other handlers
    # > If you are using the raw handler, you should generally not enable any other handler
    # Note that the lz4 and zstd filters are not available in the manylinux2014 container (CentOS Linux
    # release 7.9.2009 (Core))
    ENABLED_FORMATS = ('7zip', 'ar', 'cab', 'cpio', 'iso9660', 'lha', 'rar', 'rar5', 'tar', 'xar', 'warc', 'zip')

    def __init__(
        self,
        file: Union[str, IO[bytes]],
        encoding='utf-8',
        passwords: Optional[Sequence[Union[str, bytes]]] = None,
        bufferSize=1024 * 1024,
    ):
        self.encoding = encoding
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
            self._set_passwords(passwords or [])
            self._try_to_open(allowArchives=True)
        except ArchiveError as exception:
            logger.info("Was not able to open %s as given archive. Try to undo compressions next.", self._file)
            logger.info("Exception: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG))
            try:
                laffi.read_free(self._archive)
                self._archive = laffi.read_new()

                self._set_passwords(passwords or [])
                self._try_to_open(allowArchives=False)
            except ArchiveError as exception2:
                raise exception2 from exception

    def format_name(self):
        return laffi.format_name(self._archive)

    def filter_names(self):
        allNames = [laffi.filter_name(self._archive, i) for i in range(laffi.filter_count(self._archive))]
        return [name for name in allNames if name != b'none']

    def _try_to_open(self, allowArchives: bool):
        laffi.get_read_filter_function('all')(self._archive)

        for formatToEnable in IterableArchive.ENABLED_FORMATS if allowArchives else ['raw']:
            try:
                laffi.get_read_format_function(formatToEnable)(self._archive)
            except ValueError as exception:
                # Ignore exceptions from formats that are not supported such as "rar5" in manylinux2014.
                logger.debug("Failed to enable format %s because of: %s", formatToEnable, exception, exc_info=True)

        if isinstance(self._file, str):
            laffi.read_open_filename_w(self._archive, self._file, os.stat(self._file).st_blksize)
        elif isinstance(self._file, int):
            laffi.read_open_fd(self._archive, self._file, os.fstat(self._file).st_blksize)
        elif hasattr(self._file, 'readinto'):
            self._open_with_file_object()
        else:
            raise ValueError(
                "Libarchive backend currently only works with file path, descriptor, or a file object, "
                f"but got: {self._file}"
            )

        if not allowArchives and not self.filter_names():
            raise ArchiveError("When not looking for archives, there must be at least one filter!")

    def _set_passwords(self, passwords: Sequence[Union[str, bytes]]):
        try:
            for password in passwords:
                laffi.read_add_passphrase(self._archive, password.encode() if isinstance(password, str) else password)
        except AttributeError as exception:
            raise NotImplementedError(
                f"This libarchive library version ({laffi.version_number()}) at "
                f"{laffi.libarchive_path}) does not support encryption!"
            ) from exception

    def _open_with_file_object(self):
        if not hasattr(self._file, 'readinto'):
            return
        self._buffer = ctypes.create_string_buffer(self.bufferSize)

        def read_from_file_object(_archive, _context, pointerToBufferPointer):
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

            def seek_file_object(_archive, _context, offset, whence):
                self._file.seek(offset, whence)
                return self._file.tell()

            self._file.seek(0)
            self._seekCallback = laffi.SEEK_CALLBACK(seek_file_object)
            # Needs to be assigned to self, or else, ctypes lifetime issues result in a segfault!
            laffi.read_set_seek_callback(self._archive, self._seekCallback)

        # Needs to be assigned to self, or else, ctypes lifetime issues result in a segfault!
        self._readCallback = laffi.READ_CALLBACK(read_from_file_object)
        laffi.read_open(self._archive, None, laffi.NO_OPEN_CB, self._readCallback, laffi.NO_CLOSE_CB)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def __del__(self):
        laffi.read_free(self._archive)

    def next_entry(self) -> Optional[ArchiveEntry]:
        if self._eof:
            return None

        self._entry = ArchiveEntry(self._archive, entry_index=self._entryIndex, encoding=self.encoding)
        if self._entryIndex == 0 and logger.isEnabledFor(logging.INFO):
            format_name = laffi.format_name(self._archive)
            if isinstance(format_name, bytes):
                format_name = format_name.decode()
            # We need to try and read the first entry before format_name returns anything other than 'none'.
            logger.info(
                "Successfully opened type '%s' with libarchive. Using filters: %s", format_name, self.filter_names()
            )

        self._entryIndex += 1
        if self._entry.eof:
            self._eof = True
            return None

        return self._entry

    def entry_index(self):
        return self._entryIndex

    def read_data(self, buffer, size):
        return laffi.read_data(self._archive, buffer, size)


class IterableArchiveCache:
    def __init__(self, cacheSize: int = 5):
        self.cacheSize = cacheSize
        self._cache: list[IterableArchive] = []

    def insert(self, archive: IterableArchive):
        self._cache.append(archive)
        while len(self._cache) > self.cacheSize:
            self._cache.pop(0)

    def take(self, entry_index: int) -> Optional[IterableArchive]:
        qualified = [i for i in range(len(self._cache)) if self._cache[i].entry_index() < entry_index]
        if not qualified:
            return None
        # Find the archive with the closest (highest) index not larger or equal than the requested one
        result = max(qualified, key=lambda i: self._cache[i].entry_index())
        return self._cache.pop(result)


class LibarchiveFile(io.RawIOBase):
    def __init__(
        self,
        file,
        entry_index,
        fileSize,
        passwords: Optional[Sequence[str]] = None,
        archiveCache: Optional[IterableArchiveCache] = None,
    ):
        io.RawIOBase.__init__(self)
        self.file = file
        self.fileSize = fileSize
        self.entry_index = entry_index
        self.passwords = passwords
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

        self._archive = self._archiveCache.take(self.entry_index)
        if self._archive is None:
            self._archive = IterableArchive(self.file, passwords=self.passwords)

        while True:
            self._entry = self._archive.next_entry()
            if self._entry is None:
                break
            if self._entry.entry_index == self.entry_index:
                self._refill_buffer()
                break

        if self._entry is None and self._bufferIO is None:
            raise ValueError(f"Failed to find archive entry {self.entry_index}.")

    def _refill_buffer(self):
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

        readSize = self._archive.read_data(self._buffer, sizeToRead)
        if readSize != sizeToRead:
            raise RuntimeError(f"Read {readSize} bytes but expected {self.fileSize} for entry {self.entry_index}!")
        self._bufferIO = io.BytesIO(self._buffer)

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
        raise io.UnsupportedOperation

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
        self._refill_buffer()
        return self._bufferIO.read(size)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        result = bytearray()
        while size < 0 or len(result) < size:
            read_data = self.read1(size if size < 0 else size - len(result))
            if not read_data:
                break
            result.extend(read_data)
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
class LibarchiveMountSource(SQLiteIndexMountSource):
    # fmt: off
    def __init__(
        self,
        fileOrPath             : Union[str, IO[bytes]],
        writeIndex             : bool                      = False,
        clearIndexCache        : bool                      = False,
        indexFilePath          : Optional[str]             = None,
        indexFolders           : Optional[Sequence[str]]   = None,
        encoding               : str                       = tarfile.ENCODING,
        verifyModificationTime : bool                      = False,
        transform              : Optional[tuple[str, str]] = None,
        indexMinimumFileCount  : int                       = 0,
        tarFileName            : Optional[str]             = None,
        **options
    ) -> None:
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.fileOrPath             = fileOrPath
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
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
        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='LibarchiveMountSource',
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._check_metadata,
        )

        isFileObject = False  # Not supported yet

        if self.index.index_is_loaded():
            metadata = dict(self.index.get_connection().execute('SELECT * FROM metadata;'))
            if 'backend' not in metadata or metadata['backend'] != 'libarchive':
                self.__exit__(None, None, None)
                raise InvalidIndexError("The found index was not created by the libarchive backend.")

            self.index.reload_index_read_only()
        else:
            # Open new database when we didn't find an existing one.
            # Simply open in memory without an error even if writeIndex is True but when not indication
            # for a index file location has been given.
            if writeIndex and (indexFilePath or not isFileObject):
                self.index.open_writable()
            else:
                self.index.open_in_memory()

            self._create_index()
            if self.index.index_is_loaded():
                self._store_metadata()
                self.index.reload_index_read_only()

    def _create_index(self) -> None:
        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensure_intermediary_tables()

        triedToOpen = False
        fileInfos = []
        gotAnyEntry = False
        with IterableArchive(self.fileOrPath, passwords=self.passwords) as archive:
            while True:
                entry = archive.next_entry()
                if entry is None:
                    break

                entryPath = None
                if entry.entry_index == 0 and self.tarFileName and entry.format_name() == b'raw':
                    libarchiveSuffixes = [
                        s for fid in COMPRESSION_BACKENDS['libarchive'].formats for s in FILE_FORMATS[fid].extensions
                    ]
                    fname = os.path.basename(self.tarFileName)
                    for suffix in ['gz', 'bz2', 'bzip2', 'gzip', 'xz', 'zst', 'zstd', *libarchiveSuffixes]:
                        suffix = '.' + suffix
                        if fname.lower().endswith(suffix.lower()) and len(fname) > len(suffix):
                            fname = fname[: -len(suffix)]
                            break
                    entryPath = fname

                gotAnyEntry = True
                fileInfos.append(entry.convert_to_row(entry.entry_index, self.transform, path=entryPath))
                # Contains file info SQLite row tuples! 4 -> size, 6 -> mode
                if not triedToOpen and not stat.S_ISDIR(fileInfos[-1][6]) and fileInfos[-1][4] > 0:
                    bufferSize = 1
                    buffer = ctypes.create_string_buffer(bufferSize)
                    try:
                        archive.read_data(buffer, 1)
                    except ArchiveError as exception:
                        # Very special case to delegate to py7zr somewhat smartly for encrypted 7z archives.
                        if entry.format_name() == b'7-Zip' and self.passwords and "py7zr" in sys.modules:
                            raise exception
                        if 'encrypt' in str(exception).lower():
                            logger.warning("The file contents are encrypted but not the file hierarchy!")
                            logger.warning("Specify a password with --password to also view file contents!")
                    triedToOpen = True

                if len(fileInfos) > 1000:
                    self.index.set_file_infos(fileInfos)
                    fileInfos = []

            if fileInfos:
                self.index.set_file_infos(fileInfos)
            elif not gotAnyEntry and archive.format_name() == b'tar':
                raise ArchiveError("Supposedly detected a TAR with no entries in it. Rejecting it as unknown format!")

        self.index.finalize()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} took {timer() - t0:.2f}s")

    def _store_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.store_metadata(argumentsMetadata, self.archiveFilePath)
        self.index.store_metadata_key_value('backend', 'libarchive')

    def __del__(self):
        # TODO check that all objects are really closed to avoid memory leaks
        pass

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        assert fileInfo.userdata
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)
        return cast(
            IO[bytes],
            LibarchiveFile(
                self.fileOrPath,
                tarFileInfo.offsetheader,
                fileSize=fileInfo.size,
                passwords=self.passwords,
                archiveCache=self._archiveCache,
            ),
        )

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )
