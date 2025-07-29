import contextlib
import datetime
import json
import logging
import re
import stat
import sys
import tarfile
import zipfile
from timeit import default_timer as timer
from typing import IO, Any, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from ratarmountcore.utils import overrides

try:
    # Importing this patches the zipfile module as a "side" effect!
    import fast_zip_decryption  # pylint: disable=unused-import  # noqa: F401
except (ImportError, Exception):
    with contextlib.suppress(ImportError, Exception):
        import fastzipfile  # pylint: disable=unused-import  # noqa: F401


logger = logging.getLogger(__name__)


class ZipMountSource(SQLiteIndexMountSource):
    # fmt: off
    def __init__(
        self,
        fileOrPath             : Union[str, IO[bytes]],
        writeIndex             : bool                      = False,
        clearIndexCache        : bool                      = False,
        indexFilePath          : Optional[str]             = None,
        indexFolders           : Optional[list[str]]       = None,
        encoding               : str                       = tarfile.ENCODING,
        verifyModificationTime : bool                      = False,
        indexMinimumFileCount  : int                       = 1000,
        transform              : Optional[tuple[str, str]] = None,
        **options
    ) -> None:
        # fmt: on
        if 'zipfile' not in sys.modules:
            raise RuntimeError("Did not find the zipfile module. Please use Python 3.7+.")

        # fmt: off
        self.fileObject             = zipfile.ZipFile(fileOrPath, 'r')
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.options                = options
        self.transformPattern       = transform
        # fmt: on

        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )

        ZipMountSource._find_password(self.fileObject, options.get("passwords", []))
        self.files = {info.header_offset: info for info in self.fileObject.infolist()}

        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='ZipMountSource',
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._check_metadata,
        )

        isFileObject = not isinstance(fileOrPath, str)

        if self.index.index_is_loaded():
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

    def _store_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.store_metadata(argumentsMetadata, self.archiveFilePath)

    def _convert_to_row(self, info: "zipfile.ZipInfo") -> tuple:
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
        mode = (info.external_attr >> 16) & 0o777
        if stat.S_ISLNK(info.external_attr >> 16):
            linkname = self.fileObject.read(info).decode()
            mode = mode | stat.S_IFLNK
        else:
            mode = mode | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)

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
        fileInfo : tuple = (
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
            False             ,  # 13 : is generated (parent folder)
            0                 ,  # 14 : recursion depth
        )
        # fmt: on

        return fileInfo

    def _create_index(self) -> None:
        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensure_intermediary_tables()
        self.index.set_file_infos([self._convert_to_row(info) for info in self.fileObject.infolist()])
        self.index.finalize()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} took {timer() - t0:.2f}s")

    @staticmethod
    def _find_password(fileobj: "zipfile.ZipFile", passwords):
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

        for password in [None, *passwords]:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except Exception:
                pass

        raise RuntimeError("Could not find a matching password!")

    @overrides(SQLiteIndexMountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        super().__exit__(exception_type, exception_value, exception_traceback)
        self.fileObject.close()

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # I do not see any obvious option to zipfile.ZipFile to apply the specified buffer size.
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

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self.index.try_to_open_first_file(lambda path: self.open(self.lookup(path)))
