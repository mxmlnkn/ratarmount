import io
import json
import logging
import stat
import sys
import tarfile
from timeit import default_timer as timer
from typing import IO, Any, Optional, Union

from ratarmountcore.formats import FileFormatID, replace_format_check
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import overrides

try:
    import py7zr

    if not py7zr.__version__.startswith("1.0"):
        raise ImportError("Wrong version found!")

    from py7zr.io import Py7zIO, WriterFactory

    class CachedFile(Py7zIO):
        def __init__(self, offset: int = 0, maxSize: int = sys.maxsize):
            self.file = io.BytesIO()
            self._offset = offset
            self._maxSize = maxSize
            self._position = 0

        @overrides(Py7zIO)
        def write(self, s: Union[bytes, bytearray]) -> int:
            toSkip = self._offset - self._position
            self._position += len(s)
            if toSkip < len(s) and self.file.tell() < self._maxSize:
                toRead = self._maxSize - self.file.tell()
                self.file.write(s[toSkip : toSkip + toRead])
            return self._position

        @overrides(Py7zIO)
        def read(self, size: Optional[int] = None) -> bytes:
            raise NotImplementedError

        @overrides(Py7zIO)
        def flush(self) -> None:
            pass

        @overrides(Py7zIO)
        def seek(self, offset: int, whence: int = 0) -> int:  # pylint: disable=unused-argument
            return offset

        @overrides(Py7zIO)
        def size(self) -> int:
            return self.file.tell()

    class CachedFileFactory(WriterFactory):
        # This factory only seems so useless because we are only extracting single files.
        # It makes sense when extracting multiple files.
        def __init__(self, offset: int = 0, maxSize: int = sys.maxsize):
            self._offset = offset
            self._maxSize = maxSize
            self.products: dict[str, CachedFile] = {}

        def create(self, filename) -> Py7zIO:
            product = CachedFile(self._offset, self._maxSize)
            self.products[filename] = product
            return product

    def open_in_memory(archive, target: str, offset: int = 0, maxSize: int = sys.maxsize) -> IO[bytes]:
        factory = CachedFileFactory(offset=offset, maxSize=maxSize)
        archive.reset()
        archive.extract(targets=[target], factory=factory)
        file = factory.products[target].file
        file.seek(0)
        return file

    # https://github.com/miurahr/py7zr/issues/659#issuecomment-2954260661
    replace_format_check(FileFormatID.SEVEN_ZIP, py7zr.is_7zfile)  # type: ignore

except ImportError:
    py7zr = None  # type: ignore


logger = logging.getLogger(__name__)


class Py7zrMountSource(SQLiteIndexMountSource):
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
        **options
    ) -> None:
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.options                = options
        # fmt: on

        # TODO For now, 'transform' is not supported because we need the exact path to open the file and there
        #      currently is no SQLite table column to store this information in.
        # TODO I doubt that symbolic links work because py7zr.FileInfo does not have information regarding links.

        def open_file(password: Optional[Union[str, bytes]] = None):
            return py7zr.SevenZipFile(
                # https://github.com/miurahr/py7zr/issues/659#issuecomment-2954260661
                fileOrPath,  # type: ignore
                password=None if password is None else (password if isinstance(password, str) else password.decode()),
            )

        self.fileObject = Py7zrMountSource._find_password(open_file, options.get("passwords", []))

        # Force indexes in memory because:
        #  - I have no idea what ID to write into offset or offsetheader. The "ID" for the py7zr interface
        #    is the "filename" (path). Storing a string in the int 'offset' column is not a got idea.
        #  - The py7zr interface seems to lack a way to query information about symbolic links.
        indexFilePath = ':memory:'
        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='Py7zrMountSource',
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
        argumentsToSave = ['encoding']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.store_metadata(argumentsMetadata, self.archiveFilePath)

    def _convert_to_row(self, info) -> tuple:
        mode = 0o777 | (stat.S_IFDIR if info.is_directory else stat.S_IFREG)
        mtime = info.creationtime.timestamp()
        path, name = SQLiteIndex.normpath(info.filename).rsplit("/", 1)

        # fmt: off
        fileInfo : tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            0                 ,  # 2  : header offset
            0                 ,  # 3  : data offset
            info.uncompressed ,  # 4  : file size
            mtime             ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            # Abuse the linkname to store the "ID" for file access.
            info.filename     ,  # 8  : linkname
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
        self.index.set_file_infos([self._convert_to_row(info) for info in self.fileObject.list()])
        self.index.finalize()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} took {timer() - t0:.2f}s")

    @staticmethod
    def _find_password(openFile, passwords):
        fileobj = openFile()

        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.list()
        if not files:
            for password in passwords:
                fileobj = openFile(password)
                files = fileobj.list()
                if files:
                    return fileobj

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if not file.is_directory and file.uncompressed > 0]
        if not files:
            return fileobj

        for password in [None, *passwords]:
            fileobj = openFile(password)
            try:
                open_in_memory(fileobj, files[0].filename)
                return fileobj
            except Exception:
                pass

        raise RuntimeError("Could not find a matching password!")

    @overrides(SQLiteIndexMountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        super().__exit__(exception_type, exception_value, exception_traceback)
        self.fileObject.close()

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        return open_in_memory(self.fileObject, fileInfo.linkname)

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(json.loads(metadata['arguments']), self, argumentsToCheck=['encoding'])
