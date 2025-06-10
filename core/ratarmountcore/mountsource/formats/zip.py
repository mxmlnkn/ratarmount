import contextlib
import datetime
import json
import re
import stat
import sys
import tarfile
import zipfile
from timeit import default_timer as timer
from typing import IO, Any, Dict, List, Optional, Tuple, Union

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


class ZipMountSource(SQLiteIndexMountSource):
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
        transform              : TransformPatterns         = None,
        **options
        # fmt: on
    ) -> None:
        if 'zipfile' not in sys.modules:
            raise RuntimeError("Did not find the zipfile module. Please use Python 3.7+.")

        # Disable buffering for self.rawFileObject because a buffer size of 4 MiB based on the block size is used on
        # Lustre and this leads to bad performance for random-like accesses / accesses to small files. Note that
        # buffering is not that important because zipfile is optimized to use large reads, e.g.,:
        #  - The central directory is read with a single read call:
        #    https://github.com/python/cpython/blob/b2afe2aae487ebf89897e22c01d9095944fd334f/Lib/zipfile/__init__.py#L1472
        #  - Opening a file is pretty ok, although it could be better. It does separate reads for:
        #    - The local file header:
        #      https://github.com/python/cpython/blob/b2afe2aae487ebf89897e22c01d9095944fd334f/Lib/zipfile/__init__.py#L1651
        #    - The file name field
        #      https://github.com/python/cpython/blob/b2afe2aae487ebf89897e22c01d9095944fd334f/Lib/zipfile/__init__.py#L1658C21-L1658C29
        #    - Then seeks over the extra field and creates the ZipExtFile object.
        #    - ZipExtFile only does an additional 12 B read for encrypted files, else it will behave like an
        #      unbuffered filer reader from here on out but with a minimum read size of 4096 B:
        #      https://github.com/python/cpython/blob/b2afe2aae487ebf89897e22c01d9095944fd334f/Lib/zipfile/__init__.py#L871
        # fmt: off
        self.rawFileObject          = open(fileOrPath, 'rb', buffering=0) if isinstance(fileOrPath, str) else fileOrPath
        self.fileObject             = zipfile.ZipFile(self.rawFileObject, 'r')
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.printDebug             = printDebug
        self.options                = options
        # fmt: on

        ZipMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = {info.header_offset: info for info in self.fileObject.infolist()}

        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                printDebug=self.printDebug,
                indexMinimumFileCount=indexMinimumFileCount,
                transform=transform,
                backendName='ZipMountSource',
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._checkMetadata,
        )

        isFileObject = not isinstance(fileOrPath, str)

        if self.index.indexIsLoaded():
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

    def _storeMetadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self.index, argument) for argument in argumentsToSave})
        self.index.storeMetadata(argumentsMetadata, self.archiveFilePath)

    def _convertToRow(self, info: "zipfile.ZipInfo") -> Tuple:
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

        path, name = SQLiteIndex.normpath(self.index.transformPath(info.filename)).rsplit("/", 1)

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
            False             ,  # 13 : is generated (parent folder)
            0                 ,  # 14 : recursion depth
        )
        # fmt: on

        return fileInfo

    def _createIndex(self) -> None:
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensureIntermediaryTables()
        self.index.setFileInfos([self._convertToRow(info) for info in self.fileObject.infolist()])

        # Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if self.printDebug >= 2:
            print("Resorting files by path ...")

        self.index.finalize()

        t1 = timer()
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.archiveFilePath} took {t1 - t0:.2f}s")

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
        self.rawFileObject.close()

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

    def _checkMetadata(self, metadata: Dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.checkArchiveStats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.checkMetadataArguments(
                json.loads(metadata['arguments']), self.index, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self.index.tryToOpenFirstFile(lambda path: self.open(self.getFileInfo(path)))
