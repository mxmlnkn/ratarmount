import contextlib
import datetime
import logging
import stat
import sys
import zipfile
from pathlib import Path
from typing import IO, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import overrides

try:
    # Importing this patches the zipfile module as a "side" effect!
    import fast_zip_decryption  # pylint: disable=unused-import  # noqa: F401
except (ImportError, Exception):
    with contextlib.suppress(ImportError, Exception):
        import fastzipfile  # pylint: disable=unused-import  # noqa: F401


logger = logging.getLogger(__name__)


class ZipMountSource(SQLiteIndexMountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes], Path], **options) -> None:
        if 'zipfile' not in sys.modules:
            raise RuntimeError("Did not find the zipfile module. Please use Python 3.7+.")

        if isinstance(fileOrPath, Path):
            fileOrPath = str(fileOrPath)
        self.fileObject = zipfile.ZipFile(fileOrPath, 'r')

        ZipMountSource._find_password(self.fileObject, options.get("passwords", []))
        self.files = {info.header_offset: info for info in self.fileObject.infolist()}

        indexOptions = {
            'archiveFilePath': fileOrPath if isinstance(fileOrPath, str) else None,
            'backendName': 'ZipMountSource',
        }
        super().__init__(**(options | indexOptions))
        self._finalize_index(
            lambda: self.index.set_file_infos([self._convert_to_row(info) for info in self.fileObject.infolist()])
        )

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
        if mode == 0:
            mode = 0o770 if info.is_dir() else 0o660
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
    def close(self) -> None:
        super().close()
        if fileObject := getattr(self, 'fileObject', None):
            fileObject.close()

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # I do not see any obvious option to zipfile.ZipFile to apply the specified buffer size.
        info = self.files[SQLiteIndex.get_index_userdata(fileInfo.userdata).offsetheader]
        assert isinstance(info, zipfile.ZipInfo)
        # CPython's zipfile module does handle multiple file objects being opened and reading from the
        # same underlying file object concurrently by using a _SharedFile class that even includes a lock.
        # Very nice!
        # https://github.com/python/cpython/blob/a87c46eab3c306b1c5b8a072b7b30ac2c50651c0/Lib/zipfile/__init__.py#L1569
        return self.fileObject.open(info, 'r')  # https://github.com/pauldmccarthy/indexed_gzip/issues/85
