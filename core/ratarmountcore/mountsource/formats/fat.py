import errno
import io
import os
import stat
from collections.abc import Iterable
from typing import IO, Optional, Union, cast

from ratarmountcore.formats import FileFormatID, replace_format_check
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import overrides

try:
    from ratarmountcore._external.pyfatfs import PyFATException
    from ratarmountcore._external.pyfatfs.FatIO import FatIO
    from ratarmountcore._external.pyfatfs.PyFat import PyFat
except ImportError:
    FatIO = None  # type: ignore
    PyFat = None  # type: ignore
    PyFATException = None  # type: ignore


def is_fat_image(fileObject) -> bool:
    if PyFat is None:
        return False

    offset = fileObject.tell()
    try:
        fs = PyFat()
        # TODO Avoid possibly slow full FAT parsing here. Only do some quick checks such as PyFatFS.PyFat.parse_header
        #      Calling __set_fp instead of set_fp avoids that but it is not part of the public interface per convention!
        fs._PyFat__set_fp(fileObject)  # type: ignore
        fs.is_read_only = True
        try:
            fs.parse_header()
            return True
        except (PyFATException, ValueError):
            return False
        finally:
            # Reset file object so that it does not get closed! Cannot be None because that is checked.
            fs._PyFat__fp = io.BytesIO()  # type: ignore

    finally:
        fileObject.seek(offset)


replace_format_check(FileFormatID.FAT, is_fat_image)


class FATMountSource(MountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        if PyFat is None:
            raise ImportError("Failed to find pyfatfs. Try: pip install pyfatfs")

        self.fileSystem = PyFat()
        if isinstance(fileOrPath, str):
            # TODO Probably good idea for performance on Lustre to open the file unbuffered.
            self.fileSystem.open(fileOrPath, read_only=True)
        else:
            self.fileSystem.set_fp(fileOrPath)
        self.options = options

    @staticmethod
    def _convert_fatdirectory_entry_to_file_info(entry, path) -> FileInfo:
        """
        entry: of type pyfatfs.FATDirectoryEntry.FATDirectoryEntry.
        """
        # FAT has no file permissions.
        mode = 0o777 | (stat.S_IFDIR if entry.is_directory() else stat.S_IFREG)

        # fmt: off
        return FileInfo(
            size     = entry.filesize,
            mtime    = entry.get_mtime().timestamp(),
            mode     = mode,
            linkname = "",  # FAT has no support for hard or symbolic links
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [path],
        )
        # fmt: on

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        try:
            self.fileSystem.root_dir.get_entry(path)
        except PyFATException as exception:
            if exception.errno == errno.ENOENT:
                return False
            raise exception
        return True

    def _list(self, path: str) -> Optional[Iterable]:
        try:
            directories, files, _ = self.fileSystem.root_dir.get_entry(os.path.normpath(path)).get_entries()
        except PyFATException as exception:
            if exception.errno in [errno.ENOENT, errno.ENOTDIR]:
                return None
            raise exception
        return [str(entry) for entry in directories + files]

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        # TODO I think with the low-level API, we could also get the FileInfos
        return self._list(path)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        try:
            entry = self.fileSystem.root_dir.get_entry(path)
        except PyFATException as exception:
            if exception.errno in [errno.ENOTDIR, errno.ENOENT]:
                return None
            raise exception
        return self._convert_fatdirectory_entry_to_file_info(entry, path)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        # TODO There is no option in FatIO to configure the buffering yet.
        return cast(IO[bytes], FatIO(self.fileSystem, path))

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileSystem.close()
