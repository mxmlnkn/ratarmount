from collections.abc import Iterable
from typing import IO, Any, Optional, Union

from ratarmountcore.formats import FileFormatID, replace_format_check
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import overrides

try:
    import ext4
except ImportError:
    ext4 = None  # type: ignore


def is_ext4_image(fileObject) -> bool:
    if ext4 is None:
        return False

    offset = fileObject.tell()
    try:
        ext4.Volume(fileObject)
        return True
    except Exception:
        pass
    finally:
        fileObject.seek(offset)
    return False


replace_format_check(FileFormatID.EXT4, is_ext4_image)


class EXT4MountSource(MountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], encoding: str = 'utf8', **options) -> None:
        if ext4 is None:
            raise ImportError("Failed to find python4-ext4. Try: pip install ext4")

        self.encoding = encoding
        self.fileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        self.fileObjectWasOpened = isinstance(fileOrPath, str)
        self.fileSystem = ext4.Volume(self.fileObject)
        self.options = options

    @staticmethod
    def _convert_ext4_directory_entry_to_file_info(inode) -> FileInfo:
        # fmt: off
        return FileInfo(
            size     = inode.i_size,
            mtime    = inode.i_mtime,
            mode     = inode.i_mode.value,
            linkname = "",  # TODO I don't see any data for links... in the Inode struct
            uid      = inode.i_uid,
            gid      = inode.i_gid,
            userdata = [inode.i_no],
        )
        # fmt: on

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        try:
            self.fileSystem.inode_at(path)
        except FileNotFoundError:
            return False
        return True

    def _list(self, path: str, getValue) -> Optional[dict[str, Any]]:
        try:
            inode = self.fileSystem.inode_at(path)
        except FileNotFoundError:
            return None

        if not isinstance(inode, ext4.inode.Directory):
            return None

        return {
            entry.name.decode(self.encoding): getValue(self.fileSystem.inodes[entry.inode])
            for entry, _ in inode.opendir()
            if entry.name not in (b'.', b'..')
        }

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self._list(path, EXT4MountSource._convert_ext4_directory_entry_to_file_info)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self._list(path, lambda inode: inode.i_mode.value)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        try:
            return self._convert_ext4_directory_entry_to_file_info(self.fileSystem.inode_at(path))
        except FileNotFoundError:
            pass
        return None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        inode = fileInfo.userdata[-1]
        assert isinstance(inode, int)
        return self.fileSystem.inodes[inode].open()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.fileObjectWasOpened:
            self.fileObject.close()
