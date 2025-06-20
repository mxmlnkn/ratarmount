from typing import IO, Any, Dict, Iterable, Optional, Union

from ratarmountcore.formats import FileFormatID, replaceFormatCheck
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import overrides

try:
    import ext4
except ImportError:
    ext4 = None  # type: ignore


def isEXT4Image(fileObject) -> bool:
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


replaceFormatCheck(FileFormatID.EXT4, isEXT4Image)


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
    def _convertEXT4DirectoryEntryToFileInfo(inode) -> FileInfo:
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
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        try:
            self.fileSystem.inode_at(path)
        except FileNotFoundError:
            return False
        return True

    def _listDir(self, path: str, getValue) -> Optional[Dict[str, Any]]:
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
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self._listDir(path, EXT4MountSource._convertEXT4DirectoryEntryToFileInfo)

    @overrides(MountSource)
    def listDirModeOnly(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        return self._listDir(path, lambda inode: inode.i_mode.value)

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        try:
            return self._convertEXT4DirectoryEntryToFileInfo(self.fileSystem.inode_at(path))
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
