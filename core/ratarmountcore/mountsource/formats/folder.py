import builtins
import os
import stat
from collections.abc import Iterable
from typing import IO, Any, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import overrides


def max_up_count(path):
    if os.path.isabs(path):
        return 0
    result = 0
    upCount = 0
    for part in path.split(os.path.sep):
        if part == '..':
            upCount += 1
            result = max(result, upCount)
        elif part in ['.', '']:
            continue
        else:
            upCount -= 1
    return result


class FolderMountSource(MountSource):
    """
    This class manages one folder as mount source offering methods for listing folders, reading files, and others.
    """

    def __init__(self, path: Union[str, os.PathLike]) -> None:
        self.root = str(path)
        self._statfs = FolderMountSource._get_statfs_for_folder(self.root)

    def set_folder_descriptor(self, fd: int) -> None:
        """
        Make this mount source manage the special "." folder by changing to that directory.
        Because we change to that directory it may only be used for one mount source but it also works
        when that mount source is mounted on!
        """
        os.fchdir(fd)
        self.root = '.'
        self._statfs = FolderMountSource._get_statfs_for_folder(self.root)

    @staticmethod
    def _get_statfs_for_folder(path: str):
        result = os.statvfs(path)
        return {
            'f_bsize': result.f_bsize,
            'f_frsize': result.f_frsize,
            'f_blocks': result.f_blocks,
            'f_bfree': 0,
            'f_bavail': 0,
            'f_files': result.f_files,
            'f_ffree': 0,
            'f_favail': 0,
            'f_namemax': result.f_namemax,
        }

    def _realpath(self, path: str) -> str:
        """Path given relative to folder root. Leading '/' is acceptable"""
        return os.path.join(self.root, *path.strip('/').split('/'))

    @staticmethod
    def _stats_to_file_info(stats: os.stat_result, path: str, linkname: str):
        # fmt: off
        return FileInfo(
            size     = stats.st_size,
            mtime    = stats.st_mtime,
            mode     = stats.st_mode,
            linkname = linkname,
            uid      = stats.st_uid,
            gid      = stats.st_gid,
            userdata = [path],
        )
        # fmt: on

    @staticmethod
    def _dir_entry_to_file_info(dirEntry: os.DirEntry, path: str, realpath: str):
        try:
            linkname = os.readlink(realpath) if dirEntry.is_symlink() else ""
        except OSError:
            linkname = ""

        return FolderMountSource._stats_to_file_info(dirEntry.stat(follow_symlinks=False), linkname, path)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return False

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return os.path.lexists(self._realpath(path))

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """All returned file infos contain a file path string at the back of FileInfo.userdata."""

        # This is a bit of problematic design, however, the versions count from 1 for the user.
        # And as -1 means the last version, 0 should also mean the first version ...
        # Basically, I did accidentally mix user-visible versions 1+ versions with API 0+ versions,
        # leading to this problematic clash of 0 and 1.
        if fileVersion not in [0, 1] or not self.exists(path):
            return None

        realpath = self._realpath(path)
        linkname = ""
        if os.path.islink(realpath):
            linkname = os.readlink(realpath)
            # Resolve relative links that point outside the source folder because they will become invalid
            # if they are mounted onto a different path. This relatively simply logic only works under the
            # assumption that "path" is normalized, i.e., it does not contain links in its path and no double
            # slashes and no '/./'. Calling os.path.normpath would remedy the latter but ONLY under the
            # assumption that there are no symbolic links in the path, else it might make things worse.
            if (
                not os.path.isabs(linkname)
                and max_up_count(linkname) > path.strip('/').count('/')
                and os.path.exists(realpath)
            ):
                realpath = os.path.realpath(realpath)
                return self._stats_to_file_info(os.stat(realpath), realpath, "")
        return self._stats_to_file_info(os.lstat(realpath), path.lstrip('/'), linkname)

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        realpath = self._realpath(path)
        if not os.path.isdir(realpath):
            return None

        return {
            os.fsdecode(dirEntry.name): FolderMountSource._dir_entry_to_file_info(dirEntry, path, realpath)
            for dirEntry in os.scandir(realpath)
        }

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        realpath = self._realpath(path)
        if not os.path.isdir(realpath):
            return None

        # https://docs.python.org/3/library/os.html#os.scandir
        # > All os.DirEntry methods may perform a system call, but is_dir() and is_file() usually
        # > only require a system call for symbolic links; os.DirEntry.stat() always requires a
        # > system call on Unix but only requires one for symbolic links on Windows.
        # Unfortunately, I am not sure whether it would be sufficient to build the file mode from these
        # two getters. For now, I'd say that all the esoteric stuff is simply not supported.
        def make_mode(dirEntry):
            mode = stat.S_IFDIR if dirEntry.is_dir(follow_symlinks=False) else stat.S_IFREG
            if dirEntry.is_symlink():
                mode = stat.S_IFLNK
            return mode

        return {os.fsdecode(dirEntry.name): make_mode(dirEntry) for dirEntry in os.scandir(realpath)}

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return 1 if self.exists(path) else 0

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        realpath = self.get_file_path(fileInfo)
        try:
            return open(realpath, 'rb', buffering=buffering)
        except Exception as e:
            raise ValueError(f"Specified path '{realpath}' is not a file that can be read!") from e

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        return self._statfs.copy()

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        return os.listxattr(self.get_file_path(fileInfo), follow_symlinks=False) if hasattr(os, 'listxattr') else []

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        return (
            os.getxattr(self.get_file_path(fileInfo), key, follow_symlinks=False) if hasattr(os, 'getxattr') else None
        )

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def get_file_path(self, fileInfo: FileInfo) -> str:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        # Path argument is only expected to be absolute for symbolic links pointing outside self.root.
        return path if path.startswith('/') else self._realpath(path)
