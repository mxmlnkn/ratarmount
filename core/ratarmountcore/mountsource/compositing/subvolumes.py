import builtins
import os
from collections.abc import Iterable
from typing import IO, Any, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import RatarmountError, overrides


class SubvolumesMountSource(MountSource):
    def __init__(self, mountSources: dict[str, MountSource]) -> None:
        """
        mountSources : List of mount sources to mount as subfolders.
        """
        # Recursive dictionary representing folders. Keys are folder names, i.e., may not contain slashes.
        self.mountSources: dict[str, Any] = {}
        self.rootFileInfo = create_root_file_info(userdata=[None])

        for path, target in mountSources.items():
            self.mount(path, target)

    def _get_by_path(self, path: str, create: bool = False) -> Optional[Union[MountSource, dict[str, Any]]]:
        """Implements recursive lookup in self.mountSources."""
        folder = self.mountSources
        for part in path.strip('/').split('/'):
            if not part:
                continue
            if not isinstance(folder, dict):
                raise RatarmountError(f"Cannot return '{path}' because one of its parents is a mount point!")
            if part not in folder:
                if not create:
                    return None
                folder[part] = {}
            folder = folder[part]
        return folder

    def is_mountable(self, path: str) -> bool:
        # Ensuring a leading slash before calling normpath has the effect of eating all leading '/..'.
        parent, name = ('/' + os.path.normpath('/' + path).strip('/')).rsplit('/', maxsplit=1)
        try:
            folder = self._get_by_path(parent, create=True)
        except RatarmountError:
            return False
        return bool(name) and folder is not None and isinstance(folder, dict) and (name not in folder)

    def mount(self, path: str, target: MountSource) -> bool:
        """Adds a mount source at the specified path. Must not overlap with existing mounts. Return true on success."""

        # Ensuring a leading slash before calling normpath has the effect of eating all leading '/..'.
        parent, name = ('/' + os.path.normpath('/' + path).strip('/')).rsplit('/', maxsplit=1)
        if not name:
            raise RatarmountError("Mount points may not be empty!")

        try:
            folder = self._get_by_path(parent, create=True)
        except RatarmountError:
            return False

        if folder is None or not isinstance(folder, dict) or name in folder:
            raise RatarmountError("Mount point already exists!")
        folder[name] = target
        return True

    def unmount(self, path: str) -> Optional[MountSource]:
        """
        Removes a mounted path and returns the corresponding object.
        The caller can then call 'close' on it if necessary!
        """

        # Ensuring a leading / before calling normpath has the effect of eating all leading '/..'.
        path = os.path.normpath('/' + path).strip('/')

        parents = [self.mountSources]
        names: list[str] = []
        for part in path.strip('/').split('/'):
            if not part:
                continue
            folder = parents[-1]
            names.append(part)
            if not isinstance(folder, dict) or part not in folder:
                return None
            parents.append(folder[part])

        # Happens if path is '/'.
        if not names:
            return None

        mountSource = parents.pop()
        if not isinstance(mountSource, MountSource):
            # Might happen when trying to unmount some parent path of an actual mount point.
            return None

        # Remove the mount source from its parent dictionary.
        parents.pop().pop(names.pop())
        # Remove empty parent folders from the recursive dictionary.
        for folder, name in zip(reversed(parents), reversed(names)):
            if folder[name]:
                break
            folder.pop(name)

        return mountSource

    def _find_mount_source(self, path: str) -> Optional[tuple[str, str, Union[MountSource, dict[str, Any]]]]:
        """
        Implements recursive lookup in self.mountSources.
        Returns (mount point path, path in mount source, mount source).
        If the path points to a parent mount folder, then will return (path, "", None)
        """

        folder = self.mountSources
        parts = path.strip('/').split('/')
        for i, part in enumerate(parts):
            if not part:
                continue
            if part not in folder:
                return None
            folder = folder[part]
            if isinstance(folder, MountSource):
                return ('/'.join(parts[: i + 1]), '/' + '/'.join(parts[i + 1 :]), folder)
        return (path, "", folder)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return False  # Because we support mount and unmount!

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        result = self._find_mount_source(path)
        if result is None:
            return None

        subvolume, subpath, mounted = result
        if not isinstance(mounted, MountSource):
            return self.rootFileInfo.clone()

        fileInfo = mounted.lookup(subpath, fileVersion=fileVersion)
        if isinstance(fileInfo, FileInfo):
            fileInfo.userdata.append(subvolume)
            return fileInfo
        return None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        result = self._find_mount_source(path)
        if result is None:
            return 0

        _subvolume, subpath, mounted = result
        if not isinstance(mounted, MountSource):
            return 1

        return mounted.versions(subpath)

    def _list(self, path: str, onlyMode: bool) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        result = self._find_mount_source(path)
        if result is None:
            return None

        _subvolume, subpath, mounted = result
        if not isinstance(mounted, MountSource):
            return {name: self.rootFileInfo.mode if onlyMode else self.rootFileInfo.clone() for name in mounted}

        return mounted.list_mode(subpath) if onlyMode else mounted.list(subpath)

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self._list(path, onlyMode=False)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self._list(path, onlyMode=True)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        subvolume = fileInfo.userdata.pop()
        if subvolume is None:
            raise ValueError(f"Found subvolume is None for fileInfo: {fileInfo}")
        try:
            return self.mountSources[subvolume].open(fileInfo, buffering=buffering)
        finally:
            fileInfo.userdata.append(subvolume)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        subvolume = fileInfo.userdata.pop()
        if subvolume is None:
            raise ValueError(f"Found subvolume is None for fileInfo: {fileInfo}")
        try:
            return self.mountSources[subvolume].read(fileInfo, size, offset)
        finally:
            fileInfo.userdata.append(subvolume)

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        subvolume = fileInfo.userdata.pop()
        try:
            return [] if subvolume is None else self.mountSources[subvolume].list_xattr(fileInfo)
        finally:
            fileInfo.userdata.append(subvolume)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        subvolume = fileInfo.userdata.pop()
        try:
            return None if subvolume is None else self.mountSources[subvolume].get_xattr(fileInfo, key)
        finally:
            fileInfo.userdata.append(subvolume)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo) -> tuple[str, MountSource, FileInfo]:
        sourceFileInfo = fileInfo.clone()
        subvolume = sourceFileInfo.userdata.pop()

        if subvolume is None or subvolume not in self.mountSources:
            return '/', self, fileInfo
        mountSource = self.mountSources[subvolume]

        subpath, subMountSource, subFileInfo = mountSource.get_mount_source(sourceFileInfo)
        return os.path.normpath(f"/{subvolume}/{subpath}"), subMountSource, subFileInfo

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for mountSource in self.mountSources.values():
            mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def join_threads(self):
        for mountSource in self.mountSources:
            if hasattr(mountSource, 'join_threads'):
                mountSource.join_threads()
