import builtins
import os
from collections.abc import Iterable
from typing import IO, Any, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import overrides


class SubvolumesMountSource(MountSource):
    def __init__(self, mountSources: dict[str, MountSource], printDebug: int = 0) -> None:
        """
        mountSources : List of mount sources to mount as subfolders.
        """
        self.mountSources: dict[str, MountSource] = {}
        self.printDebug = printDebug
        self.rootFileInfo = create_root_file_info(userdata=[None])
        # All parent paths mapped to subfolders for quick lookup performance. Initialize with root for easier code.
        # Use dict instead of order because it preserves the order!
        self._hierarchy: dict[str, dict[str, Any]] = {'': {}}

        # Deep-copy the mountSources dictionary while also normalizing paths and checking for duplicates.
        for path, target in mountSources.items():
            self.mount(path, target)

    def mount(self, path: str, target: MountSource):
        """
        Adds a mount source or file object at the specified path.
        Duplicate mount sources on the same path will be union mounted.
        Duplicated paths involving a file object will raise an exception.
        """
        # Ensuring a leading / before calling normpath has the effect of eating all leading '/..'.
        path = os.path.normpath('/' + path).lstrip('/')
        if path in self.mountSources:
            raise ValueError(f"The target path '{path}' already exists!")

        self.mountSources[path] = target

        splitPath = path.split('/')
        for i in range(len(splitPath)):
            subpath = '/'.join(splitPath[:i])
            if subpath not in self._hierarchy:
                self._hierarchy[subpath] = {}
            self._hierarchy[subpath][splitPath[i]] = None

    def unmount(self, path: str) -> Optional[MountSource]:
        """
        Removes a mounted path and returns the corresponding object.
        The caller can then call 'close' on it if necessary!
        """
        # Ensuring a leading / before calling normpath has the effect of eating all leading '/..'.
        path = os.path.normpath('/' + path).lstrip('/')
        if path not in self.mountSources:
            return None
        mountSource = self.mountSources[path]
        del self.mountSources[path]

        splitPath = path.split('/')
        for i in range(len(splitPath)):
            subpath = '/'.join(splitPath[:i])
            if subpath in self._hierarchy and splitPath[i] in self._hierarchy[subpath]:
                del self._hierarchy[subpath][splitPath[i]]
                # Remove empty folders, but never delete the root entry!
                if subpath and not self._hierarchy[subpath]:
                    del self._hierarchy[subpath]

        return mountSource

    def _find_mount_source(self, path: str) -> Optional[tuple[str, str, MountSource]]:
        assert not path.startswith('/')
        splitPath = path.split('/')
        for i in range(1, len(splitPath) + 1):
            subvolume = '/'.join(splitPath[:i])
            if subvolume in self.mountSources:
                return (subvolume, '/' + '/'.join(splitPath[i:]), self.mountSources[subvolume])
        return None

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return all(m.is_immutable() for m in self.mountSources.values())

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        path = path.lstrip('/')
        if path in self._hierarchy:
            return self.rootFileInfo.clone()

        result = self._find_mount_source(path)
        if result is None:
            return None
        subvolume, subpath, mounted = result

        fileInfo = mounted.lookup(subpath, fileVersion=fileVersion)
        if isinstance(fileInfo, FileInfo):
            fileInfo.userdata.append(subvolume)
            return fileInfo
        return None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        path = path.lstrip('/')
        if path in self._hierarchy:
            return 1

        result = self._find_mount_source(path)
        if result is None:
            return 0

        subvolume, subpath, mounted = result
        return mounted.versions(subpath)

    def _list(self, path: str, onlyMode: bool) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        path = path.lstrip('/')
        if path in self._hierarchy:
            return dict.fromkeys(
                self._hierarchy[path].keys(), self.rootFileInfo.mode if onlyMode else self.rootFileInfo.clone()
            )

        result = self._find_mount_source(path)
        if result is None:
            return None

        subvolume, subpath, mounted = result
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
        return subvolume + '/' + subpath, subMountSource, subFileInfo

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for mountSource in self.mountSources.values():
            mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def join_threads(self):
        for mountSource in self.mountSources:
            if hasattr(mountSource, 'join_threads'):
                mountSource.join_threads()
