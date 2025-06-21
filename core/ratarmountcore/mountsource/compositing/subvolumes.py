from typing import IO, Dict, Iterable, List, Optional, Tuple, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import overrides


class SubvolumesMountSource(MountSource):
    def __init__(self, mountSources: Dict[str, MountSource], printDebug: int = 0) -> None:
        """
        mountSources : List of mount sources to mount as subfolders.
        """
        self.mountSources: Dict[str, MountSource] = mountSources
        self.printDebug = printDebug

        for name in self.mountSources:
            if '/' in name:
                raise ValueError(f"Mount source names may not contain slashes! ({name})")

        self.rootFileInfo = create_root_file_info(userdata=[None])

    def _find_mount_source(self, path: str) -> Optional[Tuple[str, str]]:
        path = path.lstrip('/')
        subvolume, subpath = path.split('/', maxsplit=1) if '/' in path else [path, ""]
        return (subvolume, '/' + subpath) if subvolume in self.mountSources else None

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return all(m.is_immutable() for m in self.mountSources.values())

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if path == '/':
            return self.rootFileInfo.clone()

        if '/' not in path.lstrip('/'):
            return self.rootFileInfo.clone() if path.lstrip('/') in self.mountSources else None

        result = self._find_mount_source(path)
        if result is None:
            return None
        subvolume, subpath = result

        fileInfo = self.mountSources[subvolume].lookup(subpath, fileVersion=fileVersion)
        if isinstance(fileInfo, FileInfo):
            fileInfo.userdata.append(subvolume)
            return fileInfo

        return None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        if path == '/':
            return 1

        result = self._find_mount_source(path)
        if result is None:
            return 0
        subvolume, subpath = result

        return self.mountSources[subvolume].versions(subpath)

    def _list(self, path: str, onlyMode: bool):
        if path == '/':
            return dict.fromkeys(self.mountSources.keys(), self.rootFileInfo.mode if onlyMode else self.rootFileInfo)

        result = self._find_mount_source(path)
        if result is None:
            return None
        subvolume, subpath = result

        return (
            self.mountSources[subvolume].list_mode(subpath) if onlyMode else self.mountSources[subvolume].list(subpath)
        )

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self._list(path, onlyMode=False)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
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
    def list_xattr(self, fileInfo: FileInfo) -> List[str]:
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
    def get_mount_source(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        sourceFileInfo = fileInfo.clone()
        subvolume = sourceFileInfo.userdata.pop()

        if subvolume is None or subvolume not in self.mountSources:
            return '/', self, fileInfo
        mountSource = self.mountSources[subvolume]

        subpath, subMountSource, subFileInfo = mountSource.get_mount_source(sourceFileInfo)
        return subvolume + '/' + subpath, subMountSource, subFileInfo

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for mountSource in self.mountSources:
            mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def join_threads(self):
        for mountSource in self.mountSources:
            if hasattr(mountSource, 'join_threads'):
                mountSource.join_threads()
