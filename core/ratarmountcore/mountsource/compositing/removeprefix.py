import builtins
import os
from collections.abc import Iterable
from typing import IO, Any, Callable, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import overrides


class RemovePrefixMountSource(MountSource):
    """
    MountSource for remove a prefix from all requested paths to effectively move the specified MountSource
    into a subfolder.
    """

    def __init__(self, path: str, mountSource: MountSource):
        # Beware, normpath leaves leading // but collapses /// and more repetitions to /!
        self.prefix = os.path.normpath('/' + path.lstrip('/')).rstrip('/') + '/'
        self.mountSource = mountSource
        self._directory_info = create_root_file_info([])

    def _parse_path(
        self, path: str, queryMountSource: Callable[[str], Any], queryDir: Callable[[str], Any], defaultValue
    ):
        if not path.startswith('/'):
            path = '/' + path
        if not path.endswith('/'):
            path = path + '/'
        if path.startswith(self.prefix):
            return queryMountSource(path[len(self.prefix) - 1 : -1])
        if self.prefix.startswith(path):
            return queryDir(self.prefix.removeprefix(path).split('/', maxsplit=1)[0])
        return defaultValue

    # Methods with 'path' argument

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self._parse_path(path, self.mountSource.list, lambda name: {name: self._directory_info.clone()}, None)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self._parse_path(path, self.mountSource.list_mode, lambda name: {name: self._directory_info.mode}, None)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self._parse_path(path, self.mountSource.lookup, lambda _: self._directory_info.clone(), None)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return self._parse_path(path, self.mountSource.versions, lambda _: 1, 0)

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return self._parse_path(path, self.mountSource.exists, lambda _: True, False)

    @overrides(MountSource)
    def is_dir(self, path: str) -> bool:
        return self._parse_path(path, self.mountSource.is_dir, lambda _: True, False)

    # Methods with 'fileInfo' argument. Comparing against self._directory_info is not beautiful but should work
    # good enough. The alternative would be to add some token to FileInfo.userdata, but that seems too verbose.

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        if fileInfo == self._directory_info:
            raise FileNotFoundError("Cannot open directory!")
        return self.mountSource.open(fileInfo, buffering)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        if fileInfo == self._directory_info:
            raise FileNotFoundError("Cannot read from directory!")
        return self.mountSource.read(fileInfo, size, offset)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo):
        if fileInfo == self._directory_info:
            return super().get_mount_source(fileInfo)
        return self.mountSource.get_mount_source(fileInfo)

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        if fileInfo == self._directory_info:
            return []
        return self.mountSource.list_xattr(fileInfo)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        if fileInfo == self._directory_info:
            return None
        return self.mountSource.get_xattr(fileInfo, key)

    # Methods that can simply be forwarded

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        return self.mountSource.statfs()

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return self.mountSource.is_immutable()

    @overrides(MountSource)
    def __enter__(self):
        self.mountSource.__enter__()
        return self

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.mountSource.__exit__(exception_type, exception_value, exception_traceback)
