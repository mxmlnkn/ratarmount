import io
import os
import stat
import time
from collections.abc import Iterable
from dataclasses import dataclass
from typing import IO, Callable, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.MountSource import create_root_file_info
from ratarmountcore.utils import overrides


class ControlLayer(MountSource):
    """
    This mount source exposes an interface to an existing ratarmount process via special files.
    """

    @dataclass
    class _EntryInfo:
        fileInfo: Optional[FileInfo] = None
        openFile: Optional[Callable[[], IO[bytes]]] = None

    def __init__(self, prefix, openOutput: Optional[Callable[[], IO[bytes]]]) -> None:
        self._files: dict[str, ControlLayer._EntryInfo] = {}
        prefix = os.path.normpath('/' + prefix)
        splitParts = prefix.split('/')
        rootFileInfo = create_root_file_info([])
        for i in range(1, len(splitParts) + 1):
            self._files['/'.join(splitParts[:i])] = ControlLayer._EntryInfo(
                fileInfo=rootFileInfo.clone(), openFile=None
            )

        if openOutput:
            filePath = prefix + '/output'
            self._files[filePath] = ControlLayer._EntryInfo(fileInfo=None, openFile=openOutput)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return False

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return path in self._files

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if path not in self._files:
            return None

        entry = self._files[path]
        if entry.fileInfo:
            return entry.fileInfo
        if not entry.openFile:
            return None

        with entry.openFile() as file:
            file.seek(0, io.SEEK_END)
            fileSize = file.tell()

        # fmt: off
        return FileInfo(
            size     = fileSize,
            mtime    = int(time.time()),
            mode     = 0o777 | stat.S_IFREG,
            linkname = "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [path],
        )
        # fmt: on

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        if not self.exists(path):
            return None
        return [
            filePath[len(path) + 1 :]
            for filePath in self._files
            if filePath.startswith(path + '/') and '/' not in filePath[len(path) + 1 :]
        ]

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        if not self.exists(path):
            return None
        return {
            filePath[len(path) + 1 :]: entry.fileInfo.mode if entry.fileInfo else 0o777 | stat.S_IFREG
            for filePath, entry in self._files.items()
            if filePath.startswith(path + '/') and '/' not in filePath[len(path) + 1 :] and filePath != path + '/'
        }

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        assert fileInfo.userdata
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)

        if path not in self._files:
            raise FileNotFoundError

        openFile = self._files[path].openFile
        if not openFile:
            raise ValueError("Cannot open this file.")
        return openFile()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass
