"""Mixin class for mount sources that delegate to multiple underlying mount sources."""

import builtins
from collections.abc import Sequence
from contextlib import ExitStack
from typing import IO, Any, Optional

from ratarmountcore.mountsource import FileInfo, MountSource, merge_statfs
from ratarmountcore.utils import overrides


class MultiMountSourceMixin(MountSource):
    """
    Mixin class providing common functionality for mount sources that delegate to multiple underlying mount sources.

    This mixin provides default implementations for methods that follow the pattern of:
    1. Storing the delegated mount source in fileInfo.userdata
    2. Popping it when performing operations
    3. Restoring it after the operation

    Subclasses must define a `mountSources` attribute of type `Sequence[MountSource]`.
    """

    mountSources: Sequence[MountSource]

    def __init__(self) -> None:
        super().__init__()
        self._exit_stack = ExitStack()

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering: int = -1) -> IO[bytes]:
        """Opens a file for reading by delegating to the appropriate mount source."""
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.open(fileInfo, buffering=buffering)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        """Reads data from a file by delegating to the appropriate mount source."""
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.read(fileInfo, size, offset)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        """Lists extended attributes by delegating to the appropriate mount source."""
        mountSource = fileInfo.userdata.pop()
        try:
            return mountSource.list_xattr(fileInfo) if isinstance(mountSource, MountSource) else []
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        """Gets an extended attribute by delegating to the appropriate mount source."""
        mountSource = fileInfo.userdata.pop()
        try:
            return mountSource.get_xattr(fileInfo, key) if isinstance(mountSource, MountSource) else None
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo) -> tuple[str, MountSource, FileInfo]:
        """Gets the mount source for a file by delegating to the appropriate mount source."""
        sourceFileInfo = fileInfo.clone()
        mountSource = sourceFileInfo.userdata.pop()

        if not isinstance(mountSource, MountSource):
            return "/", self, fileInfo

        # Because all mount sources are mounted at '/', we do not have to append
        # the mount point path returned by get_mount_source to the mount point '/'.
        return mountSource.get_mount_source(sourceFileInfo)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        """Returns whether all underlying mount sources are immutable."""
        return all(ms.is_immutable() for ms in self.mountSources)

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        """Returns merged filesystem statistics from all mount sources."""
        return merge_statfs([mountSource.statfs() for mountSource in self.mountSources])

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """Cleanup method for all mount sources."""

        # Use `ExitStack` because it is very verbose to properly aggregate multiple `__exit__` calls.
        return self._exit_stack.__exit__(exception_type, exception_value, exception_traceback)

    @overrides(MountSource)
    def __enter__(self):
        """Context manager entry point for all mount sources."""
        for mountSource in self.mountSources:
            self._exit_stack.enter_context(mountSource)

        # Use `push` instead of `enter_context`, because `enter_context` does not support `super()` as an argument.
        super().__enter__()
        self._exit_stack.push(super().__exit__)
        return self

    def join_threads(self):
        """Joins threads from all underlying mount sources that support it."""
        for mountSource in self.mountSources:
            if hasattr(mountSource, 'join_threads'):
                mountSource.join_threads()
