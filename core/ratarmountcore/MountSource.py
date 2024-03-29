#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
import dataclasses
from typing import Any, Dict, IO, Iterable, List, Optional, Union


@dataclass
class FileInfo:
    # fmt: off
    size     : int
    mtime    : float
    mode     : int
    linkname : str
    uid      : int
    gid      : int
    # By convention this is a list and MountSources should only read the last element and before forwarding the
    # FileInfo to a possibly recursively "mounted" MountSource, remove that last element belonging to it.
    # This way an arbitrary amount of userdata can be stored and it should be decidable which belongs to whom in
    # a chain of MountSource objects.
    userdata : List[Any]
    # fmt: on

    def clone(self):
        copied = dataclasses.replace(self)
        # Make a new userdata list but do not do a full deep copy because some MountSources put references
        # to MountSources into userdata and those should and can not be deep copied.
        copied.userdata = self.userdata[:]
        return copied


class MountSource(ABC):
    """
    Generic class representing a mount point. It's basically like the FUSE API but boiled down
    to the necessary methods for ratarmount.

    Similar, to FUSE, all paths should have a leading '/'.
    If there is is no leading slash, behave as if there was one.
    """

    @abstractmethod
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        pass

    @abstractmethod
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        pass

    @abstractmethod
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        pass

    def fileVersions(self, path: str) -> int:
        return 1 if self.exists(path) else 0

    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset)
            return file.read(size)

    @abstractmethod
    def isImmutable(self) -> bool:
        """
        Should return True if the mount source is known to not change over time in order to allow for optimizations.
        Meaning, all interface methods should return the same results given the same arguments at any time.
        """

    def getMountSource(self, fileInfo: FileInfo):
        """
        Returns the direct mount source to which the fileInfo belongs, a mount source specific file info,
        and the mount point of the returned mount source in respect to this (self) MountSource.
        """
        return '/', self, fileInfo

    def exists(self, path: str):
        return self.getFileInfo(path) is not None

    def isdir(self, path: str):
        fileInfo = self.getFileInfo(path)
        return fileInfo is not None and stat.S_ISDIR(fileInfo.mode)

    def __enter__(self):
        return self

    # If the derived MountSource opens some file object or similar in its constructor
    # then it should override this and close the file object.
    @abstractmethod
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass


def createRootFileInfo(userdata: List[Any]):
    return FileInfo(
        # fmt: off
        size     = 0,
        mtime    = int(time.time()),
        mode     = 0o777 | stat.S_IFDIR,
        linkname = "",
        uid      = os.getuid(),
        gid      = os.getgid(),
        userdata = userdata,
        # fmt: on
    )
