#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat
import time

from typing import Iterable, IO, List, Optional, Set, Tuple

from .MountSource import FileInfo, MountSource
from .utils import overrides


class UnionMountSource(MountSource):
    def __init__(self, mountSources: List[MountSource]) -> None:
        self.mountSources: List[MountSource] = mountSources

        self.rootFileInfo = FileInfo(
            # fmt: off
            size         = 0,
            mtime        = int(time.time()),
            mode         = 0o777 | stat.S_IFDIR,
            linkname     = "",
            uid          = os.getuid(),
            gid          = os.getgid(),
            userdata     = [None],
            # fmt: on
        )

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if path == '/':
            return self.rootFileInfo

        # We need to keep the sign of the fileVersion in order to forward it to SQLiteIndexedTar.
        # When the requested version can't be found in a mount source, increment negative specified versions
        # by the amount of versions in that mount source or decrement the initially positive version.
        if fileVersion <= 0:
            for mountSource in reversed(self.mountSources):
                fileInfo = mountSource.getFileInfo(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion += mountSource.fileVersions(path)
                if fileVersion > 0:
                    break

        else:  # fileVersion >= 1
            for mountSource in self.mountSources:
                fileInfo = mountSource.getFileInfo(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion -= mountSource.fileVersions(path)
                if fileVersion < 1:
                    break

        return None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return sum(mountSource.fileVersions(path) for mountSource in self.mountSources)

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        """
        Returns the set of all folder contents over all mount sources or None if the path was found in none of them.
        """

        files: Set[str] = set()
        folderExists = False

        for mountSource in self.mountSources:
            result = mountSource.listDir(path)
            if result is not None:
                files = files.union(result)
                folderExists = True

        return files if folderExists else None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.open(fileInfo)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.read(fileInfo, size, offset)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        sourceFileInfo = fileInfo.clone()
        mountSource = sourceFileInfo.userdata.pop()

        if not isinstance(mountSource, MountSource):
            return '/', self, fileInfo

        # Because all mount sources are mounted at '/', we do not have to append
        # the mount point path returned by getMountSource to the mount point '/'.
        return mountSource.getMountSource(sourceFileInfo)
