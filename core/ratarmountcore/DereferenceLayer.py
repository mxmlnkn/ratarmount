#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat

from typing import Dict, Iterable, IO, Optional, Tuple, Union

from .FolderMountSource import FolderMountSource
from .MountSource import FileInfo, MountSource
from .utils import overrides


class DereferenceLayer(MountSource):
    def __init__(
        self,
        mountSource: MountSource,
        mountPoint: str = "",
        maxNestedLinkDepth: int = 40,
        # pylint: disable=unused-argument
        **kwargs,
    ) -> None:
        """
        mountPoint
            The mount point of the given mount source. If a symbolic refers to this mount point, then it will
            be dereferenced by calling directly into mountSource to avoid recursive calls to FUSE via POSIX calls.
        maxNestedLinkDepth
            The maximum number of symbolic links to undo. Even the kernel does not support more than 40 by default:
            https://lwn.net/Articles/650786/
            > Linux imposes a limit of at most 40 symlinks in any one path lookup. It previously imposed
            > a further limit of eight on the maximum depth of recursion, but that was raised to 40 when
            > a separate stack was implemented, so there is now just the one limit.
        """
        self.mountSource: MountSource = mountSource
        self.maxNestedLinkDepth = maxNestedLinkDepth
        self.mountPoint = os.path.realpath(mountPoint) if mountPoint else ""

    def _dereferencePath(self, path: str, fileVersion: int = 0) -> Tuple[str, Optional[FileInfo]]:
        maxNestedLinkDepth = self.maxNestedLinkDepth
        fileInfo = self.mountSource.getFileInfo(path, fileVersion)
        # Remove the leading slash because this path is assumed to be relative to the mount source root.
        # We especially want the path to be relative in order for normpath to not collapse leading /../..
        # After the first absolute path has been encountered, it is another issue.
        path = path.lstrip('/')

        while fileInfo and stat.S_ISLNK(fileInfo.mode) and fileInfo.linkname and maxNestedLinkDepth > 0:
            # TODO In order to be 100% correct we would have to parse each path component of the linkname and resolve
            #      all possible symlinks encountered in there. I think it is overkill for now to implement this,
            #      This function is complex enough as it is.
            if os.path.isabs(fileInfo.linkname):
                newPath = fileInfo.linkname
                if os.path.commonpath([self.mountPoint, newPath]) == self.mountPoint:
                    newPath = newPath[len(self.mountPoint) :].lstrip('/')
            else:
                newPath = os.path.normpath(os.path.split(path)[0] + '/' + fileInfo.linkname)
                if newPath.startswith('../'):
                    newPath = os.path.normpath(self.mountPoint + '/' + newPath)
                    if os.path.commonpath([self.mountPoint, newPath]) == self.mountPoint:
                        newPath = newPath[len(self.mountPoint) :].lstrip('/')

            if os.path.isabs(newPath):
                # We could use realpath and stat instead of lstat to fully resolve the symbolic link in one go,
                # but then we might accidentally call into FUSE again and lock up.
                if not os.path.lexists(newPath):
                    break
                stats = os.lstat(newPath)
                try:
                    linkname = os.readlink(newPath) if stat.S_ISLNK(stats.st_mode) else ""
                except OSError:
                    linkname = ""
                newFileInfo = FolderMountSource._statsToFileInfo(stats, newPath, linkname=linkname)
                newFileInfo.userdata.append(newPath)
            else:
                newFileInfo = self.mountSource.getFileInfo(newPath)
                if newFileInfo:
                    newFileInfo.userdata.append(None)

            if newFileInfo is None:
                break

            fileInfo = newFileInfo
            path = newPath
            maxNestedLinkDepth -= 1

        return path, fileInfo

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return self.mountSource.isImmutable()

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self._dereferencePath(path, fileVersion)[1]

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return self.mountSource.fileVersions(self._dereferencePath(path)[0])

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self.mountSource.listDir(self._dereferencePath(path)[0])

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        realFilePath = fileInfo.userdata.pop()
        return self.mountSource.open(fileInfo) if realFilePath is None else open(realFilePath, 'rb')

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        realFilePath = fileInfo.userdata.pop()
        if realFilePath is None:
            return self.mountSource.read(fileInfo, size, offset)

        with open(realFilePath, 'rb') as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        realFilePath = fileInfo.userdata.pop()
        if realFilePath is None:
            return self.mountSource.getMountSource(fileInfo)

        fileInfo.userdata.append(realFilePath)
        return '/', self, fileInfo

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def joinThreads(self):
        if hasattr(self.mountSource, 'joinThreads'):
            self.mountSource.joinThreads()
