#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat
import time

from typing import Dict, Iterable, IO, List, Optional, Set, Tuple

from .MountSource import FileInfo, MountSource
from .utils import overrides


class UnionMountSource(MountSource):
    def __init__(self, mountSources: List[MountSource], printDebug: int = 0) -> None:
        self.mountSources: List[MountSource] = mountSources
        self.printDebug = printDebug
        self.folderCache: Dict[str, List[MountSource]] = {"/": self.mountSources}
        self.folderCacheDepth = 0  # depth 1 means, we only cached top-level directories.

        self.rootFileInfo = FileInfo(
            # fmt: off
            size     = 0,
            mtime    = int(time.time()),
            mode     = 0o777 | stat.S_IFDIR,
            linkname = "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [None],
            # fmt: on
        )

        if len(self.mountSources) > 1:
            self._buildFolderCache()

    def _buildFolderCache(self, maxDepth=1024, nMaxCacheSize=100000, nMaxSecondsToCache=60):
        """
        nMaxCacheSize:
            Even assuming very long file paths like 1000 chars, the cache size
            will be below 100 MB if the maximum number of elements is 100k.
        nMaxSecondsToCache:
            Another problem is the setup time, as it might take ~0.001s for each getFileInfo call
            and it shouldn't take minutes! Note that there always can be an edge case with hundred
            thousands of files in one folder, which can take an arbitrary amount of time to cache.
        """
        t0 = time.time()

        if self.printDebug >= 1:
            print(f"Building cache for union mount (timeout after {nMaxSecondsToCache}s)...")

        self.folderCache = {"/": self.mountSources}

        lastFolderCache: Dict[str, List[MountSource]] = {"/": self.mountSources}

        for depth in range(1, maxDepth):
            # This intermediary structure is used because:
            #   1. We need to only iterate over the newly added folders in the next step
            #   2. We always want to (atomically) merge results for one folder depth so that we can be sure
            #      that if a folder of a cached depth can not be found in the cache that it does not exist at all.
            newFolderCache: Dict[str, List[MountSource]] = {}

            for folder, mountSources in lastFolderCache.items():
                for mountSource in mountSources:
                    filesInFolder = mountSource.listDir(folder)
                    if not filesInFolder:
                        continue

                    for file in filesInFolder:
                        if time.time() - t0 > nMaxSecondsToCache or nMaxCacheSize <= 0:
                            return

                        fullPath = os.path.join(folder, file)
                        fileInfo = mountSource.getFileInfo(fullPath)
                        if not fileInfo or not stat.S_ISDIR(fileInfo.mode):
                            continue

                        nMaxCacheSize -= 1

                        if fullPath in newFolderCache:
                            newFolderCache[fullPath].append(mountSource)
                        else:
                            newFolderCache[fullPath] = [mountSource]

            if not newFolderCache:
                break

            self.folderCache.update(newFolderCache)
            self.folderCacheDepth = depth
            lastFolderCache = newFolderCache

        t1 = time.time()

        if self.printDebug >= 1:
            print(
                f"Cached mount sources for {len(self.folderCache)} folders up to a depth of "
                f"{self.folderCacheDepth} in {t1-t0:.3}s for faster union mount."
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
