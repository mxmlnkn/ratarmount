import builtins
import logging
import os
import stat
import time
from collections.abc import Iterable, Sequence
from typing import IO, Any, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info, merge_statfs
from ratarmountcore.utils import overrides

logger = logging.getLogger(__name__)


class UnionMountSource(MountSource):
    def __init__(
        self,
        mountSources: Sequence[MountSource],
        maxCacheDepth: int = 1024,
        maxCacheEntries: int = 100000,
        maxSecondsToCache: float = 60,
        # pylint: disable=unused-argument
        **options,
    ) -> None:
        """
        mountSources:
            List of mount sources for which to show a union view. The rightmost mount sources have
            the highest precedence. Meaning, if a file with the same name exists in multiple mount
            sources, then by default the file of the rightmost mount source will be returned.
        maxCacheEntries:
            Even assuming very long file paths like 1000 chars, the cache size
            will be below 100 MB if the maximum number of elements is 100k.
        maxSecondsToCache:
            Another problem is the setup time, as it might take ~0.001s for each lookup call
            and it shouldn't take minutes! Note that there always can be an edge case with hundred
            thousands of files in one folder, which can take an arbitrary amount of time to cache.
        """
        self.mountSources: list[MountSource] = list(mountSources)
        self.folderCache: dict[str, list[MountSource]] = {"/": self.mountSources}
        self.folderCacheDepth = 0  # depth 1 means, we only cached top-level directories.
        self.rootFileInfo = create_root_file_info(userdata=[None])

        if len(self.mountSources) > 1:
            self._build_folder_cache(maxCacheDepth, maxCacheEntries, maxSecondsToCache)

    def _build_folder_cache(self, maxCacheDepth: int, maxCacheEntries: int, maxSecondsToCache: float) -> None:
        t0 = time.time()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Building cache for union mount (timeout after {maxSecondsToCache}s)...")

        self.folderCache = {"/": [m for m in self.mountSources if m.is_immutable()]}

        lastFolderCache: dict[str, list[MountSource]] = self.folderCache.copy()

        for depth in range(1, maxCacheDepth):
            # This intermediary structure is used because:
            #   1. We need to only iterate over the newly added folders in the next step
            #   2. We always want to (atomically) merge results for one folder depth so that we can be sure
            #      that if a folder of a cached depth can not be found in the cache that it does not exist at all.
            newFolderCache: dict[str, list[MountSource]] = {}

            for folder, mountSources in lastFolderCache.items():
                for mountSource in mountSources:
                    filesInFolder = mountSource.list(folder)
                    if not filesInFolder:
                        continue

                    for file in filesInFolder:
                        if time.time() - t0 > maxSecondsToCache or maxCacheEntries <= 0:
                            return

                        fullPath = os.path.join(folder, file)
                        fileInfo = mountSource.lookup(fullPath)
                        if not fileInfo or not stat.S_ISDIR(fileInfo.mode):
                            continue

                        maxCacheEntries -= 1

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

        if logger.isEnabledFor(logging.WARNING):
            print(
                f"Cached mount sources for {len(self.folderCache)} folders up to a depth of "
                f"{self.folderCacheDepth} in {t1 - t0:.3}s for faster union mount."
            )

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return all(m.is_immutable() for m in self.mountSources)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if not path.strip('/'):
            return self.rootFileInfo.clone()

        if path in self.folderCache:
            # This case might be triggered when path is a folder
            cachedMountSources = self.folderCache[path]
        elif self.folderCache and self.folderCacheDepth > 0 and path.startswith('/'):
            # This should be the most common case, i.e., for regular files. Look up the parent folder in this case.
            parentFolder = '/'.join(path.split('/', self.folderCacheDepth + 1)[:-1])
            if not parentFolder:
                parentFolder = '/'
            cachedMountSources = self.folderCache.get(parentFolder, [])
        else:
            cachedMountSources = self.mountSources

        mountSources = [m for m in self.mountSources if not m.is_immutable() or m in cachedMountSources]

        # We need to keep the sign of the fileVersion in order to forward it to SQLiteIndexedTar.
        # When the requested version can't be found in a mount source, increment negative specified versions
        # by the amount of versions in that mount source or decrement the initially positive version.
        if fileVersion <= 0:
            for mountSource in reversed(mountSources):
                fileInfo = mountSource.lookup(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion += mountSource.versions(path)
                if fileVersion > 0:
                    break

        else:  # fileVersion >= 1
            for mountSource in mountSources:
                fileInfo = mountSource.lookup(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion -= mountSource.versions(path)
                if fileVersion < 1:
                    break

        return None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return sum(mountSource.versions(path) for mountSource in self.mountSources)

    def _list(self, path: str, onlyMode: bool):
        files: Optional[Union[set[str], dict[str, FileInfo], dict[str, int]]] = None

        for mountSource in reversed(self.mountSources):
            result = mountSource.list_mode(path) if onlyMode else mountSource.list(path)

            if files is None:
                if isinstance(result, dict):
                    files = result
                elif result is not None:
                    files = set(result)

            elif isinstance(result, dict):
                if isinstance(files, dict):
                    files.update(result)
                else:
                    files = files.union(result.keys())

            elif result is not None:
                # If one of the mount sources does not return extended information,
                # then strip it from all others and only return the names.
                if isinstance(files, dict):
                    files = set(files.keys())
                files = files.union(result)

        return files

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        """
        Returns the set of all folder contents over all mount sources or None if the path was found in none of them.
        """
        return self._list(path, onlyMode=False)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        """
        Returns the set of all folder contents over all mount sources or None if the path was found in none of them.
        """
        return self._list(path, onlyMode=True)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.open(fileInfo, buffering=buffering)
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
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        mountSource = fileInfo.userdata.pop()
        try:
            return mountSource.list_xattr(fileInfo) if isinstance(mountSource, MountSource) else []
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        mountSource = fileInfo.userdata.pop()
        try:
            return mountSource.get_xattr(fileInfo, key) if isinstance(mountSource, MountSource) else None
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo) -> tuple[str, MountSource, FileInfo]:
        sourceFileInfo = fileInfo.clone()
        mountSource = sourceFileInfo.userdata.pop()

        if not isinstance(mountSource, MountSource):
            return '/', self, fileInfo

        # Because all mount sources are mounted at '/', we do not have to append
        # the mount point path returned by get_mount_source to the mount point '/'.
        return mountSource.get_mount_source(sourceFileInfo)

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        return merge_statfs([mountSource.statfs() for mountSource in self.mountSources])

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for mountSource in self.mountSources:
            mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def join_threads(self):
        for mountSource in self.mountSources:
            if hasattr(mountSource, 'join_threads'):
                mountSource.join_threads()
