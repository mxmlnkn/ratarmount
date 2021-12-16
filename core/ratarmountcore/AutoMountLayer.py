#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat
import time
import traceback

from dataclasses import dataclass
from typing import Dict, IO, Iterable, List, Optional, Tuple

from .compressions import stripSuffixFromTarFile
from .factory import openMountSource
from .FolderMountSource import FolderMountSource
from .MountSource import FileInfo, MountSource
from .utils import overrides


class AutoMountLayer(MountSource):
    """
    This mount source takes another mount source and automatically shows the contents of files which are archives.
    The detailed behavior can be controlled using options.
    """

    __slots__ = ('mounted', 'options')

    @dataclass
    class MountInfo:
        mountSource: MountSource
        rootFileInfo: FileInfo

    def __init__(self, mountSource: MountSource, **options) -> None:
        self.options = options
        self.printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

        rootFileInfo = FileInfo(
            # fmt: off
            size         = 0,
            mtime        = int(time.time()),
            mode         = 0o555 | stat.S_IFDIR,
            linkname     = "",
            uid          = os.getuid(),
            gid          = os.getgid(),
            userdata     = ['/'],
            # fmt: on
        )

        # Mount points are specified without trailing slash and with leading slash
        # representing root of this mount source.
        # Disable false positive introduced when updating pylint from 2.6 to 2.12.
        # It now thinks that the assignment is to AutoMountLayer instead of self.mounted.
        # pylint: disable=used-before-assignment
        self.mounted: Dict[str, AutoMountLayer.MountInfo] = {'/': AutoMountLayer.MountInfo(mountSource, rootFileInfo)}

        if not self.options.get('recursive', False) or self.options.get('lazyMounting', False):
            return

        # Go over all files and mount archives and even archives in those archives
        foldersToWalk = ['/']
        while foldersToWalk:
            folder = foldersToWalk.pop()
            fileNames = self.listDir(folder)
            if not fileNames:
                continue

            for fileName in fileNames:
                filePath = os.path.join(folder, fileName)
                if self.isdir(filePath):
                    foldersToWalk.append(filePath)
                else:
                    mountPoint = self._tryToMountFile(filePath)
                    if mountPoint:
                        foldersToWalk.append(mountPoint)

    def _simplyFindMounted(self, path: str) -> Tuple[str, str]:
        """See _findMounted. This is split off to avoid convoluted recursions during lazy mounting."""

        leftPart = path
        rightParts: List[str] = []
        while '/' in leftPart:
            if leftPart in self.mounted:
                return leftPart, '/' + '/'.join(rightParts)

            parts = leftPart.rsplit('/', 1)
            leftPart = parts[0]
            rightParts.insert(0, parts[1])

        assert '/' in self.mounted
        return '/', path

    def _tryToMountFile(self, path: str) -> Optional[str]:
        """
        Returns the mount point path if it has been successfully mounted.
        path: Path inside this mount source. May include recursively mounted mount points.
              Should contain a leading slash.
        """

        # For better performance, only look at the suffix not at the magic bytes.
        strippedFilePath = stripSuffixFromTarFile(path)
        if strippedFilePath == path:
            return None

        mountPoint = strippedFilePath if self.options.get('stripRecursiveTarExtension', False) else path
        if mountPoint in self.mounted:
            return None

        # Use _simplyFindMounted instead of _findMounted or self.open to avoid recursions caused by lazy mounting!
        parentMountPoint, pathInsideParentMountPoint = self._simplyFindMounted(path)
        parentMountSource = self.mounted[parentMountPoint].mountSource

        try:
            archiveFileInfo = parentMountSource.getFileInfo(pathInsideParentMountPoint)
            if archiveFileInfo is None:
                return None

            _, deepestMountSource, deepestFileInfo = parentMountSource.getMountSource(archiveFileInfo)
            if isinstance(deepestMountSource, FolderMountSource):
                # Open from file path on host file system in order to write out TAR index files.
                mountSource = openMountSource(deepestMountSource.getFilePath(deepestFileInfo), **self.options)
            else:
                # This will fail with StenciledFile objects as returned by SQLiteIndexedTar mount sources and when
                # given to backends like indexed_xxx, which do expect the file object to have a valid fileno.
                mountSource = openMountSource(
                    parentMountSource.open(archiveFileInfo),
                    tarFileName=pathInsideParentMountPoint.rsplit('/', 1)[-1],
                    **self.options
                )
        except Exception as e:
            print("[Warning] Mounting of '" + path + "' failed because of:", e)
            if self.printDebug >= 3:
                traceback.print_exc()
            print()
            return None

        rootFileInfo = archiveFileInfo.clone()
        rootFileInfo.mode = (rootFileInfo.mode & 0o777) | stat.S_IFDIR
        rootFileInfo.linkname = ""
        rootFileInfo.userdata = [mountPoint]
        mountInfo = AutoMountLayer.MountInfo(mountSource, rootFileInfo)

        # TODO What if the mount point already exists, e.g., because stripRecursiveTarExtension is true and there
        #      are multiple archives with the same name but different extesions?
        self.mounted[mountPoint] = mountInfo
        if self.printDebug >= 2:
            print("Recursively mounted:", mountPoint)
            print()

        return mountPoint

    def _findMounted(self, path: str) -> Tuple[str, str]:
        """
        Returns the mount point, which can be found in self.mounted, and the rest of the path.
        Basically, it splits path at the appropriate mount point boundary.
        Because of the recursive mounting, there might be multiple mount points fitting the path.
        The longest, i.e., the deepest mount point will be returned.
        """

        if self.options.get('recursive', False) and self.options.get('lazyMounting', False):
            subPath = "/"
            # First go from higher paths to deeper ones and try to mount all parent archives lazily.
            for part in path.lstrip(os.path.sep).split(os.path.sep):
                subPath = os.path.join(subPath, part)
                if subPath not in self.mounted:
                    self._tryToMountFile(subPath)

        return self._simplyFindMounted(path)

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Return file info for given path. Note that all returned file infos contain MountInfo
        or a file path string at the back of FileInfo.userdata.
        """
        # TODO: Add support for the .versions API in order to access the underlying TARs if stripRecursiveTarExtension
        #       is false? Then again, SQLiteIndexedTar is not able to do this either, so it might be inconsistent.

        # It might be arguably that we could simply let the mount source handle returning file infos for the root
        # directory but only we know the permissions of the parent folder and can apply them to the root directory.
        mountPoint, pathInMountPoint = self._findMounted(path)
        mountInfo = self.mounted[mountPoint]
        if pathInMountPoint == '/':
            return mountInfo.rootFileInfo

        fileInfo = mountInfo.mountSource.getFileInfo(pathInMountPoint, fileVersion)
        if fileInfo:
            fileInfo.userdata.append(mountPoint)
            return fileInfo

        return None

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        mountPoint, pathInMountPoint = self._findMounted(path)
        files = self.mounted[mountPoint].mountSource.listDir(pathInMountPoint)
        if not files:
            return None
        files = set(files)

        # Check whether we need to add recursive mount points to this directory listing
        if self.options.get('recursive', False) and self.options.get('stripRecursiveTarExtension', False):
            for mountPoint in self.mounted:
                folder, folderName = os.path.split(mountPoint)
                if folder == path and folderName and folderName not in files:
                    files.add(folderName)

        return files

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        mountPoint, pathInMountPoint = self._findMounted(path)
        return self.mounted[mountPoint].mountSource.fileVersions(pathInMountPoint)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        _, mountSource, sourceFileInfo = self.getMountSource(fileInfo)
        return mountSource.open(sourceFileInfo)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        _, mountSource, sourceFileInfo = self.getMountSource(fileInfo)
        return mountSource.read(sourceFileInfo, size, offset)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        mountPoint = fileInfo.userdata[-1]
        assert isinstance(mountPoint, str)
        mountSource = self.mounted[mountPoint].mountSource

        sourceFileInfo = fileInfo.clone()
        sourceFileInfo.userdata.pop()

        deeperMountPoint, deeperMountSource, deeperFileInfo = mountSource.getMountSource(sourceFileInfo)
        return os.path.join(mountPoint, deeperMountPoint.lstrip('/')), deeperMountSource, deeperFileInfo
