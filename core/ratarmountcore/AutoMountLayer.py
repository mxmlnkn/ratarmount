#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import stat
import traceback

from dataclasses import dataclass
from typing import Dict, IO, Iterable, List, Optional, Tuple, Union

from .compressions import stripSuffixFromTarFile
from .factory import openMountSource
from .FolderMountSource import FolderMountSource
from .MountSource import FileInfo, MountSource, createRootFileInfo
from .SQLiteIndexedTar import SQLiteIndexedTar, SQLiteIndexedTarUserData
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
        self.recursionDepth: int = -1 if self.options.get('recursive', False) else 0
        if 'recursionDepth' in self.options:
            self.recursionDepth = int(self.options['recursionDepth'])
        self.lazyMounting: bool = self.options.get('lazyMounting', False)
        self.printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

        rootFileInfo = createRootFileInfo(userdata=['/'])

        # Mount points are specified without trailing slash and with leading slash
        # representing root of this mount source.
        # Disable false positive introduced when updating pylint from 2.6 to 2.12.
        # It now thinks that the assignment is to AutoMountLayer instead of self.mounted.
        # pylint: disable=used-before-assignment
        self.mounted: Dict[str, AutoMountLayer.MountInfo] = {'/': AutoMountLayer.MountInfo(mountSource, rootFileInfo)}

        if self.lazyMounting:
            return

        # Go over all files and mount archives and even archives in those archives
        foldersToWalk = ['/']
        recursionDepth = 0
        while foldersToWalk and (recursionDepth < self.recursionDepth or self.recursionDepth < 0):
            newFoldersToWalk = []
            for folder in foldersToWalk:
                fileNames = self.listDir(folder)
                if not fileNames:
                    continue

                for fileName in fileNames:
                    filePath = os.path.join(folder, fileName)
                    if self.isdir(filePath):
                        newFoldersToWalk.append(filePath)
                    else:
                        mountPoint = self._tryToMountFile(filePath)
                        if mountPoint:
                            newFoldersToWalk.append(mountPoint)

            recursionDepth += 1
            foldersToWalk = newFoldersToWalk

    def _getRecursionDepth(self, path: str) -> int:
        parts = path.split('/')
        mountLayers = 0
        for depth in range(len(parts)):
            if '/'.join(parts[:depth]) in self.mounted:
                mountLayers += 1
        return mountLayers

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

        recursionDepth = self._getRecursionDepth(path)
        if self.recursionDepth >= 0 and recursionDepth > self.recursionDepth:
            return None

        mountPoint = strippedFilePath if self.options.get('stripRecursiveTarExtension', False) else path
        # https://unix.stackexchange.com/questions/655155/how-to-repeatedly-unpack-tar-gz-files-that-are-within-the-tar-gz-itself
        if 'transformRecursiveMountPoint' in self.options:
            pattern = self.options['transformRecursiveMountPoint']
            if isinstance(pattern, (tuple, list)) and len(pattern) == 2:
                mountPoint = '/' + re.sub(pattern[0], pattern[1], mountPoint).lstrip('/')

        if mountPoint in self.mounted:
            return None

        # Use _simplyFindMounted instead of _findMounted or self.open to avoid recursions caused by lazy mounting!
        parentMountPoint, pathInsideParentMountPoint = self._simplyFindMounted(path)
        parentMountSource = self.mounted[parentMountPoint].mountSource

        archiveFileInfo = parentMountSource.getFileInfo(pathInsideParentMountPoint)
        if archiveFileInfo is None:
            return None

        # Do not mount uncompressed TARs inside SQLiteIndexedTar when they already were mounted recursively!
        mountSourceResult = parentMountSource.getMountSource(archiveFileInfo)
        if mountSourceResult:
            realMountSource = mountSourceResult[1]
            if (
                isinstance(realMountSource, SQLiteIndexedTar)
                and realMountSource.mountRecursively
                and archiveFileInfo.userdata
            ):
                indexedTarData = archiveFileInfo.userdata[0]
                if isinstance(indexedTarData, SQLiteIndexedTarUserData) and indexedTarData.istar:
                    return None

        try:
            options = self.options.copy()
            options['recursive'] = recursionDepth + 1 < self.recursionDepth or self.recursionDepth < 0

            _, deepestMountSource, deepestFileInfo = parentMountSource.getMountSource(archiveFileInfo)
            if isinstance(deepestMountSource, FolderMountSource):
                # Open from file path on host file system in order to write out TAR index files.
                # Care has to be taken if a folder is bind mounted onto itself because then it can happen that
                # the file open triggers a recursive FUSE call, which then hangs up everything.
                mountSource = openMountSource(deepestMountSource.getFilePath(deepestFileInfo), **options)
            else:
                # This will fail with StenciledFile objects as returned by SQLiteIndexedTar mount sources and when
                # given to backends like indexed_zstd, which do expect the file object to have a valid fileno.
                mountSource = openMountSource(
                    parentMountSource.open(archiveFileInfo),
                    tarFileName=pathInsideParentMountPoint.rsplit('/', 1)[-1],
                    **options
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
        #      are multiple archives with the same name but different extensions?
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

        if self.recursionDepth and self.lazyMounting:
            subPath = "/"
            # First go from higher paths to deeper ones and try to mount all parent archives lazily.
            for part in path.lstrip('/').split('/'):
                subPath = os.path.join(subPath, part)

                if self.recursionDepth >= 0 and self._getRecursionDepth(subPath) > self.recursionDepth:
                    break

                if subPath not in self.mounted:
                    self._tryToMountFile(subPath)

        return self._simplyFindMounted(path)

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return self.mounted['/'].mountSource.isImmutable()

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

        originalFileVersions = 0
        if mountPoint != '/' and pathInMountPoint == '/':
            originalFileVersions = self.mounted['/'].mountSource.fileVersions(path)

        def normalizeFileVersion(version, versions):
            return ((version - 1) % versions + 1) % versions if versions > 1 else version

        # fileVersion=0 is the most recent. Version 1..fileVersions number from the first occurrence / oldest
        # version to the most recent, i.e., fileVersion = 0 is equivalent to fileVersion = fileVersions.
        fileVersions = self.fileVersions(path)
        fileVersion = normalizeFileVersion(fileVersion, fileVersions)
        if fileVersion == 0 and pathInMountPoint == '/':
            return mountInfo.rootFileInfo

        if fileVersions <= 1 or pathInMountPoint != '/' or fileVersion == 0 or fileVersion > originalFileVersions:
            fileInfo = mountInfo.mountSource.getFileInfo(pathInMountPoint, fileVersion - originalFileVersions)
            if fileInfo:
                fileInfo.userdata.append(mountPoint)
            return fileInfo

        # We are here if: fileVersions > 1 and 0 < fileVersion <= originalFileVersions and pathInMountPoint == '/'
        fileInfo = self.mounted['/'].mountSource.getFileInfo(path, fileVersion % originalFileVersions)
        if fileInfo:
            fileInfo.userdata.append('/')
        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        mountPoint, pathInMountPoint = self._findMounted(path)
        files = self.mounted[mountPoint].mountSource.listDir(pathInMountPoint)
        if not files:
            return None

        if not isinstance(files, dict):
            files = set(files)

        # Check whether we need to add recursive mount points to this directory listing
        # The outer if is only a performance optimization. In general, it should be possible to remove it.
        # In case that 'files' also contains stat results, we might have to overwrite some results with
        # the results for a mount point!
        if (
            not isinstance(files, set)
            or self.options.get('stripRecursiveTarExtension', False)
            or self.options.get('transformRecursiveMountPoint', False)
        ):
            for mountPoint, mountInfo in self.mounted.items():
                folder, folderName = os.path.split(mountPoint)
                # This also potentially updates a archive file stat result with a stat result for a folder type!
                if folder == path and folderName and folderName:
                    if isinstance(files, set):
                        files.add(folderName)
                    else:
                        files.update({folderName: mountInfo.rootFileInfo})

        return files

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        mountPoint, pathInMountPoint = self._findMounted(path)
        fileVersions = self.mounted[mountPoint].mountSource.fileVersions(pathInMountPoint)
        if mountPoint != '/' and pathInMountPoint == '/':
            fileVersions += self.mounted['/'].mountSource.fileVersions(path)
        return fileVersions

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

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for _, mountInfo in self.mounted.items():
            mountInfo.mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def joinThreads(self):
        for _, mountInfo in self.mounted.items():
            if hasattr(mountInfo.mountSource, 'joinThreads'):
                mountInfo.mountSource.joinThreads()
