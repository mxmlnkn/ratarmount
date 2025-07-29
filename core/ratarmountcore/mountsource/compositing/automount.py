import builtins
import logging
import os
import re
import stat
from collections.abc import Iterable
from dataclasses import dataclass
from typing import IO, Any, Optional, Union

from ratarmountcore.compressions import check_for_split_file_in, strip_suffix_from_archive
from ratarmountcore.mountsource import FileInfo, MountSource, merge_statfs
from ratarmountcore.mountsource.compositing.singlefile import SingleFileMountSource
from ratarmountcore.mountsource.factory import open_mount_source
from ratarmountcore.mountsource.formats.folder import FolderMountSource
from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar, SQLiteIndexedTarUserData
from ratarmountcore.StenciledFile import JoinedFileFromFactory
from ratarmountcore.utils import RatarmountError, determine_recursion_depth, overrides

logger = logging.getLogger(__name__)


class AutoMountLayer(MountSource):
    """
    This mount source takes another mount source and automatically shows the contents of files which are archives.
    The detailed behavior can be controlled using options.
    """

    __slots__ = ('mounted', 'options')

    _FIRST_SPLIT_EXTENSION_REGEX = re.compile("[.]([a]+|[A]+|0*[01])")

    @dataclass
    class MountInfo:
        mountSource: MountSource
        rootFileInfo: FileInfo
        recursionDepth: int

    def __init__(self, mountSource: MountSource, **options) -> None:
        if isinstance(mountSource, AutoMountLayer):
            # Stacking can make sense and should work when they have non-overlapping(!) recursion depths.
            raise RatarmountError("AutoMountLayer must not be stacked directly onto each other.")

        self.options = options
        self.maxRecursionDepth: int = determine_recursion_depth(**options)
        self.lazyMounting: bool = self.options.get('lazyMounting', False)

        rootFileInfo = mountSource.lookup('/')
        assert rootFileInfo
        rootFileInfo.userdata.append('/')

        # Mount points are specified without trailing slash and with leading slash
        # representing root of this mount source.
        # Disable false positive introduced when updating pylint from 2.6 to 2.12.
        # It now thinks that the assignment is to AutoMountLayer instead of self.mounted.
        # pylint: disable=used-before-assignment
        self.mounted: dict[str, AutoMountLayer.MountInfo] = {
            '/': AutoMountLayer.MountInfo(mountSource, rootFileInfo, 0)
        }

        if self.lazyMounting:
            return

        # Go over all files and mount archives and even archives in those archives
        foldersToWalk = ['/']
        while foldersToWalk:
            newFoldersToWalk = []
            for folder in foldersToWalk:
                if self.get_recursion_depth(folder) > self.maxRecursionDepth:
                    continue

                fileNames = self.list(folder)
                if not fileNames:
                    continue

                for fileName in fileNames:
                    filePath = os.path.join(folder, fileName)
                    if self.is_dir(filePath):
                        newFoldersToWalk.append(filePath)
                    else:
                        mountPoint = self._try_to_mount_file(filePath)
                        if mountPoint:
                            newFoldersToWalk.append(mountPoint)

            foldersToWalk = newFoldersToWalk

    def _simply_find_mounted(self, path: str) -> tuple[str, str]:
        """See _find_mounted. This is split off to avoid convoluted recursions during lazy mounting."""

        leftPart = path
        rightParts: list[str] = []
        while '/' in leftPart:
            if leftPart in self.mounted:
                return leftPart, '/' + '/'.join(rightParts)

            parts = leftPart.rsplit('/', 1)
            leftPart = parts[0]
            rightParts.insert(0, parts[1])

        assert '/' in self.mounted
        return '/', path

    def get_recursion_depth(self, path: str) -> int:
        mountPoint, pathInMountPoint = self._simply_find_mounted(path)
        mountInfo = self.mounted[mountPoint]
        fileInfo = mountInfo.mountSource.lookup(pathInMountPoint)

        # +1 because, by definition each mount source adds one recursion.
        # There can be no passthrough MountSource here because they are only created on archives..
        return mountInfo.recursionDepth + (
            sum(
                userdata.recursiondepth
                for userdata in fileInfo.userdata
                if isinstance(userdata, SQLiteIndexedTarUserData)
            )
            + 1
            if fileInfo
            else 0
        )

    def _try_to_mount_file(self, path: str) -> Optional[str]:
        """
        Returns the mount point path if it has been successfully mounted.
        path: Path inside this mount source. May include recursively mounted mount points.
              Should contain a leading slash.
        """

        recursionDepth = self.get_recursion_depth(path)
        if recursionDepth > self.maxRecursionDepth:
            return None

        # For better performance, only look at the suffix not at the magic bytes.
        strippedFilePath = strip_suffix_from_archive(path)
        maybeSplitFile = strippedFilePath == path
        if maybeSplitFile:
            # Do this manual check first to avoid an expensive MountSource.list call.
            strippedFilePath, extension = os.path.splitext(path)
            if not AutoMountLayer._FIRST_SPLIT_EXTENSION_REGEX.fullmatch(extension):
                return None
        if strippedFilePath == path:
            return None

        # Determine the mount point and check whether it already is mounted!
        mountPoint = strippedFilePath if self.options.get('stripRecursiveTarExtension', False) else path
        # https://unix.stackexchange.com/questions/655155/
        #   how-to-repeatedly-unpack-tar-gz-files-that-are-within-the-tar-gz-itself
        if 'transformRecursiveMountPoint' in self.options:
            pattern = self.options['transformRecursiveMountPoint']
            if isinstance(pattern, (tuple, list)) and len(pattern) == 2:
                mountPoint = '/' + re.sub(pattern[0], pattern[1], mountPoint).lstrip('/')
        if mountPoint in self.mounted:
            return None

        # Use _simply_find_mounted instead of _find_mounted or self.open to avoid recursions caused by lazy mounting!
        parentMountPoint, pathInsideParentMountPoint = self._simply_find_mounted(path)
        parentMountInfo = self.mounted[parentMountPoint]
        parentMountSource = parentMountInfo.mountSource

        archiveFileInfo = parentMountSource.lookup(pathInsideParentMountPoint)
        if archiveFileInfo is None:
            return None

        # Do not mount uncompressed TARs inside SQLiteIndexedTar when they already were mounted recursively!
        mountSourceResult = parentMountSource.get_mount_source(archiveFileInfo)
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

        # Now comes the expensive final check for a split file after we did all the cheaper checks.
        # We need to list the whole parent folder to successfully check for split files.
        joinedFile: Optional[IO[bytes]] = None
        if maybeSplitFile:
            parentFolder, splitCandidateName = os.path.split(path)
            listResult = parentMountSource.list_mode(parentFolder)
            if not listResult:
                return None

            parentFolderList = listResult.keys() if isinstance(listResult, dict) else listResult
            splitFileResult = check_for_split_file_in(splitCandidateName, parentFolderList)
            if not splitFileResult:
                return None

            filePaths = ('/' + f'{parentFolder}/{part}'.lstrip('/') for part in splitFileResult[0])

            def open_file(filePath: str):
                fileInfo = parentMountSource.lookup(filePath)
                if not fileInfo:
                    raise RatarmountError(f"Could not open file {filePath} in mount source {mountSource}!")
                return parentMountSource.open(fileInfo)

            joinedFile = JoinedFileFromFactory(
                [(lambda filePath=filePath: open_file(filePath)) for filePath in filePaths]  # type: ignore
            )

        try:
            options = self.options.copy()
            options['recursionDepth'] = max(0, self.maxRecursionDepth - recursionDepth)

            _, deepestMountSource, deepestFileInfo = parentMountSource.get_mount_source(archiveFileInfo)
            if joinedFile:
                mountSource: MountSource = SingleFileMountSource(os.path.split(mountPoint)[1], joinedFile)
            elif isinstance(deepestMountSource, FolderMountSource):
                # Open from file path on host file system in order to write out TAR index files.
                # Care has to be taken if a folder is bind mounted onto itself because then it can happen that
                # the file open triggers a recursive FUSE call, which then hangs up everything.
                mountSource = open_mount_source(deepestMountSource.get_file_path(deepestFileInfo), **options)
            else:
                # This will fail with StenciledFile objects as returned by SQLiteIndexedTar mount sources and when
                # given to backends like indexed_zstd, which do expect the file object to have a valid fileno.
                mountSource = open_mount_source(
                    parentMountSource.open(archiveFileInfo),
                    tarFileName=pathInsideParentMountPoint.rsplit('/', 1)[-1],
                    **options,
                )
        except Exception as exception:
            logger.warning(
                "Mounting of '%s' failed because of: %s", path, exception, exc_info=logger.isEnabledFor(logging.DEBUG)
            )
            return None

        rootFileInfo = archiveFileInfo.clone()
        rootFileInfo.mode = (rootFileInfo.mode & 0o777) | stat.S_IFDIR
        rootFileInfo.linkname = ""
        rootFileInfo.userdata = [mountPoint]
        mountInfo = AutoMountLayer.MountInfo(mountSource, rootFileInfo, recursionDepth)

        # TODO What if the mount point already exists, e.g., because stripRecursiveTarExtension is true and there
        #      are multiple archives with the same name but different extensions?
        self.mounted[mountPoint] = mountInfo
        logger.info("Recursively mounted: %s", mountPoint)

        return mountPoint

    def _find_mounted(self, path: str) -> tuple[str, str]:
        """
        Returns the mount point, which can be found in self.mounted, and the rest of the path.
        Basically, it splits path at the appropriate mount point boundary.
        Because of the recursive mounting, there might be multiple mount points fitting the path.
        The longest, i.e., the deepest mount point will be returned.
        """

        if self.maxRecursionDepth and self.lazyMounting:
            subPath = "/"
            # First go from higher paths to deeper ones and try to mount all parent archives lazily.
            for part in path.lstrip('/').split('/'):
                subPath = os.path.join(subPath, part)

                if self.get_recursion_depth(subPath) > self.maxRecursionDepth:
                    break

                if subPath not in self.mounted:
                    self._try_to_mount_file(subPath)

        return self._simply_find_mounted(path)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return self.mounted['/'].mountSource.is_immutable()

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Return file info for given path. Note that all returned file infos contain MountInfo
        or a file path string at the back of FileInfo.userdata.
        """
        # TODO: Add support for the .versions API in order to access the underlying TARs if stripRecursiveTarExtension
        #       is false? Then again, SQLiteIndexedTar is not able to do this either, so it might be inconsistent.

        # It might be arguably that we could simply let the mount source handle returning file infos for the root
        # directory but only we know the permissions of the parent folder and can apply them to the root directory.
        mountPoint, pathInMountPoint = self._find_mounted(path)
        mountInfo = self.mounted[mountPoint]

        originalFileVersions = 0
        if mountPoint != '/' and pathInMountPoint == '/':
            originalFileVersions = self.mounted['/'].mountSource.versions(path)

        def normalize_file_version(version, versions):
            return ((version - 1) % versions + 1) % versions if versions > 1 else version

        # fileVersion=0 is the most recent. Version 1..fileVersions number from the first occurrence / oldest
        # version to the most recent, i.e., fileVersion = 0 is equivalent to fileVersion = fileVersions.
        fileVersions = self.versions(path)
        fileVersion = normalize_file_version(fileVersion, fileVersions)
        if fileVersion == 0 and pathInMountPoint == '/':
            return mountInfo.rootFileInfo.clone()

        if fileVersions <= 1 or pathInMountPoint != '/' or fileVersion == 0 or fileVersion > originalFileVersions:
            fileInfo = mountInfo.mountSource.lookup(pathInMountPoint, fileVersion - originalFileVersions)
            if fileInfo:
                fileInfo.userdata.append(mountPoint)
            return fileInfo

        # We are here if: fileVersions > 1 and 0 < fileVersion <= originalFileVersions and pathInMountPoint == '/'
        fileInfo = self.mounted['/'].mountSource.lookup(path, fileVersion % originalFileVersions)
        if fileInfo:
            fileInfo.userdata.append('/')
        return fileInfo

    def _append_mount_points(self, path: str, files, onlyMode: bool):
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
                        files.update(
                            {folderName: mountInfo.rootFileInfo.mode if onlyMode else mountInfo.rootFileInfo.clone()}
                        )

        return files

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        mountPoint, pathInMountPoint = self._find_mounted(path)
        return self._append_mount_points(
            path, self.mounted[mountPoint].mountSource.list(pathInMountPoint), onlyMode=False
        )

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        mountPoint, pathInMountPoint = self._find_mounted(path)
        return self._append_mount_points(
            path, self.mounted[mountPoint].mountSource.list_mode(pathInMountPoint), onlyMode=True
        )

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        mountPoint, pathInMountPoint = self._find_mounted(path)
        fileVersions = self.mounted[mountPoint].mountSource.versions(pathInMountPoint)
        if mountPoint != '/' and pathInMountPoint == '/':
            fileVersions += self.mounted['/'].mountSource.versions(path)
        return fileVersions

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        _, mountSource, sourceFileInfo = self.get_mount_source(fileInfo)
        return mountSource.open(sourceFileInfo, buffering=buffering)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        _, mountSource, sourceFileInfo = self.get_mount_source(fileInfo)
        return mountSource.read(sourceFileInfo, size, offset)

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        mountPoint = fileInfo.userdata[-1]
        assert isinstance(mountPoint, str)
        if fileInfo == self.mounted[mountPoint].rootFileInfo:
            return []

        _, mountSource, sourceFileInfo = self.get_mount_source(fileInfo)
        return mountSource.list_xattr(sourceFileInfo)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        mountPoint = fileInfo.userdata[-1]
        assert isinstance(mountPoint, str)
        if fileInfo == self.mounted[mountPoint].rootFileInfo:
            return None

        _, mountSource, sourceFileInfo = self.get_mount_source(fileInfo)
        return mountSource.get_xattr(sourceFileInfo, key)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo) -> tuple[str, MountSource, FileInfo]:
        mountPoint = fileInfo.userdata[-1]
        assert isinstance(mountPoint, str)
        mountSource = self.mounted[mountPoint].mountSource

        sourceFileInfo = fileInfo.clone()
        sourceFileInfo.userdata.pop()

        deeperMountPoint, deeperMountSource, deeperFileInfo = mountSource.get_mount_source(sourceFileInfo)
        return os.path.join(mountPoint, deeperMountPoint.lstrip('/')), deeperMountSource, deeperFileInfo

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        return merge_statfs([mountInfo.mountSource.statfs() for _, mountInfo in self.mounted.items()])

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        for mountInfo in self.mounted.values():
            mountInfo.mountSource.__exit__(exception_type, exception_value, exception_traceback)

    def join_threads(self):
        for mountInfo in self.mounted.values():
            if hasattr(mountInfo.mountSource, 'join_threads'):
                mountInfo.mountSource.join_threads()
