#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import stat


from typing import IO, Iterable, Optional, Tuple

from .MountSource import FileInfo, MountSource
from .utils import overrides


class FileVersionLayer(MountSource):
    """
    This bind mount like layer makes it possible to access older file versions if there multiple ones in the given
    mount source. The interface provides for each file <file path> a hidden folder <file path.versions> containing
    all available versions.

    This class also resolves hardlinks. This functionality is mixed in here because self-referencing hardlinks
    should be resolved by showing older versions of a file and only this layer knows about file versioning.

    TODO If there already exists a file <file path.versions> then this special folder will not be available!
    """

    def __init__(self, mountSource: MountSource):
        self.mountSource: MountSource = mountSource

    def _decodeVersionsPathAPI(self, filePath: str) -> Optional[Tuple[str, bool, int]]:
        """
        Do a loop over the parent path parts to resolve possible versions in parent folders.
        Note that multiple versions of a folder always are union mounted. So, for the path to a file
        inside those folders the exact version of a parent folder can simply be removed for lookup.
        Therefore, translate something like: /foo.version/3/bar.version/2/mimi.version/1 into
        /foo/bar/mimi.version/1
        This is possibly time-costly but requesting a different version from the most recent should
        be a rare occurence and FUSE also checks all parent parts before accessing a file so it
        might only slow down access by roughly factor 2.
        """

        # TODO make it work for files ending with '.versions'.
        # Currently, this feature would be hidden by those files. But, I think this should be quite rare.
        # I could allow arbitrary amounts of dots like '....versions' but then it wouldn't be discernible
        # for ...versions whether the versions of ..versions or .versions file was requested. I could add
        # a rule for the decision, like ...versions shows the versions of .versions and ....versions for
        # ..versions, however, all of this might require an awful lot of file existence checking.
        # My first idea was to use hidden subfolders for each file like path/to/file/.versions/1 but FUSE
        # checks the parents in a path that they are directories first, so getattr or readdir is not even
        # called for path/to/file/.versions if path/to/file is not a directory.
        # Another alternative might be one hidden folder at the root for a parallel file tree, like
        # /.versions/path/to/file/3 but that runs into similar problems when trying to specify the file
        # version or if a .versions root directory exists.

        filePathParts = filePath.lstrip('/').split('/')
        filePath = ''
        pathIsSpecialVersionsFolder = False
        fileVersion = None  # Not valid if None or parentIsVersions is True
        for part in filePathParts:
            # Skip over the exact version specified
            if pathIsSpecialVersionsFolder:
                try:
                    fileVersion = int(part)
                    assert str(fileVersion) == part
                except Exception:
                    return None
                pathIsSpecialVersionsFolder = False
                continue

            # Simply append normal existing folders
            tmpFilePath = '/'.join([filePath, part])
            if self.mountSource.getFileInfo(tmpFilePath):
                filePath = tmpFilePath
                fileVersion = 0
                continue

            # If current path does not exist, check if it is a special versions path
            if part.endswith('.versions') and len(part) > len('.versions'):
                pathIsSpecialVersionsFolder = True
                fileVersion = 0
                filePath = tmpFilePath[: -len('.versions')]
                continue

            # Parent path does not exist and is not a versions path, so any subpaths also won't exist either
            return None

        if fileVersion is None:
            return None

        return filePath, pathIsSpecialVersionsFolder, (0 if pathIsSpecialVersionsFolder else fileVersion)

    @staticmethod
    def _isHardLink(fileInfo: FileInfo) -> bool:
        # Note that S_ISLNK checks for symbolic links. Hardlinks (at least from tarfile)
        # return false for S_ISLNK but still have a linkname!
        return bool(not stat.S_ISREG(fileInfo.mode) and not stat.S_ISLNK(fileInfo.mode) and fileInfo.linkname)

    @staticmethod
    def _resolveHardLinks(mountSource: MountSource, path: str) -> Optional[FileInfo]:
        """path : Simple path. Should contain no special versioning folders!"""

        fileInfo = mountSource.getFileInfo(path)
        if not fileInfo:
            return None

        resolvedPath = '/' + fileInfo.linkname.lstrip('/') if FileVersionLayer._isHardLink(fileInfo) else None
        fileVersion = 0
        hardLinkCount = 0

        while resolvedPath and hardLinkCount < 128:  # For comparison, the maximum symbolic link chain in Linux is 40.
            # Link targets are relative to the mount source. That's why we need the mount point to get the full path
            # in respect to this mount source. And we must a file info object for this mount source, so we have to
            # get that using the full path instead of calling getFileInfo on the deepest mount source.
            mountPoint, _, _ = mountSource.getMountSource(fileInfo)

            resolvedPath = os.path.join(mountPoint, resolvedPath.lstrip('/'))

            if resolvedPath != path:
                # The file version is only of importance to resolve self-references.
                # It seems undecidable to me whether to return the given fileVersion or 0 here.
                # Returning 0 would feel more correct because the we switched to another file and the version
                # for that file is the most recent one.
                # However, resetting the file version to 0 means that if there is a cycle, i.e., two hardlinks
                # of different names referencing each other, than the file version will always be reset to 0
                # and we have no break condition, entering an infinite loop.
                # The most correct version would be to track the version of each path in a map and count up the
                # version per path.
                # TODO Is such a hardlink cycle even possible?!
                fileVersion = 0
            else:
                # If file is referencing itself, try to access earlier version of it.
                # The check for fileVersion against the total number of available file versions is omitted because
                # that check is done implicitly inside the mount sources getFileInfo method!
                fileVersion = fileVersion + 1 if fileVersion >= 0 else fileVersion - 1

            path = resolvedPath
            fileInfo = mountSource.getFileInfo(path, fileVersion)
            if not fileInfo:
                return None

            resolvedPath = '/' + fileInfo.linkname.lstrip('/') if FileVersionLayer._isHardLink(fileInfo) else None
            hardLinkCount += 1

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        files = self.mountSource.listDir(path)
        if files is not None:
            return files

        # If no folder was found, check whether the special .versions folder was requested
        try:
            result = self._decodeVersionsPathAPI(path)
        except Exception:
            return None

        if not result:
            return None
        path, pathIsSpecialVersionsFolder, _ = result

        if not pathIsSpecialVersionsFolder:
            return self.mountSource.listDir(path)

        # Print all available versions of the file at filePath as the contents of the special '.versions' folder
        return [str(version + 1) for version in range(self.mountSource.fileVersions(path))]

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """Resolves special file version specifications in the path."""

        assert fileVersion == 0

        fileInfo = FileVersionLayer._resolveHardLinks(self.mountSource, path)
        if fileInfo:
            return fileInfo

        # If no file was found, check if a special .versions folder to an existing file/folder was queried.
        versionsInfo = self._decodeVersionsPathAPI(path)
        if not versionsInfo:
            return None
        path, pathIsSpecialVersionsFolder, fileVersion = versionsInfo

        # 2.) Check if the request was for the special .versions folder and return its contents or stats
        # At this point, path is assured to actually exist!
        if pathIsSpecialVersionsFolder:
            parentFileInfo = self.mountSource.getFileInfo(path)
            assert parentFileInfo

            fileInfo = FileInfo(
                # fmt: off
                size     = 0,
                mtime    = parentFileInfo.mtime,
                mode     = 0o777 | stat.S_IFDIR,
                linkname = "",
                uid      = parentFileInfo.uid,
                gid      = parentFileInfo.gid,
                # I think this does not matter because currently userdata is only used in read calls,
                # which should only be given non-directory files and this is a directory
                userdata = [],
                # fmt: on
            )

            return fileInfo

        # 3.) At this point the request is for an actually older version of a file or folder
        return self.mountSource.getFileInfo(path, fileVersion=fileVersion)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        # TODO return 1 for special .versions folders and files contained in there?
        return self.mountSource.fileVersions(path)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        return self.mountSource.open(fileInfo)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        return self.mountSource.read(fileInfo, size, offset)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        return self.mountSource.getMountSource(fileInfo)
