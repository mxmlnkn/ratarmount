#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os
import stat
import time
from typing import Dict, IO, Iterable, List, Optional, Union

from .MountSource import FileInfo, MountSource, createRootFileInfo
from .utils import overrides

try:
    import rarfile
except ImportError:
    pass


class RarMountSource(MountSource):
    # Basically copy paste of ZipMountSource because the interfaces are very similar
    # I'm honestly not sure how it works that well as it does. It does have some problems
    # when trying to mount .tar.bz2 or .tar.xz inside rar files recursively but it works
    # reasonably well for .tar.gz and .zip considering that seeking seems to be broken:
    # https://github.com/markokr/rarfile/issues/73

    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject = rarfile.RarFile(fileOrPath, 'r')
        RarMountSource._findPassword(self.fileObject, options.get("passwords", []))

        self.files = {RarMountSource._cleanPath(info.filename): info for info in self.fileObject.infolist()}
        self.options = options

    @staticmethod
    def _cleanPath(path):
        result = os.path.normpath(path) + ('/' if path.endswith('/') else '')
        while result.startswith('../'):
            result = result[3:]
        return result

    @staticmethod
    def _findPassword(fileobj: "rarfile.RarFile", passwords):
        if not fileobj.needs_password():
            return None

        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.infolist()
        if not files:
            for password in passwords:
                fileobj.setpassword(password)
                files = fileobj.infolist()
                if files:
                    return password

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if file.is_file()]
        if not files:
            return None
        for password in passwords:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except (rarfile.PasswordRequired, rarfile.BadRarFile):
                pass

        raise rarfile.PasswordRequired("Could not find a matching password!")

    @staticmethod
    def _convertToFileInfo(normalizedPath: str, info: "rarfile.RarInfo") -> FileInfo:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        if info.date_time:
            dtime = datetime.datetime(*info.date_time)
            dtime = dtime.replace(tzinfo=datetime.timezone.utc)
            mtime = dtime.timestamp() if info.date_time else 0
        else:
            mtime = 0

        # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
        linkname = ""
        if info.file_redir:
            linkname = info.file_redir[2]
            mode = 0o555 | stat.S_IFLNK

        fileInfo = FileInfo(
            # fmt: off
            size     = info.file_size,
            mtime    = mtime,
            mode     = mode,
            linkname = linkname,
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [(normalizedPath, info)],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    # TODO How to behave with files in archive with absolute paths? Currently, they would never be shown.
    @staticmethod
    def _getName(folderPath, filePath):
        if not filePath.startswith(folderPath):
            return None

        filePath = filePath[len(folderPath) :].strip('/')
        if not filePath:
            return None

        # This effectively adds all parent paths as folders. It is easy to create
        # RARs and ZIPs with nested files without information on the parent directories!
        if '/' in filePath:
            firstSlash = filePath.index('/')
            filePath = filePath[:firstSlash]

        return filePath

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        path = path.strip('/')
        if path:
            path += '/'

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return {
            self._getName(path, normalizedPath): self._convertToFileInfo(normalizedPath, info)
            for normalizedPath, info in self.files.items()
            if self._getName(path, normalizedPath)
        }

    @overrides(MountSource)
    def listDirModeOnly(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        path = path.strip('/')
        if path:
            path += '/'

        def _getMode(info: "rarfile.RarInfo") -> int:
            mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
            if info.file_redir:
                mode = 0o555 | stat.S_IFLNK
            return mode

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return {
            self._getName(path, normalizedPath): _getMode(info)
            for normalizedPath, info in self.files.items()
            if self._getName(path, normalizedPath)
        }

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'
        if pathAsDir == '/':
            return [createRootFileInfo(userdata=[None])]

        infoList = [
            RarMountSource._convertToFileInfo(normalizedPath, info)
            for normalizedPath, info in self.files.items()
            if normalizedPath.rstrip('/') == path.lstrip('/')
        ]

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any(info.userdata[-1][0] == pathAsDir for info in infoList) and any(
            normalizedPath.rstrip('/').startswith(pathAsDir) for normalizedPath, info in self.files.items()
        ):
            infoList.append(
                FileInfo(
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
            )

        return infoList

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        infos = self._getFileInfos(path)
        return infos[fileVersion] if -len(infos) <= fileVersion < len(infos) else None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return len(self._getFileInfos(path))

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # I do not see any obvious option to rarfile.RarFile to apply the specified buffer size.
        info = fileInfo.userdata[-1][1]
        assert isinstance(info, rarfile.RarInfo)
        return self.fileObject.open(info, 'r')

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileObject.close()
