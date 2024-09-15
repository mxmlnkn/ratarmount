#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import errno
import os
import stat
from typing import Dict, IO, Iterable, Optional, Union

from .MountSource import FileInfo, MountSource
from .utils import overrides

try:
    from pyfatfs.FatIO import FatIO
    from pyfatfs.PyFat import PyFat
    from pyfatfs import PyFATException
except ImportError:
    FatIO = None  # type: ignore
    PyFat = None  # type: ignore


class FATMountSource(MountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileSystem = PyFat()
        if isinstance(fileOrPath, str):
            # TODO Probably good idea for performance on Lustre to open the file unbuffered.
            self.fileSystem.open(fileOrPath, read_only=True)
        else:
            self.fileSystem.set_fp(fileOrPath)
        self.options = options

    @staticmethod
    def _convertFATDirectoryEntryToFileInfo(entry, path) -> FileInfo:
        """
        entry: of type pyfatfs.FATDirectoryEntry.FATDirectoryEntry.
        """
        mode = 0o555 | (stat.S_IFDIR if entry.is_directory() else stat.S_IFREG)

        return FileInfo(
            # fmt: off
            size     = entry.filesize,
            mtime    = entry.get_mtime().timestamp(),
            mode     = mode,
            linkname = "",  # FAT has no support for hard or symbolic links
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [path],
            # fmt: on
        )

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        try:
            self.fileSystem.root_dir.get_entry(path)
        except PyFATException as exception:
            if exception.errno == errno.ENOENT:
                return False
            raise exception
        return True

    def _listDir(self, path: str) -> Optional[Iterable]:
        try:
            directories, files, _ = self.fileSystem.root_dir.get_entry(os.path.normpath(path)).get_entries()
        except PyFATException as exception:
            if exception.errno in [errno.ENOENT, errno.ENOTDIR]:
                return None
            raise exception
        return [str(entry) for entry in directories + files]

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        # TODO I think with the low-level API, we could also get the FileInfos
        return self._listDir(path)

    @overrides(MountSource)
    def listDirModeOnly(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        # TODO I think with the low-level API, we could also get the FileInfos
        return self._listDir(path)

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        try:
            entry = self.fileSystem.root_dir.get_entry(path)
        except PyFATException as exception:
            if exception.errno in [errno.ENOTDIR, errno.ENOENT]:
                return None
            raise exception
        return self._convertFATDirectoryEntryToFileInfo(entry, path)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        # TODO There is no option in FatIO to configure the buffering yet.
        return FatIO(self.fileSystem, path)

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileSystem.close()
