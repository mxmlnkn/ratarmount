#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import stat
import threading
import time
from typing import Any, cast, Dict, IO, Iterable, Optional, Union

from .MountSource import FileInfo, MountSource
from .StenciledFile import RawStenciledFile, StenciledFile
from .utils import overrides


class SingleFileMountSource(MountSource):
    """MountSource exposing a single file as a mount source."""

    def __init__(self, path: str, fileobj: IO[bytes]):
        """
        fileobj: The given file object to be mounted. It may be advisable for this file object to be unbuffered
                 because opening file objects via this mount source will add additional buffering if not disabled.
        """
        self.path = '/' + path.lstrip('/')
        if self.path.endswith('/'):
            raise ValueError("File object must belong to a non-folder path!")

        self.fileObjectLock = threading.Lock()
        self.fileobj = fileobj
        self.mtime = int(time.time())
        self.size: int = self.fileobj.seek(0, io.SEEK_END)

        fileno = None
        try:
            fileno = self.fileobj.fileno()
        except Exception:
            pass

        self._statfs = {}
        if fileno is not None:
            statfs = os.fstat(fileno)
            self._statfs = {
                'f_bsize': statfs.st_blksize,
                'f_frsize': statfs.st_blksize,
                'f_blocks': statfs.st_blocks,
                'f_bfree': 0,
                'f_bavail': 0,
                'f_ffree': 0,
                'f_favail': 0,
            }

    def _createFileInfo(self):
        # This must be a function and cannot be cached into a member in order to avoid userdata being a shared list!
        return FileInfo(
            # fmt: off
            size     = self.size,
            mtime    = self.mtime,
            mode     = int(0o777 | stat.S_IFREG),
            linkname = '',
            uid      = 0,
            gid      = 0,
            userdata = [],
            # fmt: on
        )

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        pathWithSlash = path.rstrip('/') + '/'
        if self.path.startswith(pathWithSlash):
            return [self.path[len(pathWithSlash) :].split('/', maxsplit=1)[0]]
        return None

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        pathWithSlash = path.rstrip('/') + '/'
        if self.path.startswith(pathWithSlash):
            fileInfo = FileInfo(
                # fmt: off
                size     = 0,
                mtime    = self.mtime,
                mode     = int(0o777 | stat.S_IFDIR),
                linkname = '',
                uid      = 0,
                gid      = 0,
                userdata = [],
                # fmt: on
            )
            return fileInfo

        return self._createFileInfo() if path == self.path else None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        if fileInfo != self._createFileInfo():
            raise ValueError("Only files may be opened!")

        # Use StenciledFile so that the returned file objects can be independently seeked!
        if buffering == 0:
            return cast(
                IO[bytes],
                RawStenciledFile(fileStencils=[(self.fileobj, 0, self.size)], fileObjectLock=self.fileObjectLock),
            )
        return cast(
            IO[bytes],
            StenciledFile(
                fileStencils=[(self.fileobj, 0, self.size)],
                fileObjectLock=self.fileObjectLock,
                bufferSize=io.DEFAULT_BUFFER_SIZE if buffering <= 0 else buffering,
            ),
        )

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def statfs(self) -> Dict[str, Any]:
        return self._statfs.copy()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileobj.close()

    def isdir(self, path: str):
        fileInfo = self.getFileInfo(path)
        return fileInfo is not None and stat.S_ISDIR(fileInfo.mode)
