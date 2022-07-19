#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import io
import os
import stat
import time
from typing import cast, Dict, IO, Iterable, List, Optional, Union

from .MountSource import FileInfo, MountSource
from .utils import overrides


try:
    import libarchive
except ImportError:
    pass


class FileInsideArchive(io.RawIOBase):
    """
    The seek implementation reopens the file on seeking back.
    """

    def __init__(self, reopen, file_size):
        io.RawIOBase.__init__(self)
        self.reopen = reopen
        self.fileobj = reopen()
        self.file_size = file_size

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self.fileobj.close()

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        return self.fileobj.read(size)

    def _skip(self, size: int) -> None:
        BLKSIZE = 128 * 1024
        while size > 0:
            if size < BLKSIZE:
                data = self.fileobj.read(size)
            else:
                data = self.fileobj.read(BLKSIZE)
            if not data:
                break
            size -= len(data)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        here = self.tell()
        if whence == io.SEEK_CUR:
            offset += here
        elif whence == io.SEEK_END:
            offset += self.file_size

        if offset >= here:
            self._skip(offset - here)
            return self.tell()

        self.fileobj = self.reopen()
        self._skip(offset)
        return self.tell()

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.fileobj.tell()


class LibArchiveMountSource(MountSource):
    # Basically copy paste of ZipMountSource because the interfaces are very similar

    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject: libarchive.SeekableArchive = libarchive.SeekableArchive(fileOrPath, mode='r')
        for password in options.get("passwords", []):
            self.fileObject.add_passphrase(password)
        self.files = [e for e in self.fileObject]
        self.options = options

    @staticmethod
    def _convertToFileInfo(entry: libarchive.Entry) -> FileInfo:
        if entry.issym():
            linkname = entry.symlink
            mode = 0o555 | stat.S_IFLNK
        else:
            mode = 0o555 | (stat.S_IFDIR if entry.isdir() else stat.S_IFREG)
            linkname = ""

        fileInfo = FileInfo(
            # fmt: off
            size     = entry.size,
            mtime    = entry.mtime,
            mode     = mode,
            linkname = linkname,
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [entry],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        path = path.strip('/')
        if path:
            path += '/'

        # TODO How to behave with files in archive with absolute paths? Currently, they would never be shown.
        def getName(filePath):
            if not filePath.startswith(path):
                return None

            filePath = filePath[len(path) :].strip('/')
            if not filePath:
                return None

            # This effectively adds all parent paths as folders. It is easy to create
            # RARs and ZIPs with nested files without information on the parent directories!
            if '/' in filePath:
                firstSlash = filePath.index('/')
                filePath = filePath[:firstSlash]

            return filePath

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return {getName(e.pathname): self._convertToFileInfo(e) for e in self.files if getName(e.pathname)}

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        infoList = [
            LibArchiveMountSource._convertToFileInfo(e)
            for e in self.files
            if e.pathname.rstrip('/') == path.lstrip('/')
        ]

        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any(info.userdata[-1].pathname == pathAsDir for info in infoList) and any(
            e.pathname.rstrip('/').startswith(pathAsDir) for e in self.files
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
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        entry: libarchive.Entry = fileInfo.userdata[-1]
        assert isinstance(entry, libarchive.Entry)
        return cast(IO[bytes], FileInsideArchive(lambda: self.fileObject.readstream(entry.pathname), entry.size))

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)
