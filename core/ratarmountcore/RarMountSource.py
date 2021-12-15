#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import io
import os
import stat
import time
from typing import cast, IO, Iterable, List, Optional, Union

from .MountSource import FileInfo, MountSource
from .utils import overrides

try:
    import rarfile
except ImportError:
    pass


class RawFileInsideRar(io.RawIOBase):
    """
    This class works around the CRC error issue by reopening the file when seeking back.
    This will be slower for uncompressed files but not for compressed files because
    the seek implementation of rarfile also reopens the file on seeking back.
    https://github.com/markokr/rarfile/issues/73
    https://rarfile.readthedocs.io/api.html#rarfile.RarExtFile.seek
    > On uncompressed files, the seeking works by actual seeks so it’s fast.
    > On compressed files it's slow - forward seeking happens by reading ahead,
    > backwards by re-opening and decompressing from the start.
    """

    def __init__(self, reopen, file_size):
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
        return self.fileobj.seekable()

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        return self.fileobj.read(size)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            offset += self.tell()
        elif whence == io.SEEK_END:
            offset += self.file_size

        if offset >= self.tell():
            return self.fileobj.seek(offset, whence)

        self.fileobj = self.reopen()
        return self.fileobj.seek(offset, io.SEEK_SET)

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.fileobj.tell()


class RarMountSource(MountSource):
    # Basically copy paste of ZipMountSource because the interfaces are very similar
    # I'm honestly not sure how it works that well as it does. It does have some problems
    # when trying to mount .tar.bz2 or .tar.xz inside rar files recursively but it works
    # reasonably well for .tar.gz and .zip considering that seeking seems to be broken:
    # https://github.com/markokr/rarfile/issues/73

    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject = rarfile.RarFile(fileOrPath, 'r')
        RarMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = self.fileObject.infolist()
        self.options = options

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
    def _convertToFileInfo(info: "rarfile.RarInfo") -> FileInfo:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        dtime = datetime.datetime(*info.date_time)
        dtime = dtime.replace(tzinfo=datetime.timezone.utc)
        mtime = dtime.timestamp() if info.date_time else 0

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
            userdata = [info],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
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
        return set(getName(info.filename) for info in self.files if getName(info.filename))

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        infoList = [
            RarMountSource._convertToFileInfo(info)
            for info in self.files
            if info.filename.rstrip('/') == path.lstrip('/')
        ]

        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any(info.userdata[-1].filename == pathAsDir for info in infoList) and any(
            info.filename.rstrip('/').startswith(pathAsDir) for info in self.files
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
        info = fileInfo.userdata[-1]
        assert isinstance(info, rarfile.RarInfo)
        return cast(IO[bytes], RawFileInsideRar(lambda: self.fileObject.open(info, 'r'), info.file_size))

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)
