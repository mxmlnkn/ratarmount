#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os
import stat
import time

from typing import IO, Iterable, List, Optional, Union

from .compressions import zipfile
from .MountSource import FileInfo, MountSource
from .utils import overrides


class ZipMountSource(MountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject = zipfile.ZipFile(fileOrPath, 'r')
        ZipMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = self.fileObject.infolist()
        self.options = options

    @staticmethod
    def _findPassword(fileobj: "zipfile.ZipFile", passwords):
        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.infolist()
        if not files:
            for password in passwords:
                fileobj.setpassword(password)
                files = fileobj.infolist()
                if files:
                    return password

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if not file.is_dir() and file.file_size > 0]
        if not files:
            return None

        for password in [None] + passwords:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except Exception:
                pass

        raise RuntimeError("Could not find a matching password!")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileObject.close()

    @staticmethod
    def _convertToFileInfo(info: "zipfile.ZipInfo", zipFile: "zipfile.ZipFile") -> FileInfo:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        mtime = datetime.datetime(*info.date_time, tzinfo=datetime.timezone.utc).timestamp() if info.date_time else 0

        # According to section 4.5.7 in the .ZIP file format specification, links are supported:
        # https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
        # The Python zipfile module has no API for links: https://bugs.python.org/issue45286
        # However, the file mode exposes whether it's a link and the file mode is shown by ZipInfo.__repr__.
        # For that, it uses the OS-dependent external_attr member. See also the ZIP specification on that:
        # > 4.4.15 external file attributes: (4 bytes)
        # >   The mapping of the external attributes is host-system dependent (see 'version made by').
        # >   For MS-DOS, the low order byte is the MS-DOS directory attribute byte.
        # >   If input came from standard input, this field is set to zero.

        # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
        linkname = ""
        if stat.S_ISLNK(info.external_attr >> 16):
            linkname = zipFile.read(info).decode()
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

        # TODO How to behave with files in zip with absolute paths? Currently, they would never be shown.
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

        # ZipInfo.filename is wrongly named as it returns the full path inside the archive not just the name part
        return set(getName(info.filename) for info in self.files if getName(info.filename))

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        infoList = [
            ZipMountSource._convertToFileInfo(info, self.fileObject)
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
        assert isinstance(info, zipfile.ZipInfo)
        return self.fileObject.open(info, 'r')  # https://github.com/pauldmccarthy/indexed_gzip/issues/85

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)
