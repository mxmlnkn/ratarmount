import builtins
import contextlib
import datetime
import os
import stat
import sys
import time
from collections.abc import Iterable
from typing import IO, Optional, Union

from ratarmountcore.formats import FileFormatID, replace_format_check
from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import overrides

with contextlib.suppress(ImportError):
    import rarfile


def is_rar_file(fileObject: IO[bytes]) -> bool:
    # @see https://www.rarlab.com/technote.htm#rarsign
    # > RAR 5.0 signature consists of 8 bytes: 0x52 0x61 0x72 0x21 0x1A 0x07 0x01 0x00.
    # > You need to search for this signature in supposed archive from beginning and up to maximum SFX module size.
    # > Just for comparison this is RAR 4.x 7 byte length signature: 0x52 0x61 0x72 0x21 0x1A 0x07 0x00.
    # > Self-extracting module (SFX)
    # > Any data preceding the archive signature. Self-extracting module size and contents is not defined.
    # > At the moment of writing this documentation RAR assumes the maximum SFX module size to not exceed 1 MB,
    # > but this value can be increased in the future.
    oldPosition = fileObject.tell()
    if fileObject.read(6) == b'Rar!\x1a\x07':
        return True
    if 'rarfile' in sys.modules:
        fileObject.seek(oldPosition)
        fileObject.seek(oldPosition)
        if rarfile.is_rarfile_sfx(fileObject):
            return True
    return False


replace_format_check(FileFormatID.RAR, is_rar_file)


class RarMountSource(MountSource):
    # Basically copy paste of ZipMountSource because the interfaces are very similar.
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        if 'rarfile' not in sys.modules:
            raise RuntimeError("Did not find the rarfile module. Try: pip install rarfile")

        self.fileObject = rarfile.RarFile(fileOrPath, 'r')
        RarMountSource._find_password(self.fileObject, options.get("passwords", []))

        self.files = {RarMountSource._clean_path(info.filename): info for info in self.fileObject.infolist()}
        self.options = options

    @staticmethod
    def _clean_path(path):
        result = os.path.normpath(path) + ('/' if path.endswith('/') else '')
        while result.startswith('../'):
            result = result[3:]
        return result

    @staticmethod
    def _find_password(fileobj: "rarfile.RarFile", passwords):
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
    def _get_mode(info: "rarfile.RarInfo") -> int:
        return info.mode | (stat.S_IFLNK if info.file_redir else (stat.S_IFDIR if info.is_dir() else stat.S_IFREG))

    @staticmethod
    def _convert_to_file_info(normalizedPath: str, info: "rarfile.RarInfo") -> FileInfo:
        if info.date_time:
            dtime = datetime.datetime(*info.date_time)
            dtime = dtime.replace(tzinfo=datetime.timezone.utc)
            mtime = dtime.timestamp() if info.date_time else 0
        else:
            mtime = 0

        # fmt: off
        return FileInfo(
            size     = info.file_size,
            mtime    = mtime,
            mode     = RarMountSource._get_mode(info),
            # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
            linkname = info.file_redir[2] if info.file_redir else "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [(normalizedPath, info)],
        )
        # fmt: on

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    # TODO How to behave with files in archive with absolute paths? Currently, they would never be shown.
    @staticmethod
    def _get_name(folderPath, filePath):
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
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        path = path.strip('/')
        if path:
            path += '/'

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return {
            self._get_name(path, normalizedPath): self._convert_to_file_info(normalizedPath, info)
            for normalizedPath, info in self.files.items()
            if self._get_name(path, normalizedPath)
        }

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        path = path.strip('/')
        if path:
            path += '/'

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return {
            self._get_name(path, normalizedPath): RarMountSource._get_mode(info)
            for normalizedPath, info in self.files.items()
            if self._get_name(path, normalizedPath)
        }

    def _lookups(self, path: str) -> builtins.list[FileInfo]:
        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'
        if pathAsDir == '/':
            return [create_root_file_info(userdata=[None])]

        infoList = [
            RarMountSource._convert_to_file_info(normalizedPath, info)
            for normalizedPath, info in self.files.items()
            if normalizedPath.rstrip('/') == path.lstrip('/')
        ]

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any(info.userdata[-1][0] == pathAsDir for info in infoList) and any(
            normalizedPath.rstrip('/').startswith(pathAsDir) for normalizedPath, info in self.files.items()
        ):
            # fmt: off
            infoList.append(
                FileInfo(
                    size     = 0,
                    mtime    = time.time(),
                    mode     = 0o777 | stat.S_IFDIR,
                    linkname = "",
                    uid      = os.getuid(),
                    gid      = os.getgid(),
                    userdata = [None],
                )
            )
            # fmt: on

        return infoList

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        infos = self._lookups(path)
        return infos[fileVersion] if -len(infos) <= fileVersion < len(infos) else None

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return len(self._lookups(path))

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # I do not see any obvious option to rarfile.RarFile to apply the specified buffer size.
        info = fileInfo.userdata[-1][1]
        assert isinstance(info, rarfile.RarInfo)
        return self.fileObject.open(info, 'r')

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileObject.close()
