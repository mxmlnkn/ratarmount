import builtins
import dataclasses
import logging
import os
import stat
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import IO, Any, Optional, Union

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class FileInfo:
    # fmt: off
    size     : int
    mtime    : float
    mode     : int
    linkname : str
    uid      : int
    gid      : int
    # By convention this is a list and MountSources should only read the last element and before forwarding the
    # FileInfo to a possibly recursively "mounted" MountSource, remove that last element belonging to it.
    # This way an arbitrary amount of userdata can be stored and it should be decidable which belongs to whom in
    # a chain of MountSource objects.
    userdata : list[Any]
    # fmt: on

    def clone(self):
        copied = dataclasses.replace(self)
        # Make a new userdata list but do not do a full deep copy because some MountSources put references
        # to MountSources into userdata and those should and can not be deep copied.
        copied.userdata = self.userdata[:]
        return copied


class MountSource(ABC):
    """
    Generic class representing a mount point. It's basically like the FUSE API but boiled down
    to the necessary methods for ratarmount.

    Similar, to FUSE, all paths should have a leading '/'.
    If there is is no leading slash, behave as if there was one.
    """

    @abstractmethod
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        pass

    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        """
        This function can and should be overwritten with something that is faster than list
        because only a simple path -> mode mapping needs to be returned, not all file metadata.
        This method is custom-tailored for FUSE readdir, i.e., the returned mode is not guaranteed
        to include file permissions, only the S_IFREG, S_IFLINK, S_IFDIR flags, maybe all S_IFMT flags
        in the future.
        """
        result = self.list(path)
        if isinstance(result, dict):
            return {path: fileInfo.mode for path, fileInfo in result.items()}
        return result

    @abstractmethod
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        pass

    @abstractmethod
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        """
        buffering : Behaves similarly to Python's built-in open call. A value of 0 should disable buffering.
                    Any value larger than 1 should be the buffer size. The default of -1 may result in
                    a default buffer size equal to the file(system)'s block size or Python's io.DEFAULT_BUFFER_SIZE.
        """

    def statfs(self) -> dict[str, Any]:
        """
        Returns a dictionary with keys named like the POSIX statvfs struct.
        https://pubs.opengroup.org/onlinepubs/009695399/basedefs/sys/statvfs.h.html
        Keys may be missing. Either an empty dictionary should be returned, or at least f_bsize and f_namemax
        should be initialized because that's what libfuse returns per default if the statfs call is not implemented.
        If statfs is not implemented / the returned dictionary call is empty, libfuse will return default values:
            {'f_bsize': 512, 'f_namemax': 255}
        https://github.com/libfuse/libfuse/blob/373ddc7eae7b0c684fc4ab29d8addfa3b9e99e1e/lib/fuse.c#L1962-L1975
        """
        return {}

    def versions(self, path: str) -> int:
        return 1 if self.exists(path) else 0

    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        # Because we only do a single seek before closing the file again, buffering makes no sense.
        with self.open(fileInfo, buffering=0) as file:
            file.seek(offset)
            return file.read(size)

    @abstractmethod
    def is_immutable(self) -> bool:
        """
        Should return True if the mount source is known to not change over time in order to allow for optimizations.
        Meaning, all interface methods should return the same results given the same arguments at any time.
        """

    def get_mount_source(self, fileInfo: FileInfo):
        """
        Returns the direct mount source to which the fileInfo belongs, a mount source specific file info,
        and the mount point of the returned mount source in respect to this (self) MountSource.
        """
        return '/', self, fileInfo

    def exists(self, path: str) -> bool:
        return self.lookup(path) is not None

    def is_dir(self, path: str) -> bool:
        fileInfo = self.lookup(path)
        return fileInfo is not None and stat.S_ISDIR(fileInfo.mode)

    # pylint: disable=unused-argument
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        """
        Should return list of extended file attribute keys for the given fileInfo.
        """
        return []

    # pylint: disable=unused-argument
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        """
        Should return value for given fileInfo and extended file attribute key.
        """
        return None

    def __enter__(self):
        return self

    # If the derived MountSource opens some file object or similar in its constructor
    # then it should override this and close the file object.
    @abstractmethod
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass


def create_root_file_info(userdata: list[Any]):
    # fmt: off
    return FileInfo(
        size     = 0,
        mtime    = time.time(),
        mode     = 0o777 | stat.S_IFDIR,
        linkname = "",
        uid      = os.getuid(),
        gid      = os.getgid(),
        userdata = userdata,
    )
    # fmt: on


def merge_statfs(values: Iterable[dict[str, Any]]):
    result = {}
    for statfs in values:
        for key, value in statfs.items():
            if key not in result:
                result[key] = value
                continue

            if key in ('f_bsize', 'f_frsize'):
                result[key] = max(result[key], value)
                continue

            if key == 'f_namemax':
                result[key] = min(result[key], value)
                continue

            if result[key] != value:
                logger.warning("Failed to merge statfs values (%s, %s) for key: %s.", value, result[key], key)
    return result
