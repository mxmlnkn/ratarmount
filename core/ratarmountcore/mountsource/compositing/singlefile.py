import contextlib
import io
import os
import stat
import threading
import time
from typing import IO, Any, Dict, Iterable, Optional, Union, cast

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.StenciledFile import RawStenciledFile, StenciledFile
from ratarmountcore.utils import overrides


class SingleFileMountSource(MountSource):
    """MountSource exposing a single file as a mount source."""

    # It makes no sense to merge this class into SubvolumesMountSource because of the file locking and file size
    # for each file that needs to be saved and it would require adding if-else into almost every method in
    # SubvolumesMountSource. Having a simple SingleFileMountSource to be used with SubvolumesMountSource is far
    # more elegant. The only difference might be how to handle multiple mount points/files per parent folder.
    # This is currently not possible because SingleFileMountSource implies a single folder containing a single file.
    # It could be emulated with UnionMountSource or by extending SingleFileMountSource to support multiple files,
    # but then it would add inconsistencies when these multiple files have different full paths / implied parents...

    def __init__(self, path: str, fileobj: IO[bytes]):
        """
        fileobj: The given file object to be mounted. It may be advisable for this file object to be unbuffered
                 because opening file objects via this mount source will add additional buffering if not disabled.
        """
        self.path = os.path.normpath('/' + path).lstrip('/')
        if self.path.endswith('/') or not self.path:
            raise ValueError("File object must belong to a non-folder path!")

        self.fileObjectLock = threading.Lock()
        self.fileobj = fileobj
        self.mtime = int(time.time())
        self.size: int = self.fileobj.seek(0, io.SEEK_END)

        fileno = None
        with contextlib.suppress(Exception):
            fileno = self.fileobj.fileno()

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

    def _create_file_info(self):
        # This must be a function and cannot be cached into a member in order to avoid userdata being a shared list!
        # fmt: off
        return FileInfo(
            size     = self.size,
            mtime    = self.mtime,
            mode     = int(0o777 | stat.S_IFREG),
            linkname = '',
            uid      = 0,
            gid      = 0,
            userdata = [],
        )
        # fmt: on

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        pathWithSlash = (path.strip('/') + '/').lstrip('/')  # append / to be able to use startswith correctly
        if self.path.startswith(pathWithSlash):
            return [self.path[len(pathWithSlash) :].split('/', maxsplit=1)[0]]
        return None

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        pathWithSlash = (path.strip('/') + '/').lstrip('/')  # append / to be able to use startswith correctly
        if self.path.startswith(pathWithSlash):
            # fmt: off
            return FileInfo(
                size     = 0,
                mtime    = self.mtime,
                mode     = int(0o777 | stat.S_IFDIR),
                linkname = '',
                uid      = 0,
                gid      = 0,
                userdata = [],
            )
            # fmt: on

        return self._create_file_info() if path.strip('/') == self.path else None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        if fileInfo != self._create_file_info():
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
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def statfs(self) -> Dict[str, Any]:
        return self._statfs.copy()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileobj.close()
