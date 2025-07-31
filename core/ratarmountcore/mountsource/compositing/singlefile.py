import contextlib
import io
import os
import stat
import threading
import time
from collections.abc import Iterable
from typing import IO, Any, Callable, Optional, Union, cast, final

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.StenciledFile import RawStenciledFile, StenciledFile
from ratarmountcore.utils import overrides


def _get_mode(fileobj: IO[bytes]) -> int:
    return 0o111 | (0o222 if fileobj.writable() else 0) | (0o444 if fileobj.readable() else 0)


@final
class SingleFileMountSource(MountSource):
    """MountSource exposing a single file as a mount source."""

    # It makes no sense to merge this class into SubvolumesMountSource because of the file locking and file size
    # for each file that needs to be saved and it would require adding if-else into almost every method in
    # SubvolumesMountSource. Having a simple SingleFileMountSource to be used with SubvolumesMountSource is far
    # more elegant. The only difference might be how to handle multiple mount points/files per parent folder.
    # This is currently not possible because SingleFileMountSource implies a single folder containing a single file.
    # It could be emulated with UnionMountSource or by extending SingleFileMountSource to support multiple files,
    # but then it would add inconsistencies when these multiple files have different full paths / implied parents...

    def __init__(self, path: str, fileobj: Union[IO[bytes], Callable[[int], IO[bytes]]]):
        """
        fileobj: The given file object to be mounted. It may be advisable for this file object to be unbuffered
                 because opening file objects via this mount source will add additional buffering if not disabled.
        """
        self.path = os.path.normpath('/' + path).lstrip('/')
        if not self.path or '/' in self.path:
            raise ValueError("File object must belong to a non-folder path!")

        self.mtime = time.time()
        self._file_info: Optional[FileInfo] = None
        self._file_object: Optional[IO[bytes]] = None
        self._statfs = {}

        if callable(fileobj):
            self._open_file = fileobj
            return

        # Construct an file-opener callable from an existing file object.

        self._file_info = self._create_file_info(
            isdir=False, size=fileobj.seek(0, io.SEEK_END), mode=_get_mode(fileobj)
        )
        self._file_lock = threading.Lock()
        self._file_object = fileobj

        def open_shared_file(buffering: int):
            if not self._file_object:
                raise RuntimeError("No file object to open!")
            if self._file_info is None:
                raise RuntimeError("File size should have been initialized!")

            # Use StenciledFile so that the returned file objects can be independently seeked!
            if buffering == 0:
                return cast(
                    IO[bytes],
                    RawStenciledFile(
                        fileStencils=[(self._file_object, 0, self._file_info.size)], fileObjectLock=self._file_lock
                    ),
                )
            return cast(
                IO[bytes],
                StenciledFile(
                    fileStencils=[(self._file_object, 0, self._file_info.size)],
                    fileObjectLock=self._file_lock,
                    bufferSize=io.DEFAULT_BUFFER_SIZE if buffering <= 0 else buffering,
                ),
            )

        self._open_file = open_shared_file

        fileno = None
        with contextlib.suppress(Exception):
            fileno = fileobj.fileno()

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

    def _create_file_info(self, isdir: bool, size: Optional[int] = None, mode: Optional[int] = None):
        if self._file_info:
            if size is None:
                size = self._file_info.size
            if mode is None:
                mode = self._file_info.mode & 0o777

        if size is None or mode is None:
            if hasattr(self, '_open_file'):
                with self._open_file(-1) as file:
                    if size is None:
                        size = file.seek(0, io.SEEK_END) if file.seekable() else 0
                        if size is None:
                            size = file.tell()
                        if size is None:
                            size = 0
                    if mode is None:
                        mode = _get_mode(file)
            else:
                if size is None:
                    size = 0
                if mode is None:
                    mode = 0o777

        # fmt: off
        return FileInfo(
            size     = size,
            mtime    = self.mtime,
            mode     = mode | (stat.S_IFDIR if isdir else stat.S_IFREG),
            linkname = '',
            uid      = self._file_info.uid if self._file_info else os.getuid(),
            gid      = self._file_info.gid if self._file_info else os.getgid(),
            userdata = [],
        )
        # fmt: on

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return [self.path] if not path or path == '/' else None

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if not path or path == '/':
            return self._create_file_info(size=0, isdir=True)
        if path.strip('/') == self.path:
            return self._create_file_info(isdir=False)
        return None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        if fileInfo != self._create_file_info(size=fileInfo.size, mode=fileInfo.mode & 0o777, isdir=False):
            raise ValueError("Only files may be opened!")
        return self._open_file(buffering)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        return self._statfs.copy()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self._file_object:
            self._file_object.close()

    def join_threads(self):
        if hasattr(self._file_object, 'join_threads'):
            self._file_object.join_threads()
