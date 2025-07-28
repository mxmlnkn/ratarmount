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
        self._size: Optional[int] = None
        self._statfs = {}
        self._file_object: Optional[IO[bytes]] = None

        if callable(fileobj):
            self._open_file = fileobj
            return

        # Construct an file-opener callable from an existing file object.

        self._size = fileobj.seek(0, io.SEEK_END)
        self._file_lock = threading.Lock()
        self._file_object = fileobj

        def open_shared_file(buffering: int):
            if not self._file_object:
                raise RuntimeError("No file object to open!")
            if self._size is None:
                raise RuntimeError("File size should have been initialized!")

            # Use StenciledFile so that the returned file objects can be independently seeked!
            if buffering == 0:
                return cast(
                    IO[bytes],
                    RawStenciledFile(fileStencils=[(self._file_object, 0, self._size)], fileObjectLock=self._file_lock),
                )
            return cast(
                IO[bytes],
                StenciledFile(
                    fileStencils=[(self._file_object, 0, self._size)],
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

    def _get_size(self) -> int:
        if self._size is None:
            with self._open_file(-1) as file:
                if file.seekable():
                    return file.seek(0, io.SEEK_END)
            return 0
        return self._size

    def _create_file_info(self, size: int, mode: int):
        # This must be a function and cannot be cached into a member in order to avoid userdata being a shared list!
        # fmt: off
        return FileInfo(
            size     = size,
            mtime    = self.mtime,
            mode     = 0o777 | mode,
            linkname = '',
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [],
        )
        # fmt: on

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return [self.path] if not path or path == '/' else None

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if not path or path == '/':
            return self._create_file_info(size=0, mode=stat.S_IFDIR)
        if path.strip('/') == self.path:
            return self._create_file_info(size=self._get_size(), mode=stat.S_IFREG)
        return None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        if fileInfo != self._create_file_info(size=fileInfo.size, mode=stat.S_IFREG):
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
