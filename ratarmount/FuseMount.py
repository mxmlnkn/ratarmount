import contextlib
import ctypes
import errno
import io
import os
import subprocess
import sys
import tempfile
import time
import traceback
from typing import IO, Any, Callable, Optional, Union, cast

from ratarmountcore.mountsource import FileInfo, MountSource

# These imports can be particularly expensive when all fsspec backends are installed.
from ratarmountcore.mountsource.compositing.automount import AutoMountLayer
from ratarmountcore.mountsource.compositing.removeprefix import RemovePrefixMountSource
from ratarmountcore.mountsource.compositing.singlefile import SingleFileMountSource
from ratarmountcore.mountsource.compositing.subvolumes import SubvolumesMountSource
from ratarmountcore.mountsource.compositing.union import UnionMountSource
from ratarmountcore.mountsource.compositing.versioning import FileVersionLayer
from ratarmountcore.mountsource.factory import open_mount_source
from ratarmountcore.mountsource.formats.folder import FolderMountSource
from ratarmountcore.utils import ceil_div, determine_recursion_depth, overrides

from .fuse import fuse
from .WriteOverlay import WritableFolderMountSource


def split_command_line(command: bytes, name: bytes = b'ratarmount') -> list[str]:
    if not command.startswith(name):
        raise ValueError(f"Command must start with: {name.decode()}")

    if command == name:
        return [name.decode()]

    delimiter = command[len(name) : len(name) + 1]
    command = command[len(name) + 1 :]
    if delimiter not in (b'\0', b'\n', b' '):
        raise ValueError(f"Command must start with {name.decode()} followed by null, newline, or space as delimiter.")

    # Check for the common case, i.e., when using 'echo' to write to this file instead of printf,
    # which adds an unwanted newline, implying an '\n' argument. If that is really wanted, then
    # add another '\n' because only one trailing '\n' will be stripped.
    if delimiter == b' ' and command and command[-1] == ord('\n'):
        command = command[:-1]

    return [part.decode() for part in command.split(delimiter)]


class CommandFile(io.RawIOBase):
    def __init__(self, callback: Callable[[list[str]], Any]):
        self._buffer = bytearray()
        self._callback = callback

    def writable(self) -> bool:
        return True

    def write(self, data):
        if self.closed:  # pylint: disable=using-constant-test  # Bug?
            raise ValueError("I/O operation on closed file.")
        self._buffer.extend(data)
        return len(data)

    def close(self):
        if self.closed:  # pylint: disable=using-constant-test  # Bug?
            return

        try:
            super().close()
            if not self._buffer:
                return

            arguments = split_command_line(bytes(self._buffer))
            self._callback(arguments)
        except Exception as exception:
            traceback.print_exc()
            raise ValueError from exception

    def tell(self):
        return len(self._buffer)


class FuseMount(fuse.Operations):
    """
    This class implements the fusepy interface in order to create a mounted file system view to a MountSource.
    This class itself is a relatively thin wrapper around the ratarmountcore mount sources.
    It also handles the write overlay because it does not fit into the MountSource interface and because it
    must be part of the UnionMountSource for correct file versioning but at the same time it must know of the
    union mount source.

    Documentation for FUSE methods can be found in the fusepy or libfuse headers. There seems to be no complete
    rendered documentation aside from the header comments.

    https://github.com/fusepy/fusepy/blob/master/fuse.py
    https://github.com/libfuse/libfuse/blob/master/include/fuse.h
    https://man7.org/linux/man-pages/man3/errno.3.html

    All path arguments for overridden fusepy methods do have a leading slash ('/')!
    This is why MountSource also should expect leading slashes in all paths.
    """

    # Use a relatively large minimum 256 KiB block size to get filesystem users to use larger reads
    # because reads have a relative large overhead because of the fusepy, libfuse, kernel FUSE, SQLite,
    # ratarmountcore, StenciledFile, and other layers they have to go through.
    MINIMUM_BLOCK_SIZE = 256 * 1024

    use_ns = True

    def __init__(self, pathToMount: Union[str, list[str]], mountPoint: str, **options) -> None:
        self.mountPoint = os.path.realpath(mountPoint)
        self.mountPointFd: Optional[int] = None
        self.mountPointWasCreated = False
        self.selfBindMount: Optional[FolderMountSource] = None

        self.printDebug: int = int(options.get('printDebug', 0))
        self.writeOverlay: Optional[WritableFolderMountSource] = None
        self.overlayPath: Optional[str] = None

        # Maps handles to either opened I/O objects or os module file handles for the writeOverlay and the open flags.
        self.openedFiles: dict[int, tuple[int, Union[IO[bytes], int]]] = {}
        self.lastFileHandle: int = 0  # It will be incremented before being returned. It can't hurt to never return 0.

        self.logFile: Optional[IO[str]] = None
        # Log file location to be used when enableControlInterface is True but self.logFile is not set by the user.
        self._tmpLogFile: Optional[Any] = None
        self._enableControlInterface = bool(options.pop('controlInterface', False))
        self._controlLayerPrefix = "/.ratarmount-control/"
        # Ratarmount subprocesses started via /.ratarmount-control/command. Will be terminated on close.
        self._subprocesses: list[subprocess.Popen] = []

        # Only open the log file at the end shortly before it is needed to not end up with an empty file on error.
        # Read it from 'options' as soon as possible to not forward it as a MountSource options.
        logFilePath: str = options.pop('logFile', '')
        if logFilePath:
            logFilePath = os.path.realpath(logFilePath)

        # This check is important for the self-bind test below, which assumes a folder.
        if os.path.exists(self.mountPoint) and not os.path.isdir(self.mountPoint):
            raise ValueError(f"Mount point '{self.mountPoint}' must either not exist or be a directory!")

        if not isinstance(pathToMount, list):
            try:
                os.fspath(pathToMount)
                pathToMount = [pathToMount]
            except Exception:
                pass

        hadPathsToMount = bool(pathToMount)
        pathToMount = list(filter(lambda x: os.path.exists(x) or '://' in x, pathToMount))
        if hadPathsToMount and not pathToMount:
            raise ValueError("No paths to mount left over after filtering!")

        options['writeIndex'] = True

        # Explicitly enable recursion if it was specified implictily via recursionDepth.
        if 'recursive' not in options and determine_recursion_depth(**options) > 0:
            options['recursive'] = True

        # Add write overlay as folder mount source to read from with highest priority.
        if 'writeOverlay' in options and isinstance(options['writeOverlay'], str) and options['writeOverlay']:
            self.overlayPath = os.path.realpath(options['writeOverlay'])
            if not os.path.exists(self.overlayPath):
                os.makedirs(self.overlayPath, exist_ok=True)
            pathToMount.append(self.overlayPath)

        # Take care that bind-mounting folders to itself works
        mountSources: list[tuple[str, MountSource]] = []

        for path in pathToMount:
            if os.path.realpath(path) != self.mountPoint:
                # This also will create or load the block offsets for compressed formats
                mountSources.append((os.path.basename(path), open_mount_source(path, **options)))
                continue

            if self.mountPointFd is not None:
                continue

            mountSource = FolderMountSource(path)
            mountSources.append((os.path.basename(path), mountSource))
            self.selfBindMount = mountSource
            self.mountPointFd = os.open(self.mountPoint, os.O_RDONLY)

            # Lazy mounting can result in locking recursive calls into our own FUSE mount point.
            # Opening the archives is already handled correctly without calling FUSE inside AutoMountLayer.
            # Here we need to ensure that indexes are not tried to being read from or written to our own
            # FUSE mount point.
            if options.get('lazyMounting', False):

                def points_into_mount_point(pathToTest):
                    return os.path.commonpath([pathToTest, self.mountPoint]) == self.mountPoint

                hasIndexPath = False

                if 'indexFilePath' in options and isinstance(options['indexFilePath'], str):
                    indexFilePath = options['indexFilePath']
                    # Strip a single file://, not any more because URL chaining is supported by fsspec.
                    if options['indexFilePath'].count('://') == 1:
                        fileURLPrefix = 'file://'
                        indexFilePath = indexFilePath.removeprefix(fileURLPrefix)
                    if '://' not in indexFilePath:
                        indexFilePath = os.path.realpath(options['indexFilePath'])

                    if points_into_mount_point(indexFilePath):
                        del options['indexFilePath']
                    else:
                        options['indexFilePath'] = indexFilePath
                        hasIndexPath = True

                if 'indexFolders' in options and isinstance(options['indexFolders'], list):
                    indexFolders = options['indexFolders']
                    newIndexFolders = []
                    for folder in indexFolders:
                        if points_into_mount_point(folder):
                            continue
                        newIndexFolders.append(os.path.realpath(folder))
                    options['indexFolders'] = newIndexFolders
                    if newIndexFolders:
                        hasIndexPath = True

                # Force in-memory indexes if no folder remains because the default for no indexFilePath being
                # specified would be in a file in the same folder as the archive.
                if not hasIndexPath:
                    options['indexFilePath'] = ':memory:'

        # Open log file.
        openLog: Optional[Callable[[int], IO[bytes]]] = None
        if not logFilePath and self._enableControlInterface:
            self._tmpLogFile = tempfile.NamedTemporaryFile('w+', encoding='utf-8', suffix='.ratarmount.log')
            logFilePath = self._tmpLogFile.name
        if logFilePath:
            self.logFile = open(logFilePath, "w+", buffering=1, encoding='utf-8')

            def _get_log(_buffering: int = 0) -> IO[bytes]:
                if self.logFile is None:
                    raise RuntimeError("Log file has not been initialized!")
                self.logFile.seek(0)
                # Quite expensive copy, but just so much easier. Else we would have to write our own class
                # to support this case of "write UTF-8", "read bytes" with some kind of adapter class.
                # Normally, the log should not grow to more than a dozen megabytes. Somehow adding a limit
                # to that would be much more useful.
                # Using BufferedReader effectively removes the writable()==True implementation of BytesIO,
                # so that the file permission will be correct in SingleFileMountSource!
                return io.BufferedReader(cast(io.RawIOBase, io.BytesIO(self.logFile.read().encode())))

            openLog = _get_log

        if self._enableControlInterface:
            controlFiles = [
                SingleFileMountSource('command', (lambda _: cast(IO[bytes], CommandFile(self._parse_command))))
            ]
            if openLog:
                controlFiles.append(SingleFileMountSource('output', openLog))

            controlLayer = RemovePrefixMountSource(self._controlLayerPrefix, UnionMountSource(controlFiles))
            mountSources.append(('/', controlLayer))

        def create_multi_mount() -> MountSource:
            if not options.get('disableUnionMount', False):
                return UnionMountSource([x[1] for x in mountSources], **options)

            # Create unique keys.
            submountSources: dict[str, MountSource] = {}
            suffix = 1
            for key, mountSource in mountSources:
                if key in submountSources:
                    while f"{key}.{suffix}" in submountSources:
                        suffix += 1
                    submountSources[f"{key}.{suffix}"] = mountSource
                else:
                    submountSources[key] = mountSource
            return SubvolumesMountSource(submountSources, printDebug=self.printDebug)

        if not mountSources:
            raise ValueError("Mount point is empty! Either specify some input files or enable the control interface!")

        self.mountSource: MountSource = mountSources[0][1] if len(mountSources) == 1 else create_multi_mount()

        if determine_recursion_depth(**options) > 0:
            self.mountSource = AutoMountLayer(self.mountSource, **options)

        if options.get('enableFileVersions', True):
            self.mountSource = FileVersionLayer(self.mountSource)

        # No threads should be created and still be open before FUSE forks.
        # Instead, they should be created in 'init'.
        # Therefore, close threads opened by the ParallelBZ2Reader for creating the block offsets.
        # Those threads will be automatically recreated again on the next read call.
        # Without this, the ratarmount background process won't quit even after unmounting!
        join_threads = getattr(self.mountSource, 'join_threads', None)
        if join_threads is not None:
            join_threads()

        if self.overlayPath:
            ignoredPrefixes: list[str] = []
            if self._enableControlInterface:
                ignoredPrefixes.append(self._controlLayerPrefix)
            self.writeOverlay = WritableFolderMountSource(
                self.overlayPath, self.mountSource, ignoredPrefixes=ignoredPrefixes
            )

            self.chmod = self.writeOverlay.chmod
            self.chown = self.writeOverlay.chown
            self.utimens = self.writeOverlay.utimens
            self.rename = self.writeOverlay.rename

            self.symlink = self.writeOverlay.symlink
            self.link = self.writeOverlay.link
            self.unlink = self.writeOverlay.unlink

            self.mkdir = self.writeOverlay.mkdir
            self.rmdir = self.writeOverlay.rmdir

            self.mknod = self.writeOverlay.mknod

        # Create mount point if it does not exist
        if mountPoint and not os.path.exists(mountPoint):
            os.mkdir(mountPoint)
            self.mountPointWasCreated = True

        statResults = os.lstat(self.mountPoint)
        self.mountPointInfo = {key: getattr(statResults, key) for key in dir(statResults) if key.startswith('st_')}

        if self.printDebug >= 1:
            print("Created mount point at:", self.mountPoint)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if hasattr(super(), "__exit__"):
            super().__exit__(exception_type, exception_value, exception_traceback)
        self._close()

    def _close(self) -> None:
        if logFile := getattr(self, 'logFile', None):
            try:
                if sys.stdout == logFile:
                    sys.stdout = sys.__stdout__
                if sys.stderr == logFile:
                    sys.stderr = sys.__stderr__
            except Exception as exception:
                if self.printDebug >= 1:
                    print("[Warning] Failed to restore stdout and stderr because of:", exception)

            try:
                logFile.close()
                self.logFile = None
            except Exception as exception:
                if self.printDebug >= 1:
                    print("[Warning] Failed to close log file because of:", exception)

        # Terminate or kill all ratarmount subprocesses.
        if subprocesses := getattr(self, '_subprocesses', None):
            try:
                for process in subprocesses:
                    with contextlib.suppress(Exception):
                        if process.poll() is None:
                            process.terminate()

                tStartTerminate = time.time()
                for process in subprocesses:
                    with contextlib.suppress(Exception):
                        process.wait(timeout=max(0, tStartTerminate + 2 - time.time()))

                tStartTerminate = time.time()
                for process in subprocesses:
                    try:
                        if process.poll() is None:
                            process.kill()
                            process.wait(timeout=max(0, tStartTerminate + 2 - time.time()))
                    except Exception as exception:
                        if self.printDebug >= 1:
                            print("[Warning] Failed to terminate ratarmount subprocesses because of:", exception)

                self._subprocesses.clear()
            except Exception as exception:
                if self.printDebug >= 1:
                    print("[Warning] Failed to terminate ratarmount subprocesses because of:", exception)

        try:
            if tmpLogFile := getattr(self, '_tmpLogFile', None):
                tmpLogFile.close()
        except Exception:
            pass

        try:
            if getattr(self, 'mountPointWasCreated', False) and getattr(self, 'mountPoint', None):
                os.rmdir(self.mountPoint)
                self.mountPoint = ""
        except Exception as exception:
            if self.printDebug >= 1:
                print("[Warning] Failed to remove the created mount point directory because of:", exception)

        try:
            mountPointFd = getattr(self, 'mountPointFd', None)
            if mountPointFd is not None:
                os.close(mountPointFd)
                self.mountPointFd = None
        except Exception as exception:
            if self.printDebug >= 1:
                print("[Warning] Failed to close mount point folder descriptor because of:", exception)

        try:
            # If there is some exception in the constructor, then some members may not exist!
            if hasattr(self, 'mountSource'):
                self.mountSource.__exit__(None, None, None)
        except Exception as exception:
            if self.printDebug >= 1:
                print("[Warning] Failed to tear down root mount source because of:", exception)
            if self.printDebug >= 3:
                traceback.print_exc()

    def __del__(self) -> None:
        self._close()

    def _parse_command(self, args: list[str]) -> None:
        try:
            if len(self._subprocesses) > 100:
                # Filter finished processes.
                self._subprocesses = [process for process in self._subprocesses if process.poll() is None]

            # Start command in subprocess to avoid lockups when calling into the FUSE point provided by this process.
            # Change working directory to / to avoid any potential issues with relative paths and non-dismountable
            # fusermounts in case the current working directory is the inside the FUSE mount.
            # Subprocesses will be terminated when this FuseMount instance is closed. If '-f' is not specified,
            # then the started subprocess will itself start another daemonized subprocess, which will NOT be closed
            # when this FuseMount instance is closed.
            self._subprocesses.append(
                subprocess.Popen(
                    [sys.executable, '-m', 'ratarmount', *args],
                    cwd='/',
                    stdout=self.logFile or subprocess.PIPE,
                    stderr=self.logFile or subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
            )

        except Exception as exception:
            print("[Error]", exception)
            if self.printDebug >= 3:
                traceback.print_exc()

    def _add_new_handle(self, handle, flags: int) -> int:
        # Note that fh in fuse_common.h is 64-bit and Python also supports 64-bit (long integers) out of the box.
        # So, there should practically be no overflow and file handle reuse possible.
        self.lastFileHandle += 1
        self.openedFiles[self.lastFileHandle] = (flags, handle)
        return self.lastFileHandle

    def _lookup(self, path: str) -> FileInfo:
        if self.writeOverlay and self.writeOverlay.is_deleted(path):
            raise fuse.FuseOSError(errno.ENOENT)

        fileInfo = self.mountSource.lookup(path)
        if fileInfo is None:
            raise fuse.FuseOSError(errno.ENOENT)

        if not self.writeOverlay:
            return fileInfo

        # Request exact metadata from write overlay, e.g., if the actual file in the folder
        # does not support permission changes
        result = self.mountSource.get_mount_source(fileInfo)
        subMountPoint = result[0]
        # TODO Note that if the path contains special .version versioning, then it will most likely fail
        #      to find the path in the write overlay, which is problematic for things like foo.versions/0.
        #      Would be really helpful if the file info would contain the actual path and name, too :/
        return self.writeOverlay.update_file_info(path[len(subMountPoint) :], fileInfo)

    @staticmethod
    def _redirect_output(name: str, file: IO[str]):
        with contextlib.suppress(OSError, ValueError):
            libc = ctypes.CDLL('libc.so.6')
            cstdptr = ctypes.c_void_p.in_dll(libc, name)
            if cstdptr:
                libc.fflush(cstdptr)

        pystd = getattr(sys, name, None)
        if not pystd:
            return

        # Closing the original stdout and stderr (fd=1, fd=2) and duplicating our file's fd to those,
        # makes it also work for C-code backends, i.e., libfuse with -o debug! This assumes that sys.stdout
        # and sys.stderr are using the default fds 1 and 2.
        pystdFileno = pystd.fileno()
        with contextlib.suppress(Exception):
            pystd.close()  # Also closes pystdFileno and makes it reusable.
        os.dup2(file.fileno(), pystdFileno)

        setattr(sys, name, file)

    @overrides(fuse.Operations)
    def init(self, path: str) -> None:
        if self.logFile:
            if self.printDebug >= 2:
                print("[Info] Redirecting further output into:", self.logFile)
            self._redirect_output('stdout', self.logFile)
            self._redirect_output('stderr', self.logFile)

        if self.selfBindMount is not None and self.mountPointFd is not None:
            self.selfBindMount.set_folder_descriptor(self.mountPointFd)
            if self.writeOverlay and self.writeOverlay.root == self.mountPoint:
                self.writeOverlay.set_folder_descriptor(self.mountPointFd)

    @overrides(fuse.Operations)
    def getattr(self, path: str, fh=None) -> dict[str, Any]:
        fileInfo = self._lookup(path)
        blockSize = FuseMount.MINIMUM_BLOCK_SIZE
        return {
            # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
            'st_size': fileInfo.size,
            'st_mode': fileInfo.mode,
            'st_uid': fileInfo.uid,
            'st_gid': fileInfo.gid,
            'st_mtime': int(fileInfo.mtime * 1e9),
            'st_nlink': 1,  # TODO: this is wrong for files with hardlinks,
            # `du` sums disk usage (the number of blocks used by a file) instead of the file sizes by default.
            # So, we need to return some valid values. Tar files are usually a series of 512 B blocks, but this
            # block size is also used by Python as the default read call size, so it should be something larger
            # for better performance.
            'st_blksize': blockSize,
            # Number of 512 B (!) blocks irrespective of st_blksize!
            #  - https://linux.die.net/man/2/stat
            #  - https://unix.stackexchange.com/a/521240/111050
            # We do not have information about sparse files in the index and we do not transmit sparse information
            # to FUSE anyway because there seems to be no interface for that, i.e., lseek( ..., SEEK_HOLE ) does
            # not work anyway.
            'st_blocks': ceil_div(fileInfo.size, 512),
        }

    @overrides(fuse.Operations)
    def readdir(self, path: str, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        files = self.mountSource.list_mode(path)

        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        if isinstance(files, dict):
            yield '.', self.getattr(path)['st_mode'], 0

            if path == '/':
                yield '..', self.mountPointInfo['st_mode'], 0
            else:
                yield '..', self.getattr(path.rsplit('/', 1)[0])['st_mode'], 0
        else:
            yield '.'
            yield '..'

        deletedFiles = self.writeOverlay.list_deleted(path) if self.writeOverlay else []

        if isinstance(files, dict):
            for name, mode in files.items():
                if name not in deletedFiles:
                    yield name, mode, 0
        elif files is not None:
            for key in files:
                if key not in deletedFiles:
                    yield key

    @overrides(fuse.Operations)
    def readlink(self, path: str) -> str:
        return self._lookup(path).linkname

    @overrides(fuse.Operations)
    def open(self, path: str, flags: int) -> int:
        """Returns file handle of opened path."""

        fileInfo = self._lookup(path)

        try:
            # If the flags indicate "open for modification", then still open it as read-only through the mount source
            # but store information to reopen it for write access on write calls.
            # @see https://man7.org/linux/man-pages/man2/open.2.html
            # > The argument flags must include one of the following access modes: O_RDONLY, O_WRONLY, or O_RDWR.
            return self._add_new_handle(self.mountSource.open(fileInfo, buffering=0), flags)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to open file.", fileInfo)
            raise fuse.FuseOSError(errno.EIO) from exception

    @overrides(fuse.Operations)
    def release(self, path: str, fh) -> int:
        if fh not in self.openedFiles:
            raise fuse.FuseOSError(errno.ESTALE)

        openedFile = self._resolve_file_handle(fh)
        if isinstance(openedFile, int):
            os.close(openedFile)
        else:
            openedFile.close()
            del openedFile

        return fh

    @overrides(fuse.Operations)
    def read(self, path: str, size: int, offset: int, fh) -> bytes:
        if fh in self.openedFiles:
            openedFile = self._resolve_file_handle(fh)
            if isinstance(openedFile, int):
                os.lseek(openedFile, offset, os.SEEK_SET)
                return os.read(openedFile, size)

            openedFile.seek(offset)
            return openedFile.read(size)

        # As far as I understand FUSE and my own file handle cache, this should never happen. But you never know.
        if self.printDebug >= 1:
            print("[Warning] Given file handle does not exist. Will open file before reading which might be slow.")

        fileInfo = self._lookup(path)

        try:
            return self.mountSource.read(fileInfo, size, offset)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to read data from underlying TAR file! Returning errno.EIO.")
            raise fuse.FuseOSError(errno.EIO) from exception

    # Methods for the write overlay which require file handle translations

    def _is_write_overlay_handle(self, fh):
        return self.writeOverlay and fh in self.openedFiles and isinstance(self._resolve_file_handle(fh), int)

    def _resolve_file_handle(self, fh):
        return self.openedFiles[fh][1]

    @overrides(fuse.Operations)
    def create(self, path: str, mode: int, fi=None):
        if self.writeOverlay:
            return self._add_new_handle(self.writeOverlay.create(path, mode, fi), 0)
        raise fuse.FuseOSError(errno.EROFS)

    @fuse.overrides(fuse.Operations)
    def truncate(self, path: str, length: int, fh: Optional[int] = None) -> int:
        # The existence of this method is sufficient. Without this, the 'write' callback would not be called by
        # libfuse because it seems to think that the file system is not writable, and the control layer would not work.
        if self._enableControlInterface and path.startswith(self._controlLayerPrefix):
            return 0  # Simply return success without doing anything.
        if self.writeOverlay:
            return self.writeOverlay.truncate(path, length, fh)
        raise fuse.FuseOSError(errno.EROFS)

    @overrides(fuse.Operations)
    def write(self, path: str, data, offset: int, fh) -> int:
        if not self.writeOverlay or not self._is_write_overlay_handle(fh):
            flags, openedFile = self.openedFiles[fh]

            # Normally, a simple 'writable' test should be sufficient, but I am not sure that ALL backends
            # correctly return False for 'writable' on file objects. And if not that would lead to bugs with the
            # write overlay. This means, I would have to extend the write overlay test for all archive formats!
            if not isinstance(openedFile, int) and openedFile.writable() and isinstance(openedFile, CommandFile):
                if openedFile.seekable() and openedFile.tell() != offset:
                    openedFile.seek(offset)
                return openedFile.write(data)

            if self.writeOverlay and not isinstance(openedFile, int) and (flags & (os.O_WRONLY | os.O_RDWR)):
                openedFile.close()
                self.openedFiles[fh] = (flags, self.writeOverlay.open(path, flags))

        if self.writeOverlay and self._is_write_overlay_handle(fh):
            return self.writeOverlay.write(path, data, offset, self._resolve_file_handle(fh))
        raise fuse.FuseOSError(errno.EROFS)

    @overrides(fuse.Operations)
    def flush(self, path: str, fh):
        if self.writeOverlay and self._is_write_overlay_handle(fh):
            self.writeOverlay.flush(path, self._resolve_file_handle(fh))
        return 0  # Nothing to flush, so return success

    @overrides(fuse.Operations)
    def fsync(self, path: str, datasync: int, fh):
        if self.writeOverlay and self._is_write_overlay_handle(fh):
            self.writeOverlay.fsync(path, datasync, self._resolve_file_handle(fh))
        return 0  # Nothing to flush, so return success

    @overrides(fuse.Operations)
    def statfs(self, path: str):
        # The filesystem block size is used, e.g., by Python as the default buffer size and therefore the
        # default (p)read size when possible. For network file systems such as Lustre, or block compression
        # such as in SquashFS, this proved to be highly insufficient to reach optimal performance!
        # Note that there are some efforts to get rid of Python's behavior to use the block size and to
        # increase the fixed default buffer size:
        # https://github.com/python/cpython/issues/117151
        if self.writeOverlay:
            # Merge the block size from other mount sources while throwing away b_free and similar members
            # that are set to 0 because those are read-only mount sources.
            keys = ['f_bsize', 'f_frsize']
            result = self.writeOverlay.statfs(path).copy()
            result.update({key: value for key, value in self.mountSource.statfs().items() if key in keys})

        result = self.mountSource.statfs()

        # Use a relatively large minimum 256 KiB block size to direct filesystem users to use larger reads
        # because they have a relative large overhead because of the fusepy, libfuse, kernel FUSE, SQLite,
        # ratarmountcore, StenciledFile, and other layers.
        for key in ['f_bsize', 'f_frsize']:
            result[key] = max(result.get(key, 0), FuseMount.MINIMUM_BLOCK_SIZE)
        return result

    @overrides(fuse.Operations)
    def listxattr(self, path: str):
        # Beware, keys not prefixed with "user." will not be listed by getfattr by default.
        # Use: "getfattr --match=.* mounted/foo" It seems that libfuse and the FUSE kernel module accept
        # all keys, I tried with "key1", "security.key1", "user.key1".
        return self.mountSource.list_xattr(self._lookup(path))

    @overrides(fuse.Operations)
    def getxattr(self, path: str, name, position=0):
        if position:
            # Specifically do not raise ENOSYS because libfuse will then disable getxattr calls wholly from now on,
            # but I think that small values should still work as long as position is 0.
            print(f"[Warning] Getxattr was called with position != 0 forh path '{path}' and key '{name}'.")
            print("[Warning] Please report this as an issue to the ratarmount project with details to reproduce this.")
            raise fuse.FuseOSError(errno.EOPNOTSUPP)

        value = self.mountSource.get_xattr(self._lookup(path), name)
        if value is None:
            # My system sometimes tries to request security.selinux without the key actually existing.
            # See https://man7.org/linux/man-pages/man2/getxattr.2.html#ERRORS
            raise fuse.FuseOSError(errno.ENODATA)
        return value
