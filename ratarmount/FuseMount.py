import contextlib
import ctypes
import errno
import os
import sys
import traceback
from typing import IO, Any, Dict, List, Optional, Tuple, Union

from ratarmountcore.mountsource import FileInfo, MountSource

# These imports can be particularly expensive when all fsspec backends are installed.
from ratarmountcore.mountsource.compositing.automount import AutoMountLayer
from ratarmountcore.mountsource.compositing.subvolumes import SubvolumesMountSource
from ratarmountcore.mountsource.compositing.union import UnionMountSource
from ratarmountcore.mountsource.compositing.versioning import FileVersionLayer
from ratarmountcore.mountsource.factory import open_mount_source
from ratarmountcore.mountsource.formats.folder import FolderMountSource
from ratarmountcore.utils import determine_recursion_depth, overrides

from .fuse import fuse
from .WriteOverlay import WritableFolderMountSource


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

    def __init__(self, pathToMount: Union[str, List[str]], mountPoint: str, **options) -> None:
        self.printDebug: int = int(options.get('printDebug', 0))
        self.writeOverlay: Optional[WritableFolderMountSource] = None
        self.overlayPath: Optional[str] = None

        # Only open the log file at the end shortly before it is needed to not end up with an empty file on error.
        logFilePath: str = options.get('logFile', '')
        options.pop('logFile', None)
        if logFilePath:
            logFilePath = os.path.realpath(logFilePath)

        self.mountPoint = os.path.realpath(mountPoint)
        # This check is important for the self-bind test below, which assumes a folder.
        if os.path.exists(self.mountPoint) and not os.path.isdir(self.mountPoint):
            raise ValueError("Mount point must either not exist or be a directory!")

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

        assert isinstance(pathToMount, list)
        if not pathToMount:
            raise ValueError("No paths to mount given!")
        # Take care that bind-mounting folders to itself works
        mountSources: List[Tuple[str, MountSource]] = []
        self.mountPointFd: Optional[int] = None
        self.selfBindMount: Optional[FolderMountSource] = None

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
                        if indexFilePath.startswith(fileURLPrefix):
                            indexFilePath = indexFilePath[len(fileURLPrefix) :]
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

        def create_multi_mount() -> MountSource:
            if not options.get('disableUnionMount', False):
                return UnionMountSource([x[1] for x in mountSources], **options)

            # Create unique keys.
            submountSources: Dict[str, MountSource] = {}
            suffix = 1
            for key, mountSource in mountSources:
                if key in submountSources:
                    while f"{key}.{suffix}" in submountSources:
                        suffix += 1
                    submountSources[f"{key}.{suffix}"] = mountSource
                else:
                    submountSources[key] = mountSource
            return SubvolumesMountSource(submountSources, printDebug=self.printDebug)

        self.mountSource: MountSource = mountSources[0][1] if len(mountSources) == 1 else create_multi_mount()

        if determine_recursion_depth(**options) > 0:
            self.mountSource = AutoMountLayer(self.mountSource, **options)

        # No threads should be created and still be open before FUSE forks.
        # Instead, they should be created in 'init'.
        # Therefore, close threads opened by the ParallelBZ2Reader for creating the block offsets.
        # Those threads will be automatically recreated again on the next read call.
        # Without this, the ratarmount background process won't quit even after unmounting!
        join_threads = getattr(self.mountSource, 'join_threads', None)
        if join_threads is not None:
            join_threads()

        self.mountSource = FileVersionLayer(self.mountSource)

        # Maps handles to either opened I/O objects or os module file handles for the writeOverlay and the open flags.
        self.openedFiles: Dict[int, Tuple[int, Union[IO[bytes], int]]] = {}
        self.lastFileHandle: int = 0  # It will be incremented before being returned. It can't hurt to never return 0.

        if self.overlayPath:
            self.writeOverlay = WritableFolderMountSource(self.overlayPath, self.mountSource)

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
            self.truncate = self.writeOverlay.truncate

        # Create mount point if it does not exist
        self.mountPointWasCreated = False
        if mountPoint and not os.path.exists(mountPoint):
            os.mkdir(mountPoint)
            self.mountPointWasCreated = True

        statResults = os.lstat(self.mountPoint)
        self.mountPointInfo = {key: getattr(statResults, key) for key in dir(statResults) if key.startswith('st_')}

        # Open log file.
        self.logFile: Optional[IO[str]] = None
        if logFilePath:
            self.logFile = open(logFilePath, 'w', buffering=1, encoding='utf-8')

        if self.printDebug >= 1:
            print("Created mount point at:", self.mountPoint)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if hasattr(super(), "__exit__"):
            super().__exit__(exception_type, exception_value, exception_traceback)
        self._close()

    def _close(self) -> None:
        try:
            if self.mountPointWasCreated:
                os.rmdir(self.mountPoint)
        except Exception:
            pass

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

    def __del__(self) -> None:
        self._close()

    def _add_new_handle(self, handle, flags):
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

        sys.stdout = file

    @overrides(fuse.Operations)
    def init(self, path) -> None:
        if self.logFile:
            self._redirect_output('stdout', self.logFile)
            self._redirect_output('stderr', self.logFile)

        if self.selfBindMount is not None and self.mountPointFd is not None:
            self.selfBindMount.set_folder_descriptor(self.mountPointFd)
            if self.writeOverlay and self.writeOverlay.root == self.mountPoint:
                self.writeOverlay.set_folder_descriptor(self.mountPointFd)

    @staticmethod
    def _file_info_to_dict(fileInfo: FileInfo):
        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = {"st_" + key: getattr(fileInfo, key) for key in ('size', 'mtime', 'mode', 'uid', 'gid')}
        statDict['st_mtime'] = int(statDict['st_mtime'])
        statDict['st_nlink'] = 1  # TODO: this is wrong for files with hardlinks

        # `du` sums disk usage (the number of blocks used by a file) instead of the file sizes by default.
        # So, we need to return some valid values. Tar files are usually a series of 512 B blocks, but this
        # block size is also used by Python as the default read call size, so it should be something larger
        # for better performance.
        blockSize = FuseMount.MINIMUM_BLOCK_SIZE
        statDict['st_blksize'] = blockSize
        statDict['st_blocks'] = 1 + ((fileInfo.size + blockSize - 1) // blockSize)

        return statDict

    @overrides(fuse.Operations)
    def getattr(self, path: str, fh=None) -> Dict[str, Any]:
        return self._file_info_to_dict(self._lookup(path))

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
            yield '.', self.getattr(path), 0

            if path == '/':
                yield '..', self.mountPointInfo, 0
            else:
                yield '..', self.getattr(path.rsplit('/', 1)[0]), 0
        else:
            yield '.'
            yield '..'

        deletedFiles = self.writeOverlay.list_deleted(path) if self.writeOverlay else []

        if isinstance(files, dict):
            for name, mode in files.items():
                if name not in deletedFiles:
                    yield name, {'st_mode': mode}, 0
        elif files is not None:
            for key in files:
                if key not in deletedFiles:
                    yield key

    @overrides(fuse.Operations)
    def readlink(self, path: str) -> str:
        return self._lookup(path).linkname

    @overrides(fuse.Operations)
    def open(self, path, flags):
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
    def release(self, path, fh):
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
    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        if fh in self.openedFiles:
            openedFile = self._resolve_file_handle(fh)
            if isinstance(openedFile, int):
                os.lseek(openedFile, offset, os.SEEK_SET)
                return os.read(openedFile, size)

            openedFile.seek(offset, os.SEEK_SET)
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
    def create(self, path, mode, fi=None):
        if self.writeOverlay:
            return self._add_new_handle(self.writeOverlay.create(path, mode, fi), 0)
        raise fuse.FuseOSError(errno.EROFS)

    @overrides(fuse.Operations)
    def write(self, path, data, offset, fh):
        if not self._is_write_overlay_handle(fh):
            flags, openedFile = self.openedFiles[fh]
            if self.writeOverlay and not isinstance(openedFile, int) and (flags & (os.O_WRONLY | os.O_RDWR)):
                openedFile.close()
                self.openedFiles[fh] = (flags, self.writeOverlay.open(path, flags))

        if self._is_write_overlay_handle(fh):
            return self.writeOverlay.write(path, data, offset, self._resolve_file_handle(fh))
        raise fuse.FuseOSError(errno.EROFS)

    @overrides(fuse.Operations)
    def flush(self, path, fh):
        if self._is_write_overlay_handle(fh):
            self.writeOverlay.flush(path, self._resolve_file_handle(fh))
        return 0  # Nothing to flush, so return success

    @overrides(fuse.Operations)
    def fsync(self, path, datasync, fh):
        if self._is_write_overlay_handle(fh):
            self.writeOverlay.fsync(path, datasync, self._resolve_file_handle(fh))
        return 0  # Nothing to flush, so return success

    @overrides(fuse.Operations)
    def statfs(self, path):
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
    def listxattr(self, path):
        # Beware, keys not prefixed with "user." will not be listed by getfattr by default.
        # Use: "getfattr --match=.* mounted/foo" It seems that libfuse and the FUSE kernel module accept
        # all keys, I tried with "key1", "security.key1", "user.key1".
        return self.mountSource.list_xattr(self._lookup(path))

    @overrides(fuse.Operations)
    def getxattr(self, path, name, position=0):
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
