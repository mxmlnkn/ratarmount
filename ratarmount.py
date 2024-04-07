#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import errno
import importlib
import json
import math
import os
import re
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
import urllib.parse
import urllib.request
import zipfile
from typing import Any, Callable, Dict, Iterable, IO, List, Optional, Tuple, Union

try:
    import fuse
except ImportError:
    import fusepy as fuse


try:
    import rarfile
except ImportError:
    pass

import ratarmountcore as core
from ratarmountcore import (
    AutoMountLayer,
    MountSource,
    FileVersionLayer,
    FolderMountSource,
    SQLiteIndexedTar,
    UnionMountSource,
    findModuleVersion,
    findAvailableOpen,
    openMountSource,
    overrides,
    supportedCompressions,
    stripSuffixFromTarFile,
    RatarmountError,
    SubvolumesMountSource,
    FileInfo,
)


__version__ = '0.15.0'


def hasNonEmptySupport() -> bool:
    try:
        # Check suffix of shared library
        if (
            'fuse' in sys.modules
            and hasattr(fuse, '_libfuse_path')
            and getattr(fuse, '_libfuse_path').endswith(".so.2")
        ):
            return True

        # Note that in Ubuntu 22.04 libfuse3 and libfuse2 can be installed side-by-side with fusermount 3 being
        # detected with precedence even though fusepy will use libfuse-2.9.9.
        with os.popen('fusermount -V') as pipe:
            match = re.search(r'([0-9]+)[.][0-9]+[.][0-9]+', pipe.read())
            if match:
                return int(match.group(1)) < 3
    except Exception:
        pass

    return False  # On macOS, fusermount does not exist and macfuse also seems to complain with nonempty option.


class WritableFolderMountSource(fuse.Operations):
    """
    This class manages one folder as mount source offering methods for reading and modification.
    """

    _overlayMetadataSchema = """
        CREATE TABLE "files" (
            "path"          VARCHAR(65535) NOT NULL,  /* path with leading and without trailing slash */
            "name"          VARCHAR(65535) NOT NULL,
            /* Some file systems may not support some metadata like permissions on NTFS, so also save them. */
            "mtime"         INTEGER,
            "mode"          INTEGER,
            "uid"           INTEGER,
            "gid"           INTEGER,
            "deleted"       BOOL,
            PRIMARY KEY (path,name)
        );
    """

    hiddenDatabaseName = '.ratarmount.overlay.sqlite'

    def __init__(self, path: str, mountSource: MountSource) -> None:
        if os.path.exists(path):
            if not os.path.isdir(path):
                raise ValueError("Overlay path must be a folder!")
        else:
            os.makedirs(path, exist_ok=True)

        self.root: str = path
        self.mountSource = mountSource
        self.sqlConnection = self._openSqlDb(os.path.join(path, self.hiddenDatabaseName))

        # Add table if necessary
        tables = [row[0] for row in self.sqlConnection.execute('SELECT name FROM sqlite_master WHERE type = "table";')]
        if "files" not in tables:
            self.sqlConnection.executescript(WritableFolderMountSource._overlayMetadataSchema)

        # Check that the mount source contains this overlay folder with top priority
        databaseFileInfo = self.mountSource.getFileInfo('/' + self.hiddenDatabaseName)
        assert databaseFileInfo is not None
        path, databaseMountSource, fileInfo = self.mountSource.getMountSource(databaseFileInfo)
        assert stat.S_ISREG(fileInfo.mode)
        assert isinstance(databaseMountSource, FolderMountSource)
        assert databaseMountSource.root == self.root

    @staticmethod
    def _openSqlDb(path: str, **kwargs) -> sqlite3.Connection:
        # isolation_level None is important so that changes are autocommitted because there is no manual commit call.
        sqlConnection = sqlite3.connect(path, isolation_level=None, **kwargs)
        sqlConnection.row_factory = sqlite3.Row
        sqlConnection.executescript(
            # Locking mode exclusive leads to a measurable speedup. E.g., find on 2k recursive files tar
            # improves from ~1s to ~0.4s!
            # https://blog.devart.com/increasing-sqlite-performance.html
            """
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            """
        )
        return sqlConnection

    def setFolderDescriptor(self, fd: int) -> None:
        """
        Make this mount source manage the special "." folder by changing to that directory.
        Because we change to that directory, it may only be used for one mount source but it also works
        when that mount source is mounted on!
        """
        os.fchdir(fd)
        self.root = '.'

    @staticmethod
    def _splitPath(path: str) -> Tuple[str, str]:
        result = ('/' + os.path.normpath(path).lstrip('/')).rsplit('/', 1)
        assert len(result) == 2
        return result[0], result[1]

    def _realpath(self, path: str) -> str:
        """Path given relative to folder root. Leading '/' is acceptable"""
        return os.path.join(self.root, path.lstrip(os.path.sep))

    def _ensureParentExists(self, path):
        """
        Creates parent folders for given path inside overlay folder if and only if they exist in the mount source.
        """
        parentPath = self._splitPath(path)[0]
        if not os.path.exists(self._realpath(parentPath)) and self.mountSource.isdir(parentPath):
            os.makedirs(self._realpath(parentPath), exist_ok=True)

    def _ensureFileIsModifiable(self, path):
        self._ensureParentExists(path)
        with self.mountSource.open(self.mountSource.getFileInfo(path)) as sourceObject, open(
            self._realpath(path), 'wb'
        ) as targetObject:
            shutil.copyfileobj(sourceObject, targetObject)

    def _open(self, path: str, mode):
        self._ensureParentExists(path)
        folder, name = self._splitPath(path)

        self.sqlConnection.execute(
            'INSERT OR IGNORE INTO "files" (path,name,mode,deleted) VALUES (?,?,?,?)', (folder, name, mode, False)
        )
        self.sqlConnection.execute(
            'UPDATE "files" SET deleted=0 WHERE path == (?) AND name == (?)',
            (folder, name),
        )

    def _markAsDeleted(self, path: str):
        """Hides the given path if it exists in the underlying mount source."""
        folder, name = self._splitPath(path)

        if self.mountSource.exists(path):
            self.sqlConnection.execute(
                'INSERT OR REPLACE INTO "files" (path,name,deleted) VALUES (?,?,?)', (folder, name, True)
            )
        else:
            self.sqlConnection.execute('DELETE FROM "files" WHERE (path,name) == (?,?)', (folder, name))

    def listDeleted(self, path: str) -> List[str]:
        """Return list of files marked as deleted in the given path."""
        result = self.sqlConnection.execute(
            'SELECT name FROM "files" WHERE path == (?) AND deleted == 1', (path.rstrip('/'),)
        )

        # For temporary SQLite file suffixes, see https://www.sqlite.org/tempfiles.html
        suffixes = ['', '-journal', '-shm', '-wal']
        return [x[0] for x in result] + [self.hiddenDatabaseName + suffix for suffix in suffixes]

    def isDeleted(self, path: str) -> bool:
        folder, name = self._splitPath(path)
        result = self.sqlConnection.execute(
            'SELECT COUNT(*) > 0 FROM "files" WHERE path == (?) AND name == (?) AND deleted == 1', (folder, name)
        )
        return bool(result.fetchone()[0])

    def _setMetadata(self, path: str, metadata: Dict[str, Any]):
        if not metadata:
            raise ValueError("Need arguments to know what to update.")

        allowedKeys = ["path", "name", "mtime", "mode", "uid", "gid"]
        for key in metadata:
            if key not in allowedKeys:
                raise ValueError(f"Invalid metadata key ({key}) specified")

        folder, name = self._splitPath(path)

        # https://stackoverflow.com/questions/31277027/using-placeholder-in-sqlite3-statements
        assignments = []
        values = []
        for key, value in metadata.items():
            values.append(value)
            assignments.append(f"{key} = (?)")

        self.sqlConnection.execute(
            f"""UPDATE "files" SET {', '.join(assignments)} WHERE "path" == ? and "name" == ?""",
            tuple(values) + (folder, name),
        )

    def _initFileMetadata(self, path: str):
        # Note that we do not have to check the overlay folder assuming that it is inside the (union) mount source!
        sourceFileInfo = self.mountSource.getFileInfo(path)
        if not sourceFileInfo:
            raise fuse.FuseOSError(errno.ENOENT)

        # Initialize new metadata entry from existing file
        sfi = self.mountSource.getMountSource(sourceFileInfo)[2]
        folder, name = self._splitPath(path)

        self.sqlConnection.execute(
            f'INSERT OR REPLACE INTO "files" VALUES ({",".join(["?"]*7)})',
            (folder, name, sfi.mtime, sfi.mode, sfi.uid, sfi.gid, False),
        )

    def _setFileMetadata(self, path: str, applyMetadataToFile: Callable[[str], None], metadata: Dict[str, Any]):
        folder, name = self._splitPath(path)

        existsInMetadata = self.sqlConnection.execute(
            'SELECT COUNT(*) > 1 FROM "files" WHERE "path" == (?) and "name" == (?)', (folder, name)
        ).fetchone()[0]

        if not existsInMetadata:
            self._initFileMetadata(path)
        self._setMetadata(path, metadata)

        # Apply the metadata change for the file in the overlay folder if it exists there.
        # This is only because it might be confusing for the user else but in general, the metadata in the SQLite
        # database should take precedence if e.g. the underlying file systems does not support them.
        try:
            if os.path.exists(self._realpath(path)):
                applyMetadataToFile(self._realpath(path))
        except Exception:
            traceback.print_exc()
            print("[Info] Caught exception when trying to apply metadata to real file.")
            print("[Info] It was applied in the metadata database!")

    def updateFileInfo(self, path: str, fileInfo: FileInfo):
        folder, name = self._splitPath(path)
        row = self.sqlConnection.execute(
            """SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?);""", (folder, name)
        ).fetchone()

        if not row:
            return fileInfo

        return FileInfo(
            # fmt: off
            size     = fileInfo.size,
            mtime    = row['mtime'] if row['mtime'] else fileInfo.mtime,
            mode     = row['mode'] if row['mode'] else fileInfo.mode,
            linkname = fileInfo.linkname,
            uid      = row['uid'] if row['uid'] else fileInfo.uid,
            gid      = row['gid'] if row['gid'] else fileInfo.gid,
            userdata = fileInfo.userdata,
            # fmt: on
        )

    # Metadata modification

    @overrides(fuse.Operations)
    def chmod(self, path, mode):
        self._setFileMetadata(path, lambda p: os.chmod(p, mode), {'mode': mode})

    @overrides(fuse.Operations)
    def chown(self, path, uid, gid):
        self._setFileMetadata(path, lambda p: os.chown(p, uid, gid), {'uid': uid, 'gid': gid})

    @overrides(fuse.Operations)
    def utimens(self, path, times=None):
        """Argument "times" is a (atime, mtime) tuple. If "times" is None, use the current time."""

        if times is None:
            mtime = time.time()
        else:
            mtime = times[1]

        self._setFileMetadata(path, lambda p: os.utime(p, times), {'mtime': mtime})

    @overrides(fuse.Operations)
    def rename(self, old, new):
        if not self.mountSource.exists(old) or self.isDeleted(old):
            raise fuse.FuseOSError(errno.ENOENT)

        folder, name = self._splitPath(new)

        # Delete target path from metadata database to avoid uniqueness restraint being invalidated
        self.sqlConnection.execute('DELETE FROM "files" WHERE "path" == (?) and "name" == (?)', (folder, name))
        self._setFileMetadata(old, lambda p: None, {'path': folder, 'name': name})

        if os.path.exists(self._realpath(old)):
            os.rename(self._realpath(old), self._realpath(new))
        else:
            self._ensureParentExists(new)

            with self.mountSource.open(self.mountSource.getFileInfo(old)) as sourceObject, open(
                self._realpath(new), 'wb'
            ) as targetObject:
                shutil.copyfileobj(sourceObject, targetObject)

            self._markAsDeleted(old)

    # Links

    @overrides(fuse.Operations)
    def symlink(self, target, source):
        os.symlink(source, self._realpath(target))

    @overrides(fuse.Operations)
    def link(self, target, source):
        # Can only hardlink to files which are also in the overlay folder.
        overlaySource = self._realpath(source)
        if not os.path.exists(overlaySource) and self.mountSource.getFileInfo(source):
            raise fuse.FuseOSError(errno.EXDEV)

        target = self._realpath(target)

        os.link(overlaySource, target)

    # Folders

    @overrides(fuse.Operations)
    def mkdir(self, path, mode):
        self._open(path, mode | stat.S_IFDIR)
        os.mkdir(self._realpath(path), mode)

    @overrides(fuse.Operations)
    def rmdir(self, path):
        if not self.mountSource.exists(path) or self.isDeleted(path):
            raise fuse.FuseOSError(errno.ENOENT)

        contents = self.mountSource.listDir(path)
        if contents is not None and set(contents.keys()) - set(self.listDeleted(path)):
            raise fuse.FuseOSError(errno.ENOTEMPTY)

        try:
            if os.path.exists(self._realpath(path)):
                os.rmdir(self._realpath(path))
        except Exception as exception:
            traceback.print_exc()
            raise fuse.FuseOSError(errno.EIO) from exception
        finally:
            self._markAsDeleted(path)

    # Files

    @overrides(fuse.Operations)
    def open(self, path, flags):
        # if flags & os.O_CREAT != 0:  # I hope that FUSE simple calls create in this case.
        #    self._open(path)   # what would the default mode even be?
        if not os.path.exists(self._realpath(path)):
            if not self.mountSource.exists(path):
                raise fuse.FuseOSError(errno.ENOENT)

            if flags & (os.O_WRONLY | os.O_RDWR):
                self._ensureFileIsModifiable(path)

        return os.open(self._realpath(path), flags)

    @overrides(fuse.Operations)
    def create(self, path, mode, fi=None):
        self._open(path, mode)
        return os.open(self._realpath(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)

    @overrides(fuse.Operations)
    def unlink(self, path):
        # Note that despite the name this is called for removing both, files and links.

        if not self.mountSource.exists(path) or self.isDeleted(path):
            # This is for the rare case that the file only exists in the overlay metadata database.
            self._markAsDeleted(path)
            raise fuse.FuseOSError(errno.ENOENT)

        try:
            if os.path.exists(self._realpath(path)):
                os.unlink(self._realpath(path))
        except Exception as exception:
            traceback.print_exc()
            raise fuse.FuseOSError(errno.EIO) from exception
        finally:
            self._markAsDeleted(path)

    @overrides(fuse.Operations)
    def mknod(self, path, mode, dev):
        self._ensureParentExists(path)
        os.mknod(self._realpath(path), mode, dev)

    @overrides(fuse.Operations)
    def truncate(self, path, length, fh=None):
        self._ensureFileIsModifiable(path)
        os.truncate(self._realpath(path), length)

    # Actual writing

    @overrides(fuse.Operations)
    def write(self, path, data, offset, fh):
        os.lseek(fh, offset, 0)
        return os.write(fh, data)

    # Flushing

    @overrides(fuse.Operations)
    def flush(self, path, fh):
        return os.fsync(fh)

    @overrides(fuse.Operations)
    def fsync(self, path, datasync, fh):
        return os.fsync(fh) if datasync == 0 else os.fdatasync(fh)

    @overrides(fuse.Operations)
    def statfs(self, path):
        stv = os.statvfs(self._realpath(path))
        return dict(
            (key, getattr(stv, key))
            for key in (
                'f_bavail',
                'f_bfree',
                'f_blocks',
                'f_bsize',
                'f_favail',
                'f_ffree',
                'f_files',
                'f_flag',
                'f_frsize',
                'f_namemax',
            )
        )


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

    All path arguments for overriden fusepy methods do have a leading slash ('/')!
    This is why MountSource also should expect leading slashes in all paths.
    """

    def __init__(self, pathToMount: Union[str, List[str]], mountPoint: str, **options) -> None:
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

        assert isinstance(pathToMount, list)
        pathToMount = list(filter(os.path.exists, pathToMount))

        options['writeIndex'] = True
        if 'recursive' not in options and options.get('recursionDepth', 0) != 0:
            options['recursive'] = True

        self.printDebug: int = int(options.get('printDebug', 0))
        self.writeOverlay: Optional[WritableFolderMountSource] = None
        self.overlayPath: Optional[str] = None

        # Add write overlay as folder mount source to read from with highest priority.
        if 'writeOverlay' in options and isinstance(options['writeOverlay'], str) and options['writeOverlay']:
            self.overlayPath = os.path.realpath(options['writeOverlay'])
            if not os.path.exists(self.overlayPath):
                os.makedirs(self.overlayPath, exist_ok=True)
            pathToMount.append(self.overlayPath)

        # Take care that bind-mounting folders to itself works
        mountSources: Dict[str, MountSource] = {}
        self.mountPointFd: Optional[int] = None
        self.selfBindMount: Optional[FolderMountSource] = None
        for path in pathToMount:
            if os.path.realpath(path) != self.mountPoint:
                continue

            mountSource = FolderMountSource(path)
            mountSources[os.path.basename(path)] = mountSource  # type: ignore
            self.selfBindMount = mountSource
            self.mountPointFd = os.open(self.mountPoint, os.O_RDONLY)

            # Lazy mounting can result in locking recursive calls into our own FUSE mount point.
            # Opening the archives is already handled correctly without calling FUSE inside AutoMountLayer.
            # Here we need to ensure that indexes are not tried to being read from or written to our own
            # FUSE mount point.
            if options.get('lazyMounting', False):

                def pointsIntoMountPoint(pathToTest):
                    return os.path.commonpath([pathToTest, self.mountPoint]) == self.mountPoint

                hasIndexPath = False

                if 'indexFilePath' in options and isinstance(options['indexFilePath'], str):
                    indexFilePath = os.path.realpath(options['indexFilePath'])
                    if pointsIntoMountPoint(indexFilePath):
                        del options['indexFilePath']
                    else:
                        options['indexFilePath'] = indexFilePath
                        hasIndexPath = True

                if 'indexFolders' in options and isinstance(options['indexFolders'], list):
                    indexFolders = options['indexFolders']
                    newIndexFolders = []
                    for folder in indexFolders:
                        if pointsIntoMountPoint(folder):
                            continue
                        newIndexFolders.append(os.path.realpath(folder))
                    options['indexFolders'] = newIndexFolders
                    if newIndexFolders:
                        hasIndexPath = True

                # Force in-memory indexes if no folder remains because the default for no indexFilePath being
                # specified would be in a file in the same folder as the archive.
                if not hasIndexPath:
                    options['indexFilePath'] = ':memory:'

            break

        # This also will create or load the block offsets for compressed formats
        for path in pathToMount:
            if os.path.realpath(path) == self.mountPoint:
                continue
            key = os.path.basename(path)
            if key not in mountSources:
                mountSources[key] = openMountSource(path, **options)

        self.mountSource: MountSource = (
            (
                SubvolumesMountSource(mountSources, printDebug=self.printDebug)
                if options.get('disableUnionMount', False)
                else UnionMountSource(list(mountSources.values()), **options)
            )
            if len(mountSources) != 1
            else next(iter(mountSources.values()))
        )
        if options.get('recursionDepth', 0):
            self.mountSource = AutoMountLayer(self.mountSource, **options)

        # No threads should be created and still be open before FUSE forks.
        # Instead, they should be created in 'init'.
        # Therefore, close threads opened by the ParallelBZ2Reader for creating the block offsets.
        # Those threads will be automatically recreated again on the next read call.
        # Without this, the ratarmount background process won't quit even after unmounting!
        joinThreads = getattr(self.mountSource, 'joinThreads', None)
        if joinThreads is not None:
            joinThreads()

        self.mountSource = FileVersionLayer(self.mountSource)

        self.rootFileInfo = FuseMount._makeMountPointFileInfoFromStats(os.stat(pathToMount[0]))

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

            self.statfs = self.writeOverlay.statfs

        # Create mount point if it does not exist
        self.mountPointWasCreated = False
        if mountPoint and not os.path.exists(mountPoint):
            os.mkdir(mountPoint)
            self.mountPointWasCreated = True

        statResults = os.lstat(self.mountPoint)
        self.mountPointInfo = {key: getattr(statResults, key) for key in dir(statResults) if key.startswith('st_')}

    def __del__(self) -> None:
        try:
            if self.mountPointWasCreated:
                os.rmdir(self.mountPoint)
        except Exception:
            pass

        try:
            if self.mountPointFd is not None:
                os.close(self.mountPointFd)
        except Exception:
            pass

        try:
            self.mountSource.__exit__(None, None, None)
        except Exception:
            pass

    def _addNewHandle(self, handle, flags):
        # Note that fh in fuse_common.h is 64-bit and Python also supports 64-bit (long integers) out of the box.
        # So, there should practically be no overflow and file handle reuse possible.
        self.lastFileHandle += 1
        self.openedFiles[self.lastFileHandle] = (flags, handle)
        return self.lastFileHandle

    @staticmethod
    def _makeMountPointFileInfoFromStats(stats: os.stat_result) -> FileInfo:
        # make the mount point read only and executable if readable, i.e., allow directory listing
        # clear higher bits like S_IFREG and set the directory bit instead
        mountMode = (
            (stats.st_mode & 0o777)
            | stat.S_IFDIR
            | (stat.S_IXUSR if stats.st_mode & stat.S_IRUSR != 0 else 0)
            | (stat.S_IXGRP if stats.st_mode & stat.S_IRGRP != 0 else 0)
            | (stat.S_IXOTH if stats.st_mode & stat.S_IROTH != 0 else 0)
        )

        fileInfo = FileInfo(
            # fmt: off
            size     = stats.st_size,
            mtime    = stats.st_mtime,
            mode     = mountMode,
            linkname = "",
            uid      = stats.st_uid,
            gid      = stats.st_gid,
            userdata = [],
            # fmt: on
        )

        return fileInfo

    def _getFileInfo(self, path: str) -> FileInfo:
        if self.writeOverlay and self.writeOverlay.isDeleted(path):
            raise fuse.FuseOSError(errno.ENOENT)

        fileInfo = self.mountSource.getFileInfo(path)
        if fileInfo is None:
            raise fuse.FuseOSError(errno.ENOENT)

        if not self.writeOverlay:
            return fileInfo

        # Request exact metadata from write overlay, e.g., if the actual file in the folder
        # does not support permission changes
        result = self.mountSource.getMountSource(fileInfo)
        subMountPoint = result[0]
        # TODO Note that if the path contains special .version versioning, then it will most likely fail
        #      to find the path in the write overlay, which is problematic for things like foo.versions/0.
        #      Would be really helpful if the file info would contain the actual path and name, too :/
        return self.writeOverlay.updateFileInfo(path[len(subMountPoint) :], fileInfo)

    @overrides(fuse.Operations)
    def init(self, path) -> None:
        if self.selfBindMount is not None and self.mountPointFd is not None:
            self.selfBindMount.setFolderDescriptor(self.mountPointFd)
            if self.writeOverlay and self.writeOverlay.root == self.mountPoint:
                self.writeOverlay.setFolderDescriptor(self.mountPointFd)

    @staticmethod
    def _fileInfoToDict(fileInfo: FileInfo):
        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = {"st_" + key: getattr(fileInfo, key) for key in ('size', 'mtime', 'mode', 'uid', 'gid')}
        statDict['st_mtime'] = int(statDict['st_mtime'])
        statDict['st_nlink'] = 1  # TODO: this is wrong for files with hardlinks

        # du by default sums disk usage (the number of blocks used by a file)
        # instead of file size directly. Tar files are usually a series of 512B
        # blocks, so we report a 1-block header + ceil(filesize / 512).
        statDict['st_blksize'] = 512
        statDict['st_blocks'] = 1 + ((fileInfo.size + 511) // 512)

        return statDict

    @overrides(fuse.Operations)
    def getattr(self, path: str, fh=None) -> Dict[str, Any]:
        return self._fileInfoToDict(self._getFileInfo(path))

    @overrides(fuse.Operations)
    def readdir(self, path: str, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        files = self.mountSource.listDir(path)

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

        deletedFiles = self.writeOverlay.listDeleted(path) if self.writeOverlay else []

        if isinstance(files, dict):
            for key, fileInfo in files.items():
                if key not in deletedFiles:
                    yield key, self._fileInfoToDict(fileInfo), 0
        elif files is not None:
            for key in files:
                if key not in deletedFiles:
                    yield key

    @overrides(fuse.Operations)
    def readlink(self, path: str) -> str:
        return self._getFileInfo(path).linkname

    @overrides(fuse.Operations)
    def open(self, path, flags):
        """Returns file handle of opened path."""

        fileInfo = self._getFileInfo(path)

        try:
            # If the flags indicate "open for modification", then still open it as read-only through the mount source
            # but store information to reopen it for write access on write calls.
            # @see https://man7.org/linux/man-pages/man2/open.2.html
            # > The argument flags must include one of the following access modes: O_RDONLY, O_WRONLY, or O_RDWR.
            return self._addNewHandle(self.mountSource.open(fileInfo), flags)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to open file.", fileInfo)
            raise fuse.FuseOSError(errno.EIO) from exception

    @overrides(fuse.Operations)
    def release(self, path, fh):
        if fh not in self.openedFiles:
            raise fuse.FuseOSError(errno.ESTALE)

        openedFile = self._resolveFileHandle(fh)
        if isinstance(openedFile, int):
            os.close(openedFile)
        else:
            openedFile.close()
            del openedFile

        return fh

    @overrides(fuse.Operations)
    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        if fh in self.openedFiles:
            openedFile = self._resolveFileHandle(fh)
            if isinstance(openedFile, int):
                os.lseek(openedFile, offset, os.SEEK_SET)
                return os.read(openedFile, size)

            openedFile.seek(offset, os.SEEK_SET)
            return openedFile.read(size)

        # As far as I understand FUSE and my own file handle cache, this should never happen. But you never know.
        if self.printDebug >= 1:
            print("[Warning] Given file handle does not exist. Will open file before reading which might be slow.")

        fileInfo = self._getFileInfo(path)

        try:
            return self.mountSource.read(fileInfo, size, offset)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to read data from underlying TAR file! Returning errno.EIO.")
            raise fuse.FuseOSError(errno.EIO) from exception

    # Methods for the write overlay which require file handle translations

    def _isWriteOverlayHandle(self, fh):
        return self.writeOverlay and fh in self.openedFiles and isinstance(self._resolveFileHandle(fh), int)

    def _resolveFileHandle(self, fh):
        return self.openedFiles[fh][1]

    @overrides(fuse.Operations)
    def create(self, path, mode, fi=None):
        if self.writeOverlay:
            return self._addNewHandle(self.writeOverlay.create(path, mode, fi), 0)
        return super().create(path, mode, fi)

    @overrides(fuse.Operations)
    def write(self, path, data, offset, fh):
        if not self._isWriteOverlayHandle(fh):
            flags, openedFile = self.openedFiles[fh]
            if self.writeOverlay and not isinstance(openedFile, int) and (flags & (os.O_WRONLY | os.O_RDWR)):
                openedFile.close()
                self.openedFiles[fh] = (flags, self.writeOverlay.open(path, flags))

        if self._isWriteOverlayHandle(fh):
            return self.writeOverlay.write(path, data, offset, self._resolveFileHandle(fh))
        return super().write(path, data, offset, fh)

    @overrides(fuse.Operations)
    def flush(self, path, fh):
        if self._isWriteOverlayHandle(fh):
            self.writeOverlay.flush(path, self._resolveFileHandle(fh))
        return super().flush(path, fh)

    @overrides(fuse.Operations)
    def fsync(self, path, datasync, fh):
        if self._isWriteOverlayHandle(fh):
            self.writeOverlay.fsync(path, datasync, self._resolveFileHandle(fh))
        return super().fsync(path, datasync, fh)


def checkInputFileType(
    tarFile: str, encoding: str = tarfile.ENCODING, printDebug: int = 0
) -> Tuple[str, Optional[str]]:
    """Raises an exception if it is not an accepted archive format else returns the real path and compression type."""

    if not os.path.isfile(tarFile):
        raise argparse.ArgumentTypeError(f"File '{tarFile}' is not a file!")
    tarFile = os.path.realpath(tarFile)

    result = core.checkForSplitFile(tarFile)
    if result:
        return result[0][0], 'part' + result[1]

    with open(tarFile, 'rb') as fileobj:
        fileSize = os.stat(tarFile).st_size

        # Header checks are enough for this step.
        oldOffset = fileobj.tell()
        compression = None
        for compressionId, compressionInfo in supportedCompressions.items():
            try:
                if compressionInfo.checkHeader(fileobj):
                    compression = compressionId
                    break
            finally:
                fileobj.seek(oldOffset)

        try:
            # Determining if there are many frames in zstd is O(1) with is_multiframe
            if compression != 'zst':
                raise Exception()  # early exit because we catch it anyways

            formatOpen = findAvailableOpen(compression)
            if not formatOpen:
                raise Exception()  # early exit because we catch it anyways

            zstdFile = formatOpen(fileobj)

            if not zstdFile.is_multiframe() and fileSize > 1024 * 1024:
                print(f"[Warning] The specified file '{tarFile}'")
                print("[Warning] is compressed using zstd but only contains one zstd frame. This makes it ")
                print("[Warning] impossible to use true seeking! Please (re)compress your TAR using multiple ")
                print("[Warning] frames in order for ratarmount to do be able to do fast seeking to requested ")
                print("[Warning] files. Else, each file access will decompress the whole TAR from the beginning!")
                print("[Warning] You can try out t2sz for creating such archives:")
                print("[Warning] https://github.com/martinellimarco/t2sz")
                print("[Warning] Here you can find a simple bash script demonstrating how to do this:")
                print("[Warning] https://github.com/mxmlnkn/ratarmount#xz-and-zst-files")
                print()
        except Exception:
            pass

        if compression not in supportedCompressions:
            if SQLiteIndexedTar._detectTar(fileobj, encoding, printDebug=printDebug):
                return tarFile, compression

            if printDebug >= 2:
                print(f"Archive '{tarFile}' (compression: {compression}) can't be opened!")

            raise argparse.ArgumentTypeError(f"Archive '{tarFile}' can't be opened!")

    if not findAvailableOpen(compression):
        moduleNames = [module.name for module in supportedCompressions[compression].modules]
        raise argparse.ArgumentTypeError(
            f"Cannot open a {compression} compressed TAR file '{fileobj.name}' "
            f"without any of these modules: {moduleNames}"
        )

    return tarFile, compression


def _removeDuplicatesStable(iterable: Iterable):
    seen = set()
    deduplicated = []
    for x in iterable:
        if x not in seen:
            deduplicated.append(x)
            seen.add(x)
    return deduplicated


class _CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    def add_arguments(self, actions):
        actions = sorted(actions, key=lambda x: getattr(x, 'option_strings'))
        super().add_arguments(actions)


class PrintVersionAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        print("ratarmount", __version__)
        print("ratarmountcore", core.__version__)

        print()
        print("System Software:")
        print()
        print("Python", sys.version.split(' ', maxsplit=1)[0])

        try:
            fusermountVersion = subprocess.run(
                ["fusermount", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
            ).stdout.strip()
            print("FUSE", re.sub('.* ([0-9][.][0-9.]+).*', r'\1', fusermountVersion.decode()))
        except Exception:
            pass

        print("libsqlite3", sqlite3.sqlite_version)

        print()
        print("Compression Backends:")
        print()

        def printModuleVersion(moduleName: str):
            try:
                importlib.import_module(moduleName)
            except ImportError:
                pass

            if moduleName in sys.modules:
                moduleVersion = findModuleVersion(sys.modules[moduleName])
                if moduleVersion:
                    print(moduleName, moduleVersion)

        modules = set(module.name for _, info in supportedCompressions.items() for module in info.modules)
        for moduleName in sorted(list(modules)):
            printModuleVersion(moduleName)

        mappedFilesFolder = f"/proc/{os.getpid()}/map_files"
        if os.path.isdir(mappedFilesFolder):
            libraries = set(
                os.readlink(os.path.join(mappedFilesFolder, link)) for link in os.listdir(mappedFilesFolder)
            )
            # Only look for shared libraries with versioning suffixed. Ignore all ending on .so.
            libraries = set(library for library in libraries if '.so.' in library)

            if libraries:
                print()
                print("Versioned Loaded Shared Libraries:")
                print()

            for library in sorted(list(libraries)):
                print(library.rsplit('/', maxsplit=1)[-1])

        sys.exit(0)


def getXdgCacheHome():
    # https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
    # > $XDG_CACHE_HOME defines the base directory relative to which user-specific non-essential data files should
    # > be stored. If $XDG_CACHE_HOME is either not set or empty, a default equal to $HOME/.cache should be used.
    path = os.environ.get('XDG_CACHE_HOME', '')
    return path if path else os.path.join('~', '.cache')


class PrintOSSAttributionAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        licenses = []
        for name, githubPath in [
            ("fusepy", "/fusepy/fusepy/master/LICENSE"),
            ("python-xz", "/Rogdham/python-xz/master/LICENSE.txt"),
            ("rarfile", "/markokr/rarfile/master/LICENSE"),
            ("libfuse", "/libfuse/libfuse/master/LGPL2.txt"),
            ("libsqlite3", "/sqlite/sqlite/master/LICENSE.md"),
            ("cpython", "/python/cpython/main/LICENSE"),
            ("libzstd-seek", "/martinellimarco/libzstd-seek/main/LICENSE"),
            ("zstd", "/facebook/zstd/dev/LICENSE"),
            ("zlib", "/madler/zlib/master/LICENSE"),
            ("ratarmountcore", "/mxmlnkn/ratarmount/master/core/LICENSE"),
            ("indexed_gzip", "/mxmlnkn/indexed_gzip/master/LICENSE"),
            ("indexed_zstd", "/martinellimarco/indexed_zstd/master/LICENSE"),
            ("rapidgzip", "/mxmlnkn/rapidgzip/master/LICENSE-MIT"),
        ]:
            licenseUrl = "https://raw.githubusercontent.com" + githubPath
            licenseContents = urllib.request.urlopen(licenseUrl).read().decode()
            homepage = "https://github.com" + '/'.join(githubPath.split('/', 3)[:3])
            licenses.append((name, homepage, licenseContents))

        for moduleName, url, licenseContents in sorted(licenses):
            print(f"# {moduleName}\n\n{url}\n\n\n```\n{licenseContents}\n```\n\n")

        sys.exit(0)


def _parseArgs(rawArgs: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        formatter_class=_CustomFormatter,
        add_help=False,
        description='''\
With ratarmount, you can:
  - Mount a (compressed) TAR file to a folder for read-only access
  - Mount a compressed file to `<mountpoint>/<filename>`
  - Bind mount a folder to another folder for read-only access
  - Union mount a list of TARs, compressed files, and folders to a mount point
    for read-only access
''',
        epilog='''\
# Metadata Index Cache

In order to reduce the mounting time, the created index for random access
to files inside the tar will be saved to one of these locations. These
locations are checked in order and the first, which works sufficiently, will
be used. This is the default location order:

  1. <path to tar>.index.sqlite
  2. ~/.ratarmount/<path to tar: '/' -> '_'>.index.sqlite
     E.g., ~/.ratarmount/_media_cdrom_programm.tar.index.sqlite

This list of fallback folders can be overwritten using the `--index-folders`
option. Furthermore, an explicitly named index file may be specified using
the `--index-file` option. If `--index-file` is used, then the fallback
folders, including the default ones, will be ignored!

# Bind Mounting

The mount sources can be TARs and/or folders.  Because of that, ratarmount
can also be used to bind mount folders read-only to another path similar to
`bindfs` and `mount --bind`. So, for:

    ratarmount folder mountpoint

all files in `folder` will now be visible in mountpoint.

# Union Mounting

If multiple mount sources are specified, the sources on the right side will be
added to or update existing files from a mount source left of it. For example:

    ratarmount folder1 folder2 mountpoint

will make both, the files from folder1 and folder2, visible in mountpoint.
If a file exists in both multiple source, then the file from the rightmost
mount source will be used, which in the above example would be `folder2`.

If you want to update / overwrite a folder with the contents of a given TAR,
you can specify the folder both as a mount source and as the mount point:

    ratarmount folder file.tar folder

The FUSE option -o nonempty will be automatically added if such a usage is
detected. If you instead want to update a TAR with a folder, you only have to
swap the two mount sources:

    ratarmount file.tar folder folder

# File versions

If a file exists multiple times in a TAR or in multiple mount sources, then
the hidden versions can be accessed through special <file>.versions folders.
For example, consider:

    ratarmount folder updated.tar mountpoint

and the file `foo` exists both in the folder and as two different versions
in `updated.tar`. Then, you can list all three versions using:

    ls -la mountpoint/foo.versions/
        dr-xr-xr-x 2 user group     0 Apr 25 21:41 .
        dr-x------ 2 user group 10240 Apr 26 15:59 ..
        -r-x------ 2 user group   123 Apr 25 21:41 1
        -r-x------ 2 user group   256 Apr 25 21:53 2
        -r-x------ 2 user group  1024 Apr 25 22:13 3

In this example, the oldest version has only 123 bytes while the newest and
by default shown version has 1024 bytes. So, in order to look at the oldest
version, you can simply do:

    cat mountpoint/foo.versions/1

Note that these version numbers are the same as when used with tar's
`--occurrence=N` option.

## Prefix Removal

Use `ratarmount -o modules=subdir,subdir=<prefix>` to remove path prefixes
using the FUSE `subdir` module. Because it is a standard FUSE feature, the
`-o ...` argument should also work for other FUSE applications.

When mounting an archive created with absolute paths, e.g.,
`tar -P cf /var/log/apt/history.log`, you would see the whole `var/log/apt`
hierarchy under the mount point. To avoid that, specified prefixes can be
stripped from paths so that the mount target directory **directly** contains
`history.log`. Use `ratarmount -o modules=subdir,subdir=/var/log/apt/` to do
so. The specified path to the folder inside the TAR will be mounted to root,
i.e., the mount point.

# Compressed non-TAR files

If you want a compressed file not containing a TAR, e.g., `foo.bz2`, then
you can also use ratarmount for that. The uncompressed view will then be
mounted to `<mountpoint>/foo` and you will be able to leverage ratarmount's
seeking capabilities when opening that file.
''',
    )

    commonGroup = parser.add_argument_group("Optional Arguments")
    positionalGroup = parser.add_argument_group("Positional Options")
    indexGroup = parser.add_argument_group("Index Options")
    recursionGroup = parser.add_argument_group("Recursion Options")
    tarGroup = parser.add_argument_group("Tar Options")
    writeGroup = parser.add_argument_group("Write Overlay Options")
    advancedGroup = parser.add_argument_group("Advanced Options")

    defaultParallelization = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()

    # fmt: off
    commonGroup.add_argument(
        '-h', '--help', action='help', default=argparse.SUPPRESS,
        help='Show this help message and exit.')

    indexGroup.add_argument(
        '-c', '--recreate-index', action='store_true', default=False,
        help='If specified, pre-existing .index files will be deleted and newly created.')

    commonGroup.add_argument(
        '-r', '--recursive', action='store_true', default=False,
        help='Mount archives inside archives recursively. Same as --recursion-depth -1.')

    commonGroup.add_argument(
        '-u', '--unmount', action='store_true',
        help='Unmount the given mount point. Equivalent to calling "fusermount -u".')

    commonGroup.add_argument(
        '-P', '--parallelization', type=int, default=0,
        help='If an integer other than 1 is specified, then the threaded parallel bzip2 decoder will be used '
             'specified amount of block decoder threads. Further threads with lighter work may be started. '
             f'A value of 0 will use all the available cores ({defaultParallelization}).')

    commonGroup.add_argument(
        '-v', '--version', action=PrintVersionAction, nargs=0, default=argparse.SUPPRESS,
        help='Print version information and exit.')

    commonGroup.add_argument(
        '--password', type=str, default='',
        help='Specify a single password which shall be used for RAR and ZIP files.')

    # Index Options

    indexGroup.add_argument(
        '--verify-mtime', action='store_true',
        help='By default, only the TAR file size is checked to match the one in the found existing ratarmount index. '
             'If this option is specified, then also check the modification timestamp. But beware that the mtime '
             'might change during copying or downloading without the contents changing. So, this check might cause '
             'false positives.')

    indexGroup.add_argument(
        '--index-file', type=str,
        help='Specify a path to the .index.sqlite file. Setting this will disable fallback index folders. '
             'If the given path is ":memory:", then the index will not be written out to disk.')

    indexFolders = ['', os.path.join( "~", ".ratarmount")]
    xdgCacheHome = getXdgCacheHome()
    if xdgCacheHome and os.path.isdir(os.path.expanduser(xdgCacheHome)):
        indexFolders.insert(1, os.path.join(xdgCacheHome, 'ratarmount'))
    containsComma = any(',' in folder for folder in indexFolders)
    indexFoldersAsString = json.dumps(indexFolders) if containsComma else ','.join(indexFolders)

    indexGroup.add_argument(
        '--index-folders', default=indexFoldersAsString,
        help='Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for suitability '
             'in the given order. An empty path will be interpreted as the location in which the TAR resides. '
             'If the argument begins with a bracket "[", then it will be interpreted as a JSON-formatted list. '
             'If the argument contains a comma ",", it will be interpreted as a comma-separated list of folders. '
             'Else, the whole string will be interpreted as one folder path. Examples: '
             '--index-folders ",~/.foo" will try to save besides the TAR and if that does not work, in ~/.foo. '
             '--index-folders \'["~/.ratarmount", "foo,9000"]\' will never try to save besides the TAR. '
             '--index-folder ~/.ratarmount will only test ~/.ratarmount as a storage location and nothing else. '
             'Instead, it will first try ~/.ratarmount and the folder "foo,9000". ')

    # Recursion Options

    # TODO The recursion depth is only heeded by AutoMountLayer but not by SQLiteIndexedTar.
    #      One problem is that it requires an update to the index metadata information and
    #      the other problem is that the AutoMountLayer would have to ask how deep the recursion
    #      for a particular path is so that it can correctly stop recursive mounting and the
    #      combined recursion depth.
    recursionGroup.add_argument(
        '--recursion-depth', type=int, default=None,
        help='This option takes precedence over --recursive. '
             'Mount archives inside the mounted archives recursively up to the given depth. '
             'A negative value represents infinite depth. '
             'A value of 0 will turn off recursion (same as not specifying --recursive in the first place). '
             'A value of 1 will recursively mount all archives in the given archives but not any deeper. '
             'Note that this only has an effect when creating an index. '
             'If an index already exists, then this option will be effectively ignored. '
             'Recreate the index if you want change the recursive mounting policy anyways.')

    recursionGroup.add_argument(
        '-l', '--lazy', action='store_true', default=False,
        help='When used with recursively bind-mounted folders, TAR files inside the mounted folder will only be '
             'mounted on first access to it.')

    recursionGroup.add_argument(
        '-s', '--strip-recursive-tar-extension', action='store_true',
        help='If true, then recursively mounted TARs named <file>.tar will be mounted at <file>/. '
             'This might lead to folders of the same name being overwritten, so use with care. '
             'The index needs to be (re)created to apply this option!')

    recursionGroup.add_argument(
        '--transform-recursive-mount-point', type=str, nargs=2, metavar=('REGEX_PATTERN', 'REPLACEMENT'),
        help='Specify a regex pattern and a replacement string, which will be applied via Python\'s re module '
             'to the full path of the archive to be recursively mounted. E.g., if there are recursive archives: '
             '/folder/archive.tar.gz, you can substitute \'[.][^/]+$\' to \'\' and it will be mounted to '
             '/folder/archive.tar. Or you can replace \'^.*/([^/]+).tar.gz$\' to \'/\1\' to mount all recursive '
             'folders under the top-level without extensions.')

    # TAR Options

    tarGroup.add_argument(
        '-e', '--encoding', type=str, default=tarfile.ENCODING,
        help='Specify an input encoding used for file names among others in the TAR. '
             'This must be used when, e.g., trying to open a latin1 encoded TAR on an UTF-8 system. '
             'Possible encodings: https://docs.python.org/3/library/codecs.html#standard-encodings')

    tarGroup.add_argument(
        '-i', '--ignore-zeros', action='store_true',
        help='Ignore zeroed blocks in archive. Normally, two consecutive 512-blocks filled with zeroes mean EOF '
             'and ratarmount stops reading after encountering them. This option instructs it to read further and '
             'is useful when reading archives created with the -A option.')

    tarGroup.add_argument(
        '--gnu-incremental', dest='gnu_incremental', action='store_true', default=False,
        help='Will strip octal modification time prefixes from file paths, which appear in GNU incremental backups '
             'created with GNU tar with the --incremental or --listed-incremental options.')

    tarGroup.add_argument(
        '--no-gnu-incremental', dest='gnu_incremental', action='store_false', default=False,
        help='If specified, will never strip octal modification prefixes and will also not do automatic detection.')

    tarGroup.add_argument(
        '--detect-gnu-incremental', dest='gnu_incremental', action='store_const', const=None, default=False,
        help='If specified, will automatically try to detect GNU tar incremental files and, if so, will strip '
             'octal modification prefixes. Note that this is only a heuristic derived by testing 1000-10000 file '
             'entries. If you are sure it is an incremental TAR, use --gnu-incremental instead.')

    # Write Overlay Options

    writeGroup.add_argument(
        '-w', '--write-overlay',
        help='Specify an existing folder to be used as a write overlay. The folder itself will be union-mounted '
             'on top such that files in this folder take precedence over all other existing ones. Furthermore, '
             'all file creations and modifications will be forwarded to files in this folder. '
             'Modifying a file inside a TAR will copy that file to the overlay folder and apply the modification '
             'to that writable copy. Deleting files or folders will update the hidden metadata database inside '
             'the overlay folder.')

    writeGroup.add_argument(
        '--commit-overlay', action='store_true', default=False,
        help='Apply deletions and content modifications done in the write overlay to the archive.')

    # Advanced Options

    advancedGroup.add_argument(
        '-o', '--fuse', type=str, default='',
        help='Comma separated FUSE options. See "man mount.fuse" for help. '
             'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ')

    advancedGroup.add_argument(
        '-f', '--foreground', action='store_true', default=False,
        help='Keeps the python program in foreground so it can print debug '
             'output when the mounted path is accessed.')

    advancedGroup.add_argument(
        '-d', '--debug', type=int, default=1,
        help='Sets the debugging level. Higher means more output. Currently, 3 is the highest.')

    # Considerations for the default value:
    #   - seek times for the bz2 backend are between 0.01s and 0.1s
    #   - seek times for the gzip backend are roughly 1/10th compared to bz2 at a default spacing of 4MiB
    #     -> we could do a spacing of 40MiB (however the comparison are for another test archive, so it might not apply)
    #   - ungziping firefox 66 inflates the compressed size of 66MiB to 184MiB (~3 times more) and takes 1.4s on my PC
    #     -> to have a response time of 0.1s, it would require a spacing < 13MiB
    #   - the gzip index takes roughly 32kiB per seek point
    #   - the bzip2 index takes roughly 16B per 100-900kiB of compressed data
    #     -> for the gzip index to have the same space efficiency assuming a compression ratio of only 1,
    #        the spacing would have to be 1800MiB at which point it would become almost useless
    advancedGroup.add_argument(
        '-gs', '--gzip-seek-point-spacing', type=float,
        default=int(math.ceil(SQLiteIndexedTar.DEFAULT_GZIP_SEEK_POINT_SPACING / 1024 / 1024)),
        help='This only is applied when the index is first created or recreated with the -c option. '
             'The spacing given in MiB specifies the seek point distance in the uncompressed data. '
             'A distance of 16MiB means that archives smaller than 16MiB in uncompressed size will '
             'not benefit from faster seek times. A seek point takes roughly 32kiB. '
             'So, smaller distances lead to more responsive seeking but may explode the index size!')

    advancedGroup.add_argument(
        '-p', '--prefix', type=str, default='',
        help='[deprecated] Use "-o modules=subdir,subdir=<prefix>" instead. '
             'This standard way utilizes FUSE itself and will also work for other FUSE '
             'applications. So, it is preferable even if a bit more verbose.'
             'The specified path to the folder inside the TAR will be mounted to root. '
             'This can be useful when the archive as created with absolute paths. '
             'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
             '-p /var/log/apt/ can be specified so that the mount target directory '
             '>directly< contains history.log.')

    advancedGroup.add_argument(
        '--password-file', type=str, default='',
        help='Specify a file with newline separated passwords for RAR and ZIP files. '
             'The passwords will be tried out in order of appearance in the file.')

    moduleNames = sorted(list(set(module.name for _, info in supportedCompressions.items() for module in info.modules)))

    advancedGroup.add_argument(
        '--use-backend', type=str, action='append',
        help='Specify a backend to be used with higher priority for files which might be opened with multiple '
             'backends. Arguments specified last will have the highest priority. A comma-separated list may be '
             f'specified. Possible backends: {moduleNames}')

    advancedGroup.add_argument(
        '--oss-attributions', action=PrintOSSAttributionAction, nargs=0, default=argparse.SUPPRESS,
        help='Show licenses of used libraries.')

    advancedGroup.add_argument(
        '--disable-union-mount', action='store_true', default=False,
        help='Mounts all specified archives in equally named subfolders under the mount point.')

    advancedGroup.add_argument(
        '--union-mount-cache-max-depth', type=int, default=1024,
        help='Maximum number of folder levels to descend for building the union mount cache.')

    advancedGroup.add_argument(
        '--union-mount-cache-max-entries', type=int, default=100000,
        help='Maximum number of paths before stopping to descend into subfolders when building the union mount cache.')

    advancedGroup.add_argument(
        '--union-mount-cache-timeout', type=float, default=60,
        help='Timeout in seconds before stopping to build the union mount cache.')

    advancedGroup.add_argument(
        '--index-minimum-file-count', type=int, default=1000,
        help='Create indexes for archives with fewer than this limit of files in memory instead of '
             'creating a .index.sqlite file. This is currently not applied for TAR files because the file count '
             'only becomes known after parsing the archive, for which an index is already created.')

    advancedGroup.add_argument(
        '--transform', type=str, nargs=2, metavar=('REGEX_PATTERN', 'REPLACEMENT'),
        help='Specify a regex pattern and a replacement string, which will be applied via Python\'s re module '
             'to the full paths of all archive files.')

    # Positional Arguments

    positionalGroup.add_argument(
        'mount_source', nargs='+',
        help='The path to the TAR archive to be mounted. '
             'If multiple archives and/or folders are specified, then they will be mounted as if the arguments '
             'coming first were updated with the contents of the archives or folders specified thereafter, '
             'i.e., the list of TARs and folders will be union mounted.')
    positionalGroup.add_argument(
        'mount_point', nargs='?',
        help='The path to a folder to mount the TAR contents into. '
             'If no mount path is specified, the TAR will be mounted to a folder of the same name '
             'but without a file extension.')
    # fmt: on

    args = parser.parse_args(rawArgs)

    if args.unmount:
        if not args.mount_source or not args.mount_source[0]:
            raise argparse.ArgumentTypeError("Unmounting requires a path to the mount point!")

        # Do not test with os.path.ismount or anything other because if the FUSE process was killed without
        # unmounting, then any file system query might return with errors.
        # https://github.com/python/cpython/issues/96328#issuecomment-2027458283
        args.unmount = args.mount_source[0]
        return args

    args.gzipSeekPointSpacing = int(args.gzip_seek_point_spacing * 1024 * 1024)

    if args.recursive and args.recursion_depth is None:
        args.recursion_depth = -1
    if args.recursion_depth is None:
        args.recursion_depth = 0

    if (args.strip_recursive_tar_extension or args.transform_recursive_mount_point) and not args.recursion_depth:
        print("[Warning] The options --strip-recursive-tar-extension and --transform-recursive-mount-point")
        print("[Warning] only have an effect when used with recursive mounting.")

    if args.transform_recursive_mount_point:
        args.transform_recursive_mount_point = tuple(args.transform_recursive_mount_point)

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into the tar file path's namespace and we have to
    # manually separate them depending on the type.
    if os.path.isdir(args.mount_source[-1]) or not os.path.exists(args.mount_source[-1]):
        args.mount_point = args.mount_source[-1]
        args.mount_source = args.mount_source[:-1]
    if not args.mount_source:
        raise argparse.ArgumentTypeError(
            "You must at least specify one path to a valid TAR file or union mount source directory!"
        )

    # Sanitize different ways to specify passwords into a simple list
    # Better initialize it before calling checkMountSource, which might use args.passwords in the future.
    args.passwords = []
    if args.password:
        args.passwords.append(args.password.encode())

    if args.password_file:
        with open(args.password_file, 'rb') as file:
            args.passwords += file.read().split(b'\n')

    args.passwords = _removeDuplicatesStable(args.passwords)

    # Manually check that all specified TARs and folders exist
    def checkMountSource(path):
        try:
            return checkInputFileType(path, encoding=args.encoding, printDebug=args.debug)[0]
        except argparse.ArgumentTypeError as e:
            if (
                os.path.isdir(path)
                or zipfile.is_zipfile(path)
                or ('rarfile' in sys.modules and rarfile.is_rarfile(path))
            ):
                return os.path.realpath(path)
            raise e

    mountSources: List[str] = []
    for path in args.mount_source:
        fixedPath = checkMountSource(path)
        # Skip neighboring duplicates
        if mountSources and mountSources[-1] == fixedPath:
            if args.debug >= 2:
                print(f"[Info] Skip duplicate mount source: {fixedPath}")
            continue
        mountSources.append(fixedPath)
    args.mount_source = mountSources

    for path in args.mount_source:
        if args.mount_source.count(path) > 1:
            raise argparse.ArgumentTypeError(f"Path may not appear multiple times at different locations: {path}")

    # Automatically generate a default mount path
    if not args.mount_point:
        autoMountPoint = stripSuffixFromTarFile(args.mount_source[0])
        if args.mount_point == autoMountPoint:
            args.mount_point = os.path.splitext(args.mount_source[0])[0]
        else:
            args.mount_point = autoMountPoint
    args.mount_point = os.path.abspath(args.mount_point)

    # Preprocess the --index-folders list as a string argument
    if args.index_folders and isinstance(args.index_folders, str):
        if args.index_folders[0] == '[':
            args.index_folders = json.loads(args.index_folders)
        elif ',' in args.index_folders:
            args.index_folders = args.index_folders.split(',')
        else:
            args.index_folders = [args.index_folders]

    # Check the parallelization argument and move to global variable
    assert isinstance(args.parallelization, int)
    if args.parallelization < 0:
        raise argparse.ArgumentTypeError("Argument for parallelization must be non-negative!")
    if args.parallelization == 0:
        args.parallelization = defaultParallelization

    # Clean backend list
    supportedModuleNames = [module.name for _, info in supportedCompressions.items() for module in info.modules]
    args.prioritizedBackends = (
        [
            backend
            for backendString in args.use_backend
            for backend in backendString.split(',')
            if backend in supportedModuleNames
        ][::-1]
        if args.use_backend
        else []
    )

    return args


def cli(rawArgs: Optional[List[str]] = None) -> None:
    """Command line interface for ratarmount. Call with args = [ '--help' ] for a description."""

    # tmpArgs are only for the manual parsing. In general, rawArgs is None, meaning it reads sys.argv,
    # and maybe sometimes contains arguments when used programmatically. In that case the first argument
    # should not be the path to the script!

    args = _parseArgs(rawArgs)

    if args.unmount:
        try:
            subprocess.run(
                ["fusermount", "-u", args.unmount], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            if args.debug >= 2:
                print("[Info] Successfully called fusermount -u.")
            return
        except Exception as exception:
            if args.debug >= 2:
                print(f"[Warning] fusermount -u {args.unmount} failed with: {exception}")
            if args.debug >= 3:
                subprocess.run(["fusermount", "-V", args.unmount], check=False)

        # If called from AppImage, then try to call the user-installed fusermount because FUSE might require
        # extra permissions depending on the policy and some systems then provide a fusermount binary with
        # ownership root and the setuid flag set.
        if os.path.ismount(args.unmount):
            fusermountPath = shutil.which("fusermount")
            if fusermountPath is None:
                fusermountPath = ""
            for folder in os.environ.get("PATH", "").split(os.pathsep):
                if not folder:
                    continue
                binaryPath = os.path.join(folder, "fusermount")
                if fusermountPath != binaryPath and os.path.isfile(binaryPath) and os.access(binaryPath, os.X_OK):
                    try:
                        subprocess.run(
                            [binaryPath, "-u", args.unmount], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                        )
                        if args.debug >= 2:
                            print(f"[Info] Successfully called {binaryPath} -u.")
                        return
                    except Exception as exception:
                        if args.debug >= 2:
                            print(f"[Warning] {fusermountPath} -u {args.unmount} failed with: {exception}")
                        if args.debug >= 3:
                            subprocess.run([fusermountPath, "-V", args.unmount], check=False)

        if os.path.ismount(args.unmount):
            try:
                subprocess.run(["umount", args.unmount], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if args.debug >= 2:
                    print("[Info] Successfully called umount -u.")
                return
            except Exception as exception:
                if args.debug >= 2:
                    print(f"[Warning] umount {args.unmount} failed with: {exception}")

        # Unmounting might take some time and I had cases where fusermount returned exit code 1.
        # and still unmounted it successfully. It would be nice to automate this but it seems impossible to do
        # reliably, without any regular expression heuristics. /proc/<pid>/fd/5 links to /dev/fuse. This could
        # be used to reliable detect FUSE-providing processes, but we still wouldn't know which exact mount
        # point they provide.
        if os.path.ismount(args.unmount):
            time.sleep(1)
        if os.path.ismount(args.unmount):
            print("[Error] Failed to unmount the given mount point. Alternatively, the process providing the mount ")
            print("[Error] point can be looked for and killed, e.g., with this command:")
            print(f"""[Error]     pkill --full 'ratarmount.*{args.unmount}' -G "$( id -g )" --newest""")
        return

    if args.commit_overlay:
        if not os.path.isdir(args.write_overlay):
            raise RatarmountError("Need an existing write overlay folder for commiting changes.")

        if len(args.mount_source) != 1:
            raise RatarmountError("Currently, only modifications to a single TAR may be commited.")

        tarFile = args.mount_source[0]
        compression = None
        try:
            compression = checkInputFileType(tarFile, encoding=args.encoding, printDebug=args.debug)[1]
        except Exception as exception:
            raise RatarmountError("Currently, only modifications to a single TAR may be commited.") from exception

        if compression is not None:
            raise RatarmountError("Currently, only modifications to an uncompressed TAR may be commited.")

        try:
            with os.popen('tar --version') as pipe:
                if not re.search(r'GNU tar', pipe.read()):
                    raise RatarmountError("GNU tar is required")
        except Exception as exception:
            raise RatarmountError("Currently, GNU tar must be installed and discoverable as 'tar'.") from exception

        # Delete all files marked for deletion
        tmpFolder = tempfile.mkdtemp()
        deletionList = os.path.join(tmpFolder, "deletions.lst")
        appendList = os.path.join(tmpFolder, "append.lst")

        databasePath = os.path.join(args.write_overlay, WritableFolderMountSource.hiddenDatabaseName)
        if os.path.exists(databasePath):
            uriPath = urllib.parse.quote(databasePath)
            sqlConnection = sqlite3.connect(f"file:{uriPath}?mode=ro", uri=True)

            with open(deletionList, 'at', encoding=args.encoding) as file:
                for path, name in sqlConnection.execute("SELECT path,name FROM files WHERE deleted == 1;"):
                    file.write(f"{path}/{name}\0")

        # Delete all files to be replaced with other files
        with open(deletionList, 'at', encoding=args.encoding) as deletionListFile, open(
            appendList, 'at', encoding=args.encoding
        ) as appendListFile:
            # For temporary SQLite file suffixes, see https://www.sqlite.org/tempfiles.html
            suffixes = ['', '-journal', '-shm', '-wal']
            toBeIgnored = [WritableFolderMountSource.hiddenDatabaseName + suffix for suffix in suffixes]

            writeOverlayWithTrailingSlash = args.write_overlay
            if not writeOverlayWithTrailingSlash.endswith('/'):
                writeOverlayWithTrailingSlash += '/'

            for dirpath, _, filenames in os.walk(args.write_overlay, topdown=False):
                # dirpath should be a relative path (without leading slash) as seen from the overlay folder
                if dirpath.startswith(writeOverlayWithTrailingSlash):
                    dirpath = dirpath[len(writeOverlayWithTrailingSlash) :]
                elif dirpath == args.write_overlay:
                    dirpath = ""

                for name in filenames:
                    pathRelativeToRoot = f"{dirpath}/{name}".lstrip('/')
                    if pathRelativeToRoot in toBeIgnored:
                        continue

                    # Delete with and without leading slash because GNU tar matches exactly while
                    # ratarmount does not discern between these two cases.
                    deletionListFile.write(f"{pathRelativeToRoot}\0")
                    deletionListFile.write(f"/{pathRelativeToRoot}\0")
                    deletionListFile.write(f"./{pathRelativeToRoot}\0")

                    appendListFile.write(f"{pathRelativeToRoot}\0")

                # Append empty folders
                if not filenames and dirpath:
                    appendListFile.write(f"{dirpath}\0")

        # TODO Support compressed archives by maybe using tarfile to read from the original and write to a temporary?
        #      GNU tar does not support --delete on compressed archives unfortunately:
        #      > This option does not operate on compressed archives.
        # Suppress file not found errors because the alternative would be to manually check all files
        # to be updated whether they already exist in the archive or not.
        print("To commit the overlay folder to the archive, these commands have to be executed:")
        print()

        if os.stat(deletionList).st_size > 0:
            print(f"    tar --delete --null --verbatim-files-from --files-from='{deletionList}' \\")
            print(f"        --file '{tarFile}' 2>&1 |")
            print("       sed '/^tar: Exiting with failure/d; /^tar.*Not found in archive/d'")

        if os.stat(appendList).st_size > 0:
            print(
                f"    tar --append -C '{args.write_overlay}' --null --verbatim-files-from --files-from='{appendList}' "
                f"--file '{tarFile}'"
            )

        if os.stat(deletionList).st_size == 0 and os.stat(appendList).st_size == 0:
            print("Nothing to commit.")
            return

        print()
        print("Committing is an experimental feature!")
        print('Please confirm by entering "commit". Any other input will cancel.')
        print("> ", end='')
        try:
            if input() == 'commit':
                if os.stat(deletionList).st_size > 0:
                    tarDelete = subprocess.run(
                        [
                            "tar",
                            "--delete",
                            "--null",
                            f"--files-from={deletionList}",
                            "--file",
                            tarFile,
                        ],
                        check=False,
                        stderr=subprocess.PIPE,
                    )

                    unfilteredLines = []
                    for line in tarDelete.stderr.decode().split("\n"):
                        if (
                            'tar: Exiting with failure' not in line
                            and 'Not found in archive' not in line
                            and line.strip()
                        ):
                            unfilteredLines.append(line)

                    if unfilteredLines:
                        for line in unfilteredLines:
                            print(line)
                        raise RatarmountError("There were problems when trying to delete files.")

                if os.stat(appendList).st_size > 0:
                    subprocess.run(
                        [
                            "tar",
                            "--append",
                            "-C",
                            args.write_overlay,
                            "--null",
                            f"--files-from={appendList}",
                            "--file",
                            tarFile,
                        ],
                        check=True,
                    )

                print(f"Committed successfully. You can now remove the overlay folder at {args.write_overlay}.")
            else:
                print("Canceled")
        finally:
            shutil.rmtree(tmpFolder)

        return

    # Convert the comma separated list of key[=value] options into a dictionary for fusepy
    fusekwargs = (
        dict([option.split('=', 1) if '=' in option else (option, True) for option in args.fuse.split(',')])
        if args.fuse
        else {}
    )
    if args.prefix:
        fusekwargs['modules'] = 'subdir'
        fusekwargs['subdir'] = args.prefix

    if args.mount_point in args.mount_source and os.path.isdir(args.mount_point) and os.listdir(args.mount_point):
        if hasNonEmptySupport():
            fusekwargs['nonempty'] = True

    fuseOperationsObject = FuseMount(
        # fmt: off
        pathToMount                  = args.mount_source,
        clearIndexCache              = bool(args.recreate_index),
        recursive                    = bool(args.recursive),
        recursionDepth               = int(args.recursion_depth),
        gzipSeekPointSpacing         = int(args.gzipSeekPointSpacing),
        mountPoint                   = args.mount_point,
        encoding                     = args.encoding,
        ignoreZeros                  = bool(args.ignore_zeros),
        verifyModificationTime       = bool(args.verify_mtime),
        stripRecursiveTarExtension   = args.strip_recursive_tar_extension,
        indexFilePath                = args.index_file,
        indexFolders                 = args.index_folders,
        lazyMounting                 = bool(args.lazy),
        passwords                    = list(args.passwords),
        parallelization              = args.parallelization,
        isGnuIncremental             = args.gnu_incremental,
        writeOverlay                 = args.write_overlay,
        printDebug                   = int(args.debug),
        transformRecursiveMountPoint = args.transform_recursive_mount_point,
        transform                    = args.transform,
        prioritizedBackends          = args.prioritizedBackends,
        disableUnionMount            = args.disable_union_mount,
        maxCacheDepth                = args.union_mount_cache_max_depth,
        maxCacheEntries              = args.union_mount_cache_max_entries,
        maxSecondsToCache            = args.union_mount_cache_timeout,
        indexMinimumFileCount        = args.index_minimum_file_count,
        # fmt: on
    )

    try:
        fuse.FUSE(
            operations=fuseOperationsObject,
            mountpoint=args.mount_point,
            foreground=args.foreground,
            nothreads=True,  # Can't access SQLite database connection object from multiple threads
            **fusekwargs,
        )
    except RuntimeError as exception:
        raise RatarmountError(
            "FUSE mountpoint could not be created. See previous output for more information."
        ) from exception


def main():
    args = sys.argv[1:]
    debug = 1
    for i in range(len(args) - 1):
        if args[i] in ['-d', '--debug'] and args[i + 1].isdecimal():
            try:
                debug = int(args[i + 1])
            except ValueError:
                continue

    try:
        cli(args)
    except (FileNotFoundError, RatarmountError, argparse.ArgumentTypeError) as exception:
        print("[Error]", exception)
        if debug >= 3:
            traceback.print_exc()


if __name__ == '__main__':
    main()
