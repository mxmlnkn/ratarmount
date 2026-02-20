import errno
import functools
import os
import shutil
import sqlite3
import stat
import subprocess
import tarfile
import tempfile
import time
import traceback
import urllib.parse
from collections.abc import Mapping
from typing import Any, Callable, Optional

from ratarmountcore.formats import is_tar
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.formats.folder import FolderMountSource
from ratarmountcore.utils import RatarmountError, overrides

from .fuse import fuse


def check_ignored_prefixes(parameter_name: str = 'path'):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not self.ignoredPrefixes:
                return func(self, *args, **kwargs)

            if parameter_name in kwargs:
                path = kwargs[parameter_name]
            elif args:
                # We assume that the first parameter is the one to be checked.
                path = args[0]
            else:
                return func(self, *args, **kwargs)

            # Check if path starts with any ignored prefix.
            if any(path.startswith(prefix) for prefix in self.ignoredPrefixes):
                raise FileNotFoundError(f"Accessing forbidden folder: {path}")

            return func(self, *args, **kwargs)

        return wrapper

    return decorator


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

    def __init__(self, path: str, mountSource: MountSource, ignoredPrefixes: Optional[list[str]] = None) -> None:
        if os.path.lexists(path):
            if not os.path.isdir(path):
                raise ValueError("Overlay path must be a folder!")
        else:
            os.makedirs(path, exist_ok=True)

        self.root: str = path
        self.mountSource = mountSource
        self.sqlConnection = self._open_sql_db(os.path.join(path, self.hiddenDatabaseName))
        self._statfs = self._get_statfs_for_folder(self.root)
        self.ignoredPrefixes = (
            [os.path.normpath('/' + prefix.strip('/')) + '/' for prefix in ignoredPrefixes]
            if ignoredPrefixes is not None
            else []
        )

        # Add table if necessary
        tables = [row[0] for row in self.sqlConnection.execute('SELECT name FROM sqlite_master WHERE type = "table";')]
        if "files" not in tables:
            self.sqlConnection.executescript(WritableFolderMountSource._overlayMetadataSchema)

        # Check that the mount source contains this overlay folder with top priority
        databaseFileInfo = self.mountSource.lookup('/' + self.hiddenDatabaseName)
        assert databaseFileInfo is not None
        path, databaseMountSource, fileInfo = self.mountSource.get_mount_source(databaseFileInfo)
        assert stat.S_ISREG(fileInfo.mode)
        assert isinstance(databaseMountSource, FolderMountSource)
        assert databaseMountSource.root == self.root

    @staticmethod
    def _get_statfs_for_folder(path: str) -> dict[str, Any]:
        result = os.statvfs(path)
        return {
            key: getattr(result, key)
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
        }

    @staticmethod
    def _open_sql_db(path: str, **kwargs) -> sqlite3.Connection:
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

    def set_folder_descriptor(self, fd: int) -> None:
        """
        Make this mount source manage the special "." folder by changing to that directory.
        Because we change to that directory, it may only be used for one mount source but it also works
        when that mount source is mounted on!
        """
        os.fchdir(fd)
        self.root = '.'
        self._statfs = self._get_statfs_for_folder(self.root)

    @staticmethod
    def _split_path(path: str) -> tuple[str, str]:
        result = ('/' + os.path.normpath(path).lstrip('/')).rsplit('/', 1)
        assert len(result) == 2
        return result[0], result[1]

    def _realpath(self, path: str) -> str:
        """Path given relative to folder root. Leading '/' is acceptable"""
        return os.path.join(self.root, path.lstrip(os.path.sep))

    def _ensure_parent_exists(self, path):
        """
        Creates parent folders for given path inside overlay folder if and only if they exist in the mount source.
        """
        parentPath = self._split_path(path)[0]
        if not os.path.exists(self._realpath(parentPath)) and self.mountSource.is_dir(parentPath):
            os.makedirs(self._realpath(parentPath), exist_ok=True)

    def _ensure_file_is_modifiable(self, path):
        self._ensure_parent_exists(path)
        with (
            self.mountSource.open(self.mountSource.lookup(path)) as sourceObject,
            open(self._realpath(path), 'wb') as targetObject,
        ):
            shutil.copyfileobj(sourceObject, targetObject)

    def _open(self, path: str, mode):
        self._ensure_parent_exists(path)
        folder, name = self._split_path(path)

        self.sqlConnection.execute(
            'INSERT OR IGNORE INTO "files" (path,name,mode,deleted) VALUES (?,?,?,?)', (folder, name, mode, False)
        )
        self.sqlConnection.execute(
            'UPDATE "files" SET deleted=0 WHERE path == (?) AND name == (?)',
            (folder, name),
        )

    def _mark_as_deleted(self, path: str):
        """Hides the given path if it exists in the underlying mount source."""
        folder, name = self._split_path(path)

        if self.mountSource.exists(path):
            self.sqlConnection.execute(
                'INSERT OR REPLACE INTO "files" (path,name,deleted) VALUES (?,?,?)', (folder, name, True)
            )
        else:
            self.sqlConnection.execute('DELETE FROM "files" WHERE (path,name) == (?,?)', (folder, name))

    def list_deleted(self, path: str) -> list[str]:
        """Return list of files marked as deleted in the given path."""
        result = self.sqlConnection.execute(
            'SELECT name FROM "files" WHERE path == (?) AND deleted == 1', (path.rstrip('/'),)
        )

        # For temporary SQLite file suffixes, see https://www.sqlite.org/tempfiles.html
        suffixes = ['', '-journal', '-shm', '-wal']
        return [x[0] for x in result] + [self.hiddenDatabaseName + suffix for suffix in suffixes]

    def is_deleted(self, path: str) -> bool:
        folder, name = self._split_path(path)
        result = self.sqlConnection.execute(
            'SELECT COUNT(*) > 0 FROM "files" WHERE path == (?) AND name == (?) AND deleted == 1', (folder, name)
        )
        return bool(result.fetchone()[0])

    def _set_metadata(self, path: str, metadata: dict[str, Any]):
        if not metadata:
            raise ValueError("Need arguments to know what to update.")

        allowedKeys = ["path", "name", "mtime", "mode", "uid", "gid"]
        for key in metadata:
            if key not in allowedKeys:
                raise ValueError(f"Invalid metadata key ({key}) specified")

        folder, name = self._split_path(path)

        # https://stackoverflow.com/questions/31277027/using-placeholder-in-sqlite3-statements
        assignments = []
        values = []
        for key, value in metadata.items():
            values.append(value)
            assignments.append(f"{key} = (?)")

        self.sqlConnection.execute(
            f"""UPDATE "files" SET {', '.join(assignments)} WHERE "path" == ? and "name" == ?""",
            (*values, folder, name),
        )

    def _init_file_metadata(self, path: str):
        # Note that we do not have to check the overlay folder assuming that it is inside the (union) mount source!
        sourceFileInfo = self.mountSource.lookup(path)
        if not sourceFileInfo:
            raise fuse.FuseOSError(errno.ENOENT)

        # Initialize new metadata entry from existing file
        sfi = self.mountSource.get_mount_source(sourceFileInfo)[2]
        folder, name = self._split_path(path)

        self.sqlConnection.execute(
            f'INSERT OR REPLACE INTO "files" VALUES ({",".join(["?"] * 7)})',
            (folder, name, sfi.mtime, sfi.mode, sfi.uid, sfi.gid, False),
        )

    def _set_file_metadata(self, path: str, applyMetadataToFile: Callable[[str], None], metadata: dict[str, Any]):
        folder, name = self._split_path(path)

        existsInMetadata = self.sqlConnection.execute(
            'SELECT COUNT(*) > 0 FROM "files" WHERE "path" == (?) and "name" == (?)', (folder, name)
        ).fetchone()[0]

        if not existsInMetadata:
            self._init_file_metadata(path)
        self._set_metadata(path, metadata)

        # Apply the metadata change for the file in the overlay folder if it exists there.
        # This is only because it might be confusing for the user else but in general, the metadata in the SQLite
        # database should take precedence if e.g. the underlying file systems does not support them.
        try:
            if os.path.lexists(self._realpath(path)):
                applyMetadataToFile(self._realpath(path))
        except Exception:
            traceback.print_exc()
            print("[Info] Caught exception when trying to apply metadata to real file.")
            print("[Info] It was applied in the metadata database!")

    def update_file_info(self, path: str, fileInfo: FileInfo) -> FileInfo:
        folder, name = self._split_path(path)
        row = self.sqlConnection.execute(
            """SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?);""", (folder, name)
        ).fetchone()

        if not row:
            return fileInfo

        # fmt: off
        return FileInfo(
            size     = fileInfo.size,
            mtime    = row['mtime'] if row['mtime'] is not None else fileInfo.mtime,
            mode     = row['mode'] if row['mode'] is not None else fileInfo.mode,
            linkname = fileInfo.linkname,
            uid      = row['uid'] if row['uid'] is not None else fileInfo.uid,
            gid      = row['gid'] if row['gid'] is not None else fileInfo.gid,
            userdata = fileInfo.userdata,
        )
        # fmt: on

    # Metadata modification

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def chmod(self, path: str, mode: int):
        self._set_file_metadata(path, lambda p: os.chmod(p, mode), {'mode': mode})

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def chown(self, path: str, uid: int, gid: int):
        data = {}
        if uid != -1:
            data['uid'] = uid
        if gid != -1:
            data['gid'] = gid
        # os.chown
        # > Change the owner and group id of path to the numeric uid and gid. To leave one of the ids unchanged,
        # > set it to -1.
        # No reason to change the file owner in the overlay folder, which may often not even be possible.
        self._set_file_metadata(path, lambda p: None, data)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def utimens(self, path: str, times: Optional[tuple[int, int]] = None):
        """Argument "times" is a (atime, mtime) tuple. If "times" is None, use the current time."""

        mtime = time.time() if times is None else times[1] / 1e9
        ns = (int(mtime * 1e9), int(mtime * 1e9)) if times is None else times

        self._set_file_metadata(path, lambda p: os.utime(p, ns=ns), {'mtime': mtime})

    @overrides(fuse.Operations)
    def rename(self, old: str, new: str):
        if not self.mountSource.exists(old) or self.is_deleted(old):
            raise fuse.FuseOSError(errno.ENOENT)

        folder, name = self._split_path(new)

        # Delete target path from metadata database to avoid uniqueness restraint being invalidated
        self.sqlConnection.execute('DELETE FROM "files" WHERE "path" == (?) and "name" == (?)', (folder, name))
        self._set_file_metadata(old, lambda p: None, {'path': folder, 'name': name})

        self._ensure_parent_exists(new)
        if os.path.lexists(self._realpath(old)):
            os.rename(self._realpath(old), self._realpath(new))
        else:
            fileInfo = self.mountSource.lookup(old)
            if fileInfo is None:
                raise fuse.FuseOSError(errno.ENOENT)

            with self.mountSource.open(fileInfo) as sourceObject, open(self._realpath(new), 'wb') as targetObject:
                shutil.copyfileobj(sourceObject, targetObject)

            self._mark_as_deleted(old)

    # Links

    @overrides(fuse.Operations)
    @check_ignored_prefixes('target')
    def symlink(self, target: str, source: str):
        os.symlink(source, self._realpath(target))

    @overrides(fuse.Operations)
    @check_ignored_prefixes('target')
    def link(self, target: str, source: str):
        # Can only hardlink to files which are also in the overlay folder.
        overlaySource = self._realpath(source)
        if not os.path.lexists(overlaySource) and self.mountSource.lookup(source):
            raise fuse.FuseOSError(errno.EXDEV)

        target = self._realpath(target)

        os.link(overlaySource, target)

    # Folders

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def mkdir(self, path: str, mode: int):
        self._open(path, mode | stat.S_IFDIR)
        os.mkdir(self._realpath(path), mode)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def rmdir(self, path: str):
        if not self.mountSource.exists(path) or self.is_deleted(path):
            raise fuse.FuseOSError(errno.ENOENT)

        contents = self.mountSource.list_mode(path)
        if contents is not None:
            keys = contents.keys() if isinstance(contents, Mapping) else contents
            if set(keys) - set(self.list_deleted(path)):
                raise fuse.FuseOSError(errno.ENOTEMPTY)

        try:
            if os.path.exists(self._realpath(path)):
                os.rmdir(self._realpath(path))
        except Exception as exception:
            traceback.print_exc()
            raise fuse.FuseOSError(errno.EIO) from exception
        finally:
            self._mark_as_deleted(path)

    # Files

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def open(self, path: str, flags: int):
        # if flags & os.O_CREAT != 0:  # I hope that FUSE simple calls create in this case.
        #    self._open(path)   # what would the default mode even be?
        if not os.path.exists(self._realpath(path)):
            if not self.mountSource.exists(path):
                raise fuse.FuseOSError(errno.ENOENT)

            if flags & (os.O_WRONLY | os.O_RDWR):
                self._ensure_file_is_modifiable(path)

        return os.open(self._realpath(path), flags)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def create(self, path: str, mode: int, fi=None):
        self._open(path, mode)
        # TODO Use the correct flags according after fixing argument forwarding in mfusepy.
        #      https://github.com/mxmlnkn/ratarmount/issues/172#issuecomment-3312526348
        # I see no downside to always adding read-mode by default, until then.
        return os.open(self._realpath(path), os.O_RDWR | os.O_CREAT | os.O_TRUNC, mode)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def unlink(self, path: str):
        # Note that despite the name this is called for removing both, files and links.

        if not self.mountSource.exists(path) or self.is_deleted(path):
            # This is for the rare case that the file only exists in the overlay metadata database.
            self._mark_as_deleted(path)
            raise fuse.FuseOSError(errno.ENOENT)

        try:
            if os.path.lexists(self._realpath(path)):
                os.unlink(self._realpath(path))
        except Exception as exception:
            traceback.print_exc()
            raise fuse.FuseOSError(errno.EIO) from exception
        finally:
            self._mark_as_deleted(path)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def mknod(self, path: str, mode: int, dev: int):
        self._ensure_parent_exists(path)
        os.mknod(self._realpath(path), mode, dev)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def truncate(self, path: str, length: int, fh=None):
        self._ensure_file_is_modifiable(path)
        os.truncate(self._realpath(path), length)

    # Actual writing

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def write(self, path: str, data, offset: int, fh):
        os.lseek(fh, offset, 0)
        return os.write(fh, data)

    # Flushing

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def flush(self, path: str, fh):
        return os.fsync(fh)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def fsync(self, path: str, datasync: int, fh):
        return os.fsync(fh) if datasync == 0 else os.fdatasync(fh)

    @overrides(fuse.Operations)
    @check_ignored_prefixes('path')
    def statfs(self, path: str):
        return self._statfs.copy()


def commit_overlay(writeOverlay: str, tarFile: str, encoding: str = tarfile.ENCODING, printDebug: int = 0) -> None:
    if not os.path.isdir(writeOverlay):
        raise RatarmountError("Need an existing write overlay folder for committing changes.")

    if not os.path.isfile(tarFile):
        raise RatarmountError(f"Specified TAR '{tarFile}' to commit to does not exist or is not a file!")

    with open(tarFile, 'rb') as file:
        if not is_tar(file, encoding=encoding):
            raise RatarmountError("Currently, only modifications to an uncompressed TAR may be committed.")

    try:
        with os.popen('tar --version') as pipe:
            if 'GNU tar' not in pipe.read():
                raise RatarmountError("GNU tar is required")
    except Exception as exception:
        raise RatarmountError("Currently, GNU tar must be installed and discoverable as 'tar'.") from exception

    # Delete all files marked for deletion
    tmpFolder = tempfile.mkdtemp()
    deletionList = os.path.join(tmpFolder, "deletions.lst")
    appendList = os.path.join(tmpFolder, "append.lst")

    def add_to_deletion_file(deletionListFile, pathRelativeToRoot: str):
        # Delete with and without leading slash because GNU tar matches exactly while
        # ratarmount does not discern between these two cases.
        deletionListFile.write(f"{pathRelativeToRoot}\0")
        deletionListFile.write(f"/{pathRelativeToRoot}\0")
        deletionListFile.write(f"./{pathRelativeToRoot}\0")

    databasePath = os.path.join(writeOverlay, WritableFolderMountSource.hiddenDatabaseName)
    if os.path.exists(databasePath):
        uriPath = urllib.parse.quote(databasePath)
        sqlConnection = sqlite3.connect(f"file:{uriPath}?mode=ro", uri=True)

        with open(deletionList, 'a', encoding=encoding) as deletionListFile:
            for path, name in sqlConnection.execute("SELECT path,name FROM files WHERE deleted == 1;"):
                add_to_deletion_file(deletionListFile, f"{path}/{name}".lstrip('/'))

    # Delete all files to be replaced with other files
    with (
        open(deletionList, 'a', encoding=encoding) as deletionListFile,
        open(appendList, 'a', encoding=encoding) as appendListFile,
    ):
        # For temporary SQLite file suffixes, see https://www.sqlite.org/tempfiles.html
        suffixes = ['', '-journal', '-shm', '-wal']
        toBeIgnored = [WritableFolderMountSource.hiddenDatabaseName + suffix for suffix in suffixes]

        writeOverlayWithTrailingSlash = writeOverlay
        if not writeOverlayWithTrailingSlash.endswith('/'):
            writeOverlayWithTrailingSlash += '/'

        for dirpath, _, filenames in os.walk(writeOverlay, topdown=False):
            # dirpath should be a relative path (without leading slash) as seen from the overlay folder
            if dirpath.startswith(writeOverlayWithTrailingSlash):
                dirpath = dirpath[len(writeOverlayWithTrailingSlash) :]
            elif dirpath == writeOverlay:
                dirpath = ""

            for name in filenames:
                pathRelativeToRoot = f"{dirpath}/{name}".lstrip('/')
                if pathRelativeToRoot in toBeIgnored:
                    continue
                add_to_deletion_file(deletionListFile, pathRelativeToRoot)
                appendListFile.write(f"{pathRelativeToRoot}\0")

            # Append empty folders
            if not filenames and dirpath:
                appendListFile.write(f"{dirpath}\0")

    if os.stat(deletionList).st_size == 0 and os.stat(appendList).st_size == 0:
        if printDebug >= 1:
            print("Nothing to commit.")
        return

    # TODO Support compressed archives by maybe using tarfile to read from the original and write to a temporary?
    #      GNU tar does not support --delete on compressed archives unfortunately:
    #      > This option does not operate on compressed archives.
    # Suppress file not found errors because the alternative would be to manually check all files
    # to be updated whether they already exist in the archive or not.
    if printDebug >= 1:
        print("To commit the overlay folder to the archive, these commands have to be executed:")
        print()

        if os.stat(deletionList).st_size > 0:
            print(f"    tar --delete --null --verbatim-files-from --files-from='{deletionList}' \\")
            print(f"        --file '{tarFile}' 2>&1 |")
            print("       sed '/^tar: Exiting with failure/d; /^tar.*Not found in archive/d'")

        if os.stat(appendList).st_size > 0:
            print(
                f"    tar --append -C '{writeOverlay}' --null --verbatim-files-from --files-from='{appendList}' "
                f"--file '{tarFile}'"
            )
        print()

    def run_without_locale(*args, check=True, **kwargs):
        adjustedEnvironment = os.environ.copy()
        for key in [k for k in adjustedEnvironment if k.startswith('LC_')]:
            del adjustedEnvironment[key]
        adjustedEnvironment['LC_LANG'] = 'C'
        adjustedEnvironment['LANGUAGE'] = 'C'
        return subprocess.run(*args, env=adjustedEnvironment, check=check, **kwargs)

    if printDebug >= 1:
        print("Committing is an experimental feature!")
    print('Please confirm by entering "commit". Any other input will cancel.')
    print("> ", end='')
    try:
        if input() == 'commit':
            if os.stat(deletionList).st_size > 0:
                tarDelete = run_without_locale(
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

                unfilteredLines = [
                    line
                    for line in tarDelete.stderr.decode().split("\n")
                    if 'tar: Exiting with failure' not in line and 'Not found in archive' not in line and line.strip()
                ]

                if unfilteredLines:
                    for line in unfilteredLines:
                        print(line)
                    raise RatarmountError("There were problems when trying to delete files.")

            if os.stat(appendList).st_size > 0:
                run_without_locale(
                    [
                        "tar",
                        "--append",
                        "-C",
                        writeOverlay,
                        "--null",
                        f"--files-from={appendList}",
                        "--file",
                        tarFile,
                    ],
                    check=True,
                )

            if printDebug >= 1:
                print(f"Committed successfully. You can now remove the overlay folder at {writeOverlay}.")
        elif printDebug >= 1:
            print("Canceled")
    finally:
        shutil.rmtree(tmpFolder)
