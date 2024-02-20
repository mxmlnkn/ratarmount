#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import sqlite3
import stat
import sys
import tarfile
import time
import traceback
import urllib.parse

from typing import Any, AnyStr, Callable, Dict, IO, List, Optional, Tuple, Union
from dataclasses import dataclass

try:
    import indexed_gzip
except ImportError:
    pass

try:
    import rapidgzip
except ImportError:
    pass

from .version import __version__
from .MountSource import FileInfo, createRootFileInfo
from .compressions import TAR_COMPRESSION_FORMATS
from .SQLiteBlobFile import SQLiteBlobsFile, WriteSQLiteBlobs
from .utils import RatarmountError, IndexNotOpenError, InvalidIndexError, findModuleVersion, MismatchingIndexError


def getSqliteTables(connection: sqlite3.Connection):
    return [x[0] for x in connection.execute('SELECT name FROM sqlite_master WHERE type="table"')]


def _toVersionTuple(version: str) -> Optional[Tuple[int, int, int]]:
    versionNumbers = [re.sub('[^0-9]', '', x) for x in version.split('.')]
    if len(versionNumbers) == 3:
        return (int(versionNumbers[0]), int(versionNumbers[1]), int(versionNumbers[2]))
    return None


@dataclass
class SQLiteIndexedTarUserData:
    # fmt: off
    offset       : int
    offsetheader : int
    istar        : bool
    issparse     : bool
    # fmt: on


class SQLiteIndex:
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    # Version 0.1.0:
    #   - Initial version
    # Version 0.2.0:
    #   - Add sparse support and 'offsetheader' and 'issparse' columns to the SQLite database
    #   - Add TAR file size metadata in order to quickly check whether the TAR changed
    #   - Add 'offsetheader' to the primary key of the 'files' table so that files which were
    #     updated in the TAR can still be accessed if necessary.
    # Version 0.3.0:
    #   - Add arguments influencing the created index to 'metadata' table (ignore-zeros, recursive, ...)
    # Version 0.4.0:
    #   - Add 'gzipindexes' table, which may contain multiple blobs in contrast to 'gzipindex' table.
    # Version 0.5.0:
    #   - Add 'backend' name to 'metadata' table. Indexes created by different backends should by default
    #     be assumed to be incompatible, especially for chimera files, but also when one was created with
    #     libarchive, then it will not be readable with the SQLiteIndexedTar backend because it does not
    #     collect data offsets.
    #   - Add 'isGnuIncremental' to 'metadata' table.
    __version__ = '0.5.0'

    NUMBER_OF_METADATA_TO_VERIFY = 1000  # shouldn't take more than 1 second according to benchmarks

    # The maximum blob size configured by SQLite is exactly 1 GB, see https://www.sqlite.org/limits.html
    # Therefore, this should be smaller. Another argument for making it smaller is that this blob size
    # will be held fully in memory temporarily.
    # But, making it too small would result in too many non-backwards compatible indexes being created.
    _MAX_BLOB_SIZE = 256 * 1024 * 1024  # 256 MiB

    # TODO Would be nice for index verification to have columns for recursionlevel (INTEGER) and isgenerated (BOOL)
    #      that is true for automatically inserted parent folders that do not actually exist in the archive.
    #      How version compatible would that table change be? Test with old ratarmount versions.
    _CREATE_FILES_TABLE = """
        CREATE TABLE "files" (
            "path"          VARCHAR(65535) NOT NULL,  /* path with leading and without trailing slash */
            "name"          VARCHAR(65535) NOT NULL,
            "offsetheader"  INTEGER,  /* seek offset from TAR file where the TAR metadata for this file resides */
            "offset"        INTEGER,  /* seek offset from TAR file where these file's contents resides */
            "size"          INTEGER,
            "mtime"         REAL,
            "mode"          INTEGER,
            "type"          INTEGER,
            "linkname"      VARCHAR(65535),
            "uid"           INTEGER,
            "gid"           INTEGER,
            /* True for valid TAR files. Internally used to determine where to mount recursive TAR files. */
            "istar"         BOOL   ,
            "issparse"      BOOL   ,  /* for sparse files the file size refers to the expanded size! */
            /* See SQL benchmarks for decision on the primary key.
             * See also https://www.sqlite.org/optoverview.html
             * (path,name) tuples might appear multiple times in a TAR if it got updated.
             * In order to also be able to show older versions, we need to add
             * the offsetheader column to the primary key. */
            PRIMARY KEY (path,name,offsetheader)
        );"""

    _CREATE_FILESTMP_TABLE = """
        /* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
         * Therefore, it will not be sorted and inserting will be faster! */
        CREATE TABLE "filestmp" AS SELECT * FROM "files" WHERE 0;"""

    _CREATE_PARENT_FOLDERS_TABLE = """
        CREATE TABLE "parentfolders" (
            "path"          VARCHAR(65535) NOT NULL,
            "name"          VARCHAR(65535) NOT NULL,
            "offsetheader"  INTEGER,
            "offset"        INTEGER,
            PRIMARY KEY (path,name)
            UNIQUE (path,name)
        );"""

    _CREATE_METADATA_TABLE = """
        /* Empty table whose sole existence specifies that the archive has been fully processed.
         * Common keys: tarstats, arguments, isGnuIncremental, backendName */
        CREATE TABLE IF NOT EXISTS "metadata" (
            "key"      VARCHAR(65535) NOT NULL, /* e.g. "tarsize" */
            "value"    VARCHAR(65535) NOT NULL  /* e.g. size in bytes as integer */
        );
    """

    _CREATE_VERSIONS_TABLE = """
        /* This table's sole existence specifies that we finished iterating the tar for older ratarmount versions */
        CREATE TABLE IF NOT EXISTS "versions" (
            "name"     VARCHAR(65535) NOT NULL, /* which component the version belongs to */
            "version"  VARCHAR(65535) NOT NULL, /* free form version string */
            /* Semantic Versioning 2.0.0 (semver.org) parts if they can be specified:
             *   MAJOR version when you make incompatible API changes,
             *   MINOR version when you add functionality in a backwards compatible manner, and
             *   PATCH version when you make backwards compatible bug fixes. */
            "major"    INTEGER,
            "minor"    INTEGER,
            "patch"    INTEGER
        );
    """

    # Check some of the first and last files in the archive and some random selection in between.
    # Do not verify folders because parent folders and root get automatically added!
    # Ignore rows with isTar=True because recomputing this would require trying an expensive recursive mount.
    # We cannot simply ignore rows with NOT isTar because there will be multiple entries with the same
    # offset header for those recursive entries.
    # We also need to ignore all rows for recursive entries because they will have the parent TAR path prepended,
    # which the error check code is not expecting.
    # This check is done by creating a subquery/table that gathers all recursive archives (isTar==True) that
    # enclose the current rows offset. If there is no such archive, then the current row is non-recursive
    # and safe to check. Unfortunately, it seems that this is quadratic in complexity and basically hangs for
    # larger files.
    # FROM "files" AS t1 WHERE (mode & {stat.S_IFREG}) != 0 AND NOT EXISTS (
    #     SELECT 1 FROM "files" AS t2 WHERE isTar AND t1.offsetheader BETWEEN t2.offset AND t2.offset + t2.size - 1
    # )
    # As an alternative, exempt the path from the consistency check.
    # Note also that for pure compressed files such as simple.bz2, the offsetheader can be None.
    # These rows should also be filtered.
    FROM_REGULAR_FILES = f"""FROM "files" WHERE (mode & {stat.S_IFREG}) != 0 AND offsetheader IS NOT NULL"""

    def __init__(
        self,
        indexFilePath: Optional[str],
        indexFolders: Optional[List[str]] = None,
        archiveFilePath: Optional[str] = None,
        *,  # force all parameters after to be keyword-only
        encoding: str = tarfile.ENCODING,
        checkMetadata: Optional[Callable[[Dict[str, Any]], None]] = None,
        printDebug: int = 0,
        preferMemory: bool = False,
        indexMinimumFileCount: int = 0,
        backendName: str = '',
    ):
        """
        indexFilePath
            Path to the index file. This takes precedence over defaultIndexFilePath.
            If it is ':memory:', then the SQLite database will be kept in memory
            and not stored to the file system at any point.
        indexFolders
            Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for
            suitability in the given order. An empty path will be interpreted as the location in which
            the archive resides in.
        checkMetadata
            A verifying callback that is called when opening an existing index. It is given the
            the dictionary of metadata in the index and should thrown an exception when the index
            should not be used, e.g., because the version is incompatible.
        preferMemory
            If True, then load existing indexes and write to explicitly given index file paths but
            if no such things are given, then create the new index in memory as if indexFilePath
            = ':memory:' was specified.
        indexMinimumFileCount
            If > 0, then open new databases in memory and write them out if this threshold has been
            exceeded. It may also be written to a file if a gzip index is stored.
        backendName
            The backend name to be stored as metadata and to determine compatibility of found indexes.
        """

        if not backendName:
            raise ValueError("A non-empty backend name must be specified!")

        self.printDebug = printDebug
        self.sqlConnection: Optional[sqlite3.Connection] = None
        # Will hold the actually opened valid path to an index file
        self.indexFilePath: Optional[str] = None
        self.encoding = encoding
        self.possibleIndexFilePaths = SQLiteIndex.getPossibleIndexFilePaths(
            indexFilePath, indexFolders, archiveFilePath
        )
        # stores which parent folders were last tried to add to database and therefore do exist
        self.parentFolderCache: List[Tuple[str, str]] = []
        self.checkMetadata = checkMetadata
        self.preferMemory = preferMemory
        self.indexMinimumFileCount = indexMinimumFileCount
        self.backendName = backendName
        self._insertedRowCount = 0

        assert self.backendName

        # Ignore minimum file count option if an index file path or index folder is configured.
        # For latter, ignore the special empty folder [''], which means that the indexes are stored
        # besides the archives.
        # Why all this exceptions? Because the index-minimum-file-count, which is set by default,
        # is only intended to avoid littering folders with index files in the common use case of mounting
        # a folder of archives or single small archives recursively. If the user goes through the trouble
        # of specifying an index file or folder, then littering should not be a problem as index creations
        # are expected by the user.
        if self.indexMinimumFileCount > 0 and not indexFilePath:
            if self.printDebug >= 3:
                print(
                    f"[Info] Because of the given positive index minimum file count ({self.indexMinimumFileCount}) "
                    "and because no explicit index file path is given, try to open an SQLite database in memory first."
                )
            self.preferMemory = True

    @staticmethod
    def getPossibleIndexFilePaths(
        indexFilePath: Optional[str], indexFolders: Optional[List[str]] = None, archiveFilePath: Optional[str] = None
    ) -> List[str]:
        if indexFilePath:
            return [] if indexFilePath == ':memory:' else [os.path.abspath(os.path.expanduser(indexFilePath))]

        if not archiveFilePath:
            return []

        defaultIndexFilePath = archiveFilePath + ".index.sqlite"
        if not indexFolders:
            return [defaultIndexFilePath]

        possibleIndexFilePaths = []
        indexPathAsName = defaultIndexFilePath.replace("/", "_")
        for folder in indexFolders:
            if folder:
                indexPath = os.path.join(folder, indexPathAsName)
                possibleIndexFilePaths.append(os.path.abspath(os.path.expanduser(indexPath)))
            else:
                possibleIndexFilePaths.append(defaultIndexFilePath)
        return possibleIndexFilePaths

    def clearIndexes(self):
        for indexPath in self.possibleIndexFilePaths:
            if os.path.isfile(indexPath):
                os.remove(indexPath)

    def openExisting(self):
        """Tries to find an already existing index."""
        for indexPath in self.possibleIndexFilePaths:
            if self._tryLoadIndex(indexPath):
                self.indexFilePath = indexPath
                break

    def openInMemory(self):
        self.indexFilePath, self.sqlConnection = SQLiteIndex._openPath(':memory:')

    def openWritable(self):
        if self.possibleIndexFilePaths and not self.preferMemory:
            for indexPath in self.possibleIndexFilePaths:
                if SQLiteIndex._pathIsWritable(
                    indexPath, printDebug=self.printDebug
                ) and SQLiteIndex._pathCanBeUsedForSqlite(indexPath, printDebug=self.printDebug):
                    self.indexFilePath, self.sqlConnection = SQLiteIndex._openPath(indexPath)
                    break
        else:
            if self.printDebug >= 3 and self.preferMemory:
                print("[Info] Create new index in memory because memory is to be preferred, e.g., for small archives.")
            self.indexFilePath, self.sqlConnection = SQLiteIndex._openPath(':memory:')

        if not self.indexIsLoaded():
            raise InvalidIndexError(
                "Could not find any existing index or writable location for an index in "
                + str(self.possibleIndexFilePaths)
            )

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def close(self):
        if self.sqlConnection:
            self.sqlConnection.commit()
            self.sqlConnection.close()
            self.sqlConnection = None

    def getConnection(self) -> sqlite3.Connection:
        if self.sqlConnection:
            return self.sqlConnection
        raise IndexNotOpenError("This method can not be called without an opened index database!")

    def _storeVersionsMetadata(self) -> None:
        connection = self.getConnection()

        try:
            connection.executescript(SQLiteIndex._CREATE_VERSIONS_TABLE)
        except Exception as exception:
            print("[Warning] There was an error when adding metadata information. Index loading might not work.")
            if self.printDebug >= 2:
                print(exception)
            if self.printDebug >= 3:
                traceback.print_exc()

        try:

            def makeVersionRow(
                versionName: str, version: str
            ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
                versionNumbers = [re.sub('[^0-9]', '', x) for x in version.split('.')]
                return (
                    versionName,
                    version,
                    versionNumbers[0] if len(versionNumbers) > 0 else None,
                    versionNumbers[1] if len(versionNumbers) > 1 else None,
                    versionNumbers[2] if len(versionNumbers) > 2 else None,
                )

            versions = [
                makeVersionRow('ratarmount', __version__),
                makeVersionRow('index', SQLiteIndex.__version__),
            ]

            for moduleName in set(
                module.name
                for _, info in TAR_COMPRESSION_FORMATS.items()
                for module in info.modules
                if module.name in sys.modules
            ):
                moduleVersion = findModuleVersion(sys.modules[moduleName])
                if moduleVersion:
                    versions += [makeVersionRow(moduleName, moduleVersion)]

            connection.executemany('INSERT OR REPLACE INTO "versions" VALUES (?,?,?,?,?)', versions)
        except Exception as exception:
            print("[Warning] There was an error when adding version information.")
            if self.printDebug >= 2:
                print(exception)
            if self.printDebug >= 3:
                traceback.print_exc()

    def _storeFileMetadata(self, filePath: AnyStr) -> None:
        """Adds some consistency meta information to recognize the need to update the cached TAR index"""
        try:
            tarStats = os.stat(filePath)
            serializedTarStats = json.dumps(
                {attr: getattr(tarStats, attr) for attr in dir(tarStats) if attr.startswith('st_')}
            )
            self.storeMetadataKeyValue("tarstats", serializedTarStats)
        except Exception as exception:
            print("[Warning] There was an error when adding file metadata.")
            print("[Warning] Automatic detection of changed TAR files during index loading might not work.")
            if self.printDebug >= 2:
                print(exception)
            if self.printDebug >= 3:
                traceback.print_exc()

    def storeMetadataKeyValue(self, key: AnyStr, value: Union[str, bytes]) -> None:
        connection = self.getConnection()
        connection.executescript(SQLiteIndex._CREATE_METADATA_TABLE)

        try:
            connection.execute('INSERT OR REPLACE INTO "metadata" VALUES (?,?)', (key, value))
        except Exception as exception:
            if self.printDebug >= 2:
                print(exception)
            print("[Warning] There was an error when adding argument metadata.")
            print("[Warning] Automatic detection of changed arguments files during index loading might not work.")

        connection.commit()

    def storeMetadata(self, metadata: AnyStr, filePath: Optional[AnyStr] = None) -> None:
        self._storeVersionsMetadata()
        self.storeMetadataKeyValue('backendName', self.backendName)
        if filePath:
            self._storeFileMetadata(filePath)
        self.storeMetadataKeyValue('arguments', metadata)
        self.storeMetadataKeyValue('backendName', self.backendName)

    def dropMetadata(self):
        self.getConnection().executescript(
            """
            DROP TABLE IF EXISTS metadata;
            DROP TABLE IF EXISTS versions;
            """
        )

    @staticmethod
    def checkMetadataArguments(metadata: Dict, arguments, argumentsToCheck: List[str]):
        # Check arguments used to create the found index.
        # These are only warnings and not forcing a rebuild by default.
        # TODO: Add option to force index rebuild on metadata mismatch?
        differingArgs = []
        for arg in argumentsToCheck:
            if arg in metadata and hasattr(arguments, arg) and metadata[arg] != getattr(arguments, arg):
                differingArgs.append((arg, metadata[arg], getattr(arguments, arg)))
        if differingArgs:
            print("[Warning] The arguments used for creating the found index differ from the arguments ")
            print("[Warning] given for mounting the archive now. In order to apply these changes, ")
            print("[Warning] recreate the index using the --recreate-index option!")
            for arg, oldState, newState in differingArgs:
                print(f"[Warning] {arg}: index: {oldState}, current: {newState}")

    def checkMetadataBackend(self, metadata: Dict):
        # Because of a lack of sufficient foresight, the backend name was not added to the index in older verions.
        backendName = metadata.get('backendName')
        if isinstance(backendName, str):
            if backendName != self.backendName:
                raise MismatchingIndexError(
                    f"Cannot open an index created with backend '{backendName}'!\n"
                    f"Will stop trying to open the archive with backend '{self.backendName}'.\n"
                    f"Use --recreate-index if '{backendName}' is not installed."
                )

    def getIndexVersion(self):
        return self.getConnection().execute("""SELECT version FROM versions WHERE name == 'index';""").fetchone()[0]

    @staticmethod
    def _pathIsWritable(path: str, printDebug: int = 0) -> bool:
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            with open(path, 'wb') as file:
                file.write(b'\0' * 1024 * 1024)
            os.remove(path)

            return True

        except PermissionError:
            if printDebug >= 3:
                print(f"Insufficient permissions to write to: {path}")

        except IOError:
            if printDebug >= 2:
                traceback.print_exc()
                print("Could not create file:", path)

        return False

    @staticmethod
    def _pathCanBeUsedForSqlite(path: str, printDebug: int = 0) -> bool:
        if not SQLiteIndex._pathIsWritable(path, printDebug):
            return False

        fileExisted = os.path.isfile(path)
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            connection = SQLiteIndex._openSqlDb(path)
            connection.executescript('CREATE TABLE "files" ( "path" VARCHAR(65535) NOT NULL );')
            connection.commit()
            connection.close()
            return True
        except sqlite3.OperationalError:
            if printDebug >= 2:
                traceback.print_exc()
                print("Could not create SQLite database at:", path)
        finally:
            if not fileExisted and os.path.isfile(path):
                SQLiteIndex._uncheckedRemove(path)

        return False

    @staticmethod
    def _uncheckedRemove(path: Optional[AnyStr]):
        """
        Often cleanup is good manners but it would only be obnoxious if ratarmount crashed on unnecessary cleanup.
        """
        if not path or not os.path.exists(path):
            return

        try:
            os.remove(path)
        except Exception:
            print("[Warning] Could not remove:", path)

    @staticmethod
    def _openSqlDb(path: AnyStr, **kwargs) -> sqlite3.Connection:
        # Even when given a relative path, sqlite3.connect seems to access the path absolute instead of from the
        # current working directory! This will lead to hangs for self-bind mounts because of a recursive FUSE call.
        # I don't know how to circumvent this here. The caller must ensure that the possible index file paths
        # do not point to a self-bind mount point.
        sqlConnection = sqlite3.connect(path, **kwargs)
        sqlConnection.row_factory = sqlite3.Row
        sqlConnection.executescript(
            # Locking mode exclusive leads to a measurable speedup. E.g., find on 2k recursive files tar
            # improves from ~1s to ~0.4s!
            # https://blog.devart.com/increasing-sqlite-performance.html
            """
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
            """
        )
        return sqlConnection

    @staticmethod
    def _openPath(indexFilePath: Optional[str], printDebug: int = 0) -> Tuple[str, sqlite3.Connection]:
        indexFilePath = indexFilePath if indexFilePath else ':memory:'

        if printDebug >= 1:
            print("Creating new SQLite index database at", indexFilePath)

        sqlConnection = SQLiteIndex._openSqlDb(indexFilePath)
        tables = getSqliteTables(sqlConnection)
        if {"files", "filestmp", "parentfolders"}.intersection(tables):
            raise InvalidIndexError(
                f"The index file {indexFilePath} already seems to contain a table. Please specify --recreate-index."
            )
        sqlConnection.executescript(SQLiteIndex._CREATE_FILES_TABLE)

        return indexFilePath, sqlConnection

    def reloadIndexReadOnly(self):
        if not self.indexFilePath or self.indexFilePath == ':memory:' or not self.sqlConnection:
            return

        self.sqlConnection.commit()
        self.sqlConnection.close()

        uriPath = urllib.parse.quote(self.indexFilePath)
        # check_same_thread=False can be used because it is read-only anyway and it allows to enable FUSE multithreading
        self.sqlConnection = SQLiteIndex._openSqlDb(f"file:{uriPath}?mode=ro", uri=True, check_same_thread=False)

    def _reloadIndexOnDisk(self):
        if not self.indexFilePath or self.indexFilePath != ':memory:' or not self.sqlConnection:
            return

        oldIndexFilePath, oldSqlConnection = self.indexFilePath, self.sqlConnection
        self.preferMemory = False
        self.openWritable()
        if oldIndexFilePath == self.indexFilePath:
            if self.printDebug >= 3:
                print(f"[Info] Tried to write the database to disk but found no other path than: {self.indexFilePath}")
            self.sqlConnection.close()
            self.indexFilePath, self.sqlConnection = oldIndexFilePath, oldSqlConnection
            return

        if self.printDebug >= 3:
            print(f"[Info] Back up database from {oldIndexFilePath} -> {self.indexFilePath}")
        oldSqlConnection.commit()
        oldSqlConnection.backup(self.sqlConnection)
        oldSqlConnection.close()

    def ensureIntermediaryTables(self):
        connection = self.getConnection()
        tables = getSqliteTables(connection)

        if ("filestmp" in tables) != ("parentfolders" in tables):
            raise InvalidIndexError(
                "The index file is in an invalid state because it contains some tables and misses others. "
                "Please specify --recreate-index to overwrite the existing index."
            )

        if "filestmp" not in tables and "parentfolders" not in tables:
            connection.execute(SQLiteIndex._CREATE_FILESTMP_TABLE)
            connection.execute(SQLiteIndex._CREATE_PARENT_FOLDERS_TABLE)

    def finalize(self):
        try:
            queriedLibSqliteVersion = sqlite3.connect(":memory:").execute("select sqlite_version();").fetchone()
            libSqliteVersion = tuple(int(x) for x in queriedLibSqliteVersion[0].split('.'))
        except Exception:
            libSqliteVersion = (0, 0, 0)

        searchByTuple = """(path,name) NOT IN ( SELECT path,name"""
        searchByConcat = """path || "/" || name NOT IN ( SELECT path || "/" || name"""

        cleanUpDatabase = f"""
            INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","name",rowid;
            DROP TABLE "filestmp";
            INSERT OR IGNORE INTO "files"
                /* path name offsetheader offset size mtime mode type linkname uid gid istar issparse */
                SELECT path,name,offsetheader,offset,0,0,{int(0o555 | stat.S_IFDIR)},{int(tarfile.DIRTYPE)},"",0,0,0,0
                FROM "parentfolders"
                WHERE {searchByTuple if libSqliteVersion >= (3,22,0) else searchByConcat}
                    FROM "files" WHERE mode & (1 << 14) != 0
                )
                ORDER BY "path","name";
            DROP TABLE "parentfolders";
            PRAGMA optimize;
        """

        self.getConnection().executescript(cleanUpDatabase)

    def fileCount(self) -> int:
        return self.getConnection().execute('SELECT COUNT(*) FROM "files";').fetchone()[0]

    @staticmethod
    def _rowToFileInfo(row: Dict[str, Any]) -> FileInfo:
        userData = SQLiteIndexedTarUserData(
            # fmt: off
            offset       = row['offset'],
            offsetheader = row['offsetheader'] if 'offsetheader' in row.keys() else 0,
            istar        = row['istar'],
            issparse     = row['issparse'] if 'issparse' in row.keys() else False,
            # fmt: on
        )

        fileInfo = FileInfo(
            # fmt: off
            size     = row['size'],
            mtime    = row['mtime'],
            mode     = row['mode'],
            linkname = row['linkname'],
            uid      = row['uid'],
            gid      = row['gid'],
            userdata = [userData],
            # fmt: on
        )

        return fileInfo

    @staticmethod
    def normpath(path: str):
        # Add a leading '/' as a convention where '/' represents the TAR root folder.
        # Also strips trailing '/' except for a single '/'.
        # Partly, done because fusepy specifies paths in a mounted directory like this
        # os.normpath does not delete duplicate '/' at beginning of string!
        # os.path.normpath can remove suffixed folder/./ path specifications but it can't remove
        # a leading dot that's why we prefix a leading slash also before calling normpath.
        return '/' + os.path.normpath('/' + path).lstrip('/')

    @staticmethod
    def _queryNormpath(path: str):
        # os.path.normpath also collapses /../ into / and, because we prepend /, ../ gets collapsed to /.
        # This effect is good to have for inserting rows but not for querying rows.
        return '/' + os.path.normpath(path if path.startswith('../') else '/' + path).lstrip('/')

    def listDir(self, path: str) -> Optional[Dict[str, FileInfo]]:
        """
        Return a dictionary for the given directory path: { fileName : FileInfo, ... } or None
        if the path does not exist.

        path : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        """

        # For listing directory entries the file version can't be applied meaningfully at this abstraction layer.
        # E.g., should it affect the file version of the directory to list, or should it work on the listed files
        # instead and if so how exactly if there aren't the same versions for all files available, ...?
        # Or, are folders assumed to be overwritten by a new folder entry in a TAR or should they be union mounted?
        # If they should be union mounted, like is the case now, then the folder version only makes sense for
        # its attributes.
        #
        # Order by offsetheader in order to preserve the order they appear in the archive to potentially enable
        # faster access to the whoole archive when iterating over all files in order.
        # Note that Python's dictionary preserves the insertion order since Python 3.6.
        # https://docs.python.org/3.6/whatsnew/3.6.html#new-dict-implementation
        # While it was not a guarantee to stay that way, it is guaranteed for Python 3.7+:
        # > the insertion-order preservation nature of dict objects has been declared to be an official part of the
        # > Python language spec.
        # https://docs.python.org/3.11/library/stdtypes.html#dict.values
        # > Dictionaries preserve insertion order. Note that updating a key does not affect the order.
        # > Keys added after deletion are inserted at the end.
        rows = self.getConnection().execute(
            'SELECT * FROM "files" WHERE "path" == (?) ORDER BY "offsetheader"',
            (self._queryNormpath(path).rstrip('/'),),
        )
        directory: Dict[str, FileInfo] = {}
        gotResults = False
        for row in rows:
            gotResults = True
            if row['name']:
                directory[row['name']] = self._rowToFileInfo(row)
        return directory if gotResults else None

    def fileVersions(self, path: str) -> Dict[str, FileInfo]:
        """
        Return metadata for all versions of a file possibly appearing more than once
        in the index as a directory dictionary or an empty dictionary if the path does not exist.

        path : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        """

        if path == '/':
            return {'/': createRootFileInfo(userdata=[SQLiteIndexedTarUserData(0, 0, False, False)])}

        path, name = self._queryNormpath(path).rsplit('/', 1)
        rows = self.getConnection().execute(
            'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?) ORDER BY "offsetheader" ASC', (path, name)
        )
        result = {str(version + 1): self._rowToFileInfo(row) for version, row in enumerate(rows)}
        return result

    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Return the FileInfo to given path (directory or file) or None if the path does not exist.

        fileVersion : If the TAR contains the same file path multiple times, by default only the last one is shown.
                      But with this argument other versions can be queried. Version 1 is the oldest one.
                      Version 0 translates to the most recent one for compatibility with tar --occurrence=<number>.
                      Version -1 translates to the second most recent, and so on.
                      For listDir=True, the file version makes no sense and is ignored!
                      So, even if a folder was overwritten by a file, which is already not well supported by tar,
                      then listDir for that path will still list all contents of the overwritten folder or folders,
                      no matter the specified version. The file system layer has to take care that a directory
                      listing is not even requested in the first place if it is not a directory.
                      FUSE already does this by calling getattr for all parent folders in the specified path first.
        """

        if not isinstance(fileVersion, int):
            raise RatarmountError("The specified file version must be an integer!")

        if path == '/':
            return createRootFileInfo(userdata=[SQLiteIndexedTarUserData(0, 0, False, False)])

        path, name = self._queryNormpath(path).rsplit('/', 1)
        row = (
            self.getConnection()
            .execute(
                f"""
            SELECT * FROM "files"
            WHERE "path" == (?) AND "name" == (?)
            ORDER BY "offsetheader" {'DESC' if fileVersion is None or fileVersion <= 0 else 'ASC'}
            LIMIT 1 OFFSET (?);
            """,
                (path, name, 0 if fileVersion is None else fileVersion - 1 if fileVersion > 0 else -fileVersion),
            )
            .fetchone()
        )
        return self._rowToFileInfo(row) if row else None

    def _tryAddParentFolders(self, path: str, offsetheader: int, offset: int) -> None:
        # Add parent folders if they do not exist.
        # E.g.: path = '/a/b/c' -> paths = [('', 'a'), ('/a', 'b'), ('/a/b', 'c')]
        # Without the parentFolderCache, the additional INSERT statements increase the creation time
        # from 8.5s to 12s, so almost 50% slowdown for the 8MiB test TAR!
        pathParts = path.split("/")
        paths = [
            p
            # fmt: off
            for p in (
                ( "/".join( pathParts[:i] ), pathParts[i] )
                for i in range( 1, len( pathParts ) )
            )
            # fmt: on
            if p not in self.parentFolderCache
        ]
        if not paths:
            return

        self.parentFolderCache += paths
        # Assuming files in the TAR are sorted by hierarchy, the maximum parent folder cache size
        # gives the maximum cacheable file nesting depth. High numbers lead to higher memory usage and lookup times.
        if len(self.parentFolderCache) > 16:
            self.parentFolderCache = self.parentFolderCache[-8:]

        # TODO This method is still not perfect but I do not know how to perfect it without loosing significant
        #      performance. Currently, adding implicit folders will fail when a file is overwritten implicitly with
        #      a folder and then overwritten by a file and then again overwritten by a folder. Because the parent
        #      folder was already added implicitly the first time, the second time will be skipped.
        #      To solve this, I would have to add all parent folders for all files, which might easily explode
        #      the temporary database and the indexing performance by the folder depth.
        #      Also, I do not want to add versions for a parent folder for each implicitly added parent folder for
        #      each file, so I would have to sort out those in a post-processing step. E.g., sort by offsetheader
        #      and then clean out successive implicitly added folders as long as there is no file of the same name
        #      in between.
        #      The unmentioned alternative would be to lookup paths with LIKE but that is just madness because it
        #      will have a worse complexity of O(N) instead of O(log(N)).
        self.getConnection().executemany(
            'INSERT OR IGNORE INTO "parentfolders" VALUES (?,?,?,?)',
            [(p[0], p[1], offsetheader, offset) for p in paths],
        )

    def setFileInfos(self, rows: List[Tuple]) -> None:
        if not rows:
            return

        self._insertedRowCount += len(rows)
        if (
            self._insertedRowCount > self.indexMinimumFileCount
            and self.indexFilePath == ':memory:'
            and self.preferMemory
        ):
            if self.printDebug >= 3:
                print(
                    f"[Info] Exceeded file count threshold ({self._insertedRowCount} > {self.indexMinimumFileCount}) "
                    "for metadata held in memory. Will try to reopen the database on disk."
                )
            self.preferMemory = False
            self._reloadIndexOnDisk()

        try:
            self.getConnection().executemany(
                'INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(rows[0])) + ');', rows
            )
        except UnicodeEncodeError:
            # Fall back to separately inserting each row to find those in need of string cleaning.
            for row in rows:
                self.setFileInfo(row)
            return

        for row in rows:
            self._tryAddParentFolders(row[0], row[2], row[3])

    @staticmethod
    def _escapeInvalidCharacters(toEscape: str, encoding: str):
        try:
            toEscape.encode()
            return toEscape
        except UnicodeEncodeError:
            return toEscape.encode(encoding, 'surrogateescape').decode(encoding, 'backslashreplace')

    def setFileInfo(self, row: tuple) -> None:
        connection = self.getConnection()

        try:
            connection.execute('INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', row)
        except UnicodeEncodeError:
            print("[Warning] Problem caused by file name encoding when trying to insert this row:", row)
            print("[Warning] The file name will now be stored with the bad character being escaped")
            print("[Warning] instead of being correctly interpreted.")
            print("[Warning] Please specify a suitable file name encoding using, e.g., --encoding iso-8859-1!")
            print("[Warning] A list of possible encodings can be found here:")
            print("[Warning] https://docs.python.org/3/library/codecs.html#standard-encodings")

            checkedRow = []
            for x in list(row):  # check strings
                if isinstance(x, str):
                    checkedRow += [self._escapeInvalidCharacters(x, self.encoding)]
                else:
                    checkedRow += [x]

            connection.execute(
                'INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', tuple(checkedRow)
            )
            print("[Warning] The escaped inserted row is now:", row)
            print()

            self._tryAddParentFolders(self._escapeInvalidCharacters(row[0], self.encoding), row[2], row[3])
            return

        self._tryAddParentFolders(row[0], row[2], row[3])

    def indexIsLoaded(self) -> bool:
        """Returns true if the SQLite database has been opened for reading and a "files" table exists."""
        if not self.sqlConnection:
            return False

        try:
            self.sqlConnection.execute('SELECT * FROM "files" WHERE 0 == 1;')
        except sqlite3.OperationalError:
            self.sqlConnection = None
            return False

        return True

    def loadIndex(self, indexFilePath: AnyStr) -> None:
        """Loads the given index SQLite database and checks it for validity raising an exception if it is invalid."""
        if self.indexIsLoaded():
            return

        self.sqlConnection = self._openSqlDb(indexFilePath)
        tables = getSqliteTables(self.sqlConnection)
        versions = None
        try:
            rows = self.sqlConnection.execute('SELECT * FROM versions;')
            versions = {}
            for row in rows:
                versions[row[0]] = (row[2], row[3], row[4])
        except sqlite3.OperationalError:
            pass

        try:
            # Check indexes created with bugged bz2 decoder (bug existed when I did not store versions yet)
            if 'bzip2blocks' in tables and 'versions' not in tables:
                raise InvalidIndexError(
                    "The indexes created with version 0.3.0 through 0.3.3 for bzip2 compressed archives "
                    "are very likely to be wrong because of a bzip2 decoder bug.\n"
                    "Please delete the index or call ratarmount with the --recreate-index option!"
                )

            # Check for empty or incomplete indexes. Pretty safe to rebuild the index for these as they
            # are so invalid, noone should miss them. So, recreate index by default for these cases.
            if 'files' not in tables:
                raise InvalidIndexError("SQLite index is empty")

            if 'filestmp' in tables or 'parentfolders' in tables:
                raise InvalidIndexError("SQLite index is incomplete")

            # Check for pre-sparse support indexes
            if (
                'versions' not in tables
                or 'index' not in versions
                or len(versions['index']) < 2
                or versions['index'][1] < 2
            ):
                print("[Warning] The found outdated index does not contain any sparse file information.")
                print("[Warning] The index will also miss data about multiple versions of a file.")
                print("[Warning] Please recreate the index if you have problems with those.")

            if 'metadata' in tables:
                metadata = dict(self.sqlConnection.execute('SELECT * FROM metadata;'))
                if self.checkMetadata:
                    self.checkMetadataBackend(metadata)
                    self.checkMetadata(metadata)

        except Exception as e:
            # indexIsLoaded checks self.sqlConnection, so close it before returning because it was found to be faulty
            try:
                self.sqlConnection.close()
            except sqlite3.Error:
                pass
            self.sqlConnection = None

            raise e

        if self.indexIsLoaded() and self.sqlConnection:
            try:
                indexVersion = self.sqlConnection.execute(
                    "SELECT major,minor FROM versions WHERE name == 'index';"
                ).fetchone()

                if indexVersion:
                    indexVersionTuple = _toVersionTuple(indexVersion)
                    indexAPIVersionTuple = _toVersionTuple(SQLiteIndex.__version__)
                if indexVersionTuple and indexAPIVersionTuple and indexVersionTuple > indexAPIVersionTuple:
                    print("[Warning] The loaded index was created with a newer version of ratarmount.")
                    print("[Warning] If there are any problems, please update ratarmount or recreate the index")
                    print("[Warning] with this ratarmount version using the --recreate-index option!")
            except Exception:
                pass

        if self.printDebug >= 1:
            print(f"Successfully loaded offset dictionary from {str(indexFilePath)}")

    def _tryLoadIndex(self, indexFilePath: AnyStr) -> bool:
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile(indexFilePath):
            return False

        try:
            self.loadIndex(indexFilePath)
        except MismatchingIndexError as e:
            raise e
        except Exception as exception:
            if self.printDebug >= 3:
                traceback.print_exc()

            print("[Warning] Could not load file:", indexFilePath)
            print("[Info] Exception:", exception)
            print("[Info] Some likely reasons for not being able to load the index file:")
            print("[Info]   - The index file has incorrect read permissions")
            print("[Info]   - The index file is incomplete because ratarmount was killed during index creation")
            print("[Info]   - The index file was detected to contain errors because of known bugs of older versions")
            print("[Info]   - The index file got corrupted because of:")
            print("[Info]     - The program exited while it was still writing the index because of:")
            print("[Info]       - the user sent SIGINT to force the program to quit")
            print("[Info]       - an internal error occurred while writing the index")
            print("[Info]       - the disk filled up while writing the index")
            print("[Info]     - Rare lowlevel corruptions caused by hardware failure")

            print("[Info] This might force a time-costly index recreation, so if it happens often")
            print("       and mounting is slow, try to find out why loading fails repeatedly,")
            print("       e.g., by opening an issue on the public github page.")

            try:
                os.remove(indexFilePath)
            except OSError:
                print("[Warning] Failed to remove corrupted old cached index file:", indexFilePath)

        return self.indexIsLoaded()

    def clearCompressionOffsets(self):
        for table in ['bzip2blocks', 'gzipindex', 'gzipindexes', 'zstdblocks']:
            self.getConnection().execute(f"DROP TABLE IF EXISTS {table}")

    def synchronizeCompressionOffsets(self, fileObject: IO[bytes], compression: str):
        """
        Will load block offsets from SQLite database to backend if a fitting table exists.
        Else it will force creation and store the block offsets of the compression backend into a new table.
        """
        if compression and (not self.indexFilePath or self.indexFilePath == ':memory:'):
            if self.indexMinimumFileCount != 0 and self.printDebug >= 2:
                print(
                    f"[Info] Will try to reopen the database on disk even though the file count threshold "
                    f"({self.indexMinimumFileCount}) might not be exceeded ({self._insertedRowCount}) because "
                    f"the archive is compressed."
                )
            self._reloadIndexOnDisk()

        if not self.indexFilePath or self.indexFilePath == ':memory:':
            if self.printDebug >= 2:
                print("[Info] Will skip storing compression seek data because the database is in memory.")
                print("[Info] If the database is in memory, then this data will not be read anyway.")
            return

        # This should be called after the TAR file index is complete (loaded or created).
        # If the TAR file index was created, then tarfile has iterated over the whole file once
        # and therefore completed the implicit compression offset creation.
        db = self.getConnection()

        if compression in ['bz2', 'zst']:
            setBlockOffsets = getattr(fileObject, 'set_block_offsets')
            getBlockOffsets = getattr(fileObject, 'block_offsets')
            if not setBlockOffsets or not getBlockOffsets:
                print("[Warning] The given file object misses the expected methods for getting/setting")
                print("[Warning] the block offsets. Subsequent loads might be slow.")
                return

            if compression == 'bz2':
                table_name = 'bzip2blocks'
            elif compression == 'zst':
                table_name = 'zstdblocks'

            tables = getSqliteTables(db)
            if table_name in tables:
                try:
                    offsets = dict(db.execute(f"SELECT blockoffset,dataoffset FROM {table_name};"))
                    setBlockOffsets(offsets)
                    return
                except Exception as exception:
                    if self.printDebug >= 2:
                        print(f"[Info] Could not load {compression} block offset data. Will create it from scratch.")
                        print(exception)
                    if self.printDebug >= 3:
                        traceback.print_exc()
            else:
                print(f"[Info] The index does not yet contain {compression} block offset data. Will write it out.")

            tables = getSqliteTables(db)
            if table_name in tables:
                db.execute(f"DROP TABLE {table_name}")
            db.execute(f"CREATE TABLE {table_name} ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )")
            db.executemany(f"INSERT INTO {table_name} VALUES (?,?)", getBlockOffsets().items())
            db.commit()
            return

        if (
            hasattr(fileObject, 'import_index')
            and hasattr(fileObject, 'export_index')
            and compression in ['gz', 'zlib']
        ):
            tables = getSqliteTables(db)

            if 'gzipindex' in tables or 'gzipindexes' in tables:
                if self._loadGzipIndex(fileObject, 'gzipindexes' if 'gzipindexes' in tables else 'gzipindex'):
                    return

                if self.printDebug >= 2:
                    print("[Info] Could not load gzip block offset data. Will create it from scratch.")
            else:
                if self.printDebug >= 2:
                    print("[Info] The index does not yet contain gzip block offset data. Will write it out.")

            self._storeGzipIndex(fileObject)
            return

        # Note that for xz seeking, loading and storing block indexes is unnecessary because it has an index included!
        if compression in [None, 'xz']:
            return

        assert False, (
            f"Could not load or store block offsets for {compression} " "probably because adding support was forgotten!"
        )

    def _loadGzipIndex(self, fileObject: IO[bytes], table: str) -> bool:
        importIndex = getattr(fileObject, 'import_index')
        if not importIndex:
            return False

        connection = self.getConnection()
        try:
            t0 = time.time()
            fileobj = SQLiteBlobsFile(connection, table, 'data', buffer_size=SQLiteIndex._MAX_BLOB_SIZE)
            if 'rapidgzip' in sys.modules and isinstance(fileObject, rapidgzip.RapidgzipFile):
                importIndex(fileobj)
            else:
                # indexed_gzip 1.5.0 added support for pure Python file objects as arguments for the index!
                importIndex(fileobj=fileobj)

            # SQLiteBlobFile is rather slow to get parts of a large blob by using substr.
            # Here are some timings for 4x256 MiB blobs:
            #   buffer size / MiB | time / s
            #                 512 | 1.94 1.94 1.90 1.92 1.95
            #                 256 | 1.94 1.98 1.92 2.02 1.91
            #                 128 | 2.44 2.37 2.38 2.44 2.47
            #                  64 | 3.51 3.44 3.47 3.47 3.42
            #                  32 | 5.66 5.71 5.62 5.57 5.61
            #                  16 | 10.22 9.88 9.73 9.75 9.91
            # Writing out blobs to file / s         : 9.40 8.71 8.49 10.60 9.13
            # Importing block offsets from file / s : 0.47 0.46 0.47 0.46 0.41
            #   => With proper buffer sizes, avoiding writing out the block offsets can be 5x faster!
            # It seems to me like substr on blobs does not actually support true seeking :/
            # The blob is probably always loaded fully into memory and only then is the substring being
            # calculated. For C, there actually is an incremental blob reading interface but not for Python:
            #   https://www.sqlite.org/c3ref/blob_open.html
            #   https://bugs.python.org/issue24905
            print(f"Loading gzip block offsets took {time.time() - t0:.2f}s")

            return True

        except Exception as exception:
            if self.printDebug >= 1:
                print(
                    "[Warning] Encountered exception when trying to load gzip block offsets from database",
                    exception,
                )
            if self.printDebug >= 3:
                traceback.print_exc()

        return False

    def _storeGzipIndex(self, fileObject: IO[bytes]):
        exportIndex = getattr(fileObject, 'export_index')
        if not exportIndex:
            print("[Warning] The given file object misses the expected methods for getting/setting")
            print("[Warning] the block offsets. Subsequent loads might be slow.")
            return

        # Transparently force index to be built if not already done so. build_full_index was buggy for me.
        # Seeking from end not supported, so we have to read the whole data in in a loop
        while fileObject.read(1024 * 1024):
            pass

        # Old implementation using a temporary file before copying parts of it into blobs.
        # Tested on 1 GiB of gzip block offset data (32 GiB file with block seek point spacing 1 MiB)
        #   Time / s: 23.251 22.251 23.697 23.230 22.484
        # Timings when using WriteSQLiteBlobs to write directly into the SQLite database.
        #   Time / s: 13.029 14.884 14.110 14.229 13.807

        db = self.getConnection()
        db.execute('DROP TABLE IF EXISTS "gzipindexes"')
        db.execute('CREATE TABLE gzipindexes ( data BLOB )')

        try:
            with WriteSQLiteBlobs(db, 'gzipindexes', blob_size=SQLiteIndex._MAX_BLOB_SIZE) as gzindex:
                if 'rapidgzip' in sys.modules and isinstance(fileObject, rapidgzip.RapidgzipFile):
                    # See the following link for the exception mapping done by Cython:
                    # https://cython.readthedocs.io/en/latest/src/userguide/wrapping_CPlusPlus.html#exceptions
                    exportIndex(gzindex)
                else:
                    exportIndex(fileobj=gzindex)
        except (indexed_gzip.ZranError, RuntimeError, ValueError) as exception:
            db.execute('DROP TABLE IF EXISTS "gzipindexes"')

            print("[Warning] The GZip index required for seeking could not be written to the database!")
            print("[Info] This might happen when you are out of space in your temporary file and at the")
            print("[Info] the index file location. The gzipindex size takes roughly 32kiB per 4MiB of")
            print("[Info] uncompressed(!) bytes (0.8% of the uncompressed data) by default.")

            raise RatarmountError("Could not wrote out the gzip seek database.") from exception

        blobCount = db.execute('SELECT COUNT(*) FROM gzipindexes;').fetchone()[0]
        if blobCount == 0:
            if self.printDebug >= 2:
                print("[Warning] Did not write out any gzip seek data. This should only happen if the gzip ")
                print("[Warning] size is smaller than the gzip seek point spacing.")
        elif blobCount == 1:
            # For downwards compatibility
            db.execute('DROP TABLE IF EXISTS "gzipindex";')
            db.execute('ALTER TABLE gzipindexes RENAME TO gzipindex;')

        db.commit()

    def openGzipIndex(self) -> Optional[SQLiteBlobsFile]:
        connection = self.getConnection()
        tables = getSqliteTables(connection)
        if 'gzipindex' in tables or 'gzipindexes' in tables:
            table = 'gzipindexes' if 'gzipindexes' in tables else 'gzipindex'
            return SQLiteBlobsFile(connection, table, 'data', buffer_size=SQLiteIndex._MAX_BLOB_SIZE)

        return None
