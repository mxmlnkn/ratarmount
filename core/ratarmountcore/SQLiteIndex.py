import contextlib
import json
import logging
import os
import re
import shutil
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
import urllib.parse
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any, AnyStr, Callable, Optional, Union

try:
    import fsspec
except ImportError:
    fsspec = None  # type: ignore

with contextlib.suppress(ImportError):
    import indexed_gzip

with contextlib.suppress(ImportError):
    import rapidgzip

import builtins

from .compressions import COMPRESSION_BACKENDS, detect_compression, find_available_backend
from .formats import FILE_FORMATS, FileFormatID
from .mountsource import FileInfo, create_root_file_info
from .SQLiteBlobFile import SQLiteBlobsFile, WriteSQLiteBlobs
from .utils import (
    CompressionError,
    IndexNotOpenError,
    InvalidIndexError,
    MismatchingIndexError,
    RatarmountError,
    find_module_version,
)
from .version import __version__

logger = logging.getLogger(__name__)


def get_sqlite_tables(connection: sqlite3.Connection):
    return [x[0] for x in connection.execute('SELECT name FROM sqlite_master WHERE type="table" OR type="view"')]


def _to_version_tuple(version: str) -> Optional[tuple[int, int, int]]:
    versionNumbers = [re.sub('[^0-9]', '', x) for x in version.split('.')]
    if len(versionNumbers) == 3:
        return (int(versionNumbers[0]), int(versionNumbers[1]), int(versionNumbers[2]))
    return None


@dataclass
class SQLiteIndexedTarUserData:
    # fmt: off
    offset         : int
    offsetheader   : int
    istar          : bool
    issparse       : bool
    isgenerated    : bool
    recursiondepth : int
    # fmt: on


class SQLiteIndex:
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    # Considerations for a new backwards-incompatible index format, which probably will never happen...:
    #  - Avoid redundant prefixes by storing ID-ing them. This can be slow though :(
    #    This would not be downwards-compatible because a view was ignored by my existence check in ratarmount 1.0.0.
    #  - Maybe put each recursive archive into a separate table? the table name would be some ID that could
    #    be looked up in a separate 'mountpoints' table, which maps a path to a table ID and maybe a recursion
    #    depth.
    #  - Make it possible to undo arbitrarily many outer compressions layers.
    #    This might be doable in a backwards-compatible manner already.
    #    Simply add a new table 'compressions' to store indexes for all but the outermost compression.
    #    Old versions would only be able to undo the outermost compression, while newer versions would be able
    #    to use this additional information to speed things up.
    #    It might however be difficult to implement correctly with subfolders and such, i.e., do not simply
    #    undo the compression layer with compression-backend-open calls but implement a full "SingleFileMountSource"
    #    hierarchy.
    #     -> for backward compatibility note that the innermost layer would have to be in the existing 'gzipindex'
    #        table as the others would be undone by other mount layers in older versions!
    #     -> But(!), the index would be stored for the outermost file, i.e., it would contain files and offsets,
    #        which would be loaded by older versions, without fully undoing all compressions!
    #     -> So, probably not backward compatible after all.
    #  - Make it possible to store all indexes for recursive ZIPs, Libarchive, RAR, which already support SQLiteIndex
    #    as backend, in a single database file to speed up loading of recursive archives!
    #    Would have to be some kind of multiplexing, e.g., with 'files_<ID suffix>' tables.
    #    I would only have to make the SQLiteIndex default table adjustable.
    # May be possible in a backwards-compatible (old versions will ignore it) manner:
    #  - Make it such that recursion depth changes, at least decrementing ones, do not require
    #    a full rebuild of the index. I think I can get this to work with the current version already.
    #  - Make it possible to undo inner compression layers, heck maybe even combine all inner compressions
    #    of the same type (gzip, bzip2, zstandard), create a StenciledFile so that the number of rapidgzip
    #    background instances gets reduced.
    #    - Maybe add a simple "(get/set)CompressionIndex" interface and store indexes as blobs for each file
    #      if possible, ID'd by FileInfo, i.e., by offset/offsetheader and such or maybe even the rowid.
    #
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
    #   - Add 'backendName' to 'metadata' table. Indexes created by different backends should by default
    #     be assumed to be incompatible, especially for chimera files, but also when one was created with
    #     libarchive, then it will not be readable with the SQLiteIndexedTar backend because it does not
    #     collect data offsets.
    #   - Add 'isGnuIncremental' to 'metadata' table.
    # Version 0.6.0:
    #   - Add 'isgenerated' and 'recursiondepth' columns.
    #     From what I can gather, the SQLite index is used in such a way that adding columns does not break anything.
    #     All SELECT statements either explicitly specify columns, or access columns via row[index] or row[columnName],
    #     e.g., in _row_to_file_info and SQLiteIndexedTar._check_rows_validity.
    #     I have also checked for all users of SQLiteIndex: TAR, ZIP, SquashFS, libarchive.
    #     I created the index with the new version and accessed it with an old ratarmount version to test compatibility.
    #     Libarchive (currently) only creates SQLite indexes in memory, ergo has no compatibility issues!
    #     Unfortunately, the _check_rows_validity will fail because the loaded vs. generated tuple lengths will differ!
    #     This is only called when 'backendName' does not exist, i.e., for indexes with version < 0.5.0 and when
    #     the TAR was detected as having been appended to. So, it's probably negligible. Honestly, who even uses
    #     an older ratarmount version with a newer index. Probably only if the index was distributed somewhere, i.e.,
    #     it can be reloaded if it was overwritten, I hope.
    #   - Add 'xattrs' table
    # Version 0.7.0:
    #   - Add 'gztoolindex' table, which is similar to gzipindex but contains 1+ blobs for a compressed sparsed
    #     gztool index file format.
    __version__ = '0.7.0'

    MAGIC_BYTES = b'SQLite format 3\x00'  # If it is encrypted, the first 16 B are the (random) salt.
    NUMBER_OF_METADATA_TO_VERIFY = 1000  # shouldn't take more than 1 second according to benchmarks

    # The maximum blob size configured by SQLite is exactly 1 GB, see https://www.sqlite.org/limits.html
    # Therefore, this should be smaller. Another argument for making it smaller is that this blob size
    # will be held fully in memory temporarily.
    # But, making it too small would result in too many non-backwards compatible indexes being created.
    _MAX_BLOB_SIZE = 256 * 1024 * 1024  # 256 MiB

    _CREATE_FILES_TABLE = """
        CREATE TABLE "files" (
            "path"           VARCHAR(65535) NOT NULL,  /* path with leading and without trailing slash */
            "name"           VARCHAR(65535) NOT NULL,
            "offsetheader"   INTEGER,  /* seek offset from TAR file where the TAR metadata for this file resides */
            "offset"         INTEGER,  /* seek offset from TAR file where these file's contents resides */
            "size"           INTEGER,
            "mtime"          REAL,
            "mode"           INTEGER,
            "type"           INTEGER,
            "linkname"       VARCHAR(65535),
            "uid"            INTEGER,
            "gid"            INTEGER,
            /* True for valid TAR files. Internally used to determine where to mount recursive TAR files. */
            "istar"          BOOL   ,
            "issparse"       BOOL   ,  /* For sparse files the file size refers to the expanded size! */
            "isgenerated"    BOOL   ,  /* True for entries generated for parent folders by ratarmount. */
            "recursiondepth" INTEGER,  /* Normally 0. 1+ if the file is recursively in an archive. */
            /* See SQL benchmarks for decision on the primary key.
             * See also https://www.sqlite.org/optoverview.html
             * (path,name) tuples might appear multiple times in a TAR if it got updated.
             * In order to also be able to show older versions, we need to add
             * the offsetheader column to the primary key. */
            PRIMARY KEY (path,name,offsetheader)
        );"""

    _CREATE_XATTRS_TABLE = """
        CREATE TABLE IF NOT EXISTS "xattrkeys" ( "name" VARCHAR(65535) PRIMARY KEY );

        CREATE TABLE IF NOT EXISTS "xattrsdata" (
            "offsetheader" INTEGER,
            "keyid" INTEGER,
            "value" VARCHAR(65535),  /* Binary Data (Python Bytes) */
            PRIMARY KEY (offsetheader,keyid)
        );

        CREATE VIEW IF NOT EXISTS "xattrs" ( "offsetheader", "key", "value" ) AS
            SELECT offsetheader, xattrkeys.name, value FROM "xattrsdata"
            INNER JOIN xattrkeys ON xattrkeys.rowid = xattrsdata.keyid;

        CREATE TRIGGER IF NOT EXISTS "xattrs_insert" INSTEAD OF INSERT ON "xattrs"
        BEGIN
            INSERT OR IGNORE INTO xattrkeys(name) VALUES (NEW.key);
            INSERT OR IGNORE INTO xattrsdata(offsetheader, keyid, value) VALUES(
                NEW.offsetheader,
                (SELECT xattrkeys.rowid FROM xattrkeys WHERE name = NEW.key),
                NEW.value
            );
        END;"""

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
        indexFolders: Optional[Sequence[str]] = None,
        archiveFilePath: Optional[str] = None,
        *,  # force all parameters after to be keyword-only
        encoding: str = tarfile.ENCODING,
        preferMemory: bool = False,
        indexMinimumFileCount: int = 0,
        backendName: str = '',
        ignoreCurrentFolder: bool = False,
        deleteInvalidIndexes: bool = True,
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
        preferMemory
            If True, then load existing indexes and write to explicitly given index file paths but
            if no such things are given, then create the new index in memory as if indexFilePath
            = ':memory:' was specified.
        indexMinimumFileCount
            If > 0, then open new databases in memory and write them out if this threshold has been
            exceeded. It may also be written to a file if a gzip index is stored.
        backendName
            The backend name to be stored as metadata and to determine compatibility of found indexes.
        ignoreCurrentFolder
            If true, then do not store the index into the current path. This was introduced for URL
            opened as file objects but may be useful for any archive given via a file object.
        """

        self.sqlConnection: Optional[sqlite3.Connection] = None
        # Will hold the actually opened valid path to an index file
        self.indexFilePath: Optional[str] = None
        # This is true if the index file found was compressed or an URL and had to be downloaded
        # and/or extracted into a temporary folder.
        self.indexFilePathDeleteOnClose = False
        self.deleteInvalidIndexes = deleteInvalidIndexes
        self.encoding = encoding
        self.possibleIndexFilePaths = SQLiteIndex.get_possible_index_file_paths(
            indexFilePath,
            indexFolders,
            archiveFilePath,
            ignoreCurrentFolder,
        )
        # stores which parent folders were last tried to add to database and therefore do exist
        self.parentFolderCache: list[tuple[str, str]] = []
        self.preferMemory = preferMemory
        self.indexMinimumFileCount = indexMinimumFileCount
        self.backendName = backendName
        self._insertedRowCount = 0
        self._temporaryIndexFile: Optional[Any] = None

        # Ignore minimum file count option if an index file path or index folder is configured.
        # For latter, ignore the special empty folder [''], which means that the indexes are stored
        # besides the archives.
        # Why all this exceptions? Because the index-minimum-file-count, which is set by default,
        # is only intended to avoid littering folders with index files in the common use case of mounting
        # a folder of archives or single small archives recursively. If the user goes through the trouble
        # of specifying an index file or folder, then littering should not be a problem as index creations
        # are expected by the user.
        # Guard this feature for Python >= 3.6 because it uses sqlite3.Connection.backup, which was only
        # introduced in Python 3.7!
        if self.indexMinimumFileCount > 0 and not indexFilePath and sys.version_info[0:2] >= (3, 7):
            logger.debug(
                "Because of the given positive index minimum file count (%s) "
                "and because no explicit index file path is given, try to open an SQLite database in memory first.",
                self.indexMinimumFileCount,
            )
            self.preferMemory = True

    @staticmethod
    def get_possible_index_file_paths(
        indexFilePath: Optional[str],
        indexFolders: Optional[Sequence[str]] = None,
        archiveFilePath: Optional[str] = None,
        ignoreCurrentFolder: bool = False,
    ) -> list[str]:
        if indexFilePath == ':memory:':
            return []

        possibleIndexFilePaths = []
        if indexFilePath:
            # Prior versions did simply return indexFilePath as the only possible path if it was specified.
            # This worked well enough because if the path did not exist, it would simply be created.
            # However, for non-writable locations, or if parent folders are missing, or for remote URLs,
            # this will fail badly and result in TAR files being opened with the fallback, libarchive, instead,
            # which is unwanted behavior. It should fall back to another index storage location instead.
            # Or even better, it should escalate the error to the user, but that seems too difficult with the
            # current trial-and-error architecture for opening archives.
            if '://' not in indexFilePath:
                return [os.path.abspath(os.path.expanduser(indexFilePath))]
            possibleIndexFilePaths.append(indexFilePath)

        if not archiveFilePath:
            return possibleIndexFilePaths

        # Look for default (compressed) indexes. The compressed ones should only be used as fallbacks
        # because they are less performant and because we do not want to accidentally created index files
        # with a compressed extension even though it is uncompressed. The latter reason is also why we
        # check for file existence before adding it as a default, although I think it might not be necessary. */
        defaultIndexFilePath = archiveFilePath + ".index.sqlite"
        defaultIndexFilePaths = [defaultIndexFilePath]
        for extensions in [
            FILE_FORMATS[fid].extensions for backend in COMPRESSION_BACKENDS.values() for fid in backend.formats
        ]:
            for extension in extensions:
                path = defaultIndexFilePath + '.' + extension
                if os.path.isfile(path) and os.stat(path).st_size > 0:
                    defaultIndexFilePaths.append(path)

        if not indexFolders:
            possibleIndexFilePaths.extend(defaultIndexFilePaths)
            possibleIndexFilePaths.append(':memory:')
            return possibleIndexFilePaths

        indexPathAsName = defaultIndexFilePath.replace("/", "_")
        for folder in indexFolders:
            if folder:
                indexPath = os.path.join(folder, indexPathAsName)
                possibleIndexFilePaths.append(os.path.abspath(os.path.expanduser(indexPath)))
            elif not ignoreCurrentFolder:
                possibleIndexFilePaths.extend(defaultIndexFilePaths)
        return possibleIndexFilePaths

    def clear_indexes(self):
        for indexPath in self.possibleIndexFilePaths:
            if os.path.isfile(indexPath):
                os.remove(indexPath)

    def open_existing(self, checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None, readOnly: bool = False):
        """Tries to find an already existing index."""
        for indexPath in self.possibleIndexFilePaths:
            if self._try_load_index(indexPath, checkMetadata=checkMetadata, readOnly=readOnly):
                break

    def open_in_memory(self):
        self.indexFilePath, self.sqlConnection = SQLiteIndex._open_path(':memory:')

    def open_writable(self):
        if self.possibleIndexFilePaths and not self.preferMemory:
            for indexPath in self.possibleIndexFilePaths:
                if SQLiteIndex._path_is_writable(indexPath) and SQLiteIndex._path_can_be_used_for_sqlite(indexPath):
                    self.indexFilePath, self.sqlConnection = SQLiteIndex._open_path(indexPath)
                    break
        else:
            if self.preferMemory:
                logger.debug("Create new index in memory because memory is to be preferred, e.g., for small archives.")
            self.indexFilePath, self.sqlConnection = SQLiteIndex._open_path(':memory:')

        if not self.index_is_loaded():
            raise InvalidIndexError(
                "Could not find any existing index or writable location for an index in "
                + str(self.possibleIndexFilePaths)
            )

    def __enter__(self):
        return self

    def __del__(self):
        # This is important in case SQLiteIndex is not used with a context manager
        # and the constructor raises an exception. There is no clean way to close it in that case!
        self.close()
        if hasattr(super(), '__del__'):
            super().__del__()

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def close(self):
        if self.sqlConnection:
            try:
                self.sqlConnection.commit()
                self.sqlConnection.close()
            except sqlite3.ProgrammingError:
                # Ignore "Cannot operate on a closed database."
                pass
            self.sqlConnection = None

        self._set_index_file_path(None)

    def get_connection(self) -> sqlite3.Connection:
        if self.sqlConnection:
            return self.sqlConnection
        raise IndexNotOpenError("This method can not be called without an opened index database!")

    def _store_versions_metadata(self) -> None:
        connection = self.get_connection()

        try:
            connection.executescript(SQLiteIndex._CREATE_VERSIONS_TABLE)
        except Exception as exception:
            logger.error("There was an error when adding metadata information. Index loading might not work.")
            logger.info("Exception: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG))

        try:

            def make_version_row(
                versionName: str, version: str
            ) -> tuple[str, str, Optional[str], Optional[str], Optional[str]]:
                versionNumbers = [re.sub('[^0-9]', '', x) for x in version.split('.')]
                return (
                    versionName,
                    version,
                    versionNumbers[0] if len(versionNumbers) > 0 else None,
                    versionNumbers[1] if len(versionNumbers) > 1 else None,
                    versionNumbers[2] if len(versionNumbers) > 2 else None,
                )

            versions = [
                make_version_row('ratarmount', __version__),
                make_version_row('index', SQLiteIndex.__version__),
            ]

            for moduleName in {
                module
                for info in COMPRESSION_BACKENDS.values()
                for module, _ in info.requiredModules
                if module in sys.modules
            }:
                moduleVersion = find_module_version(sys.modules[moduleName])
                if moduleVersion:
                    versions += [make_version_row(moduleName, moduleVersion)]

            connection.executemany('INSERT OR REPLACE INTO "versions" VALUES (?,?,?,?,?)', versions)
        except Exception as exception:
            logger.error("There was an error when adding version information.")
            logger.info("Exception: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG))

    def _store_file_metadata(self, filePath: AnyStr) -> None:
        """Adds some consistency meta information to recognize the need to update the cached TAR index"""
        try:
            tarStats = os.stat(filePath)
            serializedTarStats = json.dumps(
                {attr: getattr(tarStats, attr) for attr in dir(tarStats) if attr.startswith('st_')}
            )
            self.store_metadata_key_value("tarstats", serializedTarStats)
        except Exception as exception:
            logger.error("There was an error when adding file metadata.")
            logger.error("Automatic detection of changed TAR files during index loading might not work.")
            logger.info("Exception: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG))

    def store_metadata_key_value(self, key: AnyStr, value: Union[str, bytes]) -> None:
        connection = self.get_connection()
        connection.executescript(SQLiteIndex._CREATE_METADATA_TABLE)

        try:
            connection.execute('INSERT OR REPLACE INTO "metadata" VALUES (?,?)', (key, value))
        except Exception as exception:
            logger.error("There was an error when adding argument metadata.")
            logger.error("Automatic detection of changed arguments files during index loading might not work.")
            logger.info("Exception: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG))

        connection.commit()

    def store_metadata(self, metadata: AnyStr, filePath: Optional[AnyStr] = None) -> None:
        self._store_versions_metadata()
        self.store_metadata_key_value('backendName', self.backendName)
        if filePath:
            self._store_file_metadata(filePath)
        self.store_metadata_key_value('arguments', metadata)
        self.store_metadata_key_value('backendName', self.backendName)

    def drop_metadata(self):
        self.get_connection().executescript(
            """
            DROP TABLE IF EXISTS metadata;
            DROP TABLE IF EXISTS versions;
            """
        )

    def try_to_open_first_file(self, openByPath):
        # Get first row that has the regular file bit set in mode (stat.S_IFREG == 32768 == 1<<15).
        result = self.get_connection().execute(
            f"""SELECT path,name {SQLiteIndex.FROM_REGULAR_FILES} ORDER BY "offsetheader" ASC LIMIT 1;"""
        )
        if not result:
            return
        firstFile = result.fetchone()
        if not firstFile:
            return

        logger.info(
            "The index contains no backend name. Therefore, will try to open the first file as an integrity check."
        )
        try:
            with openByPath(firstFile[0] + '/' + firstFile[1]) as file:
                file.read(1)
        except Exception as exception:
            logger.info(
                "Trying to open the first file raised an exception: %s",
                exception,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            raise InvalidIndexError("Integrity check of opening the first file failed.") from exception

    @staticmethod
    def check_archive_stats(
        archiveFilePath: Optional[str], metadata: dict[str, Any], verifyModificationTime: bool
    ) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""

        if 'tarstats' not in metadata:
            return

        if not archiveFilePath:
            raise InvalidIndexError("Archive contains file stats but cannot stat real archive!")

        storedStats = json.loads(metadata['tarstats'])
        archiveStats = os.stat(archiveFilePath)

        if (
            hasattr(archiveStats, 'st_size')
            and 'st_size' in storedStats
            and archiveStats.st_size < storedStats['st_size']
        ):
            raise InvalidIndexError(
                f"Archive for this SQLite index has shrunk in size from "
                f"{storedStats['st_size']} to {archiveStats.st_size}"
            )

        # Only happens very rarely, e.g., for more recent files with the same size.
        if (
            verifyModificationTime
            and hasattr(archiveStats, "st_mtime")
            and 'st_mtime' in storedStats
            and archiveStats.st_mtime != storedStats['st_mtime']
        ):
            raise InvalidIndexError(
                f"The modification date for the archive file {storedStats['st_mtime']} "
                f"to this SQLite index has changed ({archiveStats.st_mtime!s})",
            )

    @staticmethod
    def check_metadata_arguments(metadata: dict, arguments, argumentsToCheck: Sequence[str]):
        # Check arguments used to create the found index.
        # These are only warnings and not forcing a rebuild by default.
        # TODO: Add option to force index rebuild on metadata mismatch?
        differingArgs = [
            (arg, metadata[arg], getattr(arguments, arg))
            for arg in argumentsToCheck
            if arg in metadata and hasattr(arguments, arg) and metadata[arg] != getattr(arguments, arg)
        ]
        if differingArgs:
            logger.warning(
                "The arguments used for creating the found index differ from the arguments "
                "given for mounting the archive now. In order to apply these changes, "
                "recreate the index using the --recreate-index option!"
            )
            for arg, oldState, newState in differingArgs:
                logger.warning("%s: index: %s, current: %s", arg, oldState, newState)

    def check_metadata_backend(self, metadata: dict):
        # When opening an index without a backend via SQLiteIndexMountSource directly, it should not be checked.
        if not self.backendName:
            return

        # Because of a lack of sufficient foresight, the backend name was not added to the index in older versions.
        backendName = metadata.get('backendName')
        if isinstance(backendName, str) and backendName != self.backendName:
            raise MismatchingIndexError(
                f"Cannot open an index created with backend '{backendName}'!\n"
                f"Will stop trying to open the archive with backend '{self.backendName}'.\n"
                f"Use --recreate-index if '{backendName}' is not installed."
            )

    def get_index_version(self):
        return self.get_connection().execute("""SELECT version FROM versions WHERE name == 'index';""").fetchone()[0]

    @staticmethod
    def _path_is_writable(path: str) -> bool:
        # Writing indexes to remote filesystems currently not supported and we need to take care that URLs
        # are not interpreted as local file paths, i.e., creating an ftp: folder with a user:password@host subfolder.
        if '://' in path:
            return False

        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            file = Path(path)
            file.write_bytes(b'\0' * 1024 * 1024)
            file.unlink()

            return True

        except PermissionError:
            logger.debug("Insufficient permissions to write to: %s", path, exc_info=logger.isEnabledFor(logging.DEBUG))

        except OSError:
            logger.info("Could not create file: %s", path, exc_info=True)

        return False

    @staticmethod
    def _path_can_be_used_for_sqlite(path: str) -> bool:
        if not SQLiteIndex._path_is_writable(path):
            return False

        fileExisted = os.path.isfile(path)
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            connection = SQLiteIndex._open_sql_db(path)
            connection.executescript('CREATE TABLE "files" ( "path" VARCHAR(65535) NOT NULL );')
            connection.commit()
            connection.close()
            return True
        except sqlite3.OperationalError:
            logger.debug("Could not create SQLite database at: %s", path, exc_info=True)
        finally:
            if not fileExisted and os.path.isfile(path):
                SQLiteIndex._unchecked_remove(path)

        return False

    @staticmethod
    def _unchecked_remove(path: Optional[AnyStr]):
        """
        Often cleanup is good manners but it would only be obnoxious if ratarmount crashed on unnecessary cleanup.
        """
        if not path or not os.path.exists(path):
            return

        try:
            os.remove(path)
        except Exception:
            logger.warning("Could not remove: %s", path, exc_info=logger.isEnabledFor(logging.DEBUG))

    @staticmethod
    def _open_sql_db(path: AnyStr, **kwargs) -> sqlite3.Connection:
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
    def _open_path(indexFilePath: Optional[str]) -> tuple[str, sqlite3.Connection]:
        indexFilePath = indexFilePath or ':memory:'

        if logger.isEnabledFor(logging.WARNING):
            print("Creating new SQLite index database at", indexFilePath)

        sqlConnection = SQLiteIndex._open_sql_db(indexFilePath)
        tables = get_sqlite_tables(sqlConnection)
        if {"files", "filestmp", "parentfolders"}.intersection(tables):
            raise InvalidIndexError(
                f"The index file {indexFilePath} already seems to contain a table. Please specify --recreate-index."
            )
        sqlConnection.executescript(SQLiteIndex._CREATE_FILES_TABLE)
        sqlConnection.executescript(SQLiteIndex._CREATE_XATTRS_TABLE)

        return indexFilePath, sqlConnection

    def reload_index_read_only(self):
        if not self.indexFilePath or self.indexFilePath == ':memory:' or not self.sqlConnection:
            return

        self.sqlConnection.commit()
        self.sqlConnection.close()

        uriPath = urllib.parse.quote(self.indexFilePath)
        # check_same_thread=False can be used because it is read-only anyway and it allows to enable FUSE multithreading
        self.sqlConnection = SQLiteIndex._open_sql_db(f"file:{uriPath}?mode=ro", uri=True, check_same_thread=False)

    def _reload_index_on_disk(self):
        logger.info("Try to reopen SQLite database on disk at: %s", self.indexFilePath)
        logger.info("other index paths: %s", self.possibleIndexFilePaths)
        if not self.indexFilePath or self.indexFilePath != ':memory:' or not self.sqlConnection:
            return

        oldIndexFilePath, oldSqlConnection = self.indexFilePath, self.sqlConnection
        self.preferMemory = False
        self.open_writable()
        if oldIndexFilePath == self.indexFilePath:
            logger.info("Tried to write the database to disk but found no other path than: %s", self.indexFilePath)
            self.sqlConnection.close()
            self.indexFilePath, self.sqlConnection = oldIndexFilePath, oldSqlConnection
            return

        logger.debug("Back up database from %s -> %s", oldIndexFilePath, self.indexFilePath)
        oldSqlConnection.commit()
        oldSqlConnection.backup(self.sqlConnection)
        oldSqlConnection.close()

    def ensure_intermediary_tables(self):
        connection = self.get_connection()
        tables = get_sqlite_tables(connection)

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
            # Note that the with-statement does not automatically close a connection!
            # https://discuss.python.org/t/implicitly-close-sqlite3-connections-with-context-managers/33320
            connection = sqlite3.connect(":memory:")
            queriedLibSqliteVersion = connection.execute("select sqlite_version();").fetchone()
            connection.close()
            libSqliteVersion = tuple(int(x) for x in queriedLibSqliteVersion[0].split('.'))
        except Exception:
            libSqliteVersion = (0, 0, 0)

        searchByTuple = """(path,name) NOT IN ( SELECT path,name"""
        searchByConcat = """path || "/" || name NOT IN ( SELECT path || "/" || name"""

        cleanUpDatabase = f"""
            INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","name",rowid;
            DROP TABLE "filestmp";
            INSERT OR IGNORE INTO "files"
                /* path name offsetheader offset size mtime mode type */
                SELECT path,name,offsetheader,offset,0,0,{int(0o555 | stat.S_IFDIR)},{int(tarfile.DIRTYPE)},
                       /* linkname uid gid istar issparse isgenerated recursiondepth */
                       "",0,0,0,0,0,0
                FROM "parentfolders"
                WHERE {searchByTuple if libSqliteVersion >= (3, 22, 0) else searchByConcat}
                    FROM "files" WHERE mode & (1 << 14) != 0
                )
                ORDER BY "path","name";
            DROP TABLE "parentfolders";
            VACUUM;  /* According to benchmarks, this is important to reduce the size after the table drop! */
            PRAGMA optimize;
        """

        # Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        logger.info("Resorting files by path ...")

        self.get_connection().executescript(cleanUpDatabase)

    def file_count(self) -> int:
        return self.get_connection().execute('SELECT COUNT(*) FROM "files";').fetchone()[0]

    @staticmethod
    def _row_to_file_info(row: dict[str, Any]) -> FileInfo:
        # fmt: off
        userData = SQLiteIndexedTarUserData(
            offset         = row['offset'],
            offsetheader   = row['offsetheader'] if 'offsetheader' in row.keys() else 0,  # noqa: SIM118
            istar          = row['istar'],
            issparse       = row['issparse'] if 'issparse' in row.keys() else False,  # noqa: SIM118
            isgenerated    = row['isgenerated'] if 'isgenerated' in row.keys() else False,  # noqa: SIM118
            recursiondepth = row['recursiondepth'] if 'recursiondepth' in row.keys() else False,  # noqa: SIM118
        )
        return FileInfo(
            size     = row['size'],
            mtime    = row['mtime'],
            mode     = row['mode'],
            linkname = row['linkname'],
            uid      = row['uid'],
            gid      = row['gid'],
            userdata = [userData],
        )
        # fmt: on

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
    def _query_normpath(path: str):
        # os.path.normpath also collapses /../ into / and, because we prepend /, ../ gets collapsed to /.
        # Note that normpath does not collapse leading double slash, but all other number of leading slashes!
        # This effect is good to have for inserting rows but not for querying rows.
        return '/' + os.path.normpath(path if path.startswith('../') else '/' + path).lstrip('/')

    def list(self, path: str) -> Optional[dict[str, FileInfo]]:
        """
        Return a dictionary for the given directory path: { fileName : FileInfo, ... } or None
        if the path does not exist.

        There is no file version argument because it is hard to apply correctl.y
        Even if a folder was overwritten by a file, which is already not well supported by tar,
        then list for that path will still list all contents of the overwritten folder or folders,
        no matter the specified version. The file system layer has to take care that a directory
        listing is not even requested in the first place if it is not a directory.
        FUSE already does this by calling getattr for all parent folders in the specified path first.

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

        # https://www.sqlite.org/pragma.html#pragma_table_info
        columns = [row[0] for row in self.get_connection().execute("SELECT name FROM PRAGMA_TABLE_INFO('files');")]
        selected_columns = ['name', 'size', 'mtime', 'mode', 'linkname', 'uid', 'gid', 'offset', 'istar']
        # Both were added in index 0.2.0.
        if 'offsetheader' in columns and 'issparse' in columns:
            selected_columns += ['offsetheader', 'issparse']
        # Added in index 0.6.0.
        if 'isgenerated' in columns:
            selected_columns += ['isgenerated']
        if 'recursiondepth' in columns:
            selected_columns += ['recursiondepth']

        def row_to_file_info(cursor, row) -> tuple[str, FileInfo]:  # pylint: disable=unused-argument
            # fmt: off
            return row[0], FileInfo(
                size     = row[1],
                mtime    = row[2],
                mode     = row[3],
                linkname = row[4],
                uid      = row[5],
                gid      = row[6],
                userdata = [SQLiteIndexedTarUserData(
                    offset         = row[7],
                    offsetheader   = row[9] if len(row) > 9 else 0,
                    istar          = row[8],
                    issparse       = row[10] if len(row) > 10 else False,
                    isgenerated    = row[11] if len(row) > 11 else False,
                    recursiondepth = row[12] if len(row) > 12 else False,
                )],
            )
            # fmt: on

        oldRowFactory = self.get_connection().row_factory
        self.get_connection().row_factory = row_to_file_info
        directory: dict[str, FileInfo] = dict(
            self.get_connection().execute(
                'SELECT ' + ','.join(selected_columns) + ' FROM "files" WHERE "path" == (?) ORDER BY "offsetheader"',
                (self._query_normpath(path).rstrip('/'),),
            )
        )
        self.get_connection().row_factory = oldRowFactory

        gotResults = bool(directory)
        directory.pop('', None)
        return directory if gotResults else None

    def list_mode(self, path: str) -> Optional[dict[str, int]]:
        """
        Return a dictionary mapping file names to file modes for the given directory path or None
        if the path does not exist.

        path : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        """

        # See comments in list.
        # The only difference here is that we do not request all columns, but only two in a tuple.
        oldRowFactory = self.get_connection().row_factory
        self.get_connection().row_factory = None
        rows = self.get_connection().execute(
            'SELECT name,mode FROM "files" WHERE "path" == (?) ORDER BY "offsetheader"',
            (self._query_normpath(path).rstrip('/'),),
        )
        self.get_connection().row_factory = oldRowFactory

        directory = {row[0]: row[1] for row in rows}
        gotResults = bool(directory)
        directory.pop('', None)
        return directory if gotResults else None

    def versions(self, path: str) -> dict[str, FileInfo]:
        """
        Return metadata for all versions of a file possibly appearing more than once
        in the index as a directory dictionary or an empty dictionary if the path does not exist.

        path : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        """

        path = self._query_normpath(path)
        if path == '/':
            return {'/': create_root_file_info(userdata=[SQLiteIndexedTarUserData(0, 0, False, False, True, 0)])}

        path, name = path.rsplit('/', 1)
        rows = self.get_connection().execute(
            'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?) ORDER BY "offsetheader" ASC', (path, name)
        )
        return {str(version + 1): self._row_to_file_info(row) for version, row in enumerate(rows)}

    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Return the FileInfo to given path (directory or file) or None if the path does not exist.

        fileVersion : If the TAR contains the same file path multiple times, by default only the last one is shown.
                      But with this argument other versions can be queried. Version 1 is the oldest one.
                      Version 0 translates to the most recent one for compatibility with tar --occurrence=<number>.
                      Version -1 translates to the second most recent, and so on.
        """

        if not isinstance(fileVersion, int):
            raise RatarmountError("The specified file version must be an integer!")

        path = self._query_normpath(path)
        if path == '/':
            return create_root_file_info(userdata=[SQLiteIndexedTarUserData(0, 0, False, False, True, 0)])

        path, name = path.rsplit('/', 1)
        row = (
            self.get_connection()
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
        return self._row_to_file_info(row) if row else None

    def setxattrs(self, rows: Sequence[tuple[int, str, bytes]]):
        self.get_connection().executemany('INSERT OR REPLACE INTO "xattrs" VALUES (?,?,?)', rows)

    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        if not fileInfo.userdata:
            return []
        userData = fileInfo.userdata[-1]
        assert isinstance(userData, SQLiteIndexedTarUserData)

        if userData.isgenerated:
            return []

        try:
            # Look up by offsetheader seems to be unique enough for all implementations,
            # but currently only TAR supports attributes anyways.
            return [
                row[0]
                for row in self.get_connection().execute(
                    "SELECT key FROM xattrs WHERE offsetheader=(?);", (userData.offsetheader,)
                )
            ]
        except sqlite3.OperationalError:
            # May happen when loading old indexes that do not have the xattrs table.
            pass
        return []

    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        if not fileInfo.userdata:
            return None
        userData = fileInfo.userdata[-1]
        assert isinstance(userData, SQLiteIndexedTarUserData)

        if userData.isgenerated:
            return None

        try:
            row = (
                self.get_connection()
                .execute("SELECT value FROM xattrs WHERE (offsetheader,key)=(?,?);", (userData.offsetheader, key))
                .fetchone()
            )
            return row[0] if row else None
        except sqlite3.OperationalError:
            # May happen when loading old indexes that do not have the xattrs table.
            pass
        return None

    def _try_add_parent_folders(self, path: str, offsetheader: int, offset: int) -> None:
        # Add parent folders if they do not exist.
        # E.g.: path = '/a/b/c' -> paths = [('', 'a'), ('/a', 'b'), ('/a/b', 'c')]
        # Without the parentFolderCache, the additional INSERT statements increase the creation time
        # from 8.5s to 12s, so almost 50% slowdown for the 8MiB test TAR!
        pathParts = path.split("/")
        # fmt: off
        paths = [
            p
            for p in (
                ( "/".join( pathParts[:i] ), pathParts[i] )
                for i in range( 1, len( pathParts ) )
            )
            if p not in self.parentFolderCache
        ]
        # fmt: on
        if not paths:
            return

        self.parentFolderCache += paths
        # Assuming files in the TAR are sorted by hierarchy, the maximum parent folder cache size
        # gives the maximum cacheable file nesting depth. High numbers lead to higher memory usage and lookup times.
        if len(self.parentFolderCache) > 16:
            self.parentFolderCache = self.parentFolderCache[-8:]

        # TODO This method is still not perfect but I do not know how to perfect it without losing significant
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
        self.get_connection().executemany(
            'INSERT OR IGNORE INTO "parentfolders" VALUES (?,?,?,?)',
            [(p[0], p[1], offsetheader, offset) for p in paths],
        )

    def set_file_infos(self, rows: Sequence[tuple]) -> None:
        if not rows:
            return

        self._insertedRowCount += len(rows)
        if (
            self._insertedRowCount > self.indexMinimumFileCount
            and self.indexFilePath == ':memory:'
            and self.preferMemory
        ):
            logger.info(
                "Exceeded file count threshold (%s > %s) for metadata held in memory. "
                "Will try to reopen the database on disk.",
                self._insertedRowCount,
                self.indexMinimumFileCount,
            )
            self.preferMemory = False
            self._reload_index_on_disk()

        try:
            self.get_connection().executemany(
                'INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(rows[0])) + ');', rows
            )
        except UnicodeEncodeError:
            # Fall back to separately inserting each row to find those in need of string cleaning.
            for row in rows:
                self.set_file_info(row)
            return

        for row in rows:
            self._try_add_parent_folders(row[0], row[2], row[3])

    @staticmethod
    def _escape_invalid_characters(toEscape: str, encoding: str):
        try:
            toEscape.encode()
            return toEscape
        except UnicodeEncodeError:
            return toEscape.encode(encoding, 'surrogateescape').decode(encoding, 'backslashreplace')

    def set_file_info(self, row: tuple) -> None:
        connection = self.get_connection()

        try:
            connection.execute('INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', row)
        except UnicodeEncodeError:
            logger.warning("Problem caused by file name encoding when trying to insert this row: %s", row)
            logger.warning(
                "The file name will now be stored with the bad character being escaped instead of being correctly "
                "interpreted. Please specify a suitable file name encoding using, e.g., --encoding iso-8859-1!"
            )
            logger.warning(
                "A list of possible encodings can be found here: "
                "https://docs.python.org/3/library/codecs.html#standard-encodings"
            )

            checkedRow = []
            for x in list(row):  # check strings
                if isinstance(x, str):
                    checkedRow += [self._escape_invalid_characters(x, self.encoding)]
                else:
                    checkedRow += [x]

            connection.execute(
                'INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', tuple(checkedRow)
            )
            logger.warning("The escaped inserted row is now: %s", row)
            logger.warning("")

            self._try_add_parent_folders(self._escape_invalid_characters(row[0], self.encoding), row[2], row[3])
            return

        self._try_add_parent_folders(row[0], row[2], row[3])

    def index_is_loaded(self) -> bool:
        """Returns true if the SQLite database has been opened for reading and a "files" table exists."""
        if not self.sqlConnection:
            return False

        try:
            self.sqlConnection.execute('SELECT * FROM "files" WHERE 0 == 1;')
        except sqlite3.OperationalError:
            self.sqlConnection = None
            return False

        return True

    def _set_index_file_path(self, indexFilePath: Optional[str], deleteOnClose: bool = False):
        # This is called from __del__, so we need to account for this being called when something
        # in the constructor raises an exception and not all members of self exist.
        if (
            getattr(self, 'indexFilePath', None)
            and getattr(self, 'indexFilePathDeleteOnClose', False)
            and self.indexFilePath
            and os.path.isfile(self.indexFilePath)
        ):
            try:
                os.remove(self.indexFilePath)
            except Exception as exception:
                logger.warning(
                    "Failed to remove temporarily downloaded and/or extracted index file at: %s because of: %s",
                    self.indexFilePath,
                    exception,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )

        if hasattr(self, 'indexFilePath') and hasattr(self, 'indexFilePathDeleteOnClose'):
            self.indexFilePath = indexFilePath
            self.indexFilePathDeleteOnClose = deleteOnClose

    def _load_index(
        self, indexFilePath: str, checkMetadata: Optional[Callable[[dict[str, Any]], None]], readOnly: bool = False
    ) -> None:
        """
        Loads the given index SQLite database and checks it for validity raising an exception if it is invalid.

        checkMetadata
            A verifying callback that is called when opening an existing index. It is given the
            the dictionary of metadata in the index and should thrown an exception when the index
            should not be used, e.g., because the version is incompatible.
        """
        if self.index_is_loaded():
            return

        # Download and/or extract the file to a temporary file if necessary.

        # Strip a single file:// prefix, not any more because URL chaining is supported by fsspec,
        # to avoid useless copies to the temporary directory.
        if indexFilePath.count('://') == 1:
            fileURLPrefix = 'file://'
            indexFilePath = indexFilePath.removeprefix(fileURLPrefix)

        temporaryFolder = os.environ.get("RATARMOUNT_INDEX_TMPDIR", None)

        def _undo_compression(file):
            compression = detect_compression(file)
            if not compression or not any(
                (compression in backend.formats) for backend in COMPRESSION_BACKENDS.values()
            ):
                return None

            logger.info("Detected %s-compressed index.", compression)

            backend = find_available_backend(compression)
            if not backend:
                packages = [
                    x[1]
                    for info in COMPRESSION_BACKENDS.values()
                    for x in info.requiredModules
                    if compression in info.formats
                ]
                raise CompressionError(
                    f"Cannot open a {compression} compressed index file {indexFilePath} "
                    f"without any of these packages: {packages}"
                )

            return backend.open(file)

        def _copy_to_temp(file):
            self._temporaryIndexFile = tempfile.NamedTemporaryFile(suffix=".tmp.sqlite.index", dir=temporaryFolder)
            # TODO add progress bar / output?
            with open(self._temporaryIndexFile.name, 'wb') as targetFile:
                shutil.copyfileobj(file, targetFile)

        if '://' in indexFilePath:
            if fsspec is None:
                raise RatarmountError(
                    "Detected an URL for the index path but fsspec could not be imported!\n"
                    "Try installing it with 'pip install fsspec' or 'pip install ratarmount[full]'."
                )

            with fsspec.open(indexFilePath) as file:
                decompressedFile = _undo_compression(file)
                with decompressedFile or file as fileToCopy:
                    _copy_to_temp(fileToCopy)
        else:
            if not os.path.isfile(indexFilePath):
                return

            with open(indexFilePath, 'rb') as file:
                decompressedFile = _undo_compression(file)
                if decompressedFile:
                    with decompressedFile:
                        _copy_to_temp(decompressedFile)
                else:
                    temporaryIndexFilePath = indexFilePath

        temporaryIndexFilePath = self._temporaryIndexFile.name if self._temporaryIndexFile else indexFilePath

        # Done downloading and/or extracting the SQLite index.

        if readOnly:
            uriPath = urllib.parse.quote(temporaryIndexFilePath)
            # check_same_thread=False can be used because it is read-only and it allows to enable FUSE multithreading
            self.sqlConnection = SQLiteIndex._open_sql_db(f"file:{uriPath}?mode=ro", uri=True, check_same_thread=False)
        else:
            self.sqlConnection = SQLiteIndex._open_sql_db(temporaryIndexFilePath)
        tables = get_sqlite_tables(self.sqlConnection)
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
            # are so invalid, no one should miss them. So, recreate index by default for these cases.
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
                logger.warning("The found outdated index does not contain any sparse file information.")
                logger.warning("The index will also miss data about multiple versions of a file.")
                logger.warning("Please recreate the index if you have problems with those.")

            if 'metadata' in tables:
                metadata = dict(self.sqlConnection.execute('SELECT * FROM metadata;'))
                if checkMetadata:
                    self.check_metadata_backend(metadata)
                    checkMetadata(metadata)

        except Exception as e:
            # index_is_loaded checks self.sqlConnection, so close it before returning because it was found to be faulty
            with contextlib.suppress(sqlite3.Error):
                self.sqlConnection.close()
            self.sqlConnection = None

            raise e

        if self.index_is_loaded() and self.sqlConnection:
            try:
                indexVersion = self.sqlConnection.execute(
                    "SELECT major,minor FROM versions WHERE name == 'index';"
                ).fetchone()

                if indexVersion:
                    indexVersionTuple = _to_version_tuple(indexVersion)
                    indexAPIVersionTuple = _to_version_tuple(SQLiteIndex.__version__)
                    if indexVersionTuple and indexAPIVersionTuple and indexVersionTuple > indexAPIVersionTuple:
                        logger.warning("The loaded index was created with a newer version of ratarmount.")
                        logger.warning("If there are any problems, please update ratarmount or recreate the index")
                        logger.warning("with this ratarmount version using the --recreate-index option!")
            except Exception:
                pass

        if logger.isEnabledFor(logging.WARNING):
            message = "Successfully loaded offset dictionary from " + str(indexFilePath)
            if temporaryIndexFilePath != indexFilePath:
                message += " temporarily downloaded/decompressed into: " + str(temporaryIndexFilePath)
            print(message)

        self._set_index_file_path(temporaryIndexFilePath)

    def _try_load_index(
        self,
        indexFilePath: str,
        checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None,
        readOnly: bool = False,
    ) -> bool:
        """Calls loadIndex if index is not loaded already and provides extensive error handling."""

        if self.index_is_loaded():
            return True

        try:
            self._load_index(indexFilePath, checkMetadata=checkMetadata, readOnly=readOnly)
        except MismatchingIndexError as e:
            raise e
        except Exception as exception:
            logger.warning(
                "Could not load file: %s because of: %s",
                indexFilePath,
                exception,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            logger.warning("Some likely reasons for not being able to load the index file:")
            logger.warning("  - The index file has incorrect read permissions")
            logger.warning("  - The index file is incomplete because ratarmount was killed during index creation")
            logger.warning("  - The index file was detected to contain errors because of known bugs of older versions")
            logger.warning("  - The index file got corrupted because of:")
            logger.warning("    - The program exited while it was still writing the index because of:")
            logger.warning("      - the user sent SIGINT to force the program to quit")
            logger.warning("      - an internal error occurred while writing the index")
            logger.warning("      - the disk filled up while writing the index")
            logger.warning("    - Rare lowlevel corruptions caused by hardware failure")

            logger.warning("This might force a time-costly index recreation, so if it happens often")
            logger.warning("and mounting is slow, try to find out why loading fails repeatedly,")
            logger.warning("e.g., by opening an issue on the public github page.")

            try:
                if self.deleteInvalidIndexes and '://' not in indexFilePath:
                    os.remove(indexFilePath)
            except OSError:
                logger.warning(
                    "Failed to remove corrupted old cached index file: %s",
                    indexFilePath,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )

        return self.index_is_loaded()

    def clear_compression_offsets(self):
        for table in ['bzip2blocks', 'gzipindex', 'gzipindexes', 'gztoolindex', 'zstdblocks']:
            self.get_connection().execute(f"DROP TABLE IF EXISTS {table}")

    def synchronize_compression_offsets(self, fileObject: IO[bytes], compression: FileFormatID):
        """
        Will load block offsets from SQLite database to backend if a fitting table exists.
        Else it will force creation and store the block offsets of the compression backend into a new table.
        """
        if compression and (not self.indexFilePath or self.indexFilePath == ':memory:'):
            if self.indexMinimumFileCount != 0:
                logger.info(
                    "Will try to reopen the database on disk even though the file count threshold "
                    "(%s) might not be exceeded (%s) because the archive is compressed.",
                    self.indexMinimumFileCount,
                    self._insertedRowCount,
                )
            self._reload_index_on_disk()

        if not self.indexFilePath or self.indexFilePath == ':memory:':
            logger.info("Will skip storing compression seek data because the database is in memory.")
            logger.info("If the database is in memory, then this data will not be read anyway.")
            return

        # This should be called after the TAR file index is complete (loaded or created).
        # If the TAR file index was created, then tarfile has iterated over the whole file once
        # and therefore completed the implicit compression offset creation.
        db = self.get_connection()

        if compression in [FileFormatID.BZIP2, FileFormatID.ZSTANDARD]:
            setBlockOffsets = getattr(fileObject, 'set_block_offsets', None)
            getBlockOffsets = getattr(fileObject, 'block_offsets', None)
            if not setBlockOffsets or not getBlockOffsets:
                logger.warning(
                    "The given file object misses the expected methods for getting/setting the block offsets. "
                    "Subsequent loads might be slow."
                )
                return

            table_name = ''
            if compression == FileFormatID.BZIP2:
                table_name = 'bzip2blocks'
            elif compression == FileFormatID.ZSTANDARD:
                table_name = 'zstdblocks'

            tables = get_sqlite_tables(db)
            if table_name in tables:
                try:
                    offsets = dict(db.execute(f"SELECT blockoffset,dataoffset FROM {table_name};"))
                    setBlockOffsets(offsets)
                    return
                except Exception as exception:
                    logger.info(
                        "Could not load %s block offset data because of %s. Will create it from scratch.",
                        compression.name,
                        exception,
                        exc_info=logger.isEnabledFor(logging.DEBUG),
                    )
            else:
                logger.info("The index does not yet contain %s block offset data. Will write it out.", compression.name)

            tables = get_sqlite_tables(db)
            if table_name in tables:
                db.execute(f"DROP TABLE {table_name}")
            db.execute(f"CREATE TABLE {table_name} ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )")
            db.executemany(f"INSERT INTO {table_name} VALUES (?,?)", getBlockOffsets().items())
            db.commit()
            return

        if (
            hasattr(fileObject, 'import_index')
            and hasattr(fileObject, 'export_index')
            and compression in [FileFormatID.GZIP, FileFormatID.ZLIB]
        ):
            if self._load_gzip_index(fileObject):
                return

            logger.info("The index does not yet contain gzip block offset data. Will write it out.")

            self._store_gzip_index(fileObject)
            return

        # Note that for xz seeking, loading and storing block indexes is unnecessary because it has an index included!
        if compression == FileFormatID.XZ:
            return

        raise NotImplementedError(
            f"Could not load or store block offsets for {compression} probably because adding support was forgotten!"
        )

    def _load_gzip_index(self, fileObject: IO[bytes]) -> bool:
        importIndex = getattr(fileObject, 'import_index', None)
        if not importIndex:
            # Should not happen.
            logger.debug("%s does not have an 'import_index' method. Cannot import index.", fileObject)
            return False

        connection = self.get_connection()
        tables = get_sqlite_tables(connection)
        is_rapidgzip_file = 'rapidgzip' in sys.modules and isinstance(fileObject, rapidgzip.RapidgzipFile)
        index_tables = [table for table in ['gzipindexes', 'gzipindex'] if table in tables]
        if 'gztoolindex' in tables:
            if is_rapidgzip_file:
                index_tables.insert(0, 'gztoolindex')
            elif not index_tables:
                logger.warning("Cannot load existing index because rapidgzip is not used! Please install it.")
        if not index_tables:
            return False  # Caller will print a suitable message.

        t0 = time.time()
        for table in index_tables:
            try:
                fileobj = SQLiteBlobsFile(connection, table, 'data', buffer_size=SQLiteIndex._MAX_BLOB_SIZE)
                if is_rapidgzip_file:
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
                logger.debug("Loading gzip index took %.2f s", time.time() - t0)

                return True
            except Exception as exception:
                logger.warning(
                    "Encountered exception when trying to load gzip index from table %s: %s",
                    table,
                    exception,
                    exc_info=logger.isEnabledFor(logging.DEBUG),
                )

        return False

    def _store_gzip_index(self, fileObject: IO[bytes]):
        exportIndex = getattr(fileObject, 'export_index', None)
        if not exportIndex:
            logger.warning(
                "The given file object misses the expected methods for getting/setting the block offsets. "
                "Subsequent loads might be slow."
            )
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

        supports_gztool_index = (
            'rapidgzip' in sys.modules
            and isinstance(fileObject, rapidgzip.RapidgzipFile)
            and hasattr(rapidgzip, 'IndexFormat')
        )
        table = 'gztoolindex' if supports_gztool_index else 'gzipindexes'

        db = self.get_connection()
        db.execute(f'DROP TABLE IF EXISTS "{table}"')
        db.execute(f'CREATE TABLE {table} ( data BLOB )')

        try:
            with WriteSQLiteBlobs(db, table, blob_size=SQLiteIndex._MAX_BLOB_SIZE) as gzindex:
                if supports_gztool_index:
                    # See the following link for the exception mapping done by Cython:
                    # https://cython.readthedocs.io/en/latest/src/userguide/wrapping_CPlusPlus.html#exceptions
                    logger.info("Store gzip index in gztool index file format.")
                    exportIndex(gzindex, rapidgzip.IndexFormat.GZTOOL)
                else:
                    exportIndex(fileobj=gzindex)
        except (indexed_gzip.ZranError, RuntimeError, ValueError) as exception:
            db.execute(f'DROP TABLE IF EXISTS "{table}"')

            # Triggers false pylint positive as no escaping with %% is required when no substitutions are used:
            # https://stackoverflow.com/questions/10678229/how-can-i-selectively-escape-percent-in-python-strings
            #     #comment52793727_10678240
            # pylint: disable-next=logging-too-few-args
            logger.warning(
                "The gzip index required for seeking could not be written to the database!"
                "This might happen when you are out of space in your temporary file and at the index file location. "
                "The gzip index size takes roughly 32kiB per 4MiB of uncompressed(!) bytes "
                "(0.8% of the uncompressed data) by default."
            )

            raise RatarmountError("Could not write out the gzip seek database.") from exception

        blobCount = db.execute(f'SELECT COUNT(*) FROM {table};').fetchone()[0]
        if blobCount == 0:
            logger.warning(
                "Did not write out any gzip seek data. This should only happen if the gzip size is smaller "
                "than the gzip seek point spacing."
            )
        elif blobCount == 1 and table == 'gzipindexes':
            # For downwards compatibility
            db.execute('DROP TABLE IF EXISTS "gzipindex";')
            db.execute('ALTER TABLE gzipindexes RENAME TO gzipindex;')

        db.commit()
