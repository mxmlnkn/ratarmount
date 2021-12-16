#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import json
import os
import re
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
import traceback
import urllib

from timeit import default_timer as timer
from typing import Any, AnyStr, cast, Dict, IO, Iterable, List, Optional, Tuple, Union
from dataclasses import dataclass

try:
    import indexed_bzip2
except ImportError:
    pass
try:
    import indexed_gzip
except ImportError:
    pass

from .version import __version__
from .MountSource import FileInfo, MountSource
from .ProgressBar import ProgressBar
from .StenciledFile import StenciledFile
from .compressions import supportedCompressions
from .utils import RatarmountError, IndexNotOpenError, InvalidIndexError, CompressionError, overrides


@dataclass
class SQLiteIndexedTarUserData:
    # fmt: off
    offset       : int
    offsetheader : int
    istar        : bool
    issparse     : bool
    # fmt: on


class SQLiteIndexedTar(MountSource):
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
    #   - Add arguments influencing the created index to metadata (ignore-zeros, recursive, ...)
    # Version 0.4.0:
    #   - Added 'gzipindexes' table, which may contain multiple blobs in contrast to 'gzipindex' table.
    __version__ = '0.4.0'

    def __init__(
        # fmt: off
        self,
        tarFileName                : Optional[str]       = None,
        fileObject                 : Optional[IO[bytes]] = None,
        writeIndex                 : bool                = False,
        clearIndexCache            : bool                = False,
        indexFilePath              : Optional[str]       = None,
        indexFolders               : Optional[List[str]] = None,
        recursive                  : bool                = False,
        gzipSeekPointSpacing       : int                 = 4*1024*1024,
        encoding                   : str                 = tarfile.ENCODING,
        stripRecursiveTarExtension : bool                = False,
        ignoreZeros                : bool                = False,
        verifyModificationTime     : bool                = False,
        parallelization            : int                 = 1,
        printDebug                 : int                 = 0,
        # pylint: disable=unused-argument
        **kwargs
        # fmt: on
    ) -> None:
        """
        tarFileName : Path to the TAR file to be opened. If not specified, a fileObject must be specified.
                      If only a fileObject is given, the created index can't be cached (efficiently).
        fileObject : A io.IOBase derived object. If not specified, tarFileName will be opened.
                     If it is an instance of IndexedBzip2File, IndexedGzipFile, or IndexedZstdFile, then the offset
                     loading and storing from and to the SQLite database is managed automatically by this class.
        writeIndex : If true, then the sidecar index file will be written to a suitable location.
                     Will be ignored if indexFilePath is ':memory:' or if only fileObject is specified
                     but not tarFileName.
        clearIndexCache : If true, then check all possible index file locations for the given tarFileName/fileObject
                          combination and delete them. This also implicitly forces a recreation of the index.
        indexFilePath : Path to the index file for this TAR archive. This takes precedence over the automatically
                        chosen locations. If it is ':memory:', then the SQLite database will be kept in memory
                        and not stored to the file system at any point.
        indexFolders : Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for
                       suitability in the given order. An empty path will be interpreted as the location in which
                       the TAR resides. This overrides the default index fallback folder in ~/.ratarmount.
        recursive : If true, then TAR files inside this archive will be recursively analyzed and added to the SQLite
                    index. Currently, this recursion can only break the outermost compression layer. I.e., a .tar.bz2
                    file inside a tar.bz2 file can not be mounted recursively.
        gzipSeekPointSpacing : This controls the frequency of gzip decoder seek points, see indexed_gzip documentation.
                               Larger spacings lead to less memory usage but increase the constant seek overhead.
        encoding : Will be forwarded to tarfile. Specifies how filenames inside the TAR are encoded.
        ignoreZeros : Will be forwarded to tarfile. Specifies to not only skip zero blocks but also blocks with
                      invalid data. Setting this to true can lead to some problems but is required to correctly
                      read concatenated tars.
        stripRecursiveTarExtension : If true and if recursive is also true, then a <file>.tar inside the current
                                     tar will be mounted at <file>/ instead of <file>.tar/.
        verifyModificationTime : If true, then the index will be recreated automatically if the TAR archive has a more
                                 recent modification time than the index file.
        kwargs : Unused. Only for compatibility with generic MountSource interface.
        """

        # stores which parent folders were last tried to add to database and therefore do exist
        self.parentFolderCache: List[Tuple[str, str]] = []
        self.sqlConnection: Optional[sqlite3.Connection] = None
        self.indexFilePath = None

        # fmt: off
        self.mountRecursively           = recursive
        self.encoding                   = encoding
        self.stripRecursiveTarExtension = stripRecursiveTarExtension
        self.ignoreZeros                = ignoreZeros
        self.verifyModificationTime     = verifyModificationTime
        self.gzipSeekPointSpacing       = gzipSeekPointSpacing
        self.parallelization            = parallelization
        self.printDebug                 = printDebug
        self.isFileObject               = fileObject is not None
        # fmt: on

        # Determine an archive file name to show for debug output
        self.tarFileName: str
        if fileObject:
            self.tarFileName = tarFileName if tarFileName else '<file object>'
        else:
            if tarFileName:
                self.tarFileName = os.path.abspath(tarFileName)
            else:
                raise ValueError("At least one of tarFileName and fileObject arguments should be set!")

        # If no fileObject given, then self.tarFileName is the path to the archive to open.
        if not fileObject:
            fileObject = open(self.tarFileName, 'rb')
        fileObject.seek(0, io.SEEK_END)
        fileSize = fileObject.tell()
        fileObject.seek(0)  # Even if not interested in the file size, seeking to the start might be useful.

        # rawFileObject : Only set when opening a compressed file and only kept to keep the
        #                 compressed file handle from being closed by the garbage collector.
        # tarFileObject : File object to the uncompressed (or decompressed) TAR file to read actual data out of.
        # compression   : Stores what kind of compression the originally specified TAR file uses.
        # isTar         : Can be false for the degenerated case of only a bz2 or gz file not containing a TAR
        self.tarFileObject, self.rawFileObject, self.compression, self.isTar = SQLiteIndexedTar._openCompressedFile(
            fileObject, gzipSeekPointSpacing, encoding, self.parallelization, printDebug=self.printDebug
        )
        if not self.isTar and not self.rawFileObject:
            raise RatarmountError("File object (" + str(fileObject) + ") could not be opened as a TAR file!")

        if self.compression == 'xz':
            try:
                if len(self.tarFileObject.block_boundaries) <= 1 and (fileSize is None or fileSize > 1024 * 1024):
                    print(f"[Warning] The specified file '{self.tarFileName}'")
                    print("[Warning] is compressed using xz but only contains one xz block. This makes it ")
                    print("[Warning] impossible to use true seeking! Please (re)compress your TAR using pixz")
                    print("[Warning] (see https://github.com/vasi/pixz) in order for ratarmount to do be able ")
                    print("[Warning] to do fast seeking to requested files.")
                    print("[Warning] As it is, each file access will decompress the whole TAR from the beginning!")
                    print()
            except Exception:
                pass

        # will be used for storing indexes if current path is read-only
        if self.isFileObject:
            possibleIndexFilePaths = []
            indexPathAsName = None
        else:
            possibleIndexFilePaths = [self.tarFileName + ".index.sqlite"]
            indexPathAsName = self.tarFileName.replace("/", "_") + ".index.sqlite"

        if isinstance(indexFolders, str):
            indexFolders = [indexFolders]

        # A given index file name takes precedence and there should be no implicit fallback
        if indexFilePath:
            if indexFilePath == ':memory:':
                possibleIndexFilePaths = []
            else:
                possibleIndexFilePaths = [os.path.abspath(os.path.expanduser(indexFilePath))]
        elif indexFolders:
            # An empty path is to be interpreted as the default path right besides the TAR
            if '' not in indexFolders:
                possibleIndexFilePaths = []

            if indexPathAsName:
                for folder in indexFolders:
                    if folder:
                        indexPath = os.path.join(folder, indexPathAsName)
                        possibleIndexFilePaths.append(os.path.abspath(os.path.expanduser(indexPath)))
            else:
                writeIndex = False
        elif self.isFileObject:
            writeIndex = False

        if clearIndexCache:
            for indexPath in possibleIndexFilePaths:
                if os.path.isfile(indexPath):
                    os.remove(indexPath)

        # Try to find an already existing index
        for indexPath in possibleIndexFilePaths:
            if self._tryLoadIndex(indexPath):
                self.indexFilePath = indexPath
                break
        if self.indexIsLoaded() and self.sqlConnection:
            try:
                indexVersion = self.sqlConnection.execute(
                    "SELECT major,minor FROM versions WHERE name == 'index';"
                ).fetchone()

                if indexVersion and indexVersion > __version__:
                    print("[Warning] The loaded index was created with a newer version of ratarmount.")
                    print("[Warning] If there are any problems, please update ratarmount or recreate the index")
                    print("[Warning] with this ratarmount version using the --recreate-index option!")
            except Exception:
                pass

            self._loadOrStoreCompressionOffsets()
            self._reloadIndexReadOnly()
            return

        # Find a suitable (writable) location for the index database
        if writeIndex and indexFilePath != ':memory:':
            for indexPath in possibleIndexFilePaths:
                if self._pathIsWritable(indexPath, printDebug=self.printDebug) and self._pathCanBeUsedForSqlite(
                    indexPath, printDebug=self.printDebug
                ):
                    self.indexFilePath = indexPath
                    break

            if not self.indexFilePath:
                raise InvalidIndexError(
                    "Could not find any existing index or writable location for an index in "
                    + str(possibleIndexFilePaths)
                )

        self._createIndex(self.tarFileObject)
        self._loadOrStoreCompressionOffsets()  # store
        if self.sqlConnection:
            self._storeMetadata(self.sqlConnection)
            self._reloadIndexReadOnly()

        if self.printDebug >= 1 and self.indexFilePath and os.path.isfile(self.indexFilePath):
            # The 0-time is legacy for the automated tests
            # fmt: off
            print("Writing out TAR index to", self.indexFilePath, "took 0s",
                  "and is sized", os.stat( self.indexFilePath ).st_size, "B")
            # fmt: on

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.sqlConnection:
            self.sqlConnection.commit()
            self.sqlConnection.close()

        if self.tarFileObject:
            self.tarFileObject.close()

        if self.rawFileObject:
            self.tarFileObject.close()

    def _storeMetadata(self, connection: sqlite3.Connection) -> None:
        self._storeVersionsMetadata(connection, printDebug=self.printDebug)

        metadataTable = """
            /* empty table whose sole existence specifies that we finished iterating the tar */
            CREATE TABLE "metadata" (
                "key"      VARCHAR(65535) NOT NULL, /* e.g. "tarsize" */
                "value"    VARCHAR(65535) NOT NULL  /* e.g. size in bytes as integer */
            );
        """

        connection.executescript(metadataTable)

        # All of these require the generic "metadata" table.
        if not self.isFileObject:
            self._storeTarMetadata(connection, self.tarFileName, printDebug=self.printDebug)
        self._storeArgumentsMetadata(connection)
        connection.commit()

    @staticmethod
    def _storeVersionsMetadata(connection: sqlite3.Connection, printDebug: int = 0) -> None:
        versionsTable = """
            /* This table's sole existence specifies that we finished iterating the tar for older ratarmount versions */
            CREATE TABLE "versions" (
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
        try:
            connection.executescript(versionsTable)
        except Exception as exception:
            if printDebug >= 2:
                print(exception)
            print("[Warning] There was an error when adding metadata information. Index loading might not work.")

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
                makeVersionRow('index', SQLiteIndexedTar.__version__),
            ]

            for _, cinfo in supportedCompressions.items():
                if cinfo.moduleName in globals():
                    module = globals()[cinfo.moduleName]
                    # zipfile has no __version__ attribute and PEP 396 ensuring that was rejected 2021-04-14
                    # in favor of 'version' from importlib.metadata which does not even work with zipfile.
                    # Probably, because zipfile is a built-in module whose version would be the Python version.
                    # https://www.python.org/dev/peps/pep-0396/
                    # The "python-xz" project is imported as an "xz" module, which complicates things because
                    # there is no generic way to get the "python-xz" name from the "xz" runtime module object
                    # and importlib.metadata.version will require "python-xz" as argument.
                    if hasattr(module, '__version__'):
                        versions += [makeVersionRow(cinfo.moduleName, module.__version__)]

            connection.executemany('INSERT OR REPLACE INTO "versions" VALUES (?,?,?,?,?)', versions)
        except Exception as exception:
            print("[Warning] There was an error when adding version information.")
            if printDebug >= 3:
                print(exception)

    @staticmethod
    def _storeTarMetadata(connection: sqlite3.Connection, tarPath: AnyStr, printDebug: int = 0) -> None:
        """Adds some consistency meta information to recognize the need to update the cached TAR index"""
        try:
            tarStats = os.stat(tarPath)
            serializedTarStats = json.dumps(
                {attr: getattr(tarStats, attr) for attr in dir(tarStats) if attr.startswith('st_')}
            )
            connection.execute('INSERT INTO "metadata" VALUES (?,?)', ("tarstats", serializedTarStats))
        except Exception as exception:
            print("[Warning] There was an error when adding file metadata.")
            print("[Warning] Automatic detection of changed TAR files during index loading might not work.")
            if printDebug >= 2:
                print(exception)
            if printDebug >= 3:
                traceback.print_exc()

    def _storeArgumentsMetadata(self, connection: sqlite3.Connection) -> None:
        argumentsToSave = [
            'mountRecursively',
            'gzipSeekPointSpacing',
            'encoding',
            'stripRecursiveTarExtension',
            'ignoreZeros',
        ]

        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})

        try:
            connection.execute('INSERT INTO "metadata" VALUES (?,?)', ("arguments", argumentsMetadata))
        except Exception as exception:
            if self.printDebug >= 2:
                print(exception)
            print("[Warning] There was an error when adding argument metadata.")
            print("[Warning] Automatic detection of changed arguments files during index loading might not work.")

    @staticmethod
    def _pathIsWritable(path: AnyStr, printDebug: int = 0) -> bool:
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            with open(path, 'wb') as file:
                file.write(b'\0' * 1024 * 1024)
            os.remove(path)

            return True

        except PermissionError:
            if printDebug >= 2:
                traceback.print_exc()
                print("Could not create file:", path)

        except IOError:
            if printDebug >= 2:
                traceback.print_exc()
                print("Could not create file:", path)

        return False

    @staticmethod
    def _pathCanBeUsedForSqlite(path: AnyStr, printDebug: int = 0) -> bool:
        fileExisted = os.path.isfile(path)
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            connection = SQLiteIndexedTar._openSqlDb(path)
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
                SQLiteIndexedTar._uncheckedRemove(path)

        return False

    @staticmethod
    def _openSqlDb(path: AnyStr, **kwargs) -> sqlite3.Connection:
        sqlConnection = sqlite3.connect(path, **kwargs)
        sqlConnection.row_factory = sqlite3.Row
        sqlConnection.executescript(
            # Looking mode exclusive leads to a measurable speedup. E.g., find on 2k recursive files tar
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
    def _initializeSqlDb(indexFilePath: Optional[str], printDebug: int = 0) -> sqlite3.Connection:
        if printDebug >= 1:
            print("Creating new SQLite index database at", indexFilePath if indexFilePath else ':memory:')

        createTables = """
            CREATE TABLE "files" (
                "path"          VARCHAR(65535) NOT NULL,  /* path with leading and without trailing slash */
                "name"          VARCHAR(65535) NOT NULL,
                "offsetheader"  INTEGER,  /* seek offset from TAR file where the TAR metadata for this file resides */
                "offset"        INTEGER,  /* seek offset from TAR file where these file's contents resides */
                "size"          INTEGER,
                "mtime"         INTEGER,
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
            );
            /* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
             * Therefore, it will not be sorted and inserting will be faster! */
            CREATE TABLE "filestmp" AS SELECT * FROM "files" WHERE 0;
            CREATE TABLE "parentfolders" (
                "path"          VARCHAR(65535) NOT NULL,
                "name"          VARCHAR(65535) NOT NULL,
                "offsetheader"  INTEGER,
                "offset"        INTEGER,
                PRIMARY KEY (path,name)
                UNIQUE (path,name)
            );
        """

        sqlConnection = SQLiteIndexedTar._openSqlDb(indexFilePath if indexFilePath else ':memory:')
        tables = sqlConnection.execute('SELECT name FROM sqlite_master WHERE type = "table";')
        if {"files", "filestmp", "parentfolders"}.intersection({t[0] for t in tables}):
            raise InvalidIndexError(
                f"The index file {indexFilePath} already seems to contain a table. Please specify --recreate-index."
            )
        sqlConnection.executescript(createTables)
        return sqlConnection

    def _reloadIndexReadOnly(self):
        if not self.indexFilePath or self.indexFilePath == ':memory:' or not self.sqlConnection:
            return

        self.sqlConnection.commit()
        self.sqlConnection.close()

        uriPath = urllib.parse.quote(self.indexFilePath)
        self.sqlConnection = SQLiteIndexedTar._openSqlDb(f"file:{uriPath}?mode=ro", uri=True)

    @staticmethod
    def _tarInfoFullMode(tarInfo: tarfile.TarInfo) -> int:
        """
        Returns the full mode for a TarInfo object. Note that TarInfo.mode only contains the permission bits
        and not other bits like set for directory, symbolic links, and other special files.
        """
        return (
            tarInfo.mode
            # fmt: off
            | ( stat.S_IFDIR if tarInfo.isdir () else 0 )
            | ( stat.S_IFREG if tarInfo.isfile() else 0 )
            | ( stat.S_IFLNK if tarInfo.issym () else 0 )
            | ( stat.S_IFCHR if tarInfo.ischr () else 0 )
            | ( stat.S_IFIFO if tarInfo.isfifo() else 0 )
            # fmt: on
        )

    def _updateProgressBar(self, progressBar, fileobj: Any) -> None:
        try:
            if hasattr(fileobj, 'tell_compressed') and self.compression == 'bz2':
                # Note that because bz2 works on a bitstream the tell_compressed returns the offset in bits
                progressBar.update(fileobj.tell_compressed() // 8)
            elif hasattr(fileobj, 'tell_compressed'):
                progressBar.update(fileobj.tell_compressed())
            elif hasattr(fileobj, 'fileobj'):
                progressBar.update(fileobj.fileobj().tell())
            elif self.rawFileObject and hasattr(self.rawFileObject, 'tell'):
                progressBar.update(self.rawFileObject.tell())
            else:
                progressBar.update(fileobj.tell())
        except Exception:
            pass

    def _createIndex(
        self,
        # fmt: off
        fileObject  : Any,
        progressBar : Any = None,
        pathPrefix  : str = '',
        streamOffset: int = 0
        # fmt: on
    ) -> None:
        if self.printDebug >= 1:
            print("Creating offset dictionary for", self.tarFileName, "...")
        t0 = timer()

        # 1. If no SQL connection was given (by recursive call), open a new database file
        openedConnection = False
        if not self.indexIsLoaded() or not self.sqlConnection:
            openedConnection = True
            self.sqlConnection = self._initializeSqlDb(self.indexFilePath, printDebug=self.printDebug)

        # 2. Open TAR file reader
        loadedTarFile: Any = []  # Feign an empty TAR file if anything goes wrong
        if self.isTar:
            try:
                # r: uses seeks to skip to the next file inside the TAR while r| doesn't do any seeks.
                # r| might be slower but for compressed files we have to go over all the data once anyways.
                # Note that with ignore_zeros = True, no invalid header issues or similar will be raised even for
                # non TAR files!?
                loadedTarFile = tarfile.open(
                    # fmt:off
                    fileobj      = fileObject,
                    mode         = 'r|' if self.compression else 'r:',
                    ignore_zeros = self.ignoreZeros,
                    encoding     = self.encoding,
                    # fmt:on
                )
            except tarfile.ReadError:
                pass

        if progressBar is None:
            try:
                progressBar = ProgressBar(os.fstat(fileObject.fileno()).st_size)
            except io.UnsupportedOperation:
                pass

        # 3. Iterate over files inside TAR and add them to the database
        try:
            filesToMountRecursively = []

            for tarInfo in loadedTarFile:
                loadedTarFile.members = []  # Clear this in order to limit memory usage by tarfile
                self._updateProgressBar(progressBar, fileObject)

                # Add a leading '/' as a convention where '/' represents the TAR root folder
                # Partly, done because fusepy specifies paths in a mounted directory like this
                # os.normpath does not delete duplicate '/' at beginning of string!
                # tarInfo.name might be identical to "." or begin with "./", which is bad!
                # os.path.normpath can remove suffixed folder/./ path specifications but it can't remove
                # a leading dot.
                # TODO: Would be a nice function / line of code to test because it is very finicky.
                #       And some cases are only triggered for recursive mounts, i.e., for non-empty pathPrefix.
                fullPath = "/" + os.path.normpath(pathPrefix + "/" + tarInfo.name).lstrip('/')

                # TODO: As for the tarfile type SQLite expects int but it is generally bytes.
                #       Most of them would be convertible to int like tarfile.SYMTYPE which is b'2',
                #       but others should throw errors, like GNUTYPE_SPARSE which is b'S'.
                #       When looking at the generated index, those values get silently converted to 0?
                path, name = fullPath.rsplit("/", 1)
                # fmt: off
                fileInfo = (
                    path                              ,  # 0
                    name                              ,  # 1
                    streamOffset + tarInfo.offset     ,  # 2
                    streamOffset + tarInfo.offset_data,  # 3
                    tarInfo.size                      ,  # 4
                    tarInfo.mtime                     ,  # 5
                    self._tarInfoFullMode(tarInfo)    ,  # 6
                    tarInfo.type                      ,  # 7
                    tarInfo.linkname                  ,  # 8
                    tarInfo.uid                       ,  # 9
                    tarInfo.gid                       ,  # 10
                    False                             ,  # 11 (isTar)
                    tarInfo.issparse()                ,  # 12
                )
                # fmt: on

                if self.mountRecursively and tarInfo.isfile() and tarInfo.name.lower().endswith('.tar'):
                    filesToMountRecursively.append(fileInfo)
                else:
                    self._setFileInfo(fileInfo)
        except tarfile.ReadError as e:
            if 'unexpected end of data' in str(e):
                print(
                    "[Warning] The TAR file is incomplete. Ratarmount will work but some files might be cut off. "
                    "If the TAR file size changes, ratarmount will recreate the index during the next mounting."
                )

        # 4. Open contained TARs for recursive mounting
        oldPos = fileObject.tell()
        oldPrintName = self.tarFileName
        for fileInfo in filesToMountRecursively:
            # Strip file extension for mount point if so configured
            modifiedName = fileInfo[1]
            tarExtension = '.tar'
            if (
                self.stripRecursiveTarExtension
                and len(tarExtension) > 0
                and modifiedName.lower().endswith(tarExtension.lower())
            ):
                modifiedName = modifiedName[: -len(tarExtension)]

            # Temporarily change tarFileName for the info output of the recursive call
            self.tarFileName = os.path.join(fileInfo[0], fileInfo[1])

            # StenciledFile's tell returns the offset inside the file chunk instead of the global one,
            # so we have to always communicate the offset of this chunk to the recursive call no matter
            # whether tarfile has streaming access or seeking access!
            globalOffset = fileInfo[3]
            size = fileInfo[4]
            tarFileObject = StenciledFile(fileObject, [(globalOffset, size)])

            isTar = False
            try:
                # Do not use os.path.join here because the leading / might be missing.
                # This should instead be seen as the reverse operation of the rsplit further above.
                self._createIndex(tarFileObject, progressBar, "/".join([fileInfo[0], modifiedName]), globalOffset)
                isTar = True
            except tarfile.ReadError:
                pass
            finally:
                del tarFileObject

            if isTar:
                modifiedFileInfo = list(fileInfo)

                # if the TAR file contents could be read, we need to adjust the actual
                # TAR file's metadata to be a directory instead of a file
                mode = modifiedFileInfo[6]
                mode = (
                    (mode & 0o777)
                    | stat.S_IFDIR
                    | (stat.S_IXUSR if mode & stat.S_IRUSR != 0 else 0)
                    | (stat.S_IXGRP if mode & stat.S_IRGRP != 0 else 0)
                    | (stat.S_IXOTH if mode & stat.S_IROTH != 0 else 0)
                )

                modifiedFileInfo[0] = fileInfo[0]
                modifiedFileInfo[1] = modifiedName
                modifiedFileInfo[6] = mode
                modifiedFileInfo[11] = isTar

                self._setFileInfo(tuple(modifiedFileInfo))
            else:
                self._setFileInfo(fileInfo)

        fileObject.seek(oldPos)
        self.tarFileName = oldPrintName

        # Everything below should not be done in a recursive call of createIndex
        if streamOffset > 0:
            t1 = timer()
            if self.printDebug >= 1:
                print(f"Creating offset dictionary for {self.tarFileName} took {t1 - t0:.2f}s")
            return

        # If no file is in the TAR, then it most likely indicates a possibly compressed non TAR file.
        # In that case add that itself to the file index. This won't work when called recursively,
        # so check stream offset.
        fileCount = self.sqlConnection.execute('SELECT COUNT(*) FROM "files";').fetchone()[0]
        if fileCount == 0:
            if self.printDebug >= 3:
                print(f"Did not find any file in the given TAR: {self.tarFileName}. Assuming a compressed file.")

            try:
                tarInfo = os.fstat(fileObject.fileno())
            except io.UnsupportedOperation:
                # If fileObject doesn't have a fileno, we set tarInfo to None
                # and set the relevant statistics (such as st_mtime) to sensible defaults.
                tarInfo = None
            fname = os.path.basename(self.tarFileName)
            for suffix in ['.gz', '.bz2', '.bzip2', '.gzip', '.xz', '.zst', '.zstd']:
                if fname.lower().endswith(suffix) and len(fname) > len(suffix):
                    fname = fname[: -len(suffix)]
                    break

            # If the file object is actually an IndexedBzip2File or such, we can't directly use the file size
            # from os.stat and instead have to gather it from seek. Unfortunately, indexed_gzip does not support
            # io.SEEK_END even though it could as it has the index ...
            while fileObject.read(1024 * 1024):
                self._updateProgressBar(progressBar, fileObject)
            fileSize = fileObject.tell()

            mode = 0o777 | stat.S_IFREG  # default mode

            # fmt: off
            fileInfo = (
                ""                                   ,  # 0 path
                fname                                ,  # 1
                None                                 ,  # 2 header offset
                0                                    ,  # 3 data offset
                fileSize                             ,  # 4
                tarInfo.st_mtime if tarInfo else 0   ,  # 5
                tarInfo.st_mode if tarInfo else mode ,  # 6
                None                                 ,  # 7 TAR file type. Currently unused. Overlaps with mode
                None                                 ,  # 8 linkname
                tarInfo.st_uid if tarInfo else 0     ,  # 9
                tarInfo.st_gid if tarInfo else 0     ,  # 10
                False              ,  # 11 isTar
                False              ,  # 12 isSparse, don't care if it is actually sparse or not because it is not in TAR
            )
            # fmt: on
            self._setFileInfo(fileInfo)

        # All the code below is for database finalizing which should not be done in a recursive call of createIndex!
        if not openedConnection:
            return

        # 5. Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if self.printDebug >= 2:
            print("Resorting files by path ...")

        try:
            queriedLibSqliteVersion = sqlite3.connect(":memory:").execute("select sqlite_version();").fetchone()
            libSqliteVersion = tuple(int(x) for x in queriedLibSqliteVersion[0].split('.'))
        except Exception:
            libSqliteVersion = (0, 0, 0)

        searchByTuple = """(path,name) NOT IN ( SELECT path,name"""
        searchByConcat = """path || "/" || name NOT IN ( SELECT path || "/" || name"""

        cleanupDatabase = f"""
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
        self.sqlConnection.executescript(cleanupDatabase)

        self.sqlConnection.commit()

        t1 = timer()
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.tarFileName} took {t1 - t0:.2f}s")

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

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        fileInfo = self._getFileInfo(path, fileVersion=fileVersion)

        if fileInfo is None:
            return None

        assert isinstance(fileInfo, FileInfo)
        return fileInfo

    def _getFileInfo(
        self,
        # fmt: off
        fullPath     : str,
        listDir      : bool = False,
        listVersions : bool = False,
        fileVersion  : int  = 0
        # fmt: on
    ) -> Optional[Union[FileInfo, Dict[str, FileInfo]]]:
        """
        This is the heart of this class' public interface!

        path    : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        listDir : if True, return a dictionary for the given directory path: { fileName : FileInfo, ... }
                  if False, return simple FileInfo to given path (directory or file)
        fileVersion : If the TAR contains the same file path multiple times, by default only the last one is shown.
                      But with this argument other versions can be queried. Version 1 is the oldest one.
                      Version 0 translates to the most recent one for compatibility with tar --occurrence=<number>.
                      Version -1 translates to the second most recent, and so on.
                      For listDir=True, the file version makes no sense and is ignored!
                      So, even if a folder was overwritten by a file, which is already not well supported by tar,
                      then listDir for that path will still list all contents of the overwritten folder or folders,
                      no matter the specified version. The file system layer has to take care that a directory
                      listing is not even requeted in the first place if it is not a directory.
                      FUSE already does this by calling getattr for all parent folders in the specified path first.

        If path does not exist, always return None

        If listVersions is true, then return metadata for all versions of a file possibly appearing more than once
        in the TAR as a directory dictionary. listDir will then be ignored!
        """
        # TODO cache last listDir as most often a stat over all entries will soon follow

        if not isinstance(fileVersion, int):
            raise TypeError("The specified file version must be an integer!")
        if not self.sqlConnection:
            raise IndexNotOpenError("This method can not be called without an opened index database!")

        # also strips trailing '/' except for a single '/' and leading '/'
        fullPath = '/' + os.path.normpath(fullPath).lstrip('/')

        if listVersions:
            path, name = fullPath.rsplit('/', 1)
            rows = self.sqlConnection.execute(
                'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?) ORDER BY "offsetheader" ASC', (path, name)
            )
            result = {str(version + 1): self._rowToFileInfo(row) for version, row in enumerate(rows)}
            return result

        if listDir:
            # For listing directory entries the file version can't be applied meaningfully at this abstraction layer.
            # E.g., should it affect the file version of the directory to list, or should it work on the listed files
            # instead and if so how exactly if there aren't the same versions for all files available, ...?
            # Or, are folders assumed to be overwritten by a new folder entry in a TAR or should they be union mounted?
            # If they should be union mounted, like is the case now, then the folder version only makes sense for
            # its attributes.
            rows = self.sqlConnection.execute('SELECT * FROM "files" WHERE "path" == (?)', (fullPath.rstrip('/'),))
            directory = {}
            gotResults = False
            for row in rows:
                gotResults = True
                if row['name']:
                    directory[row['name']] = self._rowToFileInfo(row)
            return directory if gotResults else None

        path, name = fullPath.rsplit('/', 1)
        row = self.sqlConnection.execute(
            f"""
            SELECT * FROM "files"
            WHERE "path" == (?) AND "name" == (?)
            ORDER BY "offsetheader" {'DESC' if fileVersion is None or fileVersion <= 0 else 'ASC'}
            LIMIT 1 OFFSET (?);
            """,
            (path, name, 0 if fileVersion is None else fileVersion - 1 if fileVersion > 0 else -fileVersion),
        ).fetchone()
        return self._rowToFileInfo(row) if row else None

    def isDir(self, path: str) -> bool:
        """Return true if path exists and is a folder."""
        return self.listDir(path) is not None

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        """
        Usability wrapper for getFileInfo(listDir=True) with FileInfo stripped if you are sure you don't need it.
        """
        result = self._getFileInfo(path, listDir=True)
        if isinstance(result, dict):
            return result.keys()
        return None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        """
        Usability wrapper for getFileInfo(listVersions=True) with FileInfo stripped if you are sure you don't need it.
        """
        fileVersions = self._getFileInfo(path, listVersions=True)
        return len(fileVersions) if isinstance(fileVersions, dict) else 0

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        assert fileInfo.userdata
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)

        # This is not strictly necessary but it saves two file object layers and therefore might be more performant.
        # Furthermore, non-sparse files should be the much more likely case anyway.
        if not tarFileInfo.issparse:
            return cast(IO[bytes], StenciledFile(self.tarFileObject, [(tarFileInfo.offset, fileInfo.size)]))

        # The TAR file format is very simple. It's just a concatenation of TAR blocks. There is not even a
        # global header, only the TAR block headers. That's why we can simply cut out the TAR block for
        # the sparse file using StenciledFile and then use tarfile on it to expand the sparse file correctly.
        tarBlockSize = tarFileInfo.offset - tarFileInfo.offsetheader + fileInfo.size

        tarSubFile = StenciledFile(self.tarFileObject, [(tarFileInfo.offsetheader, tarBlockSize)])
        # TODO It might be better to somehow call close on tarFile but the question is where and how.
        #      It would have to be appended to the __exit__ method of fileObject like if being decorated.
        #      For now this seems to work either because fileObject does not require tarFile to exist
        #      or because tarFile is simply not closed correctly here, I'm not sure.
        #      Sparse files are kinda edge-cases anyway, so it isn't high priority as long as the tests work.
        tarFile = tarfile.open(fileobj=cast(IO[bytes], tarSubFile), mode='r:', encoding=self.encoding)
        fileObject = tarFile.extractfile(next(iter(tarFile)))
        if not fileObject:
            raise CompressionError("tarfile.extractfile returned nothing!")

        return fileObject

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        assert fileInfo.userdata
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)

        if tarFileInfo.issparse:
            with self.open(fileInfo) as file:
                file.seek(offset, os.SEEK_SET)
                return file.read(size)

        # For non-sparse files, we can simply seek to the offset and read from it.
        self.tarFileObject.seek(tarFileInfo.offset + offset, os.SEEK_SET)
        return self.tarFileObject.read(size)

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

        if not self.sqlConnection:
            raise IndexNotOpenError("This method can not be called without an opened index database!")

        # TODO This method is still not perfect but I do not know how to perfect it without loosing significant
        #      performance. Currently, adding implicit folders will fail when a file is overwritten implicitly with
        #      a folder and then overwritten by a file and then again overwritten by a folder. Because the parent
        #      folderwas already added implicitly the first time, the second time will be skipped.
        #      To solve this, I would have to add all parent folders for all files, which might easily explode
        #      the temporary database and the indexing performance by the folder depth.
        #      Also, I do not want to add versions for a parent folder for each implicitly added parent folder for
        #      each file, so I would have to sort out those in a post-processing step. E.g., sort by offsetheader
        #      and then clean out successive implicitly added folders as long as there is no file of the same name
        #      inbetween.
        #      The unmentioned alternative would be to lookup paths with LIKE but that is just madness because it
        #      will have a worse complexity of O(N) insteda of O(log(N)).
        self.sqlConnection.executemany(
            'INSERT OR IGNORE INTO "parentfolders" VALUES (?,?,?,?)',
            [(p[0], p[1], offsetheader, offset) for p in paths],
        )

    def _setFileInfo(self, row: tuple) -> None:
        if not self.sqlConnection:
            raise IndexNotOpenError("This method can not be called without an opened index database!")

        try:
            self.sqlConnection.execute('INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', row)
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
                    try:
                        x.encode()
                        checkedRow += [x]
                    except UnicodeEncodeError:
                        # fmt: off
                        checkedRow += [
                            x.encode( self.encoding, 'surrogateescape' )
                             .decode( self.encoding, 'backslashreplace' )
                         ]
                        # fmt: on
                else:
                    checkedRow += [x]

            self.sqlConnection.execute(
                'INSERT OR REPLACE INTO "files" VALUES (' + ','.join('?' * len(row)) + ');', tuple(checkedRow)
            )
            print("[Warning] The escaped inserted row is now:", row)
            print()

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
        """Loads the given index SQLite database and checks it for validity."""
        if self.indexIsLoaded():
            return

        t0 = time.time()
        self.sqlConnection = self._openSqlDb(indexFilePath)
        tables = [x[0] for x in self.sqlConnection.execute('SELECT name FROM sqlite_master WHERE type="table"')]
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

                if 'tarstats' in metadata:
                    values = json.loads(metadata['tarstats'])
                    tarStats = os.stat(self.tarFileName)

                    # fmt: off
                    if (
                        hasattr( tarStats, "st_size" )
                        and 'st_size' in values
                        and tarStats.st_size != values['st_size']
                    ):
                        raise InvalidIndexError( "TAR file for this SQLite index has changed size from",
                                                 values['st_size'], "to", tarStats.st_size)
                    # fmt: on

                    if (
                        self.verifyModificationTime
                        and hasattr(tarStats, "st_mtime")
                        and 'st_mtime' in values
                        and tarStats.st_mtime != values['st_mtime']
                    ):
                        raise InvalidIndexError(
                            "The modification date for the TAR file",
                            values['st_mtime'],
                            "to this SQLite index has changed (" + str(tarStats.st_mtime) + ")",
                        )

                # Check arguments used to create the found index. These are only warnings and not forcing a rebuild
                # by default.
                # TODO: Add --force options?
                if 'arguments' in metadata:
                    indexArgs = json.loads(metadata['arguments'])
                    argumentsToCheck = [
                        'mountRecursively',
                        'gzipSeekPointSpacing',
                        'encoding',
                        'stripRecursiveTarExtension',
                        'ignoreZeros',
                    ]
                    differingArgs = []
                    for arg in argumentsToCheck:
                        if arg in indexArgs and hasattr(self, arg) and indexArgs[arg] != getattr(self, arg):
                            differingArgs.append((arg, indexArgs[arg], getattr(self, arg)))
                    if differingArgs:
                        print("[Warning] The arguments used for creating the found index differ from the arguments ")
                        print("[Warning] given for mounting the archive now. In order to apply these changes, ")
                        print("[Warning] recreate the index using the --recreate-index option!")
                        for arg, oldState, newState in differingArgs:
                            print(f"[Warning] {arg}: index: {oldState}, current: {newState}")

        except Exception as e:
            # indexIsLoaded checks self.sqlConnection, so close it before returning because it was found to be faulty
            try:
                self.sqlConnection.close()
            except sqlite3.Error:
                pass
            self.sqlConnection = None

            raise e

        if self.printDebug >= 1:
            # Legacy output for automated tests
            print(f"Loading offset dictionary from {str(indexFilePath)} took {time.time() - t0:.2f}s")

    def _tryLoadIndex(self, indexFilePath: AnyStr) -> bool:
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile(indexFilePath):
            return False

        try:
            self.loadIndex(indexFilePath)
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
            print("[Info]       - an internal error occured while writing the index")
            print("[Info]       - the disk filled up while writing the index")
            print("[Info]     - Rare lowlevel corruptions caused by hardware failure")

            print("[Info] This might force a time-costly index recreation, so if it happens often")
            print("       and mounting is slow, try to find out why loading fails repeatedly,")
            print("       e.g., by opening an issue on the public github page.")

            try:
                os.remove(indexFilePath)
            except OSError:
                print("[Warning] Failed to remove corrupted old cached index file:", indexFilePath)

        if self.printDebug >= 3 and self.indexIsLoaded():
            print("Loaded index", indexFilePath)

        return self.indexIsLoaded()

    @staticmethod
    def _detectCompression(fileobj: IO[bytes], printDebug: int = 0) -> Optional[str]:
        if not isinstance(fileobj, io.IOBase) or not fileobj.seekable():
            return None

        oldOffset = fileobj.tell()
        for compressionId, compression in supportedCompressions.items():
            # The header check is a necessary condition not a sufficient condition.
            # Especially for gzip, which only has 2 magic bytes, false positives might happen.
            # Therefore, only use the magic bytes based check if the module could not be found
            # in order to still be able to print pinpoint error messages.
            matches = compression.checkHeader(fileobj)
            fileobj.seek(oldOffset)
            if not matches:
                continue

            if compression.moduleName not in sys.modules and matches:
                return compressionId

            try:
                compressedFileobj = compression.open(fileobj)
                # Reading 1B from a single-frame zst file might require decompressing it fully in order
                # to get uncompressed file size! Avoid that. The magic bytes should suffice mostly.
                # TODO: Make indexed_zstd not require the uncompressed size for the read call.
                if compressionId != 'zst':
                    compressedFileobj.read(1)
                compressedFileobj.close()
                fileobj.seek(oldOffset)
                return compressionId
            except Exception as e:
                if printDebug >= 2:
                    print(f"[Warning] A given file with magic bytes for {compressionId} could not be opened because:")
                    print(e)
                fileobj.seek(oldOffset)

        return None

    @staticmethod
    def _detectTar(fileobj: IO[bytes], encoding: str, printDebug: int = 0) -> bool:
        if not isinstance(fileobj, io.IOBase) or not fileobj.seekable():
            return False

        oldOffset = fileobj.tell()
        isTar = False
        try:
            with tarfile.open(fileobj=fileobj, mode='r:', encoding=encoding):
                isTar = True
        except (tarfile.ReadError, tarfile.CompressionError):
            if printDebug >= 3:
                print("[Info] File object", fileobj, "is not a TAR.")

        fileobj.seek(oldOffset)
        return isTar

    @staticmethod
    def _openCompressedFile(
        fileobj: IO[bytes], gzipSeekPointSpacing: int, encoding: str, parallelization: int, printDebug: int = 0
    ) -> Any:
        """
        Opens a file possibly undoing the compression.
        Returns (tar_file_obj, raw_file_obj, compression, isTar).
        raw_file_obj will be none if compression is None.
        """
        compression = SQLiteIndexedTar._detectCompression(fileobj, printDebug=printDebug)
        if printDebug >= 3:
            print(f"[Info] Detected compression {compression} for file object:", fileobj)

        if compression not in supportedCompressions:
            return fileobj, None, compression, SQLiteIndexedTar._detectTar(fileobj, encoding, printDebug=printDebug)

        cinfo = supportedCompressions[compression]
        if cinfo.moduleName not in sys.modules:
            raise CompressionError(
                f"Can't open a {compression} compressed file '{fileobj.name}' without {cinfo.moduleName} module!"
            )

        if compression == 'gz':
            # drop_handles keeps a file handle opening as is required to call tell() during decoding
            tar_file = indexed_gzip.IndexedGzipFile(fileobj=fileobj, drop_handles=False, spacing=gzipSeekPointSpacing)
        elif compression == 'bz2':
            tar_file = indexed_bzip2.open(fileobj, parallelization=parallelization)
        else:
            tar_file = cinfo.open(fileobj)

        return tar_file, fileobj, compression, SQLiteIndexedTar._detectTar(tar_file, encoding, printDebug=printDebug)

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

    def _loadOrStoreCompressionOffsets(self):
        if not self.indexFilePath or self.indexFilePath == ':memory:':
            if self.printDebug >= 2:
                print("[Info] Will skip storing compression seek data because the database is in memory.")
                print("[Info] If the database is in memory, then this data will not be read anyway.")
            return

        # This should be called after the TAR file index is complete (loaded or created).
        # If the TAR file index was created, then tarfile has iterated over the whole file once
        # and therefore completed the implicit compression offset creation.
        if not self.sqlConnection:
            raise IndexNotOpenError("This method can not be called without an opened index database!")
        db = self.sqlConnection
        fileObject = self.tarFileObject

        if (
            hasattr(fileObject, 'set_block_offsets')
            and hasattr(fileObject, 'block_offsets')
            and self.compression in ['bz2', 'zst']
        ):
            if self.compression == 'bz2':
                table_name = 'bzip2blocks'
            elif self.compression == 'zst':
                table_name = 'zstdblocks'

            try:
                offsets = dict(db.execute(f"SELECT blockoffset,dataoffset FROM {table_name};"))
                fileObject.set_block_offsets(offsets)
            except Exception as exception:
                if self.printDebug >= 2:
                    print(f"[Info] Could not load {self.compression} block offset data. Will create it from scratch.")
                    print(exception)
                if self.printDebug >= 3:
                    traceback.print_exc()

                tables = [x[0] for x in db.execute('SELECT name FROM sqlite_master WHERE type="table";')]
                if table_name in tables:
                    db.execute(f"DROP TABLE {table_name}")
                db.execute(f"CREATE TABLE {table_name} ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )")
                db.executemany(f"INSERT INTO {table_name} VALUES (?,?)", fileObject.block_offsets().items())
                db.commit()
            return

        if (
            # fmt: off
            hasattr( fileObject, 'import_index' )
            and hasattr( fileObject, 'export_index' )
            and self.compression == 'gz'
            # fmt: on
        ):
            tables = [x[0] for x in db.execute('SELECT name FROM sqlite_master WHERE type="table"')]

            # indexed_gzip index only has a file based API, so we need to write all the index data from the SQL
            # database out into a temporary file. For that, let's first try to use the same location as the SQLite
            # database because it should have sufficient writing rights and free disk space.
            gzindex = None
            for tmpDir in [os.path.dirname(self.indexFilePath), None]:
                if 'gzipindex' not in tables and 'gzipindexes' not in tables:
                    break

                # Try to export data from SQLite database. Note that no error checking against the existence of
                # gzipindex table is done because the exported data itself might also be wrong and we can't check
                # against this. Therefore, collate all error checking by catching exceptions.

                try:
                    gzindex = tempfile.mkstemp(dir=tmpDir)[1]
                    with open(gzindex, 'wb') as file:
                        if 'gzipindexes' in tables:
                            # Try to read index files containing very large gzip indexes
                            rows = db.execute('SELECT data FROM gzipindexes ORDER BY ROWID')
                            for row in rows:
                                file.write(row[0])
                        elif 'gzipindex' in tables:
                            # Try to read legacy index files with exactly one blob.
                            # This is how old ratarmount version read it. I.e., if there were simply more than one
                            # blob in the same tbale, then it would ignore all but the first(?!) and I am not sure
                            # what would happen in that case.
                            # So, use a differently named table if there are multiple blobs.
                            file.write(db.execute('SELECT data FROM gzipindex').fetchone()[0])
                    break
                except Exception:
                    self._uncheckedRemove(gzindex)
                    gzindex = None

            if gzindex:
                try:
                    fileObject.import_index(filename=gzindex)
                    return
                except Exception:
                    pass
                finally:
                    self._uncheckedRemove(gzindex)

            # Store the offsets into a temporary file and then into the SQLite database
            if self.printDebug >= 2:
                print("[Info] Could not load GZip Block offset data. Will create it from scratch.")

            # Transparently force index to be built if not already done so. build_full_index was buggy for me.
            # Seeking from end not supported, so we have to read the whole data in in a loop
            while fileObject.read(1024 * 1024):
                pass

            # The created index can unfortunately be pretty large and tmp might actually run out of memory!
            # Therefore, try different paths, starting with the location where the index resides.
            gzindex = None
            for tmpDir in [os.path.dirname(self.indexFilePath), None]:
                gzindex = tempfile.mkstemp(dir=tmpDir)[1]
                try:
                    fileObject.export_index(filename=gzindex)
                    break
                except indexed_gzip.ZranError:
                    self._uncheckedRemove(gzindex)
                    gzindex = None

            if not gzindex or not os.path.isfile(gzindex):
                print("[Warning] The GZip index required for seeking could not be stored in a temporary file!")
                print("[Info] This might happen when you are out of space in your temporary file and at the")
                print("[Info] the index file location. The gzipindex size takes roughly 32kiB per 4MiB of")
                print("[Info] uncompressed(!) bytes (0.8% of the uncompressed data) by default.")
                raise RuntimeError("Could not initialize the GZip seek cache.")
            if self.printDebug >= 2:
                print("Exported GZip index size:", os.stat(gzindex).st_size)

            # Clean up unreadable older data.
            if 'gzipindex' in tables:
                db.execute('DROP TABLE gzipindex')
            if 'gzipindexes' in tables:
                db.execute('DROP TABLE gzipindexes')

            # The maximum blob size configured by SQLite is exactly 1 GB, see https://www.sqlite.org/limits.html
            # Therefore, this should be smaller. Another argument for making it smaller is that this blob size
            # will be held fully in memory temporarily.
            # But, making it too small would result in too many non-backwards compatible indexes being created.
            maxBlobSize = 256 * 1024 * 1024  # 128 MiB

            # Store contents of temporary file into the SQLite database
            if os.stat(gzindex).st_size > maxBlobSize:
                db.execute('CREATE TABLE gzipindexes ( data BLOB )')
                with open(gzindex, 'rb') as file:
                    while True:
                        data = file.read(maxBlobSize)
                        if not data:
                            break

                        # I'm pretty sure that the rowid can be used to query the rows with the insertion order:
                        # https://www.sqlite.org/autoinc.html
                        # > The usual algorithm is to give the newly created row a ROWID that is one larger than the
                        #   largest ROWID in the table prior to the insert.
                        # The "usual" makes me worry a bit, but I think it is in reference to the AUTOINCREMENT feature.
                        db.execute('INSERT INTO gzipindexes VALUES (?)', (data,))
            else:
                db.execute('CREATE TABLE gzipindex ( data BLOB )')
                with open(gzindex, 'rb') as file:
                    db.execute('INSERT INTO gzipindex VALUES (?)', (file.read(),))

            db.commit()
            os.remove(gzindex)
            return

        # Note that for xz seeking, loading and storing block indexes is unnecessary because it has an index included!
        if self.compression in [None, 'xz']:
            return

        assert False, (
            f"Could not load or store block offsets for {self.compression} "
            "probably because adding support was forgotten!"
        )
