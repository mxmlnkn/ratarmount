#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import bisect
import collections
import datetime
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
from abc import ABC, abstractmethod
from timeit import default_timer as timer
import typing
from typing import Any, AnyStr, cast, Dict, IO, Iterable, List, Optional, Set, Tuple, Union
from dataclasses import dataclass
import dataclasses

# Can't do this dynamically with importlib.import_module and using supportedCompressions
# because then the static checkers like mypy and pylint won't recognize the modules!
try:
    import fuse
except ImportError:
    pass
try:
    import indexed_bzip2
except ImportError:
    pass
try:
    import indexed_gzip
except ImportError:
    pass
try:
    import indexed_zstd
except ImportError:
    pass
try:
    import lzmaffi
except ImportError:
    pass
try:
    import xz
except ImportError:
    pass
try:
    import rarfile
except ImportError:
    pass


# The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
# Therefore disable ZIP support there!
# I don't see it documented, instead I tested different Python versions with Docker.
if sys.version_info[2] > 6:
    import zipfile


__version__ = '0.9.3'


parallelization = 1


def hasNonEmptySupport() -> bool:
    try:
        with os.popen('fusermount -V') as pipe:
            match = re.search(r'([0-9]+)[.][0-9]+[.][0-9]+', pipe.read())
            if match:
                return int(match.group(1)) < 3
    except Exception:
        pass

    return False  # On macOS, fusermount does nto exist and macfuse also seems to complain with nonempty option.


# Defining lambdas does not yet check the names of entities used inside the lambda!
CompressionInfo = collections.namedtuple(
    'CompressionInfo', ['suffixes', 'doubleSuffixes', 'moduleName', 'checkHeader', 'open']
)
supportedCompressions = {
    'bz2': CompressionInfo(
        ['bz2', 'bzip2'],
        ['tb2', 'tbz', 'tbz2', 'tz2'],
        'indexed_bzip2',
        lambda x: (x.read(4)[:3] == b'BZh' and x.read(6) == (0x314159265359).to_bytes(6, 'big')),
        lambda x: indexed_bzip2.open(x, parallelization=parallelization),
    ),
    'gz': CompressionInfo(
        ['gz', 'gzip'],
        ['taz', 'tgz'],
        'indexed_gzip',
        lambda x: x.read(2) == b'\x1F\x8B',
        lambda x: indexed_gzip.IndexedGzipFile(fileobj=x),
    ),
    'rar': CompressionInfo(
        ['rar'],
        [],
        'rarfile',
        lambda x: x.read(6) == b'Rar!\x1A\x07',
        lambda x: rarfile.RarFile(x),
    ),
    'xz': CompressionInfo(
        ['xz'],
        ['txz'],
        'lzmaffi' if 'lzmaffi' in globals() else 'xz',
        lambda x: x.read(6) == b"\xFD7zXZ\x00",
        (lambda x: lzmaffi.open(x)) if 'lzmaffi' in globals() else (lambda x: xz.open(x)),
    ),
    'zip': CompressionInfo(
        ['zip'],
        [],
        'zipfile',
        lambda x: x.read(2) == b'PK',
        lambda x: zipfile.ZipFile(x),
    ),
    'zst': CompressionInfo(
        ['zst', 'zstd'],
        ['tzst'],
        'indexed_zstd',
        lambda x: x.read(4) == (0xFD2FB528).to_bytes(4, 'little'),
        lambda x: indexed_zstd.IndexedZstdFile(x.fileno()),
    ),
}


def stripSuffixFromCompressedFile(path: str) -> str:
    """Strips compression suffixes like .bz2, .gz, ..."""
    for compression in supportedCompressions.values():
        for suffix in compression.suffixes:
            if path.lower().endswith('.' + suffix.lower()):
                return path[: -(len(suffix) + 1)]

    return path


def stripSuffixFromTarFile(path: str) -> str:
    """Strips extensions like .tar.gz or .gz or .tgz, ..."""
    # 1. Try for conflated suffixes first
    for compression in supportedCompressions.values():
        for suffix in compression.doubleSuffixes + ['t' + s for s in compression.suffixes]:
            if path.lower().endswith('.' + suffix.lower()):
                return path[: -(len(suffix) + 1)]

    # 2. Remove compression suffixes
    path = stripSuffixFromCompressedFile(path)

    # 3. Remove .tar if we are left with it after the compression suffix removal
    if path.lower().endswith('.tar'):
        path = path[:-4]

    return path


printDebug = 1


class RatarmountError(Exception):
    """Base exception for ratarmount module."""


class IndexNotOpenError(RatarmountError):
    """Exception for operations executed on a closed index database."""


class InvalidIndexError(RatarmountError):
    """Exception for indexes being invalid, outdated, or created with different arguments."""


class CompressionError(RatarmountError):
    """Exception for trying to open files with unsupported compression or unavailable decompression module."""


def overrides(parentClass):
    """Simple decorator that checks that a method with the same name exists in the parent class"""

    def overrider(method):
        assert method.__name__ in dir(parentClass)
        assert callable(getattr(parentClass, method.__name__))
        return method

    return overrider


class ProgressBar:
    """Simple progress bar which keeps track of changes and prints the progress and a time estimate."""

    def __init__(self, maxValue: float):
        # fmt: off
        self.maxValue        = maxValue
        self.lastUpdateTime  = time.time()
        self.lastUpdateValue = 0.
        self.updateInterval  = 2.  # seconds
        self.creationTime    = time.time()
        # fmt: on

    def update(self, value: float) -> None:
        """Should be called whenever the monitored value changes. The progress bar is updated accordingly."""
        if self.lastUpdateTime is not None and (time.time() - self.lastUpdateTime) < self.updateInterval:
            return

        # Use whole interval since start to estimate time
        eta1 = int((time.time() - self.creationTime) / value * (self.maxValue - value))
        # Use only a shorter window interval to estimate time.
        # Accounts better for higher speeds in beginning, e.g., caused by caching effects.
        # However, this estimate might vary a lot while the other one stabilizes after some time!
        eta2 = int((time.time() - self.lastUpdateTime) / (value - self.lastUpdateValue) * (self.maxValue - value))
        print(
            "Currently at position {} of {} ({:.2f}%). "
            "Estimated time remaining with current rate: {} min {} s, with average rate: {} min {} s.".format(
                # fmt:off
                value, self.maxValue, value / self.maxValue * 100.0,
                eta2 // 60, eta2 % 60,
                eta1 // 60, eta1 % 60
                # fmt:on
            ),
            flush=True,
        )

        self.lastUpdateTime = time.time()
        self.lastUpdateValue = value


class StenciledFile(io.BufferedIOBase):
    """A file abstraction layer giving a stenciled view to an underlying file."""

    def __init__(self, fileobj: IO, stencils: List[Tuple[int, int]]) -> None:
        """
        stencils: A list tuples specifying the offset and length of the underlying file to use.
                  The order of these tuples will be kept.
                  The offset must be non-negative and the size must be positive.

        Examples:
            stencil = [(5,7)]
                Makes a new 7B sized virtual file starting at offset 5 of fileobj.
            stencil = [(0,3),(5,3)]
                Make a new 6B sized virtual file containing bytes [0,1,2,5,6,7] of fileobj.
            stencil = [(0,3),(0,3)]
                Make a 6B size file containing the first 3B of fileobj twice concatenated together.
        """

        # fmt: off
        self.fileobj = fileobj
        self.offsets = [x[0] for x in stencils]
        self.sizes   = [x[1] for x in stencils]
        self.offset  = 0
        # fmt: on

        # Calculate cumulative sizes
        self.cumsizes = [0]
        for offset, size in stencils:
            assert offset >= 0
            assert size >= 0
            self.cumsizes.append(self.cumsizes[-1] + size)

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek(0)

    def _findStencil(self, offset: int) -> int:
        """
        Return index to stencil where offset belongs to. E.g., for stencils [(3,5),(8,2)], offsets 0 to
        and including 4 will still be inside stencil (3,5), i.e., index 0 will be returned. For offset 6,
        index 1 would be returned because it now is in the second contiguous region / stencil.
        """
        # bisect_left( value ) gives an index for a lower range: value < x for all x in list[0:i]
        # Because value >= 0 and list starts with 0 we can therefore be sure that the returned i>0
        # Consider the stencils [(11,2),(22,2),(33,2)] -> cumsizes [0,2,4,6]. Seek to offset 2 should seek to 22.
        assert offset >= 0
        i = bisect.bisect_left(self.cumsizes, offset + 1) - 1
        assert i >= 0
        return i

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    @overrides(io.BufferedIOBase)
    def close(self) -> None:
        # Don't close the object given to us
        # self.fileobj.close()
        pass

    @overrides(io.BufferedIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.BufferedIOBase)
    def seekable(self) -> bool:
        return self.fileobj.seekable()

    @overrides(io.BufferedIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.BufferedIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.BufferedIOBase)
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.cumsizes[-1] - self.offset

        # This loop works in a kind of leapfrog fashion. On each even loop iteration it seeks to the next stencil
        # and on each odd iteration it reads the data and increments the offset inside the stencil!
        result = b''
        i = self._findStencil(self.offset)
        if i >= len(self.sizes):
            return result

        offsetInsideStencil = self.offset - self.cumsizes[i]
        assert offsetInsideStencil >= 0
        assert offsetInsideStencil < self.sizes[i]
        self.fileobj.seek(self.offsets[i] + offsetInsideStencil, io.SEEK_SET)

        while size > 0 and i < len(self.sizes):
            # Read as much as requested or as much as the current contiguous region / stencil still contains
            readableSize = min(size, self.sizes[i] - (self.offset - self.cumsizes[i]))
            if readableSize == 0:
                # Go to next stencil
                i += 1
                if i >= len(self.offsets):
                    break
                self.fileobj.seek(self.offsets[i])
            else:
                # Actually read data
                tmp = self.fileobj.read(readableSize)
                self.offset += len(tmp)
                result += tmp
                size -= readableSize
                # Now, either size is 0 or readableSize will be 0 in the next iteration

        return result

    @overrides(io.BufferedIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.cumsizes[-1] + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        if self.offset < 0:
            raise ValueError("Trying to seek before the start of the file!")
        if self.offset >= self.cumsizes[-1]:
            return self.offset

        return self.offset

    @overrides(io.BufferedIOBase)
    def tell(self) -> int:
        return self.offset


@dataclass
class FileInfo:
    # fmt: off
    size     : int
    mtime    : float
    mode     : int
    linkname : str
    uid      : int
    gid      : int
    # By convention this is a list and MountSources should only read the last element and before forwarding the
    # FileInfo to a possibly recursively "mounted" MountSource, remove that last element belonging to it.
    # This way an arbitrary amount of userdata can be stored and it should be decidable which belongs to whom in
    # a chain of MountSource objects.
    userdata : List[Any]
    # fmt: on

    def clone(self):
        copied = dataclasses.replace(self)
        # Make a new userdata list but do not do a full deep copy because some MountSources put references
        # to MountSources into userdata and those should and can not be deep copied.
        copied.userdata = self.userdata[:]
        return copied


@dataclass
class SQLiteIndexedTarUserData:
    # fmt: off
    offset       : int
    offsetheader : int
    istar        : bool
    issparse     : bool
    # fmt: on


class MountSource(ABC):
    """
    Generic class representing a mount point. It's basically like the FUSE API but boiled down
    to the necessary methods for ratarmount.

    Similar, to FUSE, all paths should have a leading '/'.
    If there is is no leading slash, behave as if there was one.
    """

    @abstractmethod
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        pass

    @abstractmethod
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        pass

    @abstractmethod
    def fileVersions(self, path: str) -> int:
        pass

    @abstractmethod
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        pass

    @abstractmethod
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        pass

    def getMountSource(self, fileInfo: FileInfo):
        """
        Returns the direct mount source to which the fileInfo belongs, a mount source specific file info,
        and the mount point of the returned mount source in respect to this (self) MountSource.
        """
        return '/', self, fileInfo

    def exists(self, path: str):
        return self.getFileInfo(path) is not None

    def isdir(self, path: str):
        fileInfo = self.getFileInfo(path)
        return fileInfo is not None and stat.S_ISDIR(fileInfo.mode)


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
        indexFileName              : Optional[str]       = None,
        indexFolders               : Optional[List[str]] = None,
        recursive                  : bool                = False,
        gzipSeekPointSpacing       : int                 = 4*1024*1024,
        encoding                   : str                 = tarfile.ENCODING,
        stripRecursiveTarExtension : bool                = False,
        ignoreZeros                : bool                = False,
        verifyModificationTime     : bool                = False,
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
        clearIndexCache : If true, then check all possible index file locations for the given tarFileName/fileObject
                          combination and delete them. This also implicitly forces a recreation of the index.
        indexFileName : Path to the index file for this TAR archive. This takes precedence over the automatically
                        chosen locations.
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
        self.indexFileName = None

        # fmt: off
        self.mountRecursively           = recursive
        self.encoding                   = encoding
        self.stripRecursiveTarExtension = stripRecursiveTarExtension
        self.ignoreZeros                = ignoreZeros
        self.verifyModificationTime     = verifyModificationTime
        self.gzipSeekPointSpacing       = gzipSeekPointSpacing
        # fmt: on

        self.tarFileName: str = '<file object>'
        if not fileObject:
            if not tarFileName:
                raise ValueError("At least one of tarFileName and fileObject arguments should be set!")
            self.tarFileName = os.path.abspath(tarFileName)
            fileObject = open(self.tarFileName, 'rb')
        elif tarFileName:
            # If tarFileName was specified for a file object, set self.tarFileName accordingly.
            self.tarFileName = tarFileName

        fileObject.seek(0, io.SEEK_END)
        fileSize = fileObject.tell()
        fileObject.seek(0)

        # rawFileObject : Only set when opening a compressed file and only kept to keep the
        #                 compressed file handle from being closed by the garbage collector.
        # tarFileObject : File object to the uncompressed (or decompressed) TAR file to read actual data out of.
        # compression   : Stores what kind of compression the originally specified TAR file uses.
        # isTar         : Can be false for the degenerated case of only a bz2 or gz file not containing a TAR
        self.tarFileObject, self.rawFileObject, self.compression, self.isTar = SQLiteIndexedTar._openCompressedFile(
            fileObject, gzipSeekPointSpacing, encoding
        )
        if not self.isTar and not self.rawFileObject:
            raise RatarmountError("File object (" + str(fileObject) + ") could not be opened as a TAR file!")

        if self.compression == 'xz':
            try:
                if len(self.tarFileObject.block_boundaries) <= 1 and (fileSize is None or fileSize > 1024 * 1024):
                    print("[Warning] The specified file '{}'".format(self.tarFileName))
                    print("[Warning] is compressed using xz but only contains one xz block. This makes it ")
                    print("[Warning] impossible to use true seeking! Please (re)compress your TAR using pixz")
                    print("[Warning] (see https://github.com/vasi/pixz) in order for ratarmount to do be able ")
                    print("[Warning] to do fast seeking to requested files.")
                    print("[Warning] As it is, each file access will decompress the whole TAR from the beginning!")
                    print()
            except Exception:
                pass

        if not tarFileName:
            self._createIndex(self.tarFileObject)
            # return here because we can't find a save location without any identifying name
            return

        # will be used for storing indexes if current path is read-only
        possibleIndexFilePaths = [self.tarFileName + ".index.sqlite"]
        indexPathAsName = self.tarFileName.replace("/", "_") + ".index.sqlite"
        if isinstance(indexFolders, str):
            indexFolders = [indexFolders]

        # A given index file name takes precedence and there should be no implicit fallback
        if indexFileName:
            if indexFileName == ':memory:':
                possibleIndexFilePaths = [indexFileName]
            else:
                possibleIndexFilePaths = [os.path.abspath(os.path.expanduser(indexFileName))]
        elif indexFolders:
            # An empty path is to be interpreted as the default path right besides the TAR
            if '' not in indexFolders:
                possibleIndexFilePaths = []
            for folder in indexFolders:
                if folder:
                    indexPath = os.path.join(folder, indexPathAsName)
                    possibleIndexFilePaths.append(os.path.abspath(os.path.expanduser(indexPath)))

        if clearIndexCache:
            for indexPath in possibleIndexFilePaths:
                if os.path.isfile(indexPath):
                    os.remove(indexPath)

        # Try to find an already existing index
        for indexPath in possibleIndexFilePaths:
            if self._tryLoadIndex(indexPath):
                self.indexFileName = indexPath
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
        if writeIndex:
            for indexPath in possibleIndexFilePaths:
                if self._pathIsWritable(indexPath) and self._pathCanBeUsedForSqlite(indexPath):
                    self.indexFileName = indexPath
                    break

            if not self.indexFileName:
                raise InvalidIndexError(
                    "Could not find any existing index or writable location for an index in "
                    + str(possibleIndexFilePaths)
                )

        self._createIndex(self.tarFileObject)
        self._loadOrStoreCompressionOffsets()  # store
        if self.sqlConnection:
            self._storeMetadata(self.sqlConnection)
            self._reloadIndexReadOnly()

        if printDebug >= 1 and self.indexFileName and os.path.isfile(self.indexFileName):
            # The 0-time is legacy for the automated tests
            # fmt: off
            print("Writing out TAR index to", self.indexFileName, "took 0s",
                  "and is sized", os.stat( self.indexFileName ).st_size, "B")
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
        self._storeVersionsMetadata(connection)

        metadataTable = """
            /* empty table whose sole existence specifies that we finished iterating the tar */
            CREATE TABLE "metadata" (
                "key"      VARCHAR(65535) NOT NULL, /* e.g. "tarsize" */
                "value"    VARCHAR(65535) NOT NULL  /* e.g. size in bytes as integer */
            );
        """

        connection.executescript(metadataTable)

        # All of these require the generic "metadata" table.
        self._storeTarMetadata(connection, self.tarFileName)
        self._storeArgumentsMetadata(connection)
        connection.commit()

    @staticmethod
    def _storeVersionsMetadata(connection: sqlite3.Connection) -> None:
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
    def _storeTarMetadata(connection: sqlite3.Connection, tarPath: AnyStr) -> None:
        """Adds some consistency meta information to recognize the need to update the cached TAR index"""
        try:
            tarStats = os.stat(tarPath)
            serializedTarStats = json.dumps(
                {attr: getattr(tarStats, attr) for attr in dir(tarStats) if attr.startswith('st_')}
            )
            connection.execute('INSERT INTO "metadata" VALUES (?,?)', ("tarstats", serializedTarStats))
        except Exception as exception:
            if printDebug >= 2:
                print(exception)
            print("[Warning] There was an error when adding file metadata.")
            print("[Warning] Automatic detection of changed TAR files during index loading might not work.")

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
            if printDebug >= 2:
                print(exception)
            print("[Warning] There was an error when adding argument metadata.")
            print("[Warning] Automatic detection of changed arguments files during index loading might not work.")

    @staticmethod
    def _pathIsWritable(path: AnyStr) -> bool:
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            f = open(path, 'wb')
            f.write(b'\0' * 1024 * 1024)
            f.close()
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
    def _pathCanBeUsedForSqlite(path: AnyStr) -> bool:
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
    def _initializeSqlDb(indexFileName: Optional[str]) -> sqlite3.Connection:
        if printDebug >= 1:
            print("Creating new SQLite index database at", indexFileName if indexFileName else ':memory:')

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

        sqlConnection = SQLiteIndexedTar._openSqlDb(indexFileName if indexFileName else ':memory:')
        tables = sqlConnection.execute('SELECT name FROM sqlite_master WHERE type = "table";')
        if {"files", "filestmp", "parentfolders"}.intersection({t[0] for t in tables}):
            raise InvalidIndexError(
                "The index file {} already seems to contain a table. "
                "Please specify --recreate-index.".format(indexFileName)
            )
        sqlConnection.executescript(createTables)
        return sqlConnection

    def _reloadIndexReadOnly(self):
        if not self.indexFileName or self.indexFileName == ':memory:' or not self.sqlConnection:
            return

        self.sqlConnection.commit()
        self.sqlConnection.close()

        uriPath = urllib.parse.quote(self.indexFileName)
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
        if printDebug >= 1:
            print("Creating offset dictionary for", self.tarFileName, "...")
        t0 = timer()

        # 1. If no SQL connection was given (by recursive call), open a new database file
        openedConnection = False
        if not self.indexIsLoaded() or not self.sqlConnection:
            openedConnection = True
            self.sqlConnection = self._initializeSqlDb(self.indexFileName)

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
            if printDebug >= 1:
                print("Creating offset dictionary for", self.tarFileName, "took {:.2f}s".format(t1 - t0))
            return

        # If no file is in the TAR, then it most likely indicates a possibly compressed non TAR file.
        # In that case add that itself to the file index. This won't work when called recursively,
        # so check stream offset.
        fileCount = self.sqlConnection.execute('SELECT COUNT(*) FROM "files";').fetchone()[0]
        if fileCount == 0:
            if printDebug >= 3:
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
        if printDebug >= 2:
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
                WHERE {searchByTuple if libSqliteVersion >= (2,22,0) else searchByConcat}
                    FROM "files" WHERE mode & (1 << 14) != 0
                )
                ORDER BY "path","name";
            DROP TABLE "parentfolders";
            PRAGMA optimize;
        """
        self.sqlConnection.executescript(cleanupDatabase)

        self.sqlConnection.commit()

        t1 = timer()
        if printDebug >= 1:
            print("Creating offset dictionary for", self.tarFileName, "took {:.2f}s".format(t1 - t0))

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
            """
            SELECT * FROM "files"
            WHERE "path" == (?) AND "name" == (?)
            ORDER BY "offsetheader" {}
            LIMIT 1 OFFSET (?);
            """.format(
                'DESC' if fileVersion is None or fileVersion <= 0 else 'ASC'
            ),
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
        tarFile = tarfile.open(fileobj=typing.cast(IO[bytes], tarSubFile), mode='r:', encoding=self.encoding)
        fileObject = tarFile.extractfile(next(iter(tarFile)))
        if not fileObject:
            print("tarfile.extractfile returned nothing!")
            raise fuse.FuseOSError(fuse.errno.EIO) if "fuse" in sys.modules else Exception(
                "tarfile.extractfile returned nothing!"
            )

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

    def loadIndex(self, indexFileName: AnyStr) -> None:
        """Loads the given index SQLite database and checks it for validity."""
        if self.indexIsLoaded():
            return

        t0 = time.time()
        self.sqlConnection = self._openSqlDb(indexFileName)
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
                            print("[Warning] {}: index: {}, current: {}".format(arg, oldState, newState))

        except Exception as e:
            # indexIsLoaded checks self.sqlConnection, so close it before returning because it was found to be faulty
            try:
                self.sqlConnection.close()
            except sqlite3.Error:
                pass
            self.sqlConnection = None

            raise e

        if printDebug >= 1:
            # Legacy output for automated tests
            print("Loading offset dictionary from", indexFileName, "took {:.2f}s".format(time.time() - t0))

    def _tryLoadIndex(self, indexFileName: AnyStr) -> bool:
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile(indexFileName):
            return False

        try:
            self.loadIndex(indexFileName)
        except Exception as exception:
            if printDebug >= 3:
                traceback.print_exc()

            print("[Warning] Could not load file:", indexFileName)
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
                os.remove(indexFileName)
            except OSError:
                print("[Warning] Failed to remove corrupted old cached index file:", indexFileName)

        if printDebug >= 3 and self.indexIsLoaded():
            print("Loaded index", indexFileName)

        return self.indexIsLoaded()

    @staticmethod
    def _detectCompression(fileobj: IO[bytes]) -> Optional[str]:
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
    def _detectTar(fileobj: IO[bytes], encoding: str) -> bool:
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
    def _openCompressedFile(fileobj: IO[bytes], gzipSeekPointSpacing: int, encoding: str) -> Any:
        """
        Opens a file possibly undoing the compression.
        Returns (tar_file_obj, raw_file_obj, compression, isTar).
        raw_file_obj will be none if compression is None.
        """
        compression = SQLiteIndexedTar._detectCompression(fileobj)
        if printDebug >= 3:
            print(f"[Info] Detected compression {compression} for file object:", fileobj)

        if compression not in supportedCompressions:
            return fileobj, None, compression, SQLiteIndexedTar._detectTar(fileobj, encoding)

        cinfo = supportedCompressions[compression]
        if cinfo.moduleName not in sys.modules:
            raise CompressionError(
                "Can't open a {} compressed file '{}' without {} module!".format(
                    compression, fileobj.name, cinfo.moduleName
                )
            )

        if compression == 'gz':
            # drop_handles keeps a file handle opening as is required to call tell() during decoding
            tar_file = indexed_gzip.IndexedGzipFile(fileobj=fileobj, drop_handles=False, spacing=gzipSeekPointSpacing)
        else:
            tar_file = cinfo.open(fileobj)

        return tar_file, fileobj, compression, SQLiteIndexedTar._detectTar(tar_file, encoding)

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
        if not self.indexFileName or self.indexFileName == ':memory:':
            if printDebug >= 2:
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
                offsets = dict(db.execute('SELECT blockoffset,dataoffset FROM {};'.format(table_name)))
                fileObject.set_block_offsets(offsets)
            except Exception:
                if printDebug >= 2:
                    print(
                        "[Info] Could not load {} block offset data. Will create it from scratch.".format(
                            self.compression
                        )
                    )

                tables = [x[0] for x in db.execute('SELECT name FROM sqlite_master WHERE type="table";')]
                if table_name in tables:
                    db.execute('DROP TABLE {}'.format(table_name))
                db.execute('CREATE TABLE {} ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )'.format(table_name))
                db.executemany('INSERT INTO {} VALUES (?,?)'.format(table_name), fileObject.block_offsets().items())
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
            for tmpDir in [os.path.dirname(self.indexFileName), None]:
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
            if printDebug >= 2:
                print("[Info] Could not load GZip Block offset data. Will create it from scratch.")

            # Transparently force index to be built if not already done so. build_full_index was buggy for me.
            # Seeking from end not supported, so we have to read the whole data in in a loop
            while fileObject.read(1024 * 1024):
                pass

            # The created index can unfortunately be pretty large and tmp might actually run out of memory!
            # Therefore, try different paths, starting with the location where the index resides.
            gzindex = None
            for tmpDir in [os.path.dirname(self.indexFileName), None]:
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
            if printDebug >= 2:
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

        assert (
            False
        ), "Could not load or store block offsets for {} probably because adding support was forgotten!".format(
            self.compression
        )


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


class FolderMountSource(MountSource):
    """
    This class manages one folder as mount source offering methods for listing folders, reading files, and others.
    """

    def __init__(self, path: str) -> None:
        self.root: str = path

    def setFolderDescriptor(self, fd: int) -> None:
        """
        Make this mount source manage the special "." folder by changing to that directory.
        Because we change to that directory it may only be used for one mount source but it also works
        when that mount source is mounted on!
        """
        os.fchdir(fd)
        self.root = '.'

    def _realpath(self, path: str) -> str:
        """Path given relative to folder root. Leading '/' is acceptable"""
        return os.path.join(self.root, path.lstrip(os.path.sep))

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return os.path.lexists(self._realpath(path))

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """All returned file infos contain a file path string at the back of FileInfo.userdata."""

        # This is a bit of problematic design, however, the fileVersions count from 1 for the user.
        # And as -1 means the last version, 0 should also mean the first version ...
        # Basically, I did accidentally mix user-visible versions 1+ versions with API 0+ versions,
        # leading to this problematic clash of 0 and 1.
        if fileVersion not in [0, 1] or not self.exists(path):
            return None

        realpath = self._realpath(path)

        stats = os.lstat(realpath)

        fileInfo = FileInfo(
            # fmt: off
            size     = stats.st_size,
            mtime    = stats.st_mtime,
            mode     = stats.st_mode,
            linkname = os.readlink(realpath) if os.path.islink(realpath) else "",
            uid      = stats.st_uid,
            gid      = stats.st_gid,
            userdata = [path],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        realpath = self._realpath(path)
        if not os.path.isdir(realpath):
            return None

        files = list(os.listdir(realpath))

        return files

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return 1 if self.exists(path) else 0

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        realpath = self._realpath(path)

        try:
            return open(realpath, 'rb')
        except Exception as e:
            raise ValueError("Specified path '{}' is not a file that can be read!".format(realpath)) from e

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)

    def getFilePath(self, fileInfo: FileInfo) -> str:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        return self._realpath(path)


class AutoMountLayer(MountSource):
    """
    This mount source takes another mount source and automatically shows the contents of files which are archives.
    The detailed behavior can be controlled using options.
    """

    __slots__ = ('mounted', 'options')

    @dataclass
    class MountInfo:
        mountSource: MountSource
        rootFileInfo: FileInfo

    def __init__(self, mountSource: MountSource, **options) -> None:
        self.options = options

        rootFileInfo = FileInfo(
            # fmt: off
            size         = 0,
            mtime        = int(time.time()),
            mode         = 0o555 | stat.S_IFDIR,
            linkname     = "",
            uid          = os.getuid(),
            gid          = os.getgid(),
            userdata     = ['/'],
            # fmt: on
        )

        # Mount points are specified without trailing slash and with leading slash
        # representing root of this mount source.
        # Disable false positive introduced when updating pylint from 2.6 to 2.12.
        # It now thinks that the assignment is to AutoMountLayer instead of self.mounted.
        # pylint: disable=used-before-assignment
        self.mounted: Dict[str, AutoMountLayer.MountInfo] = {'/': AutoMountLayer.MountInfo(mountSource, rootFileInfo)}

        if not self.options.get('recursive', False) or self.options.get('lazyMounting', False):
            return

        # Go over all files and mount archives and even archives in those archives
        foldersToWalk = ['/']
        while foldersToWalk:
            folder = foldersToWalk.pop()
            fileNames = self.listDir(folder)
            if not fileNames:
                continue

            for fileName in fileNames:
                filePath = os.path.join(folder, fileName)
                if self.isdir(filePath):
                    foldersToWalk.append(filePath)
                else:
                    mountPoint = self._tryToMountFile(filePath)
                    if mountPoint:
                        foldersToWalk.append(mountPoint)

    def _simplyFindMounted(self, path: str) -> Tuple[str, str]:
        """See _findMounted. This is split off to avoid convoluted recursions during lazy mounting."""

        leftPart = path
        rightParts: List[str] = []
        while '/' in leftPart:
            if leftPart in self.mounted:
                return leftPart, '/' + '/'.join(rightParts)

            parts = leftPart.rsplit('/', 1)
            leftPart = parts[0]
            rightParts.insert(0, parts[1])

        assert '/' in self.mounted
        return '/', path

    def _tryToMountFile(self, path: str) -> Optional[str]:
        """
        Returns the mount point path if it has been successfully mounted.
        path: Path inside this mount source. May include recursively mounted mount points.
              Should contain a leading slash.
        """

        # For better performance, only look at the suffix not at the magic bytes.
        strippedFilePath = stripSuffixFromTarFile(path)
        if strippedFilePath == path:
            return None

        mountPoint = strippedFilePath if self.options.get('stripRecursiveTarExtension', False) else path
        if mountPoint in self.mounted:
            return None

        # Use _simplyFindMounted instead of _findMounted or self.open to avoid recursions caused by lazy mounting!
        parentMountPoint, pathInsideParentMountPoint = self._simplyFindMounted(path)
        parentMountSource = self.mounted[parentMountPoint].mountSource

        try:
            archiveFileInfo = parentMountSource.getFileInfo(pathInsideParentMountPoint)
            if archiveFileInfo is None:
                return None

            _, deepestMountSource, deepestFileInfo = parentMountSource.getMountSource(archiveFileInfo)
            if isinstance(deepestMountSource, FolderMountSource):
                # Open from file path on host file system in order to write out TAR index files.
                mountSource = openMountSource(deepestMountSource.getFilePath(deepestFileInfo), **self.options)
            else:
                # This will fail with StenciledFile objects as returned by SQLiteIndexedTar mount sources and when
                # given to backends like indexed_xxx, which do expect the file object to have a valid fileno.
                mountSource = openMountSource(parentMountSource.open(archiveFileInfo), **self.options)
        except Exception as e:
            print("[Warning] Mounting of '" + path + "' failed because of:", e)
            if printDebug >= 3:
                traceback.print_exc()
            print()
            return None

        rootFileInfo = archiveFileInfo.clone()
        rootFileInfo.mode = (rootFileInfo.mode & 0o777) | stat.S_IFDIR
        rootFileInfo.linkname = ""
        rootFileInfo.userdata = [mountPoint]
        mountInfo = AutoMountLayer.MountInfo(mountSource, rootFileInfo)

        # TODO What if the mount point already exists, e.g., because stripRecursiveTarExtension is true and there
        #      are multiple archives with the same name but different extesions?
        self.mounted[mountPoint] = mountInfo
        if printDebug >= 2:
            print("Recursively mounted:", mountPoint)
            print()

        return mountPoint

    def _findMounted(self, path: str) -> Tuple[str, str]:
        """
        Returns the mount point, which can be found in self.mounted, and the rest of the path.
        Basically, it splits path at the appropriate mount point boundary.
        Because of the recursive mounting, there might be multiple mount points fitting the path.
        The longest, i.e., the deepest mount point will be returned.
        """

        if self.options.get('recursive', False) and self.options.get('lazyMounting', False):
            subPath = "/"
            # First go from higher paths to deeper ones and try to mount all parent archives lazily.
            for part in path.lstrip(os.path.sep).split(os.path.sep):
                subPath = os.path.join(subPath, part)
                if subPath not in self.mounted:
                    self._tryToMountFile(subPath)

        return self._simplyFindMounted(path)

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Return file info for given path. Note that all returned file infos contain MountInfo
        or a file path string at the back of FileInfo.userdata.
        """
        # TODO: Add support for the .versions API in order to access the underlying TARs if stripRecursiveTarExtension
        #       is false? Then again, SQLiteIndexedTar is not able to do this either, so it might be inconsistent.

        # It might be arguably that we could simply let the mount source handle returning file infos for the root
        # directory but only we know the permissions of the parent folder and can apply them to the root directory.
        mountPoint, pathInMountPoint = self._findMounted(path)
        mountInfo = self.mounted[mountPoint]
        if pathInMountPoint == '/':
            return mountInfo.rootFileInfo

        fileInfo = mountInfo.mountSource.getFileInfo(pathInMountPoint, fileVersion)
        if fileInfo:
            fileInfo.userdata.append(mountPoint)
            return fileInfo

        return None

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        mountPoint, pathInMountPoint = self._findMounted(path)
        files = self.mounted[mountPoint].mountSource.listDir(pathInMountPoint)
        if not files:
            return None
        files = set(files)

        # Check whether we need to add recursive mount points to this directory listing
        if self.options.get('recursive', False) and self.options.get('stripRecursiveTarExtension', False):
            for mountPoint in self.mounted:
                folder, folderName = os.path.split(mountPoint)
                if folder == path and folderName and folderName not in files:
                    files.add(folderName)

        return files

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        mountPoint, pathInMountPoint = self._findMounted(path)
        return self.mounted[mountPoint].mountSource.fileVersions(pathInMountPoint)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        _, mountSource, sourceFileInfo = self.getMountSource(fileInfo)
        return mountSource.open(sourceFileInfo)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        _, mountSource, sourceFileInfo = self.getMountSource(fileInfo)
        return mountSource.read(sourceFileInfo, size, offset)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        mountPoint = fileInfo.userdata[-1]
        assert isinstance(mountPoint, str)
        mountSource = self.mounted[mountPoint].mountSource

        sourceFileInfo = fileInfo.clone()
        sourceFileInfo.userdata.pop()

        deeperMountPoint, deeperMountSource, deeperFileInfo = mountSource.getMountSource(sourceFileInfo)
        return os.path.join(mountPoint, deeperMountPoint.lstrip('/')), deeperMountSource, deeperFileInfo


class ZipMountSource(MountSource):
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject = zipfile.ZipFile(fileOrPath, 'r')
        ZipMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = self.fileObject.infolist()
        self.options = options

    @staticmethod
    def _findPassword(fileobj: "zipfile.ZipFile", passwords):
        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.infolist()
        if not files:
            for password in passwords:
                fileobj.setpassword(password)
                files = fileobj.infolist()
                if files:
                    return password

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if not file.is_dir() and file.file_size > 0]
        if not files:
            return None

        for password in [None] + passwords:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except Exception:
                pass

        raise RuntimeError("Could not find a matching password!")

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.fileObject.close()

    @staticmethod
    def _convertToFileInfo(info: "zipfile.ZipInfo", zipFile: "zipfile.ZipFile") -> FileInfo:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        mtime = datetime.datetime(*info.date_time, tzinfo=datetime.timezone.utc).timestamp() if info.date_time else 0

        # According to section 4.5.7 in the .ZIP file format specification, links are supported:
        # https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
        # The Python zipfile module has no API for links: https://bugs.python.org/issue45286
        # However, the file mode exposes whether it's a link and the file mode is shown by ZipInfo.__repr__.
        # For that, it uses the OS-dependent external_attr member. See also the ZIP specification on that:
        # > 4.4.15 external file attributes: (4 bytes)
        # >   The mapping of the external attributes is host-system dependent (see 'version made by').
        # >   For MS-DOS, the low order byte is the MS-DOS directory attribute byte.
        # >   If input came from standard input, this field is set to zero.

        # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
        linkname = ""
        if stat.S_ISLNK(info.external_attr >> 16):
            linkname = zipFile.read(info).decode()
            mode = 0o555 | stat.S_IFLNK

        fileInfo = FileInfo(
            # fmt: off
            size     = info.file_size,
            mtime    = mtime,
            mode     = mode,
            linkname = linkname,
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [info],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        path = path.strip('/')
        if path:
            path += '/'

        # TODO How to behave with files in zip with absolute paths? Currently, they would never be shown.
        def getName(filePath):
            if not filePath.startswith(path):
                return None

            filePath = filePath[len(path) :].strip('/')
            if not filePath:
                return None

            # This effectively adds all parent paths as folders. It is easy to create
            # RARs and ZIPs with nested files without information on the parent directories!
            if '/' in filePath:
                firstSlash = filePath.index('/')
                filePath = filePath[:firstSlash]

            return filePath

        # ZipInfo.filename is wrongly named as it returns the full path inside the archive not just the name part
        return set(getName(info.filename) for info in self.files if getName(info.filename))

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        infoList = [
            ZipMountSource._convertToFileInfo(info, self.fileObject)
            for info in self.files
            if info.filename.rstrip('/') == path.lstrip('/')
        ]

        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any([info.userdata[-1].filename == pathAsDir for info in infoList]) and any(
            info.filename.rstrip('/').startswith(pathAsDir) for info in self.files
        ):
            infoList.append(
                FileInfo(
                    # fmt: off
                    size     = 0,
                    mtime    = int(time.time()),
                    mode     = 0o777 | stat.S_IFDIR,
                    linkname = "",
                    uid      = os.getuid(),
                    gid      = os.getgid(),
                    userdata = [None],
                    # fmt: on
                )
            )

        return infoList

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        infos = self._getFileInfos(path)
        return infos[fileVersion] if -len(infos) <= fileVersion < len(infos) else None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return len(self._getFileInfos(path))

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        info = fileInfo.userdata[-1]
        assert isinstance(info, zipfile.ZipInfo)
        return self.fileObject.open(info, 'r')  # https://github.com/pauldmccarthy/indexed_gzip/issues/85

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)


class RawFileInsideRar(io.RawIOBase):
    """
    This class works around the CRC error issue by reopening the file when seeking back.
    This will be slower for uncompressed files but not for compressed files because
    the seek implementation of rarfile also reopens the file on seeking back.
    https://github.com/markokr/rarfile/issues/73
    https://rarfile.readthedocs.io/api.html#rarfile.RarExtFile.seek
    > On uncompressed files, the seeking works by actual seeks so it’s fast.
    > On compressed files it's slow - forward seeking happens by reading ahead,
    > backwards by re-opening and decompressing from the start.
    """

    def __init__(self, reopen, file_size):
        self.reopen = reopen
        self.fileobj = reopen()
        self.file_size = file_size

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self.fileobj.close()

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return self.fileobj.seekable()

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        return self.fileobj.read(size)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            offset += self.tell()
        elif whence == io.SEEK_END:
            offset += self.file_size

        if offset >= self.tell():
            return self.fileobj.seek(offset, whence)

        self.fileobj = self.reopen()
        return self.fileobj.seek(offset, io.SEEK_SET)

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.fileobj.tell()


class RarMountSource(MountSource):
    # Basically copy paste of ZipMountSource because the interfaces are very similar
    # I'm honestly not sure how it works that well as it does. It does have some problems
    # when trying to mount .tar.bz2 or .tar.xz inside rar files recursively but it works
    # reasonably well for .tar.gz and .zip considering that seeking seems to be broken:
    # https://github.com/markokr/rarfile/issues/73

    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.fileObject = rarfile.RarFile(fileOrPath, 'r')
        RarMountSource._findPassword(self.fileObject, options.get("passwords", []))
        self.files = self.fileObject.infolist()
        self.options = options

    @staticmethod
    def _findPassword(fileobj: "rarfile.RarFile", passwords):
        if not fileobj.needs_password():
            return None

        # If headers are encrypted, then infolist will simply return an empty list!
        files = fileobj.infolist()
        if not files:
            for password in passwords:
                fileobj.setpassword(password)
                files = fileobj.infolist()
                if files:
                    return password

        # If headers are not encrypted, then try out passwords by trying to open the first file.
        files = [file for file in files if file.is_file()]
        if not files:
            return None
        for password in passwords:
            fileobj.setpassword(password)
            try:
                with fileobj.open(files[0]) as file:
                    file.read(1)
                return password
            except (rarfile.PasswordRequired, rarfile.BadRarFile):
                pass

        raise rarfile.PasswordRequired("Could not find a matching password!")

    @staticmethod
    def _convertToFileInfo(info: "rarfile.RarInfo") -> FileInfo:
        mode = 0o555 | (stat.S_IFDIR if info.is_dir() else stat.S_IFREG)
        dtime = datetime.datetime(*info.date_time)
        dtime = dtime.replace(tzinfo=datetime.timezone.utc)
        mtime = dtime.timestamp() if info.date_time else 0

        # file_redir is (type, flags, target) or None. Only tested for type == RAR5_XREDIR_UNIX_SYMLINK.
        linkname = ""
        if info.file_redir:
            linkname = info.file_redir[2]
            mode = 0o555 | stat.S_IFLNK

        fileInfo = FileInfo(
            # fmt: off
            size     = info.file_size,
            mtime    = mtime,
            mode     = mode,
            linkname = linkname,
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [info],
            # fmt: on
        )

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        path = path.strip('/')
        if path:
            path += '/'

        # TODO How to behave with files in archive with absolute paths? Currently, they would never be shown.
        def getName(filePath):
            if not filePath.startswith(path):
                return None

            filePath = filePath[len(path) :].strip('/')
            if not filePath:
                return None

            # This effectively adds all parent paths as folders. It is easy to create
            # RARs and ZIPs with nested files without information on the parent directories!
            if '/' in filePath:
                firstSlash = filePath.index('/')
                filePath = filePath[:firstSlash]

            return filePath

        # The "filename" member is wrongly named as it returns the full path inside the archive not just the name part.
        return set(getName(info.filename) for info in self.files if getName(info.filename))

    def _getFileInfos(self, path: str) -> List[FileInfo]:
        infoList = [
            RarMountSource._convertToFileInfo(info)
            for info in self.files
            if info.filename.rstrip('/') == path.lstrip('/')
        ]

        # If we have a fileInfo for the given directory path, then everything is fine.
        pathAsDir = path.strip('/') + '/'

        # Check whether some parent directories of files do not exist as separate entities in the archive.
        if not any([info.userdata[-1].filename == pathAsDir for info in infoList]) and any(
            info.filename.rstrip('/').startswith(pathAsDir) for info in self.files
        ):
            infoList.append(
                FileInfo(
                    # fmt: off
                    size     = 0,
                    mtime    = int(time.time()),
                    mode     = 0o777 | stat.S_IFDIR,
                    linkname = "",
                    uid      = os.getuid(),
                    gid      = os.getgid(),
                    userdata = [None],
                    # fmt: on
                )
            )

        return infoList

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        infos = self._getFileInfos(path)
        return infos[fileVersion] if -len(infos) <= fileVersion < len(infos) else None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return len(self._getFileInfos(path))

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        info = fileInfo.userdata[-1]
        assert isinstance(info, rarfile.RarInfo)
        return cast(IO[bytes], RawFileInsideRar(lambda: self.fileObject.open(info, 'r'), info.file_size))

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        with self.open(fileInfo) as file:
            file.seek(offset, os.SEEK_SET)
            return file.read(size)


class DummyFuseOperations:
    """A dummy class that is used to replace
    fuse.Operations if fusepy is not installed."""

    def init(self):
        pass

    def getattr(self):
        pass

    def read(self):
        pass

    def readdir(self):
        pass

    def readlink(self):
        pass

    def open(self):
        pass

    def release(self):
        pass


FuseOperations = fuse.Operations if 'fuse' in sys.modules else DummyFuseOperations


def openMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> MountSource:
    if isinstance(fileOrPath, str):
        if not os.path.exists(fileOrPath):
            raise Exception("Mount source does not exist!")

        if os.path.isdir(fileOrPath):
            return FolderMountSource('.' if fileOrPath == '.' else os.path.realpath(fileOrPath))

    try:
        if 'rarfile' in sys.modules and rarfile.is_rarfile(fileOrPath):
            return RarMountSource(fileOrPath, **options)
    except Exception as exception:
        if printDebug >= 1:
            print("[Info] Checking for RAR file raised an exception:", exception)
        if printDebug >= 2:
            traceback.print_exc()
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore

    try:
        if isinstance(fileOrPath, str):
            return SQLiteIndexedTar(fileOrPath, **options)

        return SQLiteIndexedTar(fileObject=fileOrPath, **options)
    except RatarmountError as exception:
        if printDebug >= 2:
            print("[Info] Checking for (compressed) TAR file raised an exception:", exception)
        if printDebug >= 3:
            traceback.print_exc()
    except Exception as exception:
        if printDebug >= 1:
            print("[Info] Checking for (compressed) TAR file raised an exception:", exception)
        if printDebug >= 3:
            traceback.print_exc()
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore

    if 'zipfile' in sys.modules:
        try:
            # is_zipfile is much too lax when testing for ZIPs because it's only testing for the central directory
            # at the end of the file not the magic bits at the beginning. Meaning, if another non-ZIP archive has
            # zip contents at the end, then it might get misclassified! Thefore, manually check for PK at start.
            # https://bugs.python.org/issue16735
            # https://bugs.python.org/issue28494
            # https://bugs.python.org/issue42096
            # https://bugs.python.org/issue45287
            # TODO This will not recognize self-extracting ZIP archives, so for now, those are simply not supported!
            if isinstance(fileOrPath, str):
                with open(fileOrPath, 'rb') as file:
                    if supportedCompressions['zip'].checkHeader(file) and zipfile.is_zipfile(fileOrPath):
                        return ZipMountSource(fileOrPath, **options)
            else:
                # TODO One problem here is when trying to read and then seek back but there also is no peek method.
                #      https://github.com/markokr/rarfile/issues/73
                if fileOrPath.read(2) == b'PK' and zipfile.is_zipfile(fileOrPath):
                    return ZipMountSource(fileOrPath, **options)
        except Exception as exception:
            if printDebug >= 1:
                print("[Info] Checking for ZIP file raised an exception:", exception)
            if printDebug >= 3:
                traceback.print_exc()
        finally:
            if hasattr(fileOrPath, 'seek'):
                fileOrPath.seek(0)  # type: ignore

    raise CompressionError("Archive to open has unrecognized format!")


class UnionMountSource(MountSource):
    def __init__(self, mountSources: List[MountSource]) -> None:
        self.mountSources: List[MountSource] = mountSources

        self.folderCache: Dict[str, List[MountSource]] = {"/": self.mountSources}
        self.folderCacheDepth = 0  # depth 1 means, we only cached top-level directories.

        self.rootFileInfo = FileInfo(
            # fmt: off
            size     = 0,
            mtime    = int(time.time()),
            mode     = 0o777 | stat.S_IFDIR,
            linkname = "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [None],
            # fmt: on
        )

        if len(self.mountSources) > 1:
            self._buildFolderCache()

    def _buildFolderCache(self, maxDepth=1024, nMaxCacheSize=100000, nMaxSecondsToCache=60):
        """
        nMaxCacheSize:
            Even assuming very long file paths like 1000 chars, the cache size
            will be below 100 MB if the maximum number of elements is 100k.
        nMaxSecondsToCache:
            Another problem is the setup time, as it might take ~0.001s for each getFileInfo call
            and it shouldn't take minutes! Note that there always can be an edge case with hundred
            thousands of files in one folder, which can take an arbitrary amount of time to cache.
        """
        t0 = time.time()

        if printDebug >= 1:
            print(f"Building cache for union mount (timeout after {nMaxSecondsToCache}s)...")

        self.folderCache = {"/": self.mountSources}

        lastFolderCache: Dict[str, List[MountSource]] = {"/": self.mountSources}

        for depth in range(1, maxDepth):
            # This intermediary structure is used because:
            #   1. We need to only iterate over the newly added folders in the next step
            #   2. We always want to (atomically) merge results for one folder depth so that we can be sure
            #      that if a folder of a cached depth can not be found in the cache that it does not exist at all.
            newFolderCache: Dict[str, List[MountSource]] = {}

            for folder, mountSources in lastFolderCache.items():
                for mountSource in mountSources:
                    filesInFolder = mountSource.listDir(folder)
                    if not filesInFolder:
                        continue

                    for file in filesInFolder:
                        if time.time() - t0 > nMaxSecondsToCache or nMaxCacheSize <= 0:
                            return

                        fullPath = os.path.join(folder, file)
                        fileInfo = mountSource.getFileInfo(fullPath)
                        if not fileInfo or not stat.S_ISDIR(fileInfo.mode):
                            continue

                        nMaxCacheSize -= 1

                        if fullPath in newFolderCache:
                            newFolderCache[fullPath].append(mountSource)
                        else:
                            newFolderCache[fullPath] = [mountSource]

            if not newFolderCache:
                break

            self.folderCache.update(newFolderCache)
            self.folderCacheDepth = depth
            lastFolderCache = newFolderCache

        t1 = time.time()

        if printDebug >= 1:
            print(
                f"Cached mount sources for {len(self.folderCache)} folders up to a depth of "
                f"{self.folderCacheDepth} in {t1-t0:.3}s for faster union mount."
            )

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if path == '/':
            return self.rootFileInfo

        if path in self.folderCache:
            # This case might be triggered when path is a folder
            mountSources = self.folderCache[path]
        elif self.folderCache and self.folderCacheDepth > 0 and path.startswith('/'):
            # This should be the most common case, i.e., for regular files. Look up the parent folder in this case.
            parentFolder = '/'.join(path.split('/', self.folderCacheDepth + 1)[:-1])
            if parentFolder not in self.folderCache:
                return None
            mountSources = self.folderCache[parentFolder]
        else:
            mountSources = self.mountSources

        # We need to keep the sign of the fileVersion in order to forward it to SQLiteIndexedTar.
        # When the requested version can't be found in a mount source, increment negative specified versions
        # by the amount of versions in that mount source or decrement the initially positive version.
        if fileVersion <= 0:
            for mountSource in reversed(mountSources):
                fileInfo = mountSource.getFileInfo(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion += mountSource.fileVersions(path)
                if fileVersion > 0:
                    break

        else:  # fileVersion >= 1
            for mountSource in mountSources:
                fileInfo = mountSource.getFileInfo(path, fileVersion=fileVersion)
                if isinstance(fileInfo, FileInfo):
                    fileInfo.userdata.append(mountSource)
                    return fileInfo
                fileVersion -= mountSource.fileVersions(path)
                if fileVersion < 1:
                    break

        return None

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return sum(mountSource.fileVersions(path) for mountSource in self.mountSources)

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        """
        Returns the set of all folder contents over all mount sources or None if the path was found in none of them.
        """

        files: Set[str] = set()
        folderExists = False

        for mountSource in self.mountSources:
            result = mountSource.listDir(path)
            if result is not None:
                files = files.union(result)
                folderExists = True

        return files if folderExists else None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.open(fileInfo)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        mountSource = fileInfo.userdata.pop()
        try:
            assert isinstance(mountSource, MountSource)
            return mountSource.read(fileInfo, size, offset)
        finally:
            fileInfo.userdata.append(mountSource)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        sourceFileInfo = fileInfo.clone()
        mountSource = sourceFileInfo.userdata.pop()

        if not isinstance(mountSource, MountSource):
            return '/', self, fileInfo

        # Because all mount sources are mounted at '/', we do not have to append
        # the mount point path returned by getMountSource to the mount point '/'.
        return mountSource.getMountSource(sourceFileInfo)


class FileVersionLayer(MountSource):
    """
    This bind mount like layer makes it possible to access older file versions if there multiple ones in the given
    mount source. The interface provides for each file <file path> a hidden folder <file path.versions> containing
    all available versions.

    This class also resolves hardlinks. This functionality is mixed in here because self-referencing hardlinks
    should be resolved by showing older versions of a file and only this layer knows about file versioning.

    TODO If there already exists a file <file path.versions> then this special folder will not be available!
    """

    def __init__(self, mountSource: MountSource):
        self.mountSource: MountSource = mountSource

    def _decodeVersionsPathAPI(self, filePath: str) -> Optional[Tuple[str, bool, int]]:
        """
        Do a loop over the parent path parts to resolve possible versions in parent folders.
        Note that multiple versions of a folder always are union mounted. So, for the path to a file
        inside those folders the exact version of a parent folder can simply be removed for lookup.
        Therefore, translate something like: /foo.version/3/bar.version/2/mimi.version/1 into
        /foo/bar/mimi.version/1
        This is possibly time-costly but requesting a different version from the most recent should
        be a rare occurence and FUSE also checks all parent parts before accessing a file so it
        might only slow down access by roughly factor 2.
        """

        # TODO make it work for files ending with '.versions'.
        # Currently, this feature would be hidden by those files. But, I think this should be quite rare.
        # I could allow arbitrary amounts of dots like '....versions' but then it wouldn't be discernible
        # for ...versions whether the versions of ..versions or .versions file was requested. I could add
        # a rule for the decision, like ...versions shows the versions of .versions and ....versions for
        # ..versions, however, all of this might require an awful lot of file existence checking.
        # My first idea was to use hidden subfolders for each file like path/to/file/.versions/1 but FUSE
        # checks the parents in a path that they are directories first, so getattr or readdir is not even
        # called for path/to/file/.versions if path/to/file is not a directory.
        # Another alternative might be one hidden folder at the root for a parallel file tree, like
        # /.versions/path/to/file/3 but that runs into similar problems when trying to specify the file
        # version or if a .versions root directory exists.

        filePathParts = filePath.lstrip('/').split('/')
        filePath = ''
        pathIsSpecialVersionsFolder = False
        fileVersion = None  # Not valid if None or parentIsVersions is True
        for part in filePathParts:
            # Skip over the exact version specified
            if pathIsSpecialVersionsFolder:
                try:
                    fileVersion = int(part)
                    assert str(fileVersion) == part
                except Exception:
                    return None
                pathIsSpecialVersionsFolder = False
                continue

            # Simply append normal existing folders
            tmpFilePath = '/'.join([filePath, part])
            if self.mountSource.getFileInfo(tmpFilePath):
                filePath = tmpFilePath
                fileVersion = 0
                continue

            # If current path does not exist, check if it is a special versions path
            if part.endswith('.versions') and len(part) > len('.versions'):
                pathIsSpecialVersionsFolder = True
                fileVersion = 0
                filePath = tmpFilePath[: -len('.versions')]
                continue

            # Parent path does not exist and is not a versions path, so any subpaths also won't exist either
            return None

        if fileVersion is None:
            raise Exception("No file version found in special versioning path specification!")

        return filePath, pathIsSpecialVersionsFolder, (0 if pathIsSpecialVersionsFolder else fileVersion)

    @staticmethod
    def _isHardLink(fileInfo: FileInfo) -> bool:
        # Note that S_ISLNK checks for symbolic links. Hardlinks (at least from tarfile)
        # return false for S_ISLNK but still have a linkname!
        return bool(not stat.S_ISREG(fileInfo.mode) and not stat.S_ISLNK(fileInfo.mode) and fileInfo.linkname)

    @staticmethod
    def _resolveHardLinks(mountSource: MountSource, path: str) -> Optional[FileInfo]:
        """path : Simple path. Should contain no special versioning folders!"""

        fileInfo = mountSource.getFileInfo(path)
        if not fileInfo:
            return None

        resolvedPath = '/' + fileInfo.linkname.lstrip('/') if FileVersionLayer._isHardLink(fileInfo) else None
        fileVersion = 0
        hardLinkCount = 0

        while resolvedPath and hardLinkCount < 128:  # For comparison, the maximum symbolic link chain in Linux is 40.
            # Link targets are relative to the mount source. That's why we need the mount point to get the full path
            # in respect to this mount source. And we must a file info object for this mount source, so we have to
            # get that using the full path instead of calling getFileInfo on the deepest mount source.
            mountPoint, _, _ = mountSource.getMountSource(fileInfo)

            resolvedPath = os.path.join(mountPoint, resolvedPath.lstrip('/'))

            if resolvedPath != path:
                # The file version is only of importance to resolve self-references.
                # It seems undecidable to me whether to return the given fileVersion or 0 here.
                # Returning 0 would feel more correct because the we switched to another file and the version
                # for that file is the most recent one.
                # However, resetting the file version to 0 means that if there is a cycle, i.e., two hardlinks
                # of different names referencing each other, than the file version will always be reset to 0
                # and we have no break condition, entering an infinite loop.
                # The most correct version would be to track the version of each path in a map and count up the
                # version per path.
                # TODO Is such a hardlink cycle even possible?!
                fileVersion = 0
            else:
                # If file is referencing itself, try to access earlier version of it.
                # The check for fileVersion against the total number of available file versions is omitted because
                # that check is done implicitly inside the mount sources getFileInfo method!
                fileVersion = fileVersion + 1 if fileVersion >= 0 else fileVersion - 1

            path = resolvedPath
            fileInfo = mountSource.getFileInfo(path, fileVersion)
            if not fileInfo:
                return None

            resolvedPath = '/' + fileInfo.linkname.lstrip('/') if FileVersionLayer._isHardLink(fileInfo) else None
            hardLinkCount += 1

        return fileInfo

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Iterable[str]]:
        files = self.mountSource.listDir(path)
        if files is not None:
            return files

        # If no folder was found, check whether the special .versions folder was requested
        try:
            result = self._decodeVersionsPathAPI(path)
        except Exception:
            return None

        if not result:
            return None
        path, pathIsSpecialVersionsFolder, _ = result

        if not pathIsSpecialVersionsFolder:
            return self.mountSource.listDir(path)

        # Print all available versions of the file at filePath as the contents of the special '.versions' folder
        return [str(version + 1) for version in range(self.mountSource.fileVersions(path))]

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """Resolves special file version specifications in the path."""

        assert fileVersion == 0

        fileInfo = FileVersionLayer._resolveHardLinks(self.mountSource, path)
        if fileInfo:
            return fileInfo

        # If no file was found, check if a special .versions folder to an existing file/folder was queried.
        versionsInfo = self._decodeVersionsPathAPI(path)
        if not versionsInfo:
            raise fuse.FuseOSError(fuse.errno.ENOENT)
        path, pathIsSpecialVersionsFolder, fileVersion = versionsInfo

        # 2.) Check if the request was for the special .versions folder and return its contents or stats
        # At this point, path is assured to actually exist!
        if pathIsSpecialVersionsFolder:
            parentFileInfo = self.mountSource.getFileInfo(path)
            assert parentFileInfo

            fileInfo = FileInfo(
                # fmt: off
                size     = 0,
                mtime    = parentFileInfo.mtime,
                mode     = 0o777 | stat.S_IFDIR,
                linkname = "",
                uid      = parentFileInfo.uid,
                gid      = parentFileInfo.gid,
                # I think this does not matter because currently userdata is only used in read calls,
                # which should only be given non-directory files and this is a directory
                userdata = [],
                # fmt: on
            )

            return fileInfo

        # 3.) At this point the request is for an actually older version of a file or folder
        fileInfo = self.mountSource.getFileInfo(path, fileVersion=fileVersion)
        if fileInfo:
            return fileInfo

        raise fuse.FuseOSError(fuse.errno.ENOENT)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        # TODO return 1 for special .versions folders and files contained in there?
        return self.mountSource.fileVersions(path)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        return self.mountSource.open(fileInfo)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        return self.mountSource.read(fileInfo, size, offset)

    @overrides(MountSource)
    def getMountSource(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        return self.mountSource.getMountSource(fileInfo)


class FuseMount(FuseOperations):  # type: ignore
    """
    This class implements the fusepy interface in order to create a mounted file system view to a MountSource.
    Tasks of this class itself:
       - Changes all file permissions to read-only
       - Get actual file contents either by directly reading from the TAR or by using StenciledFile and tarfile
       - Enabling FolderMountSource to bind to the nonempty folder under the mountpoint itself.
    Other functionalities like file versioning, hard link resolving, and union mounting are implemented by using
    the respective MountSource derived classes.

    Documentation for FUSE methods can be found in the fusepy or libfuse headers. There seems to be no complete
    rendered documentation aside from the header comments.

    https://github.com/fusepy/fusepy/blob/master/fuse.py
    https://github.com/libfuse/libfuse/blob/master/include/fuse.h
    https://man7.org/linux/man-pages/man3/errno.3.html

    All path arguments for overriden fusepy methods do have a leading slash ('/')!
    This is why MountSource also should expect leading slashes in all paths.
    """

    __slots__ = (
        'mountSource',
        'rootFileInfo',
        'mountPoint',
        'mountPointFd',
        'mountPointWasCreated',
        'selfBindMount',
        'openedFiles',
        'lastFileHandle',
    )

    def __init__(self, pathToMount: Union[str, List[str]], mountPoint: str, **options) -> None:
        if not isinstance(pathToMount, list):
            try:
                os.fspath(pathToMount)
                pathToMount = [pathToMount]
            except Exception:
                pass

        options['writeIndex'] = True

        # This also will create or load the block offsets for compressed formats
        mountSources = [openMountSource(path, **options) for path in pathToMount]

        # No threads should be created and still be open before FUSE forks.
        # Instead, they should be created in 'init'.
        # Therefore, close threads opened by the ParallelBZ2Reader for creating the block offsets.
        # Those threads will be automatically recreated again on the next read call.
        # Without this, the ratarmount background process won't quit even after unmounting!
        for mountSource in mountSources:
            if (
                isinstance(mountSource, SQLiteIndexedTar)
                and hasattr(mountSource, 'tarFileObject')
                and hasattr(mountSource.tarFileObject, 'join_threads')
            ):
                mountSource.tarFileObject.join_threads()

        self.mountSource: MountSource = UnionMountSource(mountSources)
        if options.get('recursive', False):
            self.mountSource = AutoMountLayer(self.mountSource, **options)
        self.mountSource = FileVersionLayer(self.mountSource)

        self.rootFileInfo = _makeMountPointFileInfoFromStats(os.stat(pathToMount[0]))

        self.openedFiles: Dict[int, IO[bytes]] = {}
        self.lastFileHandle: int = 0  # It will be incremented before being returned. It can't hurt to never return 0.

        # Create mount point if it does not exist
        self.mountPointWasCreated = False
        if mountPoint and not os.path.exists(mountPoint):
            os.mkdir(mountPoint)
            self.mountPointWasCreated = True
        self.mountPoint = os.path.realpath(mountPoint)

        # Take care that bind-mounting folders to itself works
        self.mountPointFd = None
        self.selfBindMount: Optional[FolderMountSource] = None
        for mountSource in mountSources:
            if isinstance(mountSource, FolderMountSource) and mountSource.root == self.mountPoint:
                self.selfBindMount = mountSource
                self.mountPointFd = os.open(self.mountPoint, os.O_RDONLY)

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

    @overrides(FuseOperations)
    def init(self, connection) -> None:
        if self.selfBindMount is not None and self.mountPointFd is not None:
            self.selfBindMount.setFolderDescriptor(self.mountPointFd)

    @overrides(FuseOperations)
    def getattr(self, path: str, fh=None) -> Dict[str, Any]:
        fileInfo = self.mountSource.getFileInfo(path)
        if not fileInfo:
            raise fuse.FuseOSError(fuse.errno.EIO)

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

    @overrides(FuseOperations)
    def readdir(self, path: str, fh):
        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        yield '.'
        yield '..'

        files = self.mountSource.listDir(path)
        if files is not None:
            for key in files:
                yield key

    @overrides(FuseOperations)
    def readlink(self, path: str) -> str:
        fileInfo = self.mountSource.getFileInfo(path)
        if not fileInfo:
            raise fuse.FuseOSError(fuse.errno.EIO)
        return fileInfo.linkname

    @overrides(FuseOperations)
    def open(self, path, flags):
        """Returns file handle of opened path."""

        fileInfo = self.mountSource.getFileInfo(path)
        if not fileInfo:
            raise fuse.FuseOSError(fuse.errno.EIO)

        try:
            self.lastFileHandle += 1
            self.openedFiles[self.lastFileHandle] = self.mountSource.open(fileInfo)
            return self.lastFileHandle
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to open file.", fileInfo)
            raise fuse.FuseOSError(fuse.errno.EIO) from exception

    @overrides(FuseOperations)
    def release(self, path, fh):
        if fh not in self.openedFiles:
            raise fuse.FuseOSError(fuse.errno.ESTALE)

        self.openedFiles[fh].close()
        del self.openedFiles[fh]
        return fh

    @overrides(FuseOperations)
    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        if fh in self.openedFiles:
            self.openedFiles[fh].seek(offset, os.SEEK_SET)
            return self.openedFiles[fh].read(size)

        # As far as I understand FUSE and my own file handle cache, this should never happen. But you never know.
        if printDebug >= 1:
            print("[Warning] Given file handle does not exist. Will open file before reading which might be slow.")

        fileInfo = self.mountSource.getFileInfo(path)
        if not fileInfo:
            raise fuse.FuseOSError(fuse.errno.EIO)

        try:
            return self.mountSource.read(fileInfo, size, offset)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to read data from underlying TAR file! Returning errno.EIO.")
            raise fuse.FuseOSError(fuse.errno.EIO) from exception


class TarFileType:
    """
    Similar to argparse.FileType but raises an exception if it is not a valid TAR file.
    """

    def __init__(self, encoding: str = tarfile.ENCODING) -> None:
        self.encoding = encoding

    def __call__(self, tarFile: str) -> Tuple[str, Optional[str]]:
        if not os.path.isfile(tarFile):
            raise argparse.ArgumentTypeError("File '{}' is not a file!".format(tarFile))

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
                if compression != 'zst' or supportedCompressions[compression].moduleName not in sys.modules:
                    raise Exception()  # early exit because we catch it ourself anyways

                zstdFile = supportedCompressions[compression].open(fileobj)

                if not zstdFile.is_multiframe() and fileSize > 1024 * 1024:
                    print("[Warning] The specified file '{}'".format(tarFile))
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
                if SQLiteIndexedTar._detectTar(fileobj, self.encoding):
                    return tarFile, compression

                if printDebug >= 2:
                    print(f"Archive '{tarFile}' (compression: {compression}) can't be opened!")

                raise argparse.ArgumentTypeError("Archive '{}' can't be opened!\n".format(tarFile))

        cinfo = supportedCompressions[compression]
        if cinfo.moduleName not in sys.modules:
            raise argparse.ArgumentTypeError(
                "Can't open a {} compressed TAR file '{}' without {} module!".format(
                    compression, fileobj.name, cinfo.moduleName
                )
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


def _parseArgs(rawArgs: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        formatter_class=_CustomFormatter,
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

    # fmt: off
    parser.add_argument(
        '-f', '--foreground', action='store_true', default = False,
        help = 'Keeps the python program in foreground so it can print debug '
               'output when the mounted path is accessed.' )

    parser.add_argument(
        '-d', '--debug', type = int, default = printDebug,
        help = 'Sets the debugging level. Higher means more output. Currently, 3 is the highest.' )

    parser.add_argument(
        '-c', '--recreate-index', action='store_true', default = False,
        help = 'If specified, pre-existing .index files will be deleted and newly created.' )

    parser.add_argument(
        '-r', '--recursive', action='store_true', default = False,
        help = 'Mount TAR archives inside the mounted TAR recursively. '
               'Note that this only has an effect when creating an index. '
               'If an index already exists, then this option will be effectively ignored. '
               'Recreate the index if you want change the recursive mounting policy anyways.' )

    parser.add_argument(
        '-l', '--lazy', action='store_true', default = False,
        help = 'When used with recursively bind-mounted folders, TAR files inside the mounted folder will only be '
               'mounted on first access to it.' )

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
    parser.add_argument(
        '-gs', '--gzip-seek-point-spacing', type = float, default = 16,
        help =
        'This only is applied when the index is first created or recreated with the -c option. '
        'The spacing given in MiB specifies the seek point distance in the uncompressed data. '
        'A distance of 16MiB means that archives smaller than 16MiB in uncompressed size will '
        'not benefit from faster seek times. A seek point takes roughly 32kiB. '
        'So, smaller distances lead to more responsive seeking but may explode the index size!' )

    parser.add_argument(
        '-p', '--prefix', type = str, default = '',
        help = '[deprecated] Use "-o modules=subdir,subdir=<prefix>" instead. '
               'This standard way utilizes FUSE itself and will also work for other FUSE '
               'applications. So, it is preferable even if a bit more verbose.'
               'The specified path to the folder inside the TAR will be mounted to root. '
               'This can be useful when the archive as created with absolute paths. '
               'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
               '-p /var/log/apt/ can be specified so that the mount target directory '
               '>directly< contains history.log.' )

    parser.add_argument(
        '--password', type = str, default = '',
        help = 'Specify a single password which shall be used for RAR and ZIP files.' )

    parser.add_argument(
        '--password-file', type = str, default = '',
        help = 'Specify a file with newline separated passwords for RAR and ZIP files. '
               'The passwords will be tried out in order of appearance in the file.' )

    parser.add_argument(
        '-e', '--encoding', type = str, default = tarfile.ENCODING,
        help = 'Specify an input encoding used for file names among others in the TAR. '
               'This must be used when, e.g., trying to open a latin1 encoded TAR on an UTF-8 system. '
               'Possible encodings: https://docs.python.org/3/library/codecs.html#standard-encodings' )

    parser.add_argument(
        '-i', '--ignore-zeros', action = 'store_true',
        help = 'Ignore zeroed blocks in archive. Normally, two consecutive 512-blocks filled with zeroes mean EOF '
               'and ratarmount stops reading after encountering them. This option instructs it to read further and '
               'is useful when reading archives created with the -A option.' )

    parser.add_argument(
        '--verify-mtime', action = 'store_true',
        help = 'By default, only the TAR file size is checked to match the one in the found existing ratarmount index. '
               'If this option is specified, then also check the modification timestamp. But beware that the mtime '
               'might change during copying or downloading without the contents changing. So, this check might cause '
               'false positives.' )

    parser.add_argument(
        '-s', '--strip-recursive-tar-extension', action = 'store_true',
        help = 'If true, then recursively mounted TARs named <file>.tar will be mounted at <file>/. '
               'This might lead to folders of the same name being overwritten, so use with care. '
               'The index needs to be (re)created to apply this option!' )

    parser.add_argument(
        '--index-file', type = str,
        help = 'Specify a path to the .index.sqlite file. Setting this will disable fallback index folders. '
               'If the given path is ":memory:", then the index will not be written out to disk.' )

    parser.add_argument(
        '--index-folders', default = "," + os.path.join( "~", ".ratarmount" ),
        help = 'Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for suitability '
               'in the given order. An empty path will be interpreted as the location in which the TAR resides. '
               'If the argument begins with a bracket "[", then it will be interpreted as a JSON-formatted list. '
               'If the argument contains a comma ",", it will be interpreted as a comma-separated list of folders. '
               'Else, the whole string will be interpreted as one folder path. Examples: '
               '--index-folders ",~/.foo" will try to save besides the TAR and if that does not work, in ~/.foo. '
               '--index-folders \'["~/.ratarmount", "foo,9000"]\' will never try to save besides the TAR. '
               '--index-folder ~/.ratarmount will only test ~/.ratarmount as a storage location and nothing else. '
               'Instead, it will first try ~/.ratarmount and the folder "foo,9000". ' )

    parser.add_argument(
        '-o', '--fuse', type = str, default = '',
        help = 'Comma separated FUSE options. See "man mount.fuse" for help. '
               'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ' )

    parser.add_argument(
        '-P', '--parallelization', type = int, default = 1,
        help = 'If an integer other than 1 is specified, then the threaded parallel bzip2 decoder will be used '
               'specified amount of block decoder threads. Further threads with lighter work may be started. '
               'A value of 0 will use all the available cores ({}).'.format(os.cpu_count()))

    parser.add_argument(
        '-v', '--version', action='store_true', help = 'Print version string.' )

    parser.add_argument(
        'mount_source', nargs = '+',
        help = 'The path to the TAR archive to be mounted. '
               'If multiple archives and/or folders are specified, then they will be mounted as if the arguments '
               'coming first were updated with the contents of the archives or folders specified thereafter, '
               'i.e., the list of TARs and folders will be union mounted.' )
    parser.add_argument(
        'mount_point', nargs = '?',
        help = 'The path to a folder to mount the TAR contents into. '
               'If no mount path is specified, the TAR will be mounted to a folder of the same name '
               'but without a file extension.' )
    # fmt: on

    args = parser.parse_args(rawArgs)

    args.gzipSeekPointSpacing = args.gzip_seek_point_spacing * 1024 * 1024

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into the tarfilepaths namespace and we have to
    # manually separate them depending on the type.
    if os.path.isdir(args.mount_source[-1]) or not os.path.exists(args.mount_source[-1]):
        args.mount_point = args.mount_source[-1]
        args.mount_source = args.mount_source[:-1]
    if not args.mount_source:
        print("[Error] You must at least specify one path to a valid TAR file or union mount source directory!")
        sys.exit(1)

    # Manually check that all specified TARs and folders exist
    def checkMountSource(path):
        if (
            os.path.isdir(path)
            or ('zipfile' in sys.modules and zipfile.is_zipfile(path))
            or ('rarfile' in sys.modules and rarfile.is_rarfile(path))
        ):
            return os.path.realpath(path)
        return TarFileType(encoding=args.encoding)(path)[0]

    args.mount_source = [checkMountSource(path) for path in args.mount_source]

    # Automatically generate a default mount path
    if not args.mount_point:
        autoMountPoint = stripSuffixFromTarFile(args.mount_source[0])
        if args.mount_point == autoMountPoint:
            args.mount_point = os.path.splitext(args.mount_source[0])[0]
        else:
            args.mount_point = autoMountPoint
    args.mount_point = os.path.abspath(args.mount_point)

    # Preprocess the --index-folders list as a string argument
    if args.index_folders:
        if args.index_folders[0] == '[':
            args.index_folders = json.loads(args.index_folders)
        elif ',' in args.index_folders:
            args.index_folders = args.index_folders.split(',')

    # Check the parallelization argument and move to global variable
    if args.parallelization < 0:
        raise argparse.ArgumentTypeError("Argument for parallelization must be non-negative!")
    global parallelization
    parallelization = args.parallelization if args.parallelization > 0 else os.cpu_count()

    # Sanitize different ways to specify passwords into a simple list
    args.passwords = []
    if args.password:
        args.passwords.append(args.password)

    if args.password_file:
        with open(args.password_file, 'rb') as file:
            args.passwords += file.read().split(b'\n')

    args.passwords = _removeDuplicatesStable(args.passwords)

    return args


def cli(rawArgs: Optional[List[str]] = None) -> None:
    """Command line interface for ratarmount. Call with args = [ '--help' ] for a description."""

    # The first argument, is the path to the script and should be ignored
    tmpArgs = sys.argv[1:] if rawArgs is None else rawArgs
    if '--version' in tmpArgs or '-v' in tmpArgs:
        print("ratarmount", __version__)
        return

    # tmpArgs are only for the manual parsing. In general, rawArgs is None, meaning it reads sys.argv,
    # and maybe sometimes contains arguments when used programmatically. In that case the first argument
    # should not be the path to the script!
    args = _parseArgs(rawArgs)

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

    global printDebug
    printDebug = args.debug

    fuseOperationsObject = FuseMount(
        # fmt: off
        pathToMount                = args.mount_source,
        clearIndexCache            = args.recreate_index,
        recursive                  = args.recursive,
        gzipSeekPointSpacing       = args.gzipSeekPointSpacing,
        mountPoint                 = args.mount_point,
        encoding                   = args.encoding,
        ignoreZeros                = args.ignore_zeros,
        verifyModificationTime     = args.verify_mtime,
        stripRecursiveTarExtension = args.strip_recursive_tar_extension,
        indexFileName              = args.index_file,
        indexFolders               = args.index_folders,
        lazyMounting               = args.lazy,
        passwords                  = args.passwords,
        # fmt: on
    )

    fuse.FUSE(
        # fmt: on
        operations=fuseOperationsObject,
        mountpoint=args.mount_point,
        foreground=args.foreground,
        nothreads=True,  # Can't access SQLite database connection object from multiple threads
        # fmt: off
        **fusekwargs
    )


if __name__ == '__main__':
    cli(sys.argv[1:])
