import contextlib
import copy
import io
import json
import math
import os
import re
import sqlite3
import stat
import sys
import tarfile
import threading
import time
import traceback
import urllib.parse
from timeit import default_timer as timer
from typing import IO, Any, Callable, Dict, Generator, Iterable, List, Optional, Tuple, Union, cast

with contextlib.suppress(ImportError):
    import rapidgzip

from ratarmountcore.BlockParallelReaders import ParallelXZReader
from ratarmountcore.compressions import COMPRESSION_BACKENDS, get_gzip_info, open_compressed_file
from ratarmountcore.formats import FileFormatID, might_be_format
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.ProgressBar import ProgressBar
from ratarmountcore.SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from ratarmountcore.StenciledFile import RawStenciledFile, StenciledFile
from ratarmountcore.utils import (
    CompressionError,
    InvalidIndexError,
    RatarmountError,
    ceil_div,
    decode_unpadded_base64,
    determine_recursion_depth,
    get_xdg_cache_home,
    overrides,
)


class _TarFileMetadataReader:
    def __init__(
        self,
        parent: 'SQLiteIndexedTar',
        setFileInfos: Callable[[List[Tuple]], None],
        setxattrs: Callable[[List[Tuple]], None],
        updateProgressBar: Callable[[], None],
        recursionDepth: int,
    ):
        self._parent = parent
        self._set_file_infos = setFileInfos
        self._setxattrs = setxattrs
        self._update_progress_bar = updateProgressBar
        self._recursionDepth = recursionDepth
        self._lastUpdateTime = time.time()

    @staticmethod
    def _get_tar_prefix(fileObject: IO[bytes], tarInfo: tarfile.TarInfo, printDebug: int) -> Optional[bytes]:
        """Get the actual prefix as stored in the TAR."""

        # Offsets taken from https://en.wikipedia.org/wiki/Tar_(computing)#UStar_format
        def extract_prefix(tarBlockOffset):
            fileObject.seek(tarBlockOffset + 345)
            return fileObject.read(155)

        def extract_name(tarBlockOffset):
            fileObject.seek(tarBlockOffset)
            return fileObject.read(100)

        def extract_size(tarBlockOffset):
            fileObject.seek(tarBlockOffset + 124)
            return int(fileObject.read(12).strip(b"\0"), 8)  # octal encoded file size TODO might also be base64

        oldPosition = fileObject.tell()

        # Normally, getting the prefix, could be as easy as calling extract_prefix.
        # But, for long-names the prefix will not be prefixed but for long links it will be prefixed by tarfile.
        # This complicates things. Also, both long link and long name are implemented by a prepended
        # tar block with the special file name "././@LongLink" and tarfile will return the header offset of the
        # corresponding GNU LongLink file header in the TarInfo object instead of the actual file header, which
        # contains the prefix.
        try:
            if extract_name(tarInfo.offset).startswith(b"././@LongLink\0"):
                nextHeaderOffset = tarInfo.offset + 512 + (extract_size(tarInfo.offset) + 512 - 1) // 512 * 512
                return extract_prefix(nextHeaderOffset)
            return extract_prefix(tarInfo.offset)

        except Exception as exception:
            if printDebug >= 1:
                print("[Warning] Encountered exception when trying to get TAR prefix", exception)
            if printDebug >= 3:
                traceback.print_exc()

        finally:
            fileObject.seek(oldPosition)

        return None

    @staticmethod
    def _tar_info_full_mode(tarInfo: tarfile.TarInfo) -> int:
        """
        Returns the full mode for a TarInfo object. Note that TarInfo.mode only contains the permission bits
        and not other bits like set for directory, symbolic links, and other special files.
        """

        # fmt: off
        return (
            tarInfo.mode
            | ( stat.S_IFDIR if tarInfo.isdir () else 0 )
            | ( stat.S_IFREG if tarInfo.isfile() or tarInfo.type == b'D' else 0 )
            | ( stat.S_IFLNK if tarInfo.issym () else 0 )
            | ( stat.S_IFCHR if tarInfo.ischr () else 0 )
            | ( stat.S_IFIFO if tarInfo.isfifo() else 0 )
        )
        # fmt: on

    @staticmethod
    def _fix_incremental_backup_name_prefixes(fileObject: IO[bytes], tarInfo: tarfile.TarInfo, printDebug: int):
        """
        Tarfile joins the TAR prefix with the file path.
        However, for incremental TARs, the prefix is an octal timestamp and should be ignored.
        This function reads the raw prefix from the TAR file and removes it from the TarInfo object's path
        if the prefix is an octal number indicating an incremental archive prefix.
        """

        if '/' not in tarInfo.name:
            return

        fixedPath = None
        prefix, name = tarInfo.name.split('/', 1)

        realPrefix = _TarFileMetadataReader._get_tar_prefix(fileObject, tarInfo, printDebug)
        encodedPrefix = prefix.encode('utf8', 'surrogateescape')

        # For names longer than 100B, GNU tar will store it using a ././@LongLink named file.
        # In this case, tarfile will ignore the truncated filename AND the octal timestamp prefix!
        # However, for long symbolic links, the prefix WILL be prepended to the @LongLink contents!
        # In order to not strip folders erroneously, test against this prefix. Unfortunately, this is
        # not perfect either because tarfile removes trailing slashes for names. So we have to
        # read the TAR information ourselves.
        # Note that the prefix contains two not always identical octal timestamps! E.g.,
        #   b'13666753432\x0013666377326\x00\x00\x00...
        # We only test for the first here as I'm not sure what the second one is.
        # In some cases instead of the octal timestamp there will be unknown binary data!
        # Because of this the data is not asserted to be octal.
        if realPrefix and realPrefix.startswith(encodedPrefix + b"\0"):
            fixedPath = name

        if fixedPath is None and printDebug >= 1:
            print(f"[Warning] ignored prefix '{encodedPrefix!r}' because it was not found in TAR header prefix.")
            print("[Warning]", realPrefix[:30] if realPrefix else realPrefix)
            print(f"[Info] TAR header offset: {tarInfo.offset}, type: {tarInfo.type!s}")
            print("[Info] name:", tarInfo.name)
            print()

        if fixedPath is not None:
            tarInfo.name = fixedPath

    # fmt: off
    @staticmethod
    def _process_tar_info(
        tarInfo          : tarfile.TarInfo,
        fileObject       : IO[bytes],
        pathPrefix       : str,
        streamOffset     : int,
        isGnuIncremental : Optional[bool],
        mountRecursively : bool,
        transform        : Callable[[str], str],
        recursionDepth   : int,
        printDebug       : int,
    ) -> Tuple[List[Tuple], List[Tuple], bool, Optional[bool]]:
        # fmt: on
        """Postprocesses a TarInfo object into one or multiple FileInfo tuples."""

        if tarInfo.type == b'D' and not isGnuIncremental:
            isGnuIncremental = True
            if printDebug >= 1:
                print(f"[Warning] A folder metadata entry ({tarInfo.name}) for GNU incremental archives")
                print("[Warning] was encountered but this archive was not automatically recognized as such!")
                print("[Warning] Please call ratarmount with the --gnu-incremental flag if there are problems.")
                print()

        if isGnuIncremental:
            _TarFileMetadataReader._fix_incremental_backup_name_prefixes(fileObject, tarInfo, printDebug)

        offsetHeader = streamOffset + tarInfo.offset

        prefix = 'SCHILY.xattr.'
        xattrs = {
            key[len(prefix) :]: value.encode('utf-8', errors='surrogateescape')
            for key, value in tarInfo.pax_headers.items()
            if key.startswith(prefix)
        }
        prefix = 'LIBARCHIVE.xattr.'
        xattrs.update(
            {
                urllib.parse.unquote(key[len(prefix) :]): decode_unpadded_base64(value)
                for key, value in tarInfo.pax_headers.items()
                if key.startswith(prefix)
            }
        )
        xattrRows = [(offsetHeader, key, value) for key, value in xattrs.items()]

        fullPath = pathPrefix + "/" + tarInfo.name
        if tarInfo.isdir():
            fullPath += "/"
        path, name = SQLiteIndex.normpath(transform(fullPath)).rsplit("/", 1)

        # TODO: As for the tarfile type SQLite expects int but it is generally bytes.
        #       Most of them would be convertible to int like tarfile.SYMTYPE which is b'2',
        #       but others should throw errors, like GNUTYPE_SPARSE which is b'S'.
        #       When looking at the generated index, those values get silently converted to 0?
        # fmt: off
        fileInfo : Tuple = (
            path                                            ,  # 0  : path
            name                                            ,  # 1  : file name
            offsetHeader                                    ,  # 2  : header offset
            streamOffset + tarInfo.offset_data              ,  # 3  : data offset
            tarInfo.size                                    ,  # 4  : file size
            tarInfo.mtime                                   ,  # 5  : modification time
            _TarFileMetadataReader._tar_info_full_mode(tarInfo),  # 6  : file mode / permissions
            tarInfo.type                                    ,  # 7  : TAR file type. Currently unused.
            tarInfo.linkname                                ,  # 8  : linkname
            tarInfo.uid                                     ,  # 9  : user ID
            tarInfo.gid                                     ,  # 10 : group ID
            False                                           ,  # 11 : is TAR (unused?)
            tarInfo.issparse()                              ,  # 12 : is sparse
            False                                           ,  # 13 : is generated (parent folder)
            recursionDepth                                  ,  # 14 : recursion depth
        )
        # fmt: on

        fileInfos = [fileInfo]

        if mountRecursively and tarInfo.isfile() and tarInfo.name.lower().endswith('.tar'):
            return fileInfos, xattrRows, True, isGnuIncremental

        # Add GNU incremental TAR directory metadata files also as directories
        if tarInfo.type == b'D':
            dirFileInfo = list(fileInfo)
            # This is only to get a unique primary key :/
            # Then again, TAR blocks are known to be on 512B boundaries, so the lower
            # bits in the offset are redundant anyway.
            dirFileInfo[2] += 1
            dirFileInfo[4] = 0  # directory entries have no size by convention
            dirFileInfo[6] = tarInfo.mode | stat.S_IFDIR
            fileInfos.append(tuple(dirFileInfo))

        return fileInfos, xattrRows, False, isGnuIncremental

    @staticmethod
    def find_tar_file_offsets(fileObject: IO[bytes], ignoreZeros: bool) -> Generator[Tuple[int, bytes], None, None]:
        """
        Generator which yields offsets in the given TAR suitable for splitting the file into sub TARs.
        Also returns the type of the TAR metadata block at the returned offset for convenience.
        """

        blockNumber = 0
        skipNextBlocks = 0
        fileObject.seek(0)

        while True:
            blockContents = fileObject.read(512)
            if len(blockContents) < 512:
                break

            # > The end of an archive is marked by at least two consecutive zero-filled records.
            if blockContents == b"\0" * 512:
                blockContents = fileObject.read(512)
                if blockContents == b"\0" * 512:
                    if ignoreZeros:
                        continue
                    break

                if len(blockContents) < 512:
                    break

            typeFlag = blockContents[156:157]

            if skipNextBlocks > 0:
                skipNextBlocks -= 1
            else:
                yield blockNumber * 512, typeFlag

            blockNumber += 1
            rawSize = blockContents[124 : 124 + 12].strip(b"\0")
            size = int(rawSize, 8) if rawSize else 0
            blockNumber += ceil_div(size, 512)
            fileObject.seek(blockNumber * 512)

            # A lot of the special files contain information about the next file, therefore keep do not yield
            # the offset of the next block so that the TAR will not be split between them.
            # K: Identifies the *next* file on the tape as having a long name.
            # L: Identifies the *next* file on the tape as having a long linkname.
            # x: Extended header with meta data for the next file in the archive (POSIX.1-2001)
            # 0: Normal file.
            if typeFlag != b'0':
                skipNextBlocks += 1

    def _open_tar(self, fileObject: IO[bytes]):
        """
        Opens the fileObject with the appropriate settings using the tarfile module.
        Instead of throwing, an empty iterable might be returned.
        """

        if not self._parent.isTar:
            return []  # Feign an empty TAR file (iterable) if anything goes wrong

        try:
            # r: uses seeks to skip to the next file inside the TAR while r| doesn't do any seeks.
            # r| might be slower but for compressed files we have to go over all the data once anyways.
            # Note that with ignore_zeros = True, no invalid header issues or similar will be raised even for
            # non TAR files!?
            # fmt:off
            return tarfile.open(
                fileobj      = fileObject,
                mode         = 'r|' if self._parent.compression else 'r:',
                ignore_zeros = self._parent.ignoreZeros,
                encoding     = self._parent.encoding,
            )
            # fmt:on
        except tarfile.ReadError:
            pass

        return []

    def _process_serial(self, fileObject: IO[bytes], pathPrefix: str, streamOffset: int) -> Iterable[Tuple]:
        """
        Opens the given fileObject using the tarfile module, iterates over all files converting their metadata to
        FileInfo tuples and inserting those into the database in a chunked manner using the given _set_file_infos.
        """

        if self._recursionDepth > self._parent.maxRecursionDepth:
            return []

        loadedTarFile: Any = self._open_tar(fileObject)

        # Iterate over files inside TAR and add them to the database
        fileInfos: List[Tuple] = []
        xattrs: List[Tuple] = []
        filesToMountRecursively: List[Tuple] = []

        # thread_time is twice as fast, which can shave off 10% of time in some tests but it is not as "correct"
        # because it does not count the sleep time of the thread, e.g., caused by waiting for I/O or even waiting
        # for work done inside multiprocessing.pool.Pool! This can lead to more than factor 10 distortions and
        # therefore is not suitable. If time.time is indeed an issue, then it should be better to use _processParallel.
        self._lastUpdateTime = time.time()

        try:
            for tarInfo in loadedTarFile:
                loadedTarFile.members = []  # Clear this in order to limit memory usage by tarfile

                # ProgressBar does a similar check like this inside 'update' but doing this outside avoids huge
                # call stacks and also avoids calling tell() on the file object in each loop iteration.
                # I could observe 10% shorter runtimes because of this with the test file:
                #     tar-with-1000-folders-with-1000-files-0B-files.tar
                if time.time() - self._lastUpdateTime >= 1:
                    self._lastUpdateTime = time.time()
                    self._update_progress_bar()

                newFileInfos, newXAttrs, mightBeTar, self._parent._isGnuIncremental = (
                    _TarFileMetadataReader._process_tar_info(
                        tarInfo,
                        fileObject=fileObject,
                        pathPrefix=pathPrefix,
                        streamOffset=streamOffset,
                        isGnuIncremental=self._parent._isGnuIncremental,
                        mountRecursively=self._recursionDepth < self._parent.maxRecursionDepth,
                        transform=self._parent.transform,
                        recursionDepth=self._recursionDepth,
                        printDebug=self._parent.printDebug,
                    )
                )

                if mightBeTar:
                    filesToMountRecursively.extend(newFileInfos)

                fileInfos.extend(newFileInfos)
                if len(fileInfos) > 1000:
                    self._set_file_infos(fileInfos)
                    fileInfos.clear()

                xattrs.extend(newXAttrs)
                if len(xattrs) > 1000:
                    self._setxattrs(xattrs)
                    xattrs.clear()

        finally:
            self._set_file_infos(fileInfos)
            self._setxattrs(xattrs)

        return filesToMountRecursively

    def process(self, fileObject: IO[bytes], pathPrefix: str, streamOffset: int) -> Iterable[Tuple]:
        """
        Iterates over all files inside the given fileObject TAR and inserts their metadata into the database using
        the given _set_file_infos.
        A list of files which might be of interest for recursive mounting of uncompressed TARs is returned.
        """

        try:
            return self._process_serial(fileObject, pathPrefix, streamOffset)

        except tarfile.ReadError as e:
            if 'unexpected end of data' in str(e):
                print(
                    "[Warning] The TAR file is incomplete. Ratarmount will work but some files might be cut off. "
                    "If the TAR file size changes, ratarmount will recreate the index during the next mounting."
                )
                if self._parent.printDebug >= 3:
                    traceback.print_exc()

        return []


class SQLiteIndexedTar(SQLiteIndexMountSource):
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    DEFAULT_GZIP_SEEK_POINT_SPACING = 16 * 1024 * 1024

    # fmt: off
    def __init__(
        self,
        tarFileName                  : Optional[Union[str, os.PathLike]] = None,
        fileObject                   : Optional[IO[bytes]]               = None,
        *,  # force all parameters after to be keyword-only
        writeIndex                   : bool                              = False,
        clearIndexCache              : bool                              = False,
        indexFilePath                : Optional[str]                     = None,
        indexFolders                 : Optional[List[str]]               = None,
        recursive                    : bool                              = False,
        gzipSeekPointSpacing         : int                               = DEFAULT_GZIP_SEEK_POINT_SPACING,
        encoding                     : str                               = tarfile.ENCODING,
        stripRecursiveTarExtension   : bool                              = False,
        ignoreZeros                  : bool                              = False,
        verifyModificationTime       : bool                              = False,
        parallelization              : int                               = 1,
        parallelizations             : Optional[Dict[str, int]]          = None,
        isGnuIncremental             : Optional[bool]                    = None,
        printDebug                   : int                               = 0,
        transformRecursiveMountPoint : Optional[Tuple[str, str]]         = None,
        transform                    : Optional[Tuple[str, str]]         = None,
        prioritizedBackends          : Optional[List[str]]               = None,
        indexMinimumFileCount        : int                               = 0,
        recursionDepth               : Optional[int]                     = None,
        # pylint: disable=unused-argument
        **kwargs
    ) -> None:
        """
        tarFileName
            Path to the TAR file to be opened. If not specified, a fileObject must be specified.
            If only a fileObject is given, the created index can't be cached (efficiently).
        fileObject
            A io.IOBase derived object. If not specified, tarFileName will be opened.
            If it is an instance of IndexedBzip2File, IndexedGzipFile, or IndexedZstdFile, then the offset
            loading and storing from and to the SQLite database is managed automatically by this class.
        writeIndex
            If true, then the sidecar index file will be written to a suitable location.
            Will be ignored if indexFilePath is ':memory:' or if only fileObject is specified
            but not tarFileName.
        clearIndexCache
            If true, then check all possible index file locations for the given tarFileName/fileObject
            combination and delete them. This also implicitly forces a recreation of the index.
        indexFilePath
            Path to the index file for this TAR archive. This takes precedence over the automatically
            chosen locations. If it is ':memory:', then the SQLite database will be kept in memory
            and not stored to the file system at any point.
        indexFolders
            Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for
            suitability in the given order. An empty path will be interpreted as the location in which
            the TAR resides.
        recursive
            If true, then TAR files inside this archive will be recursively analyzed and added to the SQLite
            index. Currently, this recursion can only break the outermost compression layer. I.e., a .tar.bz2
            file inside a tar.bz2 file can not be mounted recursively.
        gzipSeekPointSpacing
            This controls the frequency of gzip decoder seek points, see indexed_gzip documentation.
            Larger spacings lead to less memory usage but increase the constant seek overhead.
        encoding
            Will be forwarded to tarfile. Specifies how filenames inside the TAR are encoded.
        ignoreZeros
            Will be forwarded to tarfile. Specifies to not only skip zero blocks but also blocks with
            invalid data. Setting this to true can lead to some problems but is required to correctly
            read concatenated tars.
        stripRecursiveTarExtension
            If true and if recursive is also true, then a <file>.tar inside the current
            tar will be mounted at <file>/ instead of <file>.tar/.
        transformRecursiveMountPoint
            If specified, then a <path>.tar inside the current tar will be matched with the
            first argument of the tuple and replaced by the second argument. This new
            modified path is used as recursive mount point. See also Python's re.sub.
        verifyModificationTime
            If true, then the index will be recreated automatically if the TAR archive has a more
            recent modification time than the index file.
        parallelization
            The amount of parallelization to be used for necessary compression backends if it offers
            parallelization at all and if it makes sense.
        parallelizations
            Fine-granular parallelization for each compression backend. Set the empty string
            key to the default parallelization for backends not explicitly in the dictionary.
            This has higher precedence than 'parallelization'.
        isGnuIncremental
            If None, then it will be determined automatically. Behavior can be overwritten by setting
            it to a bool value. If true, then prefixes will be stripped from certain paths encountered
            with GNU incremental backups.
        kwargs
            Unused. Only for compatibility with generic MountSource interface.
        """

        self.encoding                     = encoding
        self.stripRecursiveTarExtension   = stripRecursiveTarExtension
        self.transformRecursiveMountPoint = transformRecursiveMountPoint
        self.transformPattern             = transform
        self.ignoreZeros                  = ignoreZeros
        self.verifyModificationTime       = verifyModificationTime
        self.gzipSeekPointSpacing         = gzipSeekPointSpacing
        self.printDebug                   = printDebug
        self.isFileObject                 = fileObject is not None
        self._isGnuIncremental            = isGnuIncremental
        self.hasBeenAppendedTo            = False
        self._recursionDepth              = -1
        # fmt: on
        self.prioritizedBackends: List[str] = [] if prioritizedBackends is None else prioritizedBackends
        self.maxRecursionDepth = determine_recursion_depth(recursive=recursive, recursionDepth=recursionDepth)
        self.parallelizations = copy.deepcopy(parallelizations) if parallelizations else {}
        if '' not in self.parallelizations:
            self.parallelizations[''] = parallelization

        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )

        # Determine an archive file name to show for debug output and as file name inside the mount point for
        # simple non-TAR gzip/bzip2 stream-compressed files.
        self.tarFileName: str
        if fileObject:
            self.tarFileName = str(tarFileName or '<file object>')
        else:
            if tarFileName:
                # Keep the EXACT file path, do not convert to an absolute path, or else we might trigger
                # recursive FUSE calls, which hangs everything!
                self.tarFileName = str(tarFileName)
            else:
                raise RatarmountError("At least one of tarFileName and fileObject arguments should be set!")
        self._fileNameIsURL = re.match('[A-Za-z0-9]*://', self.tarFileName) is not None

        # If no fileObject given, then self.tarFileName is the path to the archive to open.
        self._fileObjectsToCloseOnDel: List[IO[bytes]] = []
        if not fileObject:
            fileObject = open(self.tarFileName, 'rb')
            self._fileObjectsToCloseOnDel.append(fileObject)
        fileObject.seek(0, io.SEEK_END)
        fileSize = fileObject.tell()
        fileObject.seek(0)  # Even if not interested in the file size, seeking to the start might be useful.
        self._archiveFileSize = fileSize

        # rawFileObject : Only set when opening a compressed file and only kept to keep the
        #                 compressed file handle from being closed by the garbage collector.
        # tarFileObject : File object to the uncompressed (or decompressed) TAR file to read actual data out of.
        # compression   : Stores what kind of compression the originally specified TAR file uses.
        # isTar         : Can be false for the degenerated case of only a bz2 or gz file not containing a TAR
        self.compression: Optional[FileFormatID] = None
        self.tarFileObject, self.rawFileObject, self.compression = open_compressed_file(
            fileObject,
            gzipSeekPointSpacing=gzipSeekPointSpacing,
            parallelizations=self.parallelizations,
            enabledBackends=[
                backend for backend, info in COMPRESSION_BACKENDS.items() if info.delegatedArchiveBackend == 'tarfile'
            ],
            prioritizedBackends=self.prioritizedBackends,
            printDebug=self.printDebug,
        )
        self.isTar = might_be_format(self.tarFileObject, FileFormatID.TAR)
        if not self.isTar:
            if printDebug >= 3:
                print(f"[Info] File object {self.tarFileObject} from {self.tarFileName} is not a TAR.")
            if not self.rawFileObject:
                raise RatarmountError(f"File object ({fileObject!s}) could not be opened as a TAR file!")

        if self.compression:
            self._recursionDepth += 1
            # Change the default from 0 to 1 to undo the compression layer and analyze the TAR if nothing is specified.
            if not recursive and recursionDepth is None:
                self.maxRecursionDepth = 1

        # Can only be set correctly after the compression has been detected because it determines the default
        # recursion depth. This is legacy, i.e., only to add the correct value to 'metadata' to be backward compatible.
        self.mountRecursively = self.maxRecursionDepth > 0

        # Try to get block size from the real opened file.
        self.blockSize = 512
        try:
            if self.rawFileObject:
                self.blockSize = os.fstat(self.rawFileObject.fileno()).st_blksize
            elif self.tarFileObject:
                self.blockSize = os.fstat(self.tarFileObject.fileno()).st_blksize
        except Exception:
            pass

        if self.compression == FileFormatID.GZIP:
            self.blockSize = max(self.blockSize, gzipSeekPointSpacing)
        elif self.compression == FileFormatID.BZIP2:
            # There is no API yet to query the exact block size, but most bzip2 files have 900K blocks.
            # The bzip2 block size is in reference to the BWT buffer, so the decompressed block data will be
            # larger to some extend. Therefore, 1 MiB blocks should be an alright guess for all bzip2 files.
            self.blockSize = 1024 * 1024
        # TODO derive some meaningful block sizes for zstd and xz

        self.fileObjectLock = threading.Lock()

        if (
            self.compression == FileFormatID.XZ
            and len(getattr(self.tarFileObject, 'block_boundaries', (0, 0))) <= 1
            and self._archiveFileSize > 1024 * 1024
        ):
            print(f"[Warning] The specified file '{self.tarFileName}'")
            print("[Warning] is compressed using xz but only contains one xz block. This makes it ")
            print("[Warning] impossible to use true seeking! Please (re)compress your TAR using pixz")
            print("[Warning] (see https://github.com/vasi/pixz) in order for ratarmount to do be able ")
            print("[Warning] to do fast seeking to requested files.")
            print("[Warning] As it is, each file access will decompress the whole TAR from the beginning!")
            print()

        if indexFolders is None:
            indexFolders = ['', os.path.join("~", ".ratarmount")]
            xdgCacheHome = get_xdg_cache_home()
            if xdgCacheHome and os.path.isdir(os.path.expanduser(xdgCacheHome)):
                indexFolders.insert(1, os.path.join(xdgCacheHome, 'ratarmount'))
        elif isinstance(indexFolders, str):
            indexFolders = [indexFolders]

        archiveFilePath = self.tarFileName if not self.isFileObject or self._fileNameIsURL else None

        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=archiveFilePath,
                encoding=self.encoding,
                printDebug=self.printDebug,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='SQLiteIndexedTar',
                ignoreCurrentFolder=self.isFileObject and self._fileNameIsURL,
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._check_metadata,
        )

        if self.index.index_is_loaded():
            if not self.hasBeenAppendedTo:  # indirectly set by a successful call to _try_load_index
                self._load_or_store_compression_offsets()  # load
                self.index.reload_index_read_only()
                return

            # TODO Handling appended files to compressed archives would have to account for dropping the offsets,
            #      seeking to the first appended file while not processing any metadata and still showing a progress
            #      bar as well as saving the block offsets out after reading and possibly other things.
            if self.compression:
                # When loading compression offsets, the backends assume they are complete, so we have to clear them.
                self.index.clear_compression_offsets()

            pastEndOffset = self._get_past_end_offset(self.index.get_connection())
            if not self.compression and pastEndOffset and self._check_index_validity():
                archiveSize = self.tarFileObject.seek(0, io.SEEK_END)

                newShare = (archiveSize - pastEndOffset) / archiveSize
                print(f"Detected TAR being appended to. Will only analyze the newly added {newShare:.2f} % of data.")

                appendedPartAsFile = StenciledFile(
                    fileStencils=[(self.tarFileObject, pastEndOffset, archiveSize - pastEndOffset)]
                )
                self._create_index(appendedPartAsFile, streamOffset=pastEndOffset)

                self._load_or_store_compression_offsets()  # store

                self.index.drop_metadata()
                self._store_metadata()
                self.index.reload_index_read_only()
                return

            self.index.close()
            print("[Warning] The loaded index does not match the archive. Will recreate it.")

        # TODO This does and did not work correctly for recursive TARs because the outermost layer will change
        #      None to a hard value and from then on it would have been fixed to that value even when called
        #      inside createIndex.
        # Required for _create_index
        if self._isGnuIncremental is None:
            self._isGnuIncremental = self._detect_gnu_incremental(self.tarFileObject)

        # Open new database when we didn't find an existing one.
        if not self.index.index_is_loaded():
            # Simply open in memory without an error even if writeIndex is True but when no indication
            # for an index file location has been given.
            if writeIndex and (indexFilePath or self._get_archive_path() or not self.isFileObject):
                self.index.open_writable()
            else:
                self.index.open_in_memory()

        self._create_index(self.tarFileObject)
        self._load_or_store_compression_offsets()  # store
        if self.index.index_is_loaded():
            self._store_metadata()
            self.index.reload_index_read_only()

        if self.printDebug >= 1 and self.index.indexFilePath and os.path.isfile(self.index.indexFilePath):
            # The 0-time is legacy for the automated tests
            # fmt: off
            print("Writing out TAR index to", self.index.indexFilePath, "took 0s",
                  "and is sized", os.stat( self.index.indexFilePath ).st_size, "B")
            # fmt: on

    def __del__(self):
        if hasattr(self, '_fileObjectsToCloseOnDel'):
            for fileObject in self._fileObjectsToCloseOnDel:
                close = getattr(fileObject, 'close', None)
                if close and callable(close):
                    close()
        if hasattr(super(), '__del__'):
            super().__del__()

    def _detect_gnu_incremental(self, fileObject: Any) -> bool:
        """Check for GNU incremental backup TARs."""
        oldPos = fileObject.tell()

        t0 = time.time()
        try:
            # For an uncompressed 500MB TAR, this iteration took ~0.7s for 1M files roughly 30x faster than tarfile.
            # But for compressed TARs or for HDDs as opposed to SSDs, this might be much slower.
            nMaxToTry = 1000 if self.isFileObject or self.compression else 10000
            for _, typeFlag in _TarFileMetadataReader.find_tar_file_offsets(fileObject, self.ignoreZeros):
                # It seems to be possible to create mixtures of incremental archives and normal contents,
                # therefore do not check that all files must have the mtime prefix.
                if typeFlag == b'D':
                    if self.printDebug >= 1:
                        print("[Info] Detected GNU incremental TAR.")
                    return True

                nMaxToTry -= 1
                if nMaxToTry <= 0 or time.time() - t0 > 3:
                    break

        except Exception as exception:
            if self.printDebug >= 4:
                print("[Info] TAR was not recognized as GNU incremental TAR because of exception", exception)
            if self.printDebug >= 4:
                traceback.print_exc()
        finally:
            fileObject.seek(oldPos)

        return False

    def close(self):
        if self.tarFileObject:
            self.tarFileObject.close()

        if not self.isFileObject and self.rawFileObject:
            self.rawFileObject.close()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()
        super().__exit__(exception_type, exception_value, exception_traceback)

    def _get_archive_path(self) -> Optional[str]:
        return None if self.tarFileName == '<file object>' else self.tarFileName

    def _store_metadata(self) -> None:
        argumentsToSave = [
            'mountRecursively',
            'gzipSeekPointSpacing',
            'encoding',
            'stripRecursiveTarExtension',
            'transformRecursiveMountPoint',
            'transformPattern',
            'ignoreZeros',
        ]

        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        # The second argument must be a path to a file to call os.stat with, not simply a file name.
        self.index.store_metadata(argumentsMetadata, None if self.isFileObject else self.tarFileName)
        self.index.store_metadata_key_value('isGnuIncremental', '1' if self._isGnuIncremental else '0')

    def _update_progress_bar(self, progressBar, fileobj: Any) -> None:
        if not progressBar:
            return

        try:
            if (
                hasattr(fileobj, 'tell_compressed')
                and 'rapidgzip' in sys.modules
                and isinstance(fileobj, (rapidgzip.IndexedBzip2File, rapidgzip.RapidgzipFile))
            ):
                # Note that because bz2 works on a bitstream the tell_compressed returns the offset in bits
                progressBar.update(fileobj.tell_compressed() // 8)
            elif hasattr(fileobj, 'tell_compressed'):
                progressBar.update(fileobj.tell_compressed())
            elif hasattr(fileobj, 'fileobj') and callable(fileobj.fileobj):
                progressBar.update(fileobj.fileobj().tell())
            elif isinstance(fileobj, ParallelXZReader):
                blockNumber = fileobj.find_block(fileobj.tell())
                if blockNumber and blockNumber < len(fileobj.approximateCompressedBlockBoundaries):
                    progressBar.update(fileobj.approximateCompressedBlockBoundaries[blockNumber])
            elif self.rawFileObject and hasattr(self.rawFileObject, 'tell'):
                progressBar.update(self.rawFileObject.tell())
            else:
                progressBar.update(fileobj.tell())
        except Exception as exception:
            if self.printDebug >= 1:
                print("An exception occurred when trying to update the progress bar:", exception)
            if self.printDebug >= 3:
                traceback.print_exc()

    def _create_index(self, fileObject: IO[bytes], streamOffset: int = 0) -> None:
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.tarFileName} ...")
        t0 = timer()

        self.index.ensure_intermediary_tables()

        progressBar = ProgressBar(self._archiveFileSize)
        self._create_index_recursively(
            fileObject,
            progressBar=progressBar,
            pathPrefix="",
            streamOffset=streamOffset,
            recursionDepth=self._recursionDepth + 1,
        )

        # Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if self.printDebug >= 2:
            print("Resorting files by path ...")

        self.index.finalize()

        t1 = timer()
        if self.printDebug >= 1:
            print(f"Creating offset dictionary for {self.tarFileName} took {t1 - t0:.2f}s")

    def _create_index_recursively(
        self, fileObject: IO[bytes], progressBar: ProgressBar, pathPrefix: str, streamOffset: int, recursionDepth: int
    ) -> None:
        metadataReader = _TarFileMetadataReader(
            self,
            self.index.set_file_infos,
            self.index.setxattrs,
            lambda: self._update_progress_bar(progressBar, fileObject),
            recursionDepth=recursionDepth,
        )
        filesToMountRecursively = metadataReader.process(fileObject, pathPrefix, streamOffset)

        # 4. Open contained TARs for recursive mounting
        oldPos = fileObject.tell()
        oldPrintName = self.tarFileName
        for fileInfo in filesToMountRecursively:
            modifiedFolder = fileInfo[0]
            modifiedName = fileInfo[1]

            # Strip file extension for mount point if so configured
            tarExtension = '.tar'
            if (
                self.stripRecursiveTarExtension
                and len(tarExtension) > 0
                and modifiedName.lower().endswith(tarExtension.lower())
            ):
                modifiedName = modifiedName[: -len(tarExtension)]

            # Apply regex transformation to get mount point
            pattern = self.transformRecursiveMountPoint
            modifiedPath = '/' + (modifiedFolder + '/' + modifiedName).lstrip('/')
            if isinstance(pattern, (tuple, list)) and len(pattern) == 2:
                modifiedPath = '/' + re.sub(pattern[0], pattern[1], modifiedPath).lstrip('/')
                modifiedFolder, modifiedName = modifiedPath.rsplit('/', 1)

            # Temporarily change tarFileName for the info output of the recursive call
            self.tarFileName = os.path.join(fileInfo[0], fileInfo[1])

            # StenciledFile's tell returns the offset inside the file chunk instead of the global one,
            # so we have to always communicate the offset of this chunk to the recursive call no matter
            # whether tarfile has streaming access or seeking access!
            globalOffset = fileInfo[3]
            size = fileInfo[4]
            # fileObject already effectively applies streamOffset, so we can't use the globalOffset here!
            # For all supported cases, it should be fine to directly use self.tarFileObject instead of fileObject.
            # This would also save some indirections to speed up accesses.
            tarFileObject = StenciledFile([(self.tarFileObject, globalOffset, size)])

            isTar = False
            try:
                # Do not use os.path.join here because the leading / might be missing.
                # This should instead be seen as the reverse operation of the rsplit further above.
                self._create_index_recursively(
                    tarFileObject,
                    progressBar,
                    modifiedPath,
                    streamOffset=globalOffset,
                    recursionDepth=recursionDepth + 1,
                )
                isTar = True
            except tarfile.ReadError:
                pass
            finally:
                del tarFileObject

            if isTar:
                modifiedFileInfo = list(fileInfo)

                # if the TAR file contents could be read, we need to adjust the actual
                # TAR file's metadata to be a directory instead of a file.
                # Avoid overwriting that data, instead add new one such that it can be versioned
                # or ignored on depending on the recursion depth!
                mode = modifiedFileInfo[6]
                mode = (
                    (mode & 0o777)
                    | stat.S_IFDIR
                    | (stat.S_IXUSR if mode & stat.S_IRUSR != 0 else 0)
                    | (stat.S_IXGRP if mode & stat.S_IRGRP != 0 else 0)
                    | (stat.S_IXOTH if mode & stat.S_IROTH != 0 else 0)
                )

                if modifiedFolder != modifiedFileInfo[0] or modifiedName != modifiedFileInfo[1]:
                    modifiedFileInfo[0] = modifiedFolder
                    modifiedFileInfo[1] = modifiedName
                else:
                    # Increment offset and offsetheader such that the new folder is seen as a more recent version
                    # of the already existing file path for the archive if it has the same path. Else, it would
                    # be undetermined which version is to be counted as more recent when using ORDER BY offsetheader.
                    # Note that offset and offsetheader contain a lot of redundant bits anyway because they are known
                    # to be 0 modulo 512, so the original offsets can be reconstructed even after adding 1.
                    modifiedFileInfo[2] = modifiedFileInfo[2] + 1
                    modifiedFileInfo[3] = modifiedFileInfo[3] + 1
                modifiedFileInfo[6] = mode
                modifiedFileInfo[11] = isTar
                modifiedFileInfo[13] = True  # is generated, i.e., does not have xattr
                modifiedFileInfo[14] += 1  # recursion depth

                self.index.set_file_info(tuple(modifiedFileInfo))

                # Update isTar to True for the tar
                modifiedFileInfo = list(fileInfo)
                modifiedFileInfo[11] = isTar

                self.index.set_file_info(tuple(modifiedFileInfo))

        fileObject.seek(oldPos)
        self.tarFileName = oldPrintName

        # If no file is in the TAR, then it most likely indicates a possibly compressed non TAR file.
        # In that case add that itself to the file index. This will be ignored when called recursively
        # because the table will at least contain the recursive file to mount itself, i.e., file_count > 0
        if self.index.file_count() == 0:
            if self.printDebug >= 3:
                print(f"Did not find any file in the given TAR: {self.tarFileName}. Assuming a compressed file.")

            # For some reason, this happens for single-file.iso.
            # Tarfile does not raise an error but also does not find any files.
            if not self.compression:
                raise CompressionError("Tarfile returned nothing, not even an error, and the file is not compressed!")

            tarInfo: Optional[Any] = None
            try:
                tarInfo = os.fstat(fileObject.fileno())
            except io.UnsupportedOperation:
                # If fileObject doesn't have a fileno, we set tarInfo to None
                # and set the relevant statistics (such as st_mtime) to sensible defaults.
                tarInfo = None

            fname = os.path.basename(self.tarFileName)
            for suffix in ['.gz', '.bz2', '.bzip2', '.gzip', '.xz', '.zst', '.zstd', '.zz', '.zlib']:
                if fname.lower().endswith(suffix) and len(fname) > len(suffix):
                    fname = fname[: -len(suffix)]
                    break

            # Try to get original file name from gzip
            mtime = 0
            if self.rawFileObject:
                oldPos = self.rawFileObject.tell()
                self.rawFileObject.seek(0)
                try:
                    info = get_gzip_info(self.rawFileObject)
                    if info:
                        fname, mtime = info
                except Exception:
                    if self.printDebug >= 2:
                        print("[Info] Could not determine an original gzip file name probably because it is not a gzip")
                    if self.printDebug >= 3:
                        traceback.print_exc()
                finally:
                    # TODO Why does tell return negative numbers!? Problem with indexed_gzip?
                    self.rawFileObject.seek(max(0, oldPos))

            # If the file object is actually an IndexedBzip2File or such, we can't directly use the file size
            # from os.stat and instead have to gather it from seek. Unfortunately, indexed_gzip does not support
            # io.SEEK_END even though it could as it has the index ...
            while fileObject.read(1024 * 1024):
                self._update_progress_bar(progressBar, fileObject)
            fileSize = fileObject.tell()

            mode = 0o777 | stat.S_IFREG  # default mode

            # fmt: off
            fileInfo = (
                ""                                    ,  # 0  : path
                fname                                 ,  # 1  : file name
                None                                  ,  # 2  : header offset
                0                                     ,  # 3  : data offset
                fileSize                              ,  # 4  : file size
                tarInfo.st_mtime if tarInfo else mtime,  # 5  : modification time
                tarInfo.st_mode if tarInfo else mode  ,  # 6  : file mode / permissions
                None                                  ,  # 7  : TAR file type. Currently unused. Overlaps with mode
                None                                  ,  # 8  : linkname
                tarInfo.st_uid if tarInfo else 0      ,  # 9  : user ID
                tarInfo.st_gid if tarInfo else 0      ,  # 10 : group ID
                False                                 ,  # 11 : isTar
                False,  # 12 isSparse, don't care if it is actually sparse or not because it is not in TAR
                False                                 ,  # 13 : is generated (parent folder)
                recursionDepth - 1                    ,  # 14 : recursion depth
            )
            # fmt: on
            self.index.set_file_info(fileInfo)

    def _open_stencil(self, offset: int, size: int, buffering: int) -> IO[bytes]:
        if buffering == 0:
            return cast(IO[bytes], RawStenciledFile([(self.tarFileObject, offset, size)], self.fileObjectLock))
        return cast(
            IO[bytes],
            StenciledFile(
                [(self.tarFileObject, offset, size)],
                self.fileObjectLock,
                bufferSize=self.blockSize if buffering == -1 else buffering,
            ),
        )

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        assert fileInfo.userdata
        tarFileInfo = fileInfo.userdata[-1]
        assert isinstance(tarFileInfo, SQLiteIndexedTarUserData)

        # This is not strictly necessary but it saves two file object layers and therefore might be more performant.
        # Furthermore, non-sparse files should be the much more likely case anyway.
        if not tarFileInfo.issparse:
            return self._open_stencil(tarFileInfo.offset, fileInfo.size, buffering)

        # The TAR file format is very simple. It's just a concatenation of TAR blocks. There is not even a
        # global header, only the TAR block headers. That's why we can simply cut out the TAR block for
        # the sparse file using StenciledFile and then use tarfile on it to expand the sparse file correctly.
        tarBlockSize = tarFileInfo.offset - tarFileInfo.offsetheader + fileInfo.size

        tarSubFile = self._open_stencil(tarFileInfo.offsetheader, tarBlockSize, buffering)
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

    @overrides(MountSource)
    def statfs(self) -> Dict[str, Any]:
        return {
            'f_bsize': self.blockSize,
            'f_frsize': self.blockSize,
            'f_bfree': 0,
            'f_bavail': 0,
            'f_ffree': 0,
            'f_favail': 0,
        }

    @staticmethod
    def _get_past_end_offset(sqlConnection: sqlite3.Connection) -> Optional[int]:
        """
        Returns None if it cannot determine where the archive should end. Currently, because of implementation
        limitations, this may happen if the last entry in the archive is a sparse file.
        """
        # TODO Make it work with sparse files by analyzing those sparse blocks manually or maybe get tarfile to do it

        # Note that we cannot use the recorded archive file size to determine from which we need to resume
        # reading because it is not specified how many zero-byte blocks there may be at the end:
        # > At the end of the archive file there shall be two 512-byte blocks filled with binary zeros,
        # > interpreted as an end-of-archive indicator.
        # For example, GNU tar rounds up to 10 KiB for very small archives but will (have to) append further
        # files right after the the last non-zero block, which might be at offset 512 for empty files.
        # > The user can specify a blocking factor, which is the number of blocks per record.
        # > The default is 20, producing 10 KiB records.
        result = sqlConnection.execute(
            "SELECT offset + size, issparse FROM files ORDER BY offset DESC LIMIT 1"
        ).fetchone()
        if not result:
            raise InvalidIndexError("The index contains no files!")
        pastEndOffset, isSparse = result

        if isSparse:
            return None

        # Round up to next TAR block
        if pastEndOffset % 512 != 0:
            pastEndOffset += 512 - (pastEndOffset % 512)

        return pastEndOffset

    def _try_to_mark_as_appended(self, storedStats: Dict[str, Any], archiveStats: os.stat_result):
        """
        Raises an exception if it makes no sense to only try to go over the new appended data alone
        else sets self.hasBeenAppendedTo to True.
        There is one very specific usecase for which recreating the complete index would be a waste:
        When an uncompressed archive got appended a rather small amount of files.
        """

        # Sizes should be determined and larger or equal
        if (
            not hasattr(archiveStats, 'st_size')
            or 'st_size' not in storedStats
            or archiveStats.st_size < storedStats['st_size']
        ):
            raise InvalidIndexError(
                "Will not treat an archive that shrank or has indeterminable size as having been appended to!"
            )

        # Times should be determined and larger or equal
        if (
            not hasattr(archiveStats, "st_mtime")
            or 'st_mtime' not in storedStats
            or archiveStats.st_mtime < storedStats['st_mtime']
        ):
            # Always throw even for if self.verifyModificationTime is False because in this method,
            # the archive should already have been determines as different.
            raise InvalidIndexError(
                f"The modification date for the TAR file {storedStats['st_mtime']} "
                f"is older than the one stored in the SQLite index ({archiveStats.st_mtime!s})",
            )

        # Checking is expensive and would basically do the same work as creating the database anyway.
        # Therefore, only bother with the added complexity and uncertainty of the randomized index check
        # if the additional part to analyze makes up less than 66% of the total archive.
        #
        # Ignore small archives that don't require much time to process anyway.
        # The threshold is motivated by the benchmarks for "First Mounting".
        # For uncompressed archives, the limiting factor is the number of files.
        # An uncompressed TAR with 1000 64KiB files would take roughly a second.
        if archiveStats.st_size < 64 * 1024 * 1024:
            raise InvalidIndexError("The archive did change but is too small to determine as having been appended to.")

        if self.index.file_count() < SQLiteIndex.NUMBER_OF_METADATA_TO_VERIFY:
            raise InvalidIndexError(
                "The archive did change but has too few files to determine as having been appended to."
            )

        # If the archive more than tripled, then the already existing part isn't all that much in
        # comparison to the work that would have to be done anyway. And because the validity check
        # would have to only be an approximation, simply allow the up to 33% overhead to recreate
        # everything from scratch, just to be sure.
        if archiveStats.st_size > 3 * storedStats['st_size']:
            raise InvalidIndexError(
                f"TAR file for this SQLite index has more than tripled in size from "
                f"{storedStats['st_size']} to {archiveStats.st_size}"
            )

        # Note that the xz compressed version of 100k zero-byte files is only ~200KB!
        # But this should be an edge-case and with a compression ratio of ~2, even compressed archives
        # of this size should not take more than 10s, so pretty negligible in my opinion.
        #
        # For compressed archives, detecting appended archives does not help much because the bottleneck is
        # the decompression not the indexing of files. And because rapidgzip and indexed_gzip probably
        # assume that the index is complete once import_index has been called, we have to recreate the full
        # block offsets anyway.
        if self.compression:
            raise InvalidIndexError(
                f"Compressed TAR file for this SQLite index has changed size from "
                f"{storedStats['st_size']} to {archiveStats.st_size}. It cannot be treated as appended."
            )

        if self.index.get_index_version() != SQLiteIndex.__version__:
            raise InvalidIndexError("Cannot append to index of different versions!")

        if self.printDebug >= 2:
            print("[Info] Archive has probably been appended to because it is larger and more recent.")
        self.hasBeenAppendedTo = True

    def _check_metadata(self, metadata: Dict[str, Any]) -> None:
        # self._isGnuIncremental may be initialized during metadata check because it is required for some checks.
        # But, if the subsequent checks fail, then we want to restore the initial value.
        isGnuIncremental = self._isGnuIncremental
        try:
            self._check_metadata2(metadata)
        except Exception as e:
            self._isGnuIncremental = isGnuIncremental
            raise e

    def _check_metadata2(self, metadata: Dict[str, Any]) -> None:
        """
        Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible.
        Returns normally and sets self.index.hasBeenAppendedTo to True if the size of the archive increased
        but still fits.
        """

        if 'tarstats' in metadata:
            storedStats = json.loads(metadata['tarstats'])
            tarStats = os.stat(self.tarFileName)

            if hasattr(tarStats, 'st_size') and 'st_size' in storedStats:
                if tarStats.st_size < storedStats['st_size']:
                    raise InvalidIndexError(
                        f"TAR file for this SQLite index has shrunk in size from "
                        f"{storedStats['st_size']} to {tarStats.st_size}"
                    )

                if tarStats.st_size > storedStats['st_size']:
                    self._try_to_mark_as_appended(storedStats, tarStats)

            # For compressed files, the archive size check should be sufficient because even if the uncompressed
            # size does not change, the compressed size will most likely change.
            # And also it would be expensive to do because the block offsets are not yet loaded yet!
            db = self.index.sqlConnection
            pastEndOffset = self._get_past_end_offset(db) if db else None
            if not self.compression and pastEndOffset:
                # https://pubs.opengroup.org/onlinepubs/9699919799/utilities/pax.html#tag_20_92_13_01
                # > At the end of the archive file there shall be two 512-byte blocks filled with binary zeros,
                # > interpreted as an end-of-archive indicator.
                fileStencil = (self.tarFileObject, pastEndOffset, 1024)
                oldOffset = self.tarFileObject.tell()
                try:
                    with RawStenciledFile(fileStencils=[fileStencil]) as file:
                        if file.read(1025) != b"\0" * 1024:
                            if self.printDebug >= 2:
                                print(
                                    "[Info] Probably has been appended to because no EOF zero-byte blocks could "
                                    f"be found at offset: {pastEndOffset}"
                                )
                            self._try_to_mark_as_appended(storedStats, tarStats)
                finally:
                    self.tarFileObject.seek(oldOffset)

            # Only happens very rarely, e.g., for more recent files with the same size.
            if (
                not self.hasBeenAppendedTo
                and self.verifyModificationTime
                and hasattr(tarStats, "st_mtime")
                and 'st_mtime' in storedStats
                and tarStats.st_mtime != storedStats['st_mtime']
            ):
                raise InvalidIndexError(
                    f"The modification date for the TAR file {storedStats['st_mtime']} "
                    f"to this SQLite index has changed ({tarStats.st_mtime!s})",
                )

        # Check arguments used to create the found index.
        # These are only warnings and not forcing a rebuild by default.
        # TODO: Add --force options?
        if 'arguments' in metadata:
            indexArgs = json.loads(metadata['arguments'])
            argumentsToCheck = [
                'mountRecursively',
                'encoding',
                'stripRecursiveTarExtension',
                'transformRecursiveMountPoint',
                'transformPattern',
                'ignoreZeros',
            ]

            if self.compression == FileFormatID.GZIP:
                argumentsToCheck.append('gzipSeekPointSpacing')

            SQLiteIndex.check_metadata_arguments(indexArgs, self, argumentsToCheck)

        # Restore the self._isGnuIncremental flag before doing any row validation because else there could be
        # false positive warnings regarding GNU incremental detection.
        if 'isGnuIncremental' in metadata:
            value = metadata['isGnuIncremental'].lower()
            self._isGnuIncremental = value in ('true', '1')
        elif self.index.sqlConnection:
            # This can be expensive, but it should still be less expensive than rereading the first 1000 file headers
            # and checking the type through that way. There will be a breakeven point though for very large archives.
            # Then, it would be better to update the index to contain the 'isGnuIncremental' metadata key.
            self._isGnuIncremental = bool(
                self.index.sqlConnection.execute(
                    """SELECT 1 FROM "files" WHERE hex(type) = hex("D") LIMIT 1"""
                ).fetchone()
            )

        if 'backendName' not in metadata:
            # Checking the first two should already be enough to detect an index created with a different backend.
            # Do not verify folders because parent folders and root get automatically added!
            result = self.index.get_connection().execute(
                f"""SELECT * {SQLiteIndex.FROM_REGULAR_FILES} ORDER BY offset ASC LIMIT 2;"""
            )
            if not self._check_rows_validity(result):
                raise InvalidIndexError("The first two files of the index do not match.")

    def _check_index_validity(self) -> bool:
        # Check some of the first and last files in the archive and some random selection in between.
        selectFiles = "SELECT * " + SQLiteIndex.FROM_REGULAR_FILES
        result = self.index.get_connection().execute(
            f"""
            SELECT * FROM ( {selectFiles} ORDER BY offset ASC LIMIT 100 )
            UNION
            SELECT * FROM ( {selectFiles} ORDER BY RANDOM() LIMIT {SQLiteIndex.NUMBER_OF_METADATA_TO_VERIFY} )
            UNION
            SELECT * FROM ( {selectFiles} ORDER BY offset DESC LIMIT 100 )
            ORDER BY offset
        """
        )
        return self._check_rows_validity(result)

    def _check_rows_validity(self, rows) -> bool:
        t0 = time.time()

        oldOffset = self.tarFileObject.tell()
        rowCount = 0
        try:
            for row in rows:
                rowCount += 1

                # As for the stencil size, 512 B (one TAR block) would be enough for most cases except for
                # features like GNU LongLink which store additional metadata in further TAR blocks.
                offsetHeader = int(row[2])
                offsetData = int(row[3])
                headerBlockCount = max(1, math.ceil((offsetData - offsetHeader) / 512)) * 512
                with StenciledFile(
                    fileStencils=[(self.tarFileObject, offsetHeader, headerBlockCount)]
                ) as file, tarfile.open(fileobj=file, mode='r|', ignore_zeros=True, encoding=self.encoding) as archive:
                    tarInfo = next(iter(archive))
                    realFileInfos, _, _, _ = _TarFileMetadataReader._process_tar_info(
                        tarInfo,
                        file,  # only used for isGnuIncremental == True
                        "",  # pathPrefix
                        offsetHeader,  # will be added to all offsets to get the real offset
                        isGnuIncremental=self._isGnuIncremental,
                        mountRecursively=False,
                        transform=self.transform,
                        recursionDepth=0,
                        printDebug=self.printDebug,
                    )

                    if not realFileInfos:
                        return False
                    realFileInfo = realFileInfos[0]

                    # Bool columns will have been converted to int 0 or 1 when reading from SQLite.
                    # In order to compare with the read result correctly, we need to convert them to bool, too.
                    storedFileInfo = list(row)
                    for index in [-1, -2]:
                        if storedFileInfo[index] not in [0, 1]:
                            return False
                        storedFileInfo[index] = bool(storedFileInfo[index])

                    # Do not compare the path because it might have the parent TAR prepended to it for
                    # recursive TARs and this is hard to ignore any other way.
                    storedFileInfo[0] = realFileInfo[0]  # path
                    storedFileInfo[11] = realFileInfo[11]  # isTar

                    commonSize = min(len(storedFileInfo), len(realFileInfo))
                    # Do not check newly added columns such as isgenerated and recursiondepth.
                    if commonSize > 13:
                        storedFileInfo[13] = realFileInfo[13]  # is generated
                    if commonSize > 14:
                        storedFileInfo[14] = realFileInfo[14]  # recursion depth
                    if storedFileInfo[:commonSize] != list(realFileInfo)[:commonSize]:
                        if self.printDebug >= 3:
                            print("[Info] Stored file info:")
                            print("[Info]", storedFileInfo)
                            print("[Info] differs from recomputed one:")
                            print("[Info]", realFileInfo)
                        return False

            return True
        except tarfile.TarError:
            # Not even worth warning because this simply might happen if the index is not valid anymore.
            return False
        finally:
            self.tarFileObject.seek(oldOffset)

            if self.printDebug >= 2:
                t1 = time.time()
                print(f"[Info] Verifying metadata for {rowCount} files took {t1 - t0:.3f} s")

        return False

    def _load_or_store_compression_offsets(self):
        if self.compression:
            self.index.synchronize_compression_offsets(self.tarFileObject, self.compression)

    def join_threads(self):
        if hasattr(self.tarFileObject, 'join_threads'):
            self.tarFileObject.join_threads()
