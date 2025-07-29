import contextlib
import json
import logging
import os
import re
import stat
import tarfile
import threading
from timeit import default_timer as timer
from typing import IO, Any, Optional, Union, cast

from ratarmountcore.formats import find_asar_header
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from ratarmountcore.StenciledFile import RawStenciledFile, StenciledFile
from ratarmountcore.utils import overrides

logger = logging.getLogger(__name__)


# https://www.electronjs.org/docs/latest/glossary#asar
# > ASAR stands for Atom Shell Archive Format. An asar archive is a simple tar-like format
# > that concatenates files into a single file.
#
# Example header:
# {
#    "files": {
#       "tmp": {
#          "files": {}
#       },
#       "usr" : {
#          "files": {
#            "bin": {
#              "files": {
#                "ls": {
#                  "offset": "0",
#                  "size": 100,
#                  "executable": true,
#                  "integrity": {
#                    "algorithm": "SHA256",
#                    "hash": "...",
#                    "blockSize": 1024,
#                    "blocks": ["...", "..."]
#                  }
#                }
#              }
#            }
#          }
#       },
#       "etc": {
#          "files": {
#            "hosts": {
#              "offset": "200",
#              "size": 32,
#              "integrity": {
#                 "algorithm": "SHA256",
#                 "hash": "...",
#                 "blockSize": 1024,
#                 "blocks": ["...", "..."]
#               }
#            }
#          }
#       }
#    }
# }
#
# There are no permissions, symbolic links (but hard links are possible), or file times.
class ASARMountSource(SQLiteIndexMountSource):
    # fmt: off
    def __init__(
        self,
        fileOrPath             : Union[str, IO[bytes]],
        writeIndex             : bool                      = False,
        clearIndexCache        : bool                      = False,
        indexFilePath          : Optional[str]             = None,
        indexFolders           : Optional[list[str]]       = None,
        encoding               : str                       = tarfile.ENCODING,
        verifyModificationTime : bool                      = False,
        indexMinimumFileCount  : int                       = 1000,
        transform              : Optional[tuple[str, str]] = None,
        **options
    ) -> None:
        self.isFileObject           = not isinstance(fileOrPath, str)
        self.fileObject             = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        self.archiveFilePath        = fileOrPath if isinstance(fileOrPath, str) else None
        self.encoding               = encoding
        self.verifyModificationTime = verifyModificationTime
        self.options                = options
        self.transformPattern       = transform
        # fmt: on

        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )

        # Try to open file
        self._headerOffset, self._headerSize, self._dataOffset = find_asar_header(self.fileObject)
        self.fileObject.seek(self._headerOffset + self._headerSize - 1)
        if not self.fileObject.read(1):
            raise ValueError(
                f"Failed to read the full ASAR header sized {self._headerSize} from offset {self._headerOffset}!"
            )

        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='ASARMountSource',
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._check_metadata,
        )

        # Try to get block size from the real opened file.
        self.blockSize = 512
        with contextlib.suppress(Exception):
            self.blockSize = os.fstat(self.fileObject.fileno()).st_blksize

        self.fileObjectLock = threading.Lock()

        # Load or create index (copy-paste)

        if self.index.index_is_loaded():
            self.index.reload_index_read_only()
        else:
            # Open new database when we didn't find an existing one.
            # Simply open in memory without an error even if writeIndex is True but when not indication
            # for a index file location has been given.
            if writeIndex and (indexFilePath or not self.isFileObject):
                self.index.open_writable()
            else:
                self.index.open_in_memory()

            self._create_index()
            if self.index.index_is_loaded():
                self._store_metadata()
                self.index.reload_index_read_only()

    def _store_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.store_metadata(argumentsMetadata, self.archiveFilePath)

    def _convert_to_row(self, fullPath, entry: dict[str, Any], dataOffset: int) -> tuple:
        path, name = SQLiteIndex.normpath(self.transform(fullPath)).rsplit("/", 1)

        isFile = 'offset' in entry and 'size' in entry
        isDirectory = 'files' in entry
        assert isFile != isDirectory
        mode = 0o777 | (stat.S_IFDIR if isDirectory else stat.S_IFREG)

        offset = dataOffset + int(entry.get("offset", "0"))
        size = int(entry.get("size", "0"))

        # fmt: off
        fileInfo : tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            0                 ,  # 2  : header offset
            offset            ,  # 3  : data offset
            size              ,  # 4  : file size
            0                 ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            ""                ,  # 8  : linkname
            0                 ,  # 9  : user ID
            0                 ,  # 10 : group ID
            False             ,  # 11 : is TAR (unused?)
            False             ,  # 12 : is sparse
            False             ,  # 13 : is generated (parent folder)
            0                 ,  # 14 : recursion depth
        )
        # fmt: on

        return fileInfo

    def _create_index(self) -> None:
        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} ...")
        t0 = timer()

        self.index.ensure_intermediary_tables()

        # Using StenciledFile instead of josn.loads(self.fileObject.read()) to avoid yet another copy in memory,
        # to roughly halve memory usage for very large JSONs. Note that the JSON size is limited to 4 GiB because
        # the size is 32-bit.
        header = json.load(StenciledFile([(self.fileObject, self._headerOffset, self._headerSize)]))

        # This code is complex to avoid recursion.
        toProcess = [("/", header)]
        fileInfos = [self._convert_to_row("/", header, self._dataOffset)]
        while toProcess:
            prefix, entry = toProcess.pop()
            children = [(prefix + "/" + path, nestedEntry) for path, nestedEntry in entry['files'].items()]
            # toProcess must only contain folder entries, which have a 'files' key mapping to a dictionary.
            fileInfos += [self._convert_to_row(path, nestedEntry, self._dataOffset) for path, nestedEntry in children]
            # Appending leads to depth-first traversal, while prepending leads to breadth-first traversal.
            # Depth-first should be slightly less memory intensive. Note that the dictionary should only store
            # a pointer to the shared mutable Python object, but the path strings will use up memory.
            toProcess += [(path, nestedEntry) for path, nestedEntry in children if 'files' in nestedEntry]

            if len(fileInfos) > 1000:
                self.index.set_file_infos(fileInfos)
                fileInfos = []

        if fileInfos:
            self.index.set_file_infos(fileInfos)
        self.index.finalize()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} took {timer() - t0:.2f}s")

    @overrides(SQLiteIndexMountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        super().__exit__(exception_type, exception_value, exception_traceback)
        if not self.isFileObject:
            self.fileObject.close()

    def _open_stencil(self, offset: int, size: int, buffering: int) -> IO[bytes]:
        if buffering == 0:
            return cast(IO[bytes], RawStenciledFile([(self.fileObject, offset, size)], self.fileObjectLock))
        return cast(
            IO[bytes],
            StenciledFile(
                [(self.fileObject, offset, size)],
                self.fileObjectLock,
                bufferSize=self.blockSize if buffering == -1 else buffering,
            ),
        )

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        assert fileInfo.userdata
        extendedFileInfo = fileInfo.userdata[-1]
        assert isinstance(extendedFileInfo, SQLiteIndexedTarUserData)
        return self._open_stencil(extendedFileInfo.offset, fileInfo.size, buffering)

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self.index.try_to_open_first_file(lambda path: self.open(self.lookup(path)))
