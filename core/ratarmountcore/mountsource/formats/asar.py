import contextlib
import json
import logging
import os
import stat
import threading
from typing import IO, Any, Union, cast

from ratarmountcore.formats import find_asar_header
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
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
    def __init__(self, fileOrPath: Union[str, IO[bytes]], **options) -> None:
        self.isFileObject = not isinstance(fileOrPath, str)
        self.fileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath

        # Try to open file
        self._headerOffset, self._headerSize, self._dataOffset = find_asar_header(self.fileObject)
        self.fileObject.seek(self._headerOffset + self._headerSize - 1)
        if not self.fileObject.read(1):
            raise ValueError(
                f"Failed to read the full ASAR header sized {self._headerSize} from offset {self._headerOffset}!"
            )

        indexOptions = {
            'archiveFilePath': fileOrPath if isinstance(fileOrPath, str) else None,
            'backendName': 'ASARMountSource',
        }
        super().__init__(**(options | indexOptions))

        # Try to get block size from the real opened file.
        self.blockSize = 512
        with contextlib.suppress(Exception):
            self.blockSize = os.fstat(self.fileObject.fileno()).st_blksize

        self.fileObjectLock = threading.Lock()

        self._finalize_index(self._create_index)

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

    @overrides(SQLiteIndexMountSource)
    def close(self) -> None:
        super().close()
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
        return self._open_stencil(SQLiteIndex.get_index_userdata(fileInfo.userdata).offset, fileInfo.size, buffering)
