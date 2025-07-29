# pylint: disable=protected-access, import-outside-toplevel, unused-argument

import contextlib
import io
import json
import logging
import os
import re
import stat
import tarfile
import zlib
from timeit import default_timer as timer
from typing import IO, Any, Optional, Union

try:
    import deflate
except ImportError:
    deflate = None

try:
    from isal import isal_zlib
except ImportError:
    isal_zlib = None  # type: ignore

try:
    import PySquashfsImage
except ImportError:
    PySquashfsImage = None  # type: ignore

try:
    from PySquashfsImage import SquashFsImage
except ImportError:
    # We need to define this for @overrides and pytype, but it also is a nice documentation
    # for the expected members in PySquashfsImage.SquashFsImage.
    class SquashFsImage:  # type: ignore
        def __init__(self, fd, offset: int = 0, closefd: bool = True) -> None:
            self._sblk: Any = None

        def __iter__(self):
            pass

        def _get_compressor(self, compression_id):
            pass

        def _initialize(self):
            pass

        # These are not overridden, only called:

        def _read_block_list(self, start, offset, blocks):
            raise NotImplementedError

        def _read_fragment(self, fragment):
            raise NotImplementedError

        def _read_inode(self, start_block, offset):
            raise NotImplementedError

        def _opendir(self, block_start, offset):
            raise NotImplementedError

        def _dir_scan(self, start_block, offset):
            raise NotImplementedError


try:
    from PySquashfsImage.compressor import Compression, Compressor, compressors
except ImportError:
    Compressor = object

from ratarmountcore.formats import find_squashfs_offset
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex, SQLiteIndexedTarUserData
from ratarmountcore.utils import overrides

logger = logging.getLogger(__name__)


class IsalZlibDecompressor(Compressor):
    name = "gzip"

    def __init__(self):
        self._decompress = zlib.decompress if isal_zlib is None else isal_zlib.decompress

    def uncompress(self, src, size, outsize):
        return self._decompress(src)


class LibdeflateZlibDecompressor(Compressor):
    name = "gzip"

    def __init__(self):
        self._lib = deflate

    def uncompress(self, src, size, outsize):
        # Beware: https://github.com/dcwatson/deflate/issues/41
        return self._lib.zlib_decompress(src, outsize)


class LZ4Compressor(Compressor):
    name = "lz4"

    def __init__(self):
        import lz4.block

        self._lib = lz4.block

    def uncompress(self, src, size, outsize):
        return self._lib.decompress(src, outsize)


class LZMACompressor(Compressor):
    name = "lzma"

    def __init__(self, blockSize):
        self._block_size = blockSize
        try:
            import lzma
        except ImportError:
            from backports import lzma
        self._lib = lzma

    def uncompress(self, src, size, outsize):
        # https://github.com/plougher/squashfs-tools/blob/a04910367d64a5220f623944e15be282647d77ba/squashfs-tools/
        #   lzma_wrapper.c#L40
        # res = LzmaCompress(dest + LZMA_HEADER_SIZE, &outlen, src, size, dest,
        #                    &props_size, 5, block_size, 3, 0, 2, 32, 1);
        # https://github.com/jljusten/LZMA-SDK/blob/781863cdf592da3e97420f50de5dac056ad352a5/C/LzmaLib.h#L96
        # -> level=5, dictSize=block_size, lc=3, lp=0, pb=2, fb=32, numThreads=1
        # https://github.com/plougher/squashfs-tools/blob/a04910367d64a5220f623944e15be282647d77ba/squashfs-tools/
        #   lzma_wrapper.c#L30
        # For some reason, squashfs does not store raw lzma but adds a custom header of 5 B and 8 B little-endian
        # uncompressed size, which can be read with struct.unpack('<Q', src[5:5+8]))
        LZMA_PROPS_SIZE = 5
        LZMA_HEADER_SIZE = LZMA_PROPS_SIZE + 8
        return self._lib.decompress(
            src[LZMA_HEADER_SIZE:],
            format=self._lib.FORMAT_RAW,
            filters=[{"id": self._lib.FILTER_LZMA1, 'lc': 3, 'lp': 0, 'pb': 2, 'dict_size': self._block_size}],
        )


class SquashFSFile(io.RawIOBase):
    def __init__(self, image, inode) -> None:
        self._image = image
        self._inode = inode

        self._offset = 0
        self._size = inode.data
        self._block_size = image._sblk.block_size
        self._lastBlockIndex = inode.data // self._block_size

        self._blockList = []
        self._dataToBlockOffset: dict[int, int] = {}  # block offset may be negative (-size) for sparse blocks
        self._compressedBlockOffsets = []
        if inode.blocks:
            self._blockList = [
                block
                for block in image._read_block_list(inode.block_start, inode.block_offset, inode.blocks)
                if block != PySquashfsImage.SQUASHFS_INVALID_FRAG
            ]

            compressedBlockOffset = inode.start
            for i, block in enumerate(self._blockList):
                blockSize = self._size % self._block_size if i == self._lastBlockIndex else self._block_size
                assert blockSize > 0
                if block:
                    self._compressedBlockOffsets.append(compressedBlockOffset)
                    compressedBlockOffset += PySquashfsImage.SQUASHFS_COMPRESSED_SIZE_BLOCK(block)
                else:
                    # sparse file
                    self._compressedBlockOffsets.append(-blockSize)
            assert len(self._compressedBlockOffsets) == len(self._blockList)

        self._fragment = None
        if inode.frag_bytes:
            self._fragment = image._read_fragment(inode.fragment)

        self._bufferIO: Optional[IO[bytes]] = None
        self._blockIndex = 0
        self._buffer = b''
        self._refill_buffer(self._blockIndex)  # Requires self._blockList to be initialized

    def _refill_buffer(self, blockIndex: int) -> None:
        self._blockIndex = blockIndex
        self._buffer = b''

        assert self._blockIndex >= 0
        if self._blockIndex < len(self._blockList):
            block = self._blockList[self._blockIndex]
            if block:
                start = self._compressedBlockOffsets[self._blockIndex]
                self._buffer = self._image._read_data_block(start, block)
            else:
                if (self._blockIndex + 1) * self._block_size >= self._size:
                    blockSize = max(0, self._size - self._blockIndex * self._block_size)
                else:
                    blockSize = self._block_size
                self._buffer = b'\0' * blockSize
        elif self._fragment and self._blockIndex == len(self._blockList):
            fragment = self._image._read_data_block(*self._fragment)
            self._buffer = fragment[self._inode.offset : self._inode.offset + self._inode.frag_bytes]

        self._bufferIO = io.BytesIO(self._buffer)

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        """Generic implementation which uses read."""
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        return len(readBytes)

    def read1(self, size: int = -1) -> bytes:
        if not self._bufferIO:
            raise RuntimeError("Closed file cannot be read from!")
        result = self._bufferIO.read(size)
        # An empty buffer signals the end of the file!
        if result or not self._buffer:
            return result

        self._blockIndex += 1
        self._refill_buffer(self._blockIndex)
        return self._bufferIO.read(size)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        result = bytearray()
        while size < 0 or len(result) < size:
            read_data = self.read1(size if size < 0 else size - len(result))
            if not read_data:
                break
            result.extend(read_data)
        return bytes(result)

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if not self._bufferIO:
            raise RuntimeError("Closed file cannot be seeked!")

        here = self.tell()
        if whence == io.SEEK_CUR:
            offset += here
        elif whence == io.SEEK_END:
            offset += self._size

        self._offset = max(0, min(offset, self._size))
        bufferOffset = self._blockIndex * self._block_size
        if offset < bufferOffset or offset >= bufferOffset + len(self._buffer):
            self._refill_buffer(offset // self._block_size)  # Updates self._blockIndex!
        self._bufferIO.seek(offset - self._blockIndex * self._block_size)

        return self.tell()

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        # Returning self._blockIndex * self._block_size + self._bufferIO.tell() will not work when we have
        # an empty buffer after trying to read past the end of the file.
        return self._offset


# https://github.com/matteomattei/PySquashfsImage/blob/e637b26b3bc6268dd589fa1439fecf99e49a565b/PySquashfsImage/__init__.py#L82
class SquashFSImage(SquashFsImage):
    """
    Contains several improvements over the base class:
     - Does not create the whole folder hierarchy in memory when only iterating over it to avoid high memory
       usage for SquashFS images with millions of files.
     - Adds seekable, streamable file object accessor that can be opened given a single number.
     - Adds thread locks around the underlying file object so that multiple file objects can be opened and used
       from multiple threads concurrently.
     - Uses libdeflate or ISA-L if installed, which a generally faster than the standard zlib.
     - Fixes lz4 support. (Merged into PySquashfsImage upstream, but not released yet.)
     - Adds lzma support. (Merged into PySquashfsImage upstream, but not released yet.)

    Beware that we are overwriting and using "private" methods starting with underscores!
    That's why we need to pin to an exact PySquashfsImage release.
    """

    @overrides(SquashFsImage)
    def __init__(self, *args, **kwargs):
        self._real_root = None
        super().__init__(*args, **kwargs)  # Calls overridden _initialize

    @overrides(SquashFsImage)
    def _get_compressor(self, compression_id):
        if compression_id == Compression.ZLIB:
            if deflate is not None:
                return LibdeflateZlibDecompressor()
            if isal_zlib is not None:
                return IsalZlibDecompressor()
        if compression_id == Compression.LZ4:
            return LZ4Compressor()
        if compression_id == Compression.LZMA:
            return LZMACompressor(self._sblk.block_size)
        if compression_id not in compressors:
            raise ValueError("Unknown compression method " + compression_id)
        return compressors[compression_id]()

    @overrides(SquashFsImage)
    def _initialize(self):
        self._fd.seek(self._offset)
        self._read_super()
        self._read_uids_guids()
        self._read_fragment_table()
        self._read_xattrs_from_disk()
        # Moved self._root initialization into a property and _generate_root

    def _generate_root(self):
        root_block = PySquashfsImage.SQUASHFS_INODE_BLK(self._sblk.root_inode)
        root_offset = PySquashfsImage.SQUASHFS_INODE_OFFSET(self._sblk.root_inode)
        self._real_root = self._dir_scan(root_block, root_offset)

    @staticmethod
    def _join_inode_offset(start_block, offset):
        assert start_block < 2**32
        assert offset < 2**16
        return (start_block << 16) + offset

    @staticmethod
    def _split_inode_offset(inode_offset):
        return inode_offset >> 16, inode_offset & 0xFFFF

    def read_inode(self, inode_offset):
        """Newly added function over SquashFsImage that adds an accessor via a simple integer."""
        return self._read_inode(*self._split_inode_offset(inode_offset))

    @overrides(SquashFsImage)
    def __iter__(self):  # -> PySquashfsImage.file.File
        """
        Performance improved function over PySquashfsImage.__iter__ that generates data on demand instead
        of keeping all metadata in memory and returning a generator over that.
        """
        root_block = PySquashfsImage.SQUASHFS_INODE_BLK(self._sblk.root_inode)
        root_offset = PySquashfsImage.SQUASHFS_INODE_OFFSET(self._sblk.root_inode)
        root_inode_offset, root_directory = self._open_directory(root_block, root_offset)
        yield root_inode_offset, root_directory
        yield from self._recursive_inodes_iterator(root_directory)

    def _open_directory(self, start_block, offset, parent=None, name=None):
        directory = self._opendir(start_block, offset)
        if parent is not None:
            directory._parent = parent
        if name is not None:
            directory._name = name
        return self._join_inode_offset(start_block, offset), directory

    def _recursive_inodes_iterator(self, directory):  # -> PySquashfsImage.file.File
        for entry in directory.entries:
            start_block = entry["start_block"]
            offset = entry["offset"]
            if entry["type"] == PySquashfsImage.Type.DIR:
                inode_offset, subdirectory = self._open_directory(start_block, offset, directory, entry["name"])
                yield inode_offset, subdirectory
                yield from self._recursive_inodes_iterator(subdirectory)
            else:
                inode = self._read_inode(start_block, offset)
                cls = PySquashfsImage.filetype[entry["type"]]
                yield self._join_inode_offset(start_block, offset), cls(self, inode, entry["name"], directory)

    @property
    def _root(self):
        if self._real_root is None:
            self._generate_root()
        return self._real_root

    @_root.setter
    def _root(self, value):
        # super().__init__ will initialize it to None but super()._initialize should not be called!
        assert value is None

    def open(self, inode):
        return SquashFSFile(self, inode)


class SquashFSMountSource(SQLiteIndexMountSource):
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
        # fmt: on
        if isinstance(fileOrPath, str):
            openedFile = True
            file: IO[bytes] = open(fileOrPath, 'rb')
        else:
            openedFile = False
            file = fileOrPath
            file.seek(0)

        offset = find_squashfs_offset(file)
        if offset < 0:
            if openedFile:
                file.close()
            raise ValueError("Not a valid SquashFS image!")

        # fmt: off
        self.rawFileObject          = file
        self.image                  = SquashFSImage(self.rawFileObject, offset=offset)
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

        super().__init__(
            SQLiteIndex(
                indexFilePath,
                indexFolders=indexFolders,
                archiveFilePath=self.archiveFilePath,
                encoding=self.encoding,
                indexMinimumFileCount=indexMinimumFileCount,
                backendName='SquashFSMountSource',
            ),
            clearIndexCache=clearIndexCache,
            checkMetadata=self._check_metadata,
        )

        isFileObject = not isinstance(fileOrPath, str)

        if self.index.index_is_loaded():
            # self._load_or_store_compression_offsets()  # load
            self.index.reload_index_read_only()
        else:
            # Open new database when we didn't find an existing one.
            # Simply open in memory without an error even if writeIndex is True but when not indication
            # for a index file location has been given.
            if writeIndex and (indexFilePath or not isFileObject):
                self.index.open_writable()
            else:
                self.index.open_in_memory()

            self._create_index()
            # self._load_or_store_compression_offsets()  # store
            if self.index.index_is_loaded():
                self._store_metadata()
                self.index.reload_index_read_only()

    def _store_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps({argument: getattr(self, argument) for argument in argumentsToSave})
        self.index.store_metadata(argumentsMetadata, self.archiveFilePath)

    def _convert_to_row(self, inodeOffset: int, info: "PySquashfsImage.file.File") -> tuple:  # type: ignore
        mode = info.mode
        linkname = ""
        if info.is_symlink:
            linkname = info.readlink()
            mode = mode | stat.S_IFLNK
        else:
            # Note that PySquashfsImage.file.Directory inherits from file.File, i.e., info can also be a directory.
            mode = mode | (stat.S_IFDIR if info.is_dir else stat.S_IFREG)

        path, name = SQLiteIndex.normpath(self.transform(info.path)).rsplit("/", 1)

        # Currently unused. Squashfs files are stored in multiple blocks, so a single offset is insufficient.
        dataOffset = 0

        # SquashFS also returns non-zero sizes for directory, FIFOs, symbolic links, and device files
        fileSize = info.size if info.is_file else 0

        # fmt: off
        fileInfo : tuple = (
            path              ,  # 0  : path
            name              ,  # 1  : file name
            inodeOffset       ,  # 2  : header offset
            dataOffset        ,  # 3  : data offset
            fileSize          ,  # 4  : file size
            info.time         ,  # 5  : modification time
            mode              ,  # 6  : file mode / permissions
            0                 ,  # 7  : TAR file type. Currently unused. Overlaps with mode
            linkname          ,  # 8  : linkname
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

        # TODO Doing this in a chunked manner with generators would make it work better for large archives.
        fileInfos = []
        for inodeOffset, info in self.image:
            fileInfos.append(self._convert_to_row(inodeOffset, info))
        self.index.set_file_infos(fileInfos)
        self.index.finalize()

        if logger.isEnabledFor(logging.WARNING):
            print(f"Creating offset dictionary for {self.archiveFilePath} took {timer() - t0:.2f}s")

    def close(self) -> None:
        if hasattr(self, 'rawFileObject'):
            self.rawFileObject.close()

        # There is no "closed" method and it can only be closed once, else we get:
        # PySquashfsImage/__init__.py", line 131, in close
        #     self._fd.close()
        #     ^^^^^^^^^^^^^^
        # AttributeError: 'NoneType' object has no attribute 'close'
        with contextlib.suppress(AttributeError):
            self.image.close()  # pytype: disable=attribute-error

    @overrides(SQLiteIndexMountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
        super().__exit__(exception_type, exception_value, exception_traceback)
        self.close()

    def __del__(self):
        self.close()
        if hasattr(super(), '__del__'):
            super().__del__()

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # The buffering is ignored for now because SquashFS has an inherent buffering based on the block size
        # configured in the SquashFS image. It probably makes no sense to reduce or increase that buffer size.
        # Decreasing may reduce memory usage, but with Python and other things, memory usage is not a priority
        # in ratarmount as long as it is bounded for very large archives.
        assert fileInfo.userdata
        extendedFileInfo = fileInfo.userdata[-1]
        assert isinstance(extendedFileInfo, SQLiteIndexedTarUserData)
        return self.image.open(self.image.read_inode(extendedFileInfo.offsetheader))

    @overrides(MountSource)
    def statfs(self) -> dict[str, Any]:
        blockSize = 512
        with contextlib.suppress(Exception):
            blockSize = os.fstat(self.rawFileObject.fileno()).st_blksize

        blockSize = max(blockSize, self.image._sblk.block_size)
        return {
            'f_bsize': blockSize,
            'f_frsize': blockSize,
            'f_bfree': 0,
            'f_bavail': 0,
            'f_ffree': 0,
            'f_favail': 0,
            'f_namemax': 256,
        }

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self.index.try_to_open_first_file(lambda path: self.open(self.lookup(path)))
