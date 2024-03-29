#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bisect
import io
import multiprocessing.pool
import os
import signal
from typing import List, Optional


try:
    import xz
except ImportError:
    xz = None  # type: ignore

try:
    import indexed_zstd
except ImportError:
    indexed_zstd = None  # type: ignore

from .utils import overrides, LRUCache, Prefetcher

_parallelXzReaderFile = None
_parallelZstdReaderFile = None


class BlockParallelReader(io.BufferedIOBase):
    """Uses a process pool to prefetch and cache decompressed blocks"""

    # Using a thread pool, slowed down ParallelXZReader by 50%

    def __init__(
        self,
        filename: str,
        fileobj,
        blockBoundaries: List[int],
        parallelization: Optional[int],
        initWorker=None,
        initArgs=(),
    ):
        """
        blockBoundaries:
            All but the last block boundary are also the uncompressed offsets at which new blocks begin
            The last block boundary is the first past-the-file-end offset (i.e. requal to the file size)
            and can be used to deduce the size of the last block.
        """
        if not parallelization:
            parallelization = os.cpu_count()
            assert parallelization is not None, "Cannot automatically determine CPU count!"
        # keep one core for on-demand decompression
        self.parallelization: int = parallelization - 1
        assert self.parallelization >= 1, "If you do not need to parallelize, then do not use this class!"

        self.filename = filename
        self.fileobj = fileobj
        self.blockBoundaries: List[int] = blockBoundaries
        self.initWorker = BlockParallelReader._initWorker if initWorker is None else initWorker
        self.initArgs = initArgs

        self._pool = None
        self._offset = 0
        self._blockCache: LRUCache = LRUCache(2 * parallelization)
        self._prefetcher = Prefetcher(4)
        self._lastUsedBlock: int = 0

        self.requestCount: int = 0
        self.cacheHitCount: int = 0
        self.cacheMissCount: int = 0
        self.cachePrefetchCount: int = 0

        assert self.fileobj.seekable() and self.fileobj.readable()

    def join_threads(self):
        self._pool.close()
        self._pool.join()
        self._pool = None

    def _getPool(self):
        if not self._pool:
            self._pool = multiprocessing.pool.Pool(self.parallelization, self.initWorker, self.initArgs)
        return self._pool

    @staticmethod
    def _initWorker():
        """
        Ignore the interrupt signal inside the child worker processes to avoid Python backtraces for each of them.
        Aborting with Ctrl+C will still work as the main process still accepts the signal.
        """
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    @staticmethod
    def _findBlock(blockBoundaries: List[int], offset: int) -> Optional[int]:
        """
        Returns the blockNumber such that the requested offset is inside the block starting at
        self.blockBoundaries[blockNumber].
        """
        blockNumber = bisect.bisect_right(blockBoundaries, offset) - 1
        # If the found block is the last element, then the offset is actually after the EOF!
        return blockNumber if blockNumber >= 0 and blockNumber + 1 < len(blockBoundaries) else None

    @staticmethod
    def _blockSize(blockBoundaries: List[int], blockNumber) -> int:
        if blockNumber + 1 >= len(blockBoundaries) or blockNumber < 0:
            return 0
        return blockBoundaries[blockNumber + 1] - blockBoundaries[blockNumber]

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    @overrides(io.BufferedIOBase)
    def close(self) -> None:
        self.fileobj.close()
        if self._pool:
            self._pool.close()

    @overrides(io.BufferedIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.BufferedIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.BufferedIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.BufferedIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.BufferedIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            self._offset += offset
        elif whence == io.SEEK_END:
            self._offset = self.blockBoundaries[-1] + offset
        elif whence == io.SEEK_SET:
            self._offset = offset

        if self._offset < 0:
            raise ValueError("Trying to seek before the start of the file!")

        return self._offset

    @overrides(io.BufferedIOBase)
    def tell(self) -> int:
        return self._offset

    def _read(self, size: int, decodeBlock) -> bytes:
        blocks = []
        blockNumber = self._findBlock(self.blockBoundaries, self._offset)
        if blockNumber is None:
            return bytes()

        self.requestCount += 1
        firstBlockOffset = self._offset - self.blockBoundaries[blockNumber]
        blockSize = self._blockSize(self.blockBoundaries, blockNumber)
        availableSize = blockSize - firstBlockOffset

        # Shortcut to improve performance for many small reads inside the same block
        # Alternatively, it might work to wrap this in an io.BufferedRandom to yield larger reads.
        # But, I think this is a small enough workaround for such a large impact.
        if blockNumber in self._blockCache and size <= availableSize and self._lastUsedBlock == blockNumber:
            self.cacheHitCount += 1
            result = self._blockCache[blockNumber].get()[firstBlockOffset : firstBlockOffset + size]
            self._offset += len(result)
            return result

        pendingBlocks: int = sum(0 if block.ready() else 1 for block in self._blockCache.values())
        pool = self._getPool()

        # TODO do not fetch blocks which do not fit into memory and instead seek directly to them!
        #      Note that this only makes sense if less than the block size has been requested else we would run out
        #      of memory in botch events! So it makes only sense for the first and last requested block.
        while True:
            # Fetch Block
            self._lastUsedBlock = blockNumber
            self._prefetcher.fetch(blockNumber)
            if blockNumber in self._blockCache:
                fetchedBlock = self._blockCache[blockNumber]
            else:
                self.cachePrefetchCount += 1
                fetchedBlock = pool.apply_async(
                    decodeBlock,
                    (
                        self.filename,
                        self.blockBoundaries[blockNumber],
                        self._blockSize(self.blockBoundaries, blockNumber),
                    ),
                )
                self._blockCache[blockNumber] = fetchedBlock
                pendingBlocks += 1

            blocks.append(fetchedBlock)
            self._offset += min(availableSize, size)
            if size <= availableSize:
                break

            # Get metadata for next block
            blockNumber += 1
            if blockNumber + 1 >= len(self.blockBoundaries):
                break

            # Only decrement size when we are actually entering the next iteration
            size -= availableSize

            # Get block data for next iteration
            blockSize = self._blockSize(self.blockBoundaries, blockNumber)
            offsetInBlock = self._offset - self.blockBoundaries[blockNumber]
            availableSize = blockSize - offsetInBlock

        # Prefetch blocks
        toPrefetch = self._prefetcher.prefetch(self.parallelization)
        for blockToPrefetch in toPrefetch:
            blockSize = self._blockSize(self.blockBoundaries, blockToPrefetch)
            if blockSize > 0 and blockToPrefetch not in self._blockCache and pendingBlocks < self.parallelization:
                self.cachePrefetchCount += 1
                fetchedBlock = pool.apply_async(
                    decodeBlock,
                    (self.filename, self.blockBoundaries[blockToPrefetch], blockSize),
                )
                self._blockCache[blockToPrefetch] = fetchedBlock
                pendingBlocks += 1

        # Concatenate data from blocks as necessary
        result = bytes()
        while blocks:
            block = blocks.pop(0)
            if not block.ready():
                self.cacheMissCount += 1
            # Note that it is perfectly safe to call AsyncResult.get multiple times!
            toAppend = block.get()
            if firstBlockOffset > 0:
                toAppend = toAppend[firstBlockOffset:]
            if not blocks:
                toAppend = toAppend[:size]
            firstBlockOffset = 0

            result += toAppend

        # TODO fall back to reading directly from fileobj if prefetch suggests nothing at all to improve latency!
        # self.fileobj.seek(self._offset)
        # result = self.fileobj.read(size)

        return result


class ParallelXZReader(BlockParallelReader):
    def __init__(self, filename: str, parallelization: Optional[int] = None):
        fileObject = xz.open(filename, 'rb')

        blockBoundaries = fileObject.block_boundaries.copy()
        blockBoundaries.append(len(fileObject))

        super().__init__(
            filename, fileObject, blockBoundaries, parallelization, ParallelXZReader._initWorker2, (filename,)
        )
        self._openFiles()

    def _openFiles(self):
        # Opening the pool and and the files on each worker at this point might be a point to discuss
        # but it leads to uniform latencies for the subsequent read calls.
        pool = self._getPool()

        results = []
        for _ in range(self.parallelization * 4):
            results.append(pool.apply_async(os.getpid))  # will triger worker initialization, i.e., _tryOpenGlobalFile

        pids = set()
        for result in results:
            pids.add(result.get())

    @staticmethod
    def _initWorker2(filename):
        BlockParallelReader._initWorker()
        ParallelXZReader._tryOpenGlobalFile(filename)

    @staticmethod
    def _tryOpenGlobalFile(filename):
        # This is not thread-safe! But it will be executed in a process pool, in which each worker has its own
        # global variable set. Using a global variable for this is safe because we know that there is one process pool
        # per BlockParallelReader, meaning the filename is a constant for each worker.
        # pylint: disable=global-statement
        global _parallelXzReaderFile
        if _parallelXzReaderFile is None:
            _parallelXzReaderFile = xz.open(filename, 'rb')

    @staticmethod
    def _decodeBlock(filename, offset, size):
        ParallelXZReader._tryOpenGlobalFile(filename)
        _parallelXzReaderFile.seek(offset)
        return _parallelXzReaderFile.read(size)

    @overrides(io.BufferedIOBase)
    def read(self, size: int = -1) -> bytes:
        return super()._read(size, ParallelXZReader._decodeBlock)


# This one is actually mostly slower than serial decoding. Even the command line tool zstd is often slower with
# -T 16 compared to -T 1 for frame sizes 1 MiB up to 512 MiB. It might simply be that zstd decompression is too
# fast to parallelize with this approach and instead leads to more cache problems or even the file reading might
# be the limiting factor. I might have to give this up for now.
class ParallelZstdReader(BlockParallelReader):
    def __init__(self, filename: str, parallelization: Optional[int] = None):
        fileObject = indexed_zstd.IndexedZstdFile(filename)
        blockBoundaries = list(fileObject.block_offsets().values())
        super().__init__(filename, fileObject, blockBoundaries, parallelization)

    @staticmethod
    def _decodeBlock(filename, offset, size):
        # This is not thread-safe! But it will be executed in a process pool, in which each worker has its own
        # global variable set. Using a global variable for this is safe because we know that there is one process pool
        # per BlockParallelReader, meaning the filename is a constant for each worker.
        # pylint: disable=global-statement
        global _parallelZstdReaderFile
        if _parallelZstdReaderFile is None:
            _parallelZstdReaderFile = indexed_zstd.IndexedZstdFile(filename)

        _parallelZstdReaderFile.seek(offset)
        return _parallelZstdReaderFile.read(size)

    @overrides(io.BufferedIOBase)
    def read(self, size: int = -1) -> bytes:
        return super()._read(size, ParallelZstdReader._decodeBlock)
