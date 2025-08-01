# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import base64
import io
import lzma
import os
import random
import shutil
import subprocess
import sys
from pathlib import Path
from typing import IO

import indexed_zstd
import pytest
import xz

try:
    # May not be installed with Python 3.14 because of incompatibilities.
    import zstandard
except ImportError:
    zstandard = None  # type: ignore

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.BlockParallelReaders import ParallelXZReader, ParallelZstdReader  # noqa: E402


def test_find_block():
    find_block = ParallelXZReader._find_block
    for i in range(-10, 10):
        assert find_block([], i) is None
        assert find_block([0], i) is None

    assert find_block([0, 1], -1) is None
    assert find_block([0, 1], 0) == 0
    assert find_block([0, 1], 1) is None

    assert find_block([0, 2], -1) is None
    assert find_block([0, 2], 0) == 0
    assert find_block([0, 2], 1) == 0
    assert find_block([0, 2], 2) is None

    assert find_block([0, 2, 4], -1) is None
    assert find_block([0, 2, 4], 0) == 0
    assert find_block([0, 2, 4], 1) == 0
    assert find_block([0, 2, 4], 2) == 1
    assert find_block([0, 2, 4], 3) == 1
    assert find_block([0, 2, 4], 4) is None


def test_block_size():
    blockSize = ParallelXZReader._block_size
    for i in range(-10, 10):
        assert blockSize([], i) == 0
        assert blockSize([0], i) == 0

    assert blockSize([0, 1], -1) == 0
    assert blockSize([0, 1], 0) == 1
    assert blockSize([0, 1], 1) == 0

    assert blockSize([0, 2], -1) == 0
    assert blockSize([0, 2], 0) == 2
    assert blockSize([0, 2], 1) == 0

    assert blockSize([0, 2, 4], -1) == 0
    assert blockSize([0, 2, 4], 0) == 2
    assert blockSize([0, 2, 4], 1) == 2
    assert blockSize([0, 2, 4], 2) == 0


@pytest.mark.parametrize("parallelization", [1, 2, 3, os.cpu_count()])
class TestParallelXZReader:
    @staticmethod
    def _create_archive(archivePath: Path, streams: int, blocksPerStream: int, blockSize: int):
        for _ in range(streams):
            tmpPath = str(archivePath) + '.tmp'
            size = blockSize * blocksPerStream
            Path(tmpPath).write_bytes(base64.b64encode(os.urandom(size))[:size])
            subprocess.run(['xz', f'--block-size={blockSize}', '--compress', '--force', tmpPath], check=True)
            shutil.copy(tmpPath + '.xz', archivePath)

    @staticmethod
    def _test_sequential_reading(archivePath: str, bufferSize: int, parallelization: int):
        with (
            xz.open(archivePath, 'rb') as serialFile,
            (
                lzma.open(archivePath) if parallelization == 1 else ParallelXZReader(archivePath, parallelization)
            ) as parallelFile,
        ):
            bytesRead = 0
            while True:
                serialData = serialFile.read(bufferSize)
                parallelData = parallelFile.read(bufferSize)
                assert len(serialData) == len(parallelData)
                assert serialData == parallelData
                bytesRead += len(serialData)
                if len(serialData) < bufferSize:
                    break

            if hasattr(parallelFile, 'blockBoundaries'):
                assert bytesRead == parallelFile.blockBoundaries[-1]

    @staticmethod
    def _test_random_reads(archivePath: str, samples: int, parallelization: int):
        with (
            xz.open(archivePath, 'rb') as serialFile,
            (
                lzma.open(archivePath) if parallelization == 1 else ParallelXZReader(archivePath, parallelization)
            ) as parallelFile,
        ):
            if hasattr(parallelFile, 'blockBoundaries'):
                size = parallelFile.blockBoundaries[-1]
            else:
                parallelFile.seek(io.SEEK_END)
                size = parallelFile.tell()
                parallelFile.seek(0)

            for _ in range(samples):
                offset = random.randint(0, size + 1)
                size = random.randint(0, (size + 1 - offset) * 2)  # half the time read past the end

                serialFile.seek(offset)
                serialData = serialFile.read(size)
                parallelFile.seek(offset)
                parallelData = parallelFile.read(size)
                assert len(serialData) == len(parallelData)
                assert serialData == parallelData

    def test_empty(self, parallelization, tmp_path):
        archivePath = tmp_path / 'test-archive.xz'
        self._create_archive(archivePath, 1, 1, 0)
        for bufferSize in [1, 2, 100, 1024, 128 * 1000]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 10, parallelization)

    @pytest.mark.parametrize("streams", [1, 2, 7])
    @pytest.mark.parametrize("blocksPerStream", [1, 2, 7])
    @pytest.mark.parametrize("blockSize", [1, 2, 7])
    def test_small_multi_stream_block(self, parallelization, streams, blocksPerStream, blockSize, tmp_path):
        archivePath = os.path.join(str(tmp_path), 'test-archive.xz')
        self._create_archive(archivePath, streams, blocksPerStream, blockSize)
        for bufferSize in [1, 2, 100, 1024]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 20, parallelization)

    @pytest.mark.parametrize("blockSize", [10, 333, 1024, 10 * 1000, 64 * 1024])
    def test_large_multi_stream_block(self, parallelization, blockSize, tmp_path):
        archivePath = os.path.join(str(tmp_path), 'test-archive.xz')
        self._create_archive(archivePath, 3, 4, blockSize)
        for bufferSize in [37, 1024, 128 * 1000]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 200, parallelization)


class SeekableZstd:
    def __init__(self, filePath: str):
        self.name = filePath
        self.fileobj: IO[bytes] = io.BytesIO()
        self._rawFileObject = None
        self._reopen()

    def _reopen(self):
        if self.fileobj and not self.fileobj.closed:
            self.fileobj.close()
        if self._rawFileObject and not self._rawFileObject.closed:
            self._rawFileObject.close()

        self._rawFileObject = open(self.name, 'rb')
        self.fileobj = zstandard.ZstdDecompressor().stream_reader(self._rawFileObject, read_across_frames=True)

    def tell(self) -> int:
        return self.fileobj.tell()

    def read(self, size: int = -1) -> bytes:
        return self.fileobj.read(size)

    def close(self) -> None:
        return self.fileobj.close()

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        assert whence == io.SEEK_SET
        if offset > self.fileobj.tell():
            self.read(offset - self.fileobj.tell())
        elif offset < self.fileobj.tell():
            self._reopen()
            self.fileobj.read(offset)
        return offset

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.fileobj:
            self.fileobj.close()


@pytest.mark.parametrize("parallelization", [1, 2, 3, os.cpu_count()])
class TestParallelZstdReader:
    @staticmethod
    def _create_archive(archivePath: Path, streams: int, blocksPerStream: int, blockSize: int):
        for _ in range(streams):
            tmpPath = str(archivePath) + '.tmp'
            size = blockSize * blocksPerStream
            Path(tmpPath).write_bytes(base64.b64encode(os.urandom(size))[:size])
            subprocess.run(['zstd', '-q', f'--block-size={blockSize}', '--compress', '--force', tmpPath], check=True)
            shutil.copy(tmpPath + '.zst', archivePath)

    @staticmethod
    def _test_sequential_reading(archivePathLike: Path, bufferSize: int, parallelization: int):
        if zstandard is None:
            return

        archivePath = str(archivePathLike)

        with (
            indexed_zstd.IndexedZstdFile(archivePath) as serialFile,
            (
                SeekableZstd(archivePath) if parallelization == 1 else ParallelZstdReader(archivePath, parallelization)
            ) as parallelFile,
        ):
            bytesRead = 0
            while True:
                serialData = serialFile.read(bufferSize)
                parallelData = parallelFile.read(bufferSize)
                assert len(serialData) == len(parallelData)
                assert serialData == parallelData
                bytesRead += len(serialData)
                if len(serialData) < bufferSize:
                    break

            if hasattr(parallelFile, 'blockBoundaries'):
                assert bytesRead == parallelFile.blockBoundaries[-1]

    @staticmethod
    def _test_random_reads(archivePathLike: Path, samples: int, parallelization: int):
        if zstandard is None:
            return

        archivePath = str(archivePathLike)

        with (
            indexed_zstd.IndexedZstdFile(archivePath) as serialFile,
            (
                SeekableZstd(archivePath) if parallelization == 1 else ParallelZstdReader(archivePath, parallelization)
            ) as parallelFile,
        ):
            if hasattr(parallelFile, 'blockBoundaries'):
                size = parallelFile.blockBoundaries[-1]
            else:
                parallelFile.seek(io.SEEK_END)
                size = parallelFile.tell()
                if parallelization == 1:
                    parallelFile.close()
                    parallelFile = SeekableZstd(archivePath)
                else:
                    parallelFile.seek(0)

            for _ in range(samples):
                offset = random.randint(0, size + 1)
                size = random.randint(0, (size + 1 - offset) * 2)  # half the time read past the end

                serialFile.seek(offset)
                serialData = serialFile.read(size)

                # Files opened with the zstandard module cannot seek back not even in an emulated manner.
                if parallelization == 1 and offset < parallelFile.tell():
                    parallelFile.close()
                    parallelFile = SeekableZstd(archivePath)
                parallelFile.seek(offset)
                parallelData = parallelFile.read(size)

                assert len(serialData) == len(parallelData)
                assert serialData == parallelData

    def test_empty(self, parallelization, tmp_path):
        archivePath = tmp_path / 'test-archive.zst'
        self._create_archive(archivePath, 1, 1, 0)
        for bufferSize in [1, 2, 100, 1024, 128 * 1000]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 10, parallelization)

    @pytest.mark.parametrize("streams", [1, 2, 7])
    @pytest.mark.parametrize("blocksPerStream", [1, 2, 7])
    @pytest.mark.parametrize("blockSize", [1, 2, 7])
    def test_small_multi_stream_block(self, parallelization, streams, blocksPerStream, blockSize, tmp_path):
        archivePath = tmp_path / 'test-archive.zst'
        self._create_archive(archivePath, streams, blocksPerStream, blockSize)
        for bufferSize in [1, 2, 100, 1024]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 20, parallelization)

    @pytest.mark.parametrize("blockSize", [10, 333, 1024, 10 * 1000, 64 * 1024])
    def test_large_multi_stream_block(self, parallelization, blockSize, tmp_path):
        archivePath = tmp_path / 'test-archive.zst'
        self._create_archive(archivePath, 3, 4, blockSize)
        for bufferSize in [37, 1024, 128 * 1000]:
            self._test_sequential_reading(archivePath, bufferSize, parallelization)
        self._test_random_reads(archivePath, 200, parallelization)
