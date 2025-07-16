# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import concurrent.futures
import io
import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402
from ratarmountcore.StenciledFile import (  # noqa: E402
    JoinedFile,
    JoinedFileFromFactory,
    RawStenciledFile,
    StenciledFile,
)

testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write(testData)


randomTestData = os.urandom(128 * 1024)
randomTmpFile = tempfile.TemporaryFile()
randomTmpFile.write(randomTestData)


class TestStenciledFile:
    @staticmethod
    def _create_stenciled_file(file, stencils):
        return StenciledFile(fileStencils=[(file, *stencil) for stencil in stencils])

    @staticmethod
    def test_empty_file():
        assert StenciledFile([(tmpFile, 0, 0)]).read() == b""

        with StenciledFile([]) as file:
            assert file.readable()
            assert file.seekable()
            assert file.read() == b""
            assert file.seek(0, io.SEEK_SET) == 0
            assert file.seek(0, io.SEEK_CUR) == 0
            assert file.seek(0, io.SEEK_END) == 0
            assert file.read() == b""

    @staticmethod
    def test_find_stencil():
        stenciledFile = RawStenciledFile(
            [(tmpFile, *stencil) for stencil in [(1, 2), (2, 2), (0, 2), (4, 4), (1, 8), (0, 1)]]
        )
        expectedResults = [0, 0, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5]
        for offset, iExpectedStencil in enumerate(expectedResults):
            assert stenciledFile._find_stencil(offset) == iExpectedStencil

    @staticmethod
    def test_single_stencil():
        assert StenciledFile([(tmpFile, 0, 1)]).read() == b"1"
        assert StenciledFile([(tmpFile, 0, 2)]).read() == b"12"
        assert StenciledFile([(tmpFile, 0, 3)]).read() == b"123"
        assert StenciledFile([(tmpFile, 0, len(testData))]).read() == testData

    @staticmethod
    def test_1_byte_stencils():
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 1), (1, 1)]).read() == b"12"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 1), (2, 1)]).read() == b"13"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 1), (0, 1)]).read() == b"21"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 1), (1, 1), (2, 1)]).read() == b"123"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 1), (2, 1), (0, 1)]).read() == b"231"

    @staticmethod
    def test_2_byte_stencils():
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 2), (1, 2)]).read() == b"1223"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 2), (2, 2)]).read() == b"1234"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (0, 2)]).read() == b"2312"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(0, 2), (1, 2), (2, 2)]).read() == b"122334"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read() == b"233412"

    @staticmethod
    def test_read_with_size():
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(0) == b""
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(1) == b"2"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(2) == b"23"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(3) == b"233"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(4) == b"2334"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(5) == b"23341"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(6) == b"233412"
        assert TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(7) == b"233412"

    @staticmethod
    def test_seek_and_tell():
        file = TestStenciledFile._create_stenciled_file(tmpFile, [(1, 2), (2, 2), (0, 2)])

        assert file.readable()
        assert file.seekable()
        assert not file.writable()
        assert not file.closed

        for i in range(7):
            assert file.tell() == i
            file.read(1)
        for i in reversed(range(6)):
            assert file.seek(-1, io.SEEK_CUR) == i
            assert file.tell() == i
        assert file.seek(0, io.SEEK_END) == 6
        assert file.tell() == 6
        assert file.seek(20, io.SEEK_END) == 26
        assert file.tell() == 26
        assert file.read(1) == b""
        assert file.seek(-6, io.SEEK_END) == 0
        assert file.read(1) == b"2"

        with pytest.raises(io.UnsupportedOperation):
            file.fileno()

        file.close()
        assert file.closed

    @staticmethod
    def test_reading_from_shared_file():
        file1 = StenciledFile([(tmpFile, 0, len(testData))])
        file2 = StenciledFile([(tmpFile, 0, len(testData))])
        for i in range(len(testData)):
            assert file1.read(1) == testData[i : i + 1]
            assert file2.read(1) == testData[i : i + 1]

    @staticmethod
    def successive_reads_test(lock=None):
        file = StenciledFile([(randomTmpFile, 0, len(randomTestData))], lock)
        batchSize = 1024
        for i in range(len(randomTestData) // batchSize):
            assert file.read(batchSize) == randomTestData[i * batchSize : (i + 1) * batchSize]

    @staticmethod
    def test_successive_reads():
        TestStenciledFile.successive_reads_test()

    @staticmethod
    def test_multithreaded_reading():
        parallelism = 24
        with concurrent.futures.ThreadPoolExecutor(24) as pool:
            lock = threading.Lock()
            results = [pool.submit(TestStenciledFile.successive_reads_test, lock) for _ in range(parallelism)]
            for result in results:
                result.result()


class TestJoinedFile:
    @staticmethod
    def test_empty_file():
        assert JoinedFile([io.BytesIO(b"")]).read() == b""

    @staticmethod
    def test_single_file():
        assert JoinedFile([io.BytesIO(b"f")]).read() == b"f"
        assert JoinedFile([io.BytesIO(b"fo")]).read() == b"fo"
        assert JoinedFile([io.BytesIO(b"foo")]).read() == b"foo"

    @staticmethod
    def test_single_file_non_complete_read():
        assert JoinedFile([io.BytesIO(b"f")]).read(1) == b"f"
        assert JoinedFile([io.BytesIO(b"fo")]).read(1) == b"f"
        assert JoinedFile([io.BytesIO(b"foo")]).read(1) == b"f"

    @staticmethod
    def test_single_file_seek_read():
        file = JoinedFile([io.BytesIO(b"foobar")])

        assert file.readable()
        assert file.seekable()
        assert not file.writable()
        assert not file.closed

        assert file.seek(1) == 1
        assert file.read(1) == b"o"
        assert file.read() == b"obar"
        assert file.seek(-1, io.SEEK_END) == 5
        assert file.read() == b"r"

        with pytest.raises(io.UnsupportedOperation):
            file.fileno()

        file.close()
        assert file.closed

    @staticmethod
    def test_two_files_full_read():
        assert JoinedFile([io.BytesIO(b""), io.BytesIO(b"")]).read() == b""
        assert JoinedFile([io.BytesIO(b""), io.BytesIO(b"foo")]).read() == b"foo"
        assert JoinedFile([io.BytesIO(b"foo"), io.BytesIO(b"")]).read() == b"foo"
        assert JoinedFile([io.BytesIO(b"bar"), io.BytesIO(b"foo")]).read() == b"barfoo"

    @staticmethod
    def test_two_files_seak_and_read():
        file = JoinedFile([io.BytesIO(b"bar"), io.BytesIO(b"foo")])
        assert file.read(1) == b"b"
        assert file.tell() == 1

        assert file.seek(3) == 3
        assert file.tell() == 3

        assert file.read(2) == b"fo"
        assert file.tell() == 5

        assert file.seek(-4, io.SEEK_END) == 2
        assert file.tell() == 2

        assert file.read() == b"rfoo"
        assert file.tell() == 6


class TestJoinedFileFromFactory:
    @staticmethod
    def test_empty_file():
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"")]).read() == b""

    @staticmethod
    def test_single_file():
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"f")]).read() == b"f"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"fo")]).read() == b"fo"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"foo")]).read() == b"foo"

    @staticmethod
    def test_single_file_non_complete_read():
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"f")]).read(1) == b"f"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"fo")]).read(1) == b"f"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"foo")]).read(1) == b"f"

    @staticmethod
    def test_single_file_seek_read():
        file = JoinedFileFromFactory([lambda: io.BytesIO(b"foobar")])

        assert file.readable()
        assert file.seekable()
        assert not file.writable()
        assert not file.closed

        assert file.seek(1) == 1
        assert file.read(1) == b"o"
        assert file.read() == b"obar"
        assert file.seek(-1, io.SEEK_END) == 5
        assert file.read() == b"r"

        with pytest.raises(io.UnsupportedOperation):
            file.fileno()

        file.close()
        assert file.closed

    @staticmethod
    def test_two_files_full_read():
        assert JoinedFileFromFactory([lambda: io.BytesIO(b""), lambda: io.BytesIO(b"")]).read() == b""
        assert JoinedFileFromFactory([lambda: io.BytesIO(b""), lambda: io.BytesIO(b"foo")]).read() == b"foo"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"foo"), lambda: io.BytesIO(b"")]).read() == b"foo"
        assert JoinedFileFromFactory([lambda: io.BytesIO(b"bar"), lambda: io.BytesIO(b"foo")]).read() == b"barfoo"

    @staticmethod
    def test_two_files_seak_and_read():
        file = JoinedFileFromFactory([lambda: io.BytesIO(b"bar"), lambda: io.BytesIO(b"foo")])
        assert file.read(1) == b"b"
        assert file.tell() == 1

        assert file.seek(3) == 3
        assert file.tell() == 3

        assert file.read(2) == b"fo"
        assert file.tell() == 5

        assert file.seek(-4, io.SEEK_END) == 2
        assert file.tell() == 2

        assert file.read() == b"rfoo"
        assert file.tell() == 6

    @staticmethod
    def test_joining_files(tmp_path):
        files = [tmp_path / name for name in ["foo.001", "foo.002"]]
        files[0].write_bytes(b"foo")
        files[1].write_bytes(b"bar")

        factories = [lambda file=file: open(file, 'rb') for file in files]
        assert len(factories) == 2
        assert factories[0]().read() == b"foo"
        assert factories[1]().read() == b"bar"

        assert JoinedFileFromFactory(factories).read() == b"foobar"
