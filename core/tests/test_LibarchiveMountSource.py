# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import stat
import sys
import tarfile
import tempfile
import time
from pathlib import Path

import pytest
from helpers import copy_test_file, find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.libarchive import IterableArchive, LibarchiveMountSource  # noqa: E402


class TestLibarchiveMountSource:
    @staticmethod
    @pytest.mark.parametrize('compression', ['7z', 'rar', 'zip'])
    def test_simple_usage(compression):
        with copy_test_file('folder-symlink.' + compression) as path, LibarchiveMountSource(path) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                fileInfo = mountSource.lookup(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(folder) == 1
                assert mountSource.list(folder)

            for filePath in ['/foo/fighter/ufo']:
                fileInfo = mountSource.lookup(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.list(filePath)
                with mountSource.open(mountSource.lookup(filePath)) as file:
                    assert file.read() == b'iriya\n'

            # Links are not resolved by the mount source but by FUSE, i.e., descending into a link to a folder
            # will not work. This behavior may change in the future.
            for linkPath in ['/foo/jet']:
                assert mountSource.lookup(linkPath)
                assert mountSource.versions(linkPath) == 1
                assert not mountSource.list(linkPath)
                fileInfo = mountSource.lookup(linkPath)
                assert fileInfo.linkname == 'fighter'

    @staticmethod
    @pytest.mark.parametrize('compression', ['7z', 'rar', 'zip'])
    def test_transform(compression):
        with (
            copy_test_file('folder-symlink.' + compression) as path,
            LibarchiveMountSource(path, transform=("(.)/(.)", r"\1_\2")) as mountSource,
        ):
            for folder in ['/', '/foo', '/foo_fighter']:
                fileInfo = mountSource.lookup(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)
                assert mountSource.versions(folder) == 1

            for filePath in ['/foo_fighter_ufo']:
                fileInfo = mountSource.lookup(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.list(filePath)
                with mountSource.open(mountSource.lookup(filePath)) as file:
                    assert file.read() == b'iriya\n'

    @staticmethod
    # 7z : libarchive.exception.ArchiveError: The file content is encrypted, but currently not supported
    #      (errno=-1, retcode=-30, archive_p=94443813387248)
    # RAR: libarchive.exception.ArchiveError: Unsupported block header size (was 4, max is 2)
    #      (errno=84, retcode=-30, archive_p=94443813892640)
    # Basically only ZIP has encryption support provided by libarchive, much less than I would have thought.
    # https://github.com/libarchive/libarchive/issues/579#issuecomment-118440525
    # @pytest.mark.parametrize("compression", ["7z", "rar", "zip"])
    @pytest.mark.parametrize('compression', ['zip'])
    def test_password(compression):
        with (
            copy_test_file('encrypted-nested-tar.' + compression) as path,
            LibarchiveMountSource(path, passwords=['foo']) as mountSource,
        ):
            for folder in ['/', '/foo', '/foo/fighter']:
                fileInfo = mountSource.lookup(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(folder) == 1
                assert mountSource.list(folder)

            for filePath in ['/foo/fighter/ufo']:
                fileInfo = mountSource.lookup(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.list(filePath)
                with mountSource.open(mountSource.lookup(filePath)) as file:
                    assert file.read() == b'iriya\n'

    @staticmethod
    @pytest.mark.parametrize('compression', ['bz2', 'gz', 'lrz', 'lz4', 'lzip', 'lzma', 'lzo', 'xz', 'Z', 'zst'])
    def test_stream_compressed(compression):
        with (
            copy_test_file('simple.' + compression) as path,
            LibarchiveMountSource(path, passwords=['foo']) as mountSource,
        ):
            for folder in ['/']:
                fileInfo = mountSource.lookup(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(folder) == 1
                assert mountSource.list(folder)

            for filePath in ['/simple']:
                fileInfo = mountSource.lookup(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.list(filePath)
                with mountSource.open(mountSource.lookup(filePath)) as file:
                    assert file.read() == b'foo fighter\n'
                    assert file.seek(4) == 4
                    assert file.read() == b'fighter\n'

    @staticmethod
    @pytest.mark.parametrize(
        ('path', 'lineSize'),
        [
            # libarchive bug: https://github.com/libarchive/libarchive/issues/2106
            # ('two-large-files-32Ki-lines-each-1024B.7z', 1024),
            ('two-large-files-32Ki-lines-each-1023B.7z', 1023),
        ],
    )
    def test_file_independence(path, lineSize):
        with copy_test_file(path) as copiedPath, LibarchiveMountSource(copiedPath) as mountSource:
            with mountSource.open(mountSource.lookup('zeros-32-MiB.txt')) as fileWithZeros:
                expectedZeros = b'0' * (lineSize - 1) + b'\n'
                assert fileWithZeros.read(lineSize) == expectedZeros
                assert fileWithZeros.tell() == lineSize
                assert fileWithZeros.seek(-lineSize, io.SEEK_END)
                actualZeros = fileWithZeros.read()
                assert len(actualZeros) == len(expectedZeros)
                assert actualZeros == expectedZeros

            with (
                mountSource.open(mountSource.lookup('zeros-32-MiB.txt')) as fileWithZeros,
                mountSource.open(mountSource.lookup('spaces-32-MiB.txt')) as fileWithSpaces,
            ):
                expectedSpaces = b' ' * (lineSize - 1) + b'\n'
                expectedZeros = b'0' * (lineSize - 1) + b'\n'

                assert fileWithSpaces.read(lineSize) == expectedSpaces
                assert fileWithSpaces.tell() == lineSize

                assert fileWithZeros.read(lineSize) == expectedZeros
                assert fileWithZeros.tell() == lineSize

                assert fileWithSpaces.tell() == lineSize

                assert fileWithSpaces.seek(-lineSize, io.SEEK_END)
                assert fileWithZeros.seek(-lineSize, io.SEEK_END)
                assert fileWithSpaces.read() == expectedSpaces
                assert fileWithZeros.read() == expectedZeros

                # Seek backwards inside of buffer

                assert fileWithSpaces.seek(-lineSize + 1, io.SEEK_END)
                assert fileWithZeros.seek(-lineSize + 1, io.SEEK_END)
                assert fileWithSpaces.read() == expectedSpaces[1:]
                assert fileWithZeros.read() == expectedZeros[1:]

                # Seek backwards outside of buffer

                assert fileWithSpaces.seek(1) == 1
                assert fileWithSpaces.read(lineSize - 1) == expectedSpaces[1:]
                assert fileWithSpaces.tell() == lineSize

                assert fileWithZeros.seek(1) == 1
                assert fileWithZeros.read(lineSize - 1) == expectedZeros[1:]
                assert fileWithZeros.tell() == lineSize

    @staticmethod
    def test_file_object_reader():
        memoryFile = io.BytesIO(Path(find_test_file('folder-symlink.zip')).read_bytes())
        with IterableArchive(memoryFile) as archive:
            while True:
                entry = archive.next_entry()
                if entry is None:
                    break
                fileInfo = entry.convert_to_row(0, lambda x: x)
                assert fileInfo

    @staticmethod
    def _create_file(tarArchive, name, contents):
        tinfo = tarfile.TarInfo(name)
        tinfo.size = len(contents)
        tarArchive.addfile(tinfo, io.BytesIO(contents.encode()))

    @staticmethod
    def create_large_file(tarPath, compression, fileCount):
        # I have committed the resulting bz2 file to save test time.
        t0 = time.time()
        create_file = TestLibarchiveMountSource._create_file
        with tarfile.open(name=tarPath, mode='w:' + compression) as tarFile:
            for i in range(fileCount):
                create_file(tarFile, name=str(i), contents=str(i % 10))
                if i % 50_000 == 0:
                    print(f"Added {i} out of {fileCount} files to .tar.{compression} in {time.time() - t0:.3f} s")

        # contents = str(i)
        #   300k files for bz2 takes ~13 s and the resulting file is 986 KiB
        #   300k files for xz takes ~26 s and the resulting file is 259 KiB
        #   300k files for gz takes ~34 s and the resulting file is 2662 KiB
        # contents = str(i % 10)
        #   300k files for bz2 takes ~12 s and the resulting file is 580 KiB
        #   300k files for xz takes ~32 s and the resulting file is 544 KiB
        #   300k files for gz takes ~16 s and the resulting file is 2774 KiB
        # Funny how the compressed size is larger than the str(i) case for the LZ-based compressors
        # even though the uncompressed size is smaller! Only bz2 actually shrinks in compressed size!

    @staticmethod
    def _test_large_file(path):
        t0 = time.time()
        fileCount = 0
        with LibarchiveMountSource(path) as mountSource:
            t1 = time.time()
            print(f"Opening {path} took {time.time() - t0:.3f} s")  # ~5 s
            # In the worst case, reading all files can take 300k * 5s / 2 = ~9 days.
            # In the best case, it should take roughly 5s, same as when iterating over the archive in order.
            # The worst case happens if:
            #  - The returned order by list is messed up, e.g., random in the worst case, or even this is bad:
            #    0, 1, 10, 100, 1000, 10000, 100000, 100001, 100002, ...
            #  - Each file open reads the archive from the beginning instead of reusing the current libarchive handle.
            entries = mountSource.list('/')
            assert isinstance(entries, dict)
            t2 = time.time()
            print(f"Listing all {len(entries)} files took {t2 - t1:.3f} s")  # ~2 s

            fileCount = 0
            for fileName, fileInfo in entries.items():
                with mountSource.open(fileInfo) as file:
                    assert file.read() == fileName[-1:].encode()

                if fileCount > 0 and fileCount % 50_000 == 0:
                    print(f"Checked {fileCount} files' contents.")
                fileCount += 1

                # Depends on the system, but accounting for 10 times slower systems should be a good margin.
                # And we are still FAR off from the worst case time that should not happen.
                assert time.time() - t2 < 360

            print(f"Reading all {fileCount} files took {time.time() - t2:.3f} s")  # 36 s on my system

            # Test seeking back for good measure
            fileName = '2'
            fileInfo = entries.get(fileName)
            with mountSource.open(fileInfo) as file:
                assert file.read() == fileName[-1:].encode()

    @staticmethod
    @pytest.mark.parametrize('compression', ['bz2', 'gz', 'xz'])
    def test_large_file(compression):
        path = "tar-with-300-folders-with-1000-files-1B-files.tar." + compression
        with tempfile.NamedTemporaryFile(suffix='.' + path) as tmpTarFile:
            TestLibarchiveMountSource.create_large_file(tmpTarFile.name, compression=compression, fileCount=300_000)
            TestLibarchiveMountSource._test_large_file(tmpTarFile.name)
