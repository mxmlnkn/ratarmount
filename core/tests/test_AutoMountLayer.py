# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402
from ratarmountcore.mountsource.compositing.automount import AutoMountLayer  # noqa: E402
from ratarmountcore.mountsource.factory import open_mount_source  # noqa: E402


# @pytest.mark.parametrize("parallelization", [1, 2, 4])
@pytest.mark.parametrize("parallelization", [1])
class TestAutoMountLayer:
    @staticmethod
    def test_regex_mount_point_tar(parallelization):
        options = {
            'clearIndexCache': True,
            'recursive': True,
            'parallelization': parallelization,
            'transformRecursiveMountPoint': ('.*/([^/]*).tar', r'\1'),
        }

        with copy_test_file("packed-100-times.tar.gz") as path, open_mount_source(path, **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.list('/')
            assert recursivelyMounted.list('/ufo_12')
            assert recursivelyMounted.list('/ufo_00')
            assert list(recursivelyMounted.list('/ufo_00').keys()) == ['ufo']
            assert recursivelyMounted.open(recursivelyMounted.lookup('/ufo_00/ufo')).read() == b'iriya\n'

    @staticmethod
    def test_regex_mount_point_tar_gz(parallelization):
        options = {
            'clearIndexCache': True,
            'recursive': True,
            'parallelization': parallelization,
            'transformRecursiveMountPoint': ('.*/([^/]*).tar.gz', r'\1'),
        }

        # TODO Using the compressed-1000-times.tar.gz, which is ~200 KiB compressed and uncompressed leads to 12 GiB
        #      of memory usage!! And not only that, after this, that memory is not freed for the subsequent tests in
        #      other files and those other files will actually take 10x or more longer than without this test running
        #      before! It might be that the memory usage makes Python's garbage collector a bottleneck because of too
        #      many small objects?!
        with copy_test_file("compressed-100-times.tar.gz") as path, open_mount_source(path, **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.list('/')
            assert recursivelyMounted.list('/ufo_12')
            assert recursivelyMounted.list('/ufo_00')
            assert list(recursivelyMounted.list('/ufo_00').keys()) == ['ufo']
            assert recursivelyMounted.open(recursivelyMounted.lookup('/ufo_00/ufo')).read() == b'iriya\n'

    @staticmethod
    def test_regex_mount_point_gz(parallelization):
        options = {
            'clearIndexCache': True,
            'recursive': True,
            'parallelization': parallelization,
            'transformRecursiveMountPoint': ('.*/([^/]*).gz', r'\1'),
        }

        # For some reason, the test with 1000 recursion fails reproducibly after ~196 depth, therefore use only 100.
        # > Recursively mounted: /ufo_805.gz
        # >  File "core/ratarmountcore/SQLiteIndexedTar.py", line 2085, in _detectTar
        # > indexed_gzip.indexed_gzip.ZranError: zran_read returned error: ZRAN_READ_FAIL (file: n/a)
        # On MacOS, the test fails even sooner after only 7 layers:
        #     core/tests/test_AutoMountLayer.py::TestAutoMountLayer::test_regex_mount_point_gz[1]
        #     [Info] Try to open with rarfile
        #     [Info] Try to open with tarfile
        #     [Info] Detected compression gz for file object: <_io.BufferedReader
        #         name='/var/folders/24/8k4/T/tmp5/compressed-100-times.gz'>
        #     [Info] Do not reopen with rapidgzip backend because:
        #     [Info]  - is too small to qualify for parallel decompression.
        #     [Info] Undid gz file compression by using: IndexedGzipFile
        #     [...]
        #     Recursively mounted: /ufo_94
        #     [Info] Try to open with rarfile
        #     [Info] Try to open with tarfile
        #     [Info] Detected compression gz for file object: <StenciledFile>
        #     [Info] Do not reopen with rapidgzip backend because:
        #     [Info]  - the file to open is a recursive file, which limits the usability of
        #     [Info]    parallel decompression.
        #     [Info]  - is too small to qualify for parallel decompression.
        #     [Info] Undid gz file compression by using: IndexedGzipFile
        #     [Info] File object <IndexedGzipFile> is not a TAR.
        #     Creating offset dictionary for ufo_93.gz ...
        #     Did not find any file in the given TAR: ufo_93.gz. Assuming a compressed file.
        #     Resorting files by path ...
        #     Creating offset dictionary for ufo_93.gz took 0.00s
        #     [Info] Tried to write the database to disk but found no other path than: :memory:
        #     [Info] Will skip storing compression seek data because the database is in memory.
        #     [Info] If the database is in memory, then this data will not be read anyway.
        #     /Users/runner/work/_temp/b97a4216-0bf9-484c-b63a-20f214563ba6.sh: line 3: 6181
        #         Illegal instruction: 4  pytest -s --verbosity=3 --disable-warnings "$file"
        #     [Info] Opened archive with tarfile backend.
        #     Error: Process completed with exit code 132.
        # Probably a literal stack overflow because of the recursion, which results in a function return to
        # garbage data resulting in an illegal instruction?
        if sys.platform.startswith('darwin'):
            return

        with copy_test_file("compressed-100-times.gz") as path, open_mount_source(path, **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.list('/')
            assert recursivelyMounted.list('/ufo_12')
            assert recursivelyMounted.list('/ufo_00')
            fileInfo = recursivelyMounted.lookup('/ufo_00/ufo')
            assert recursivelyMounted.open(fileInfo).read() == b'iriya\n'

    @staticmethod
    def test_file_versions(parallelization):
        options = {
            'clearIndexCache': True,
            'recursive': True,
            'parallelization': parallelization,
        }

        with (
            copy_test_file("tests/double-compressed-nested-tar.tgz.tgz") as path,
            open_mount_source(path, **options) as mountSource,
        ):
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            for folder in ['/', '/nested-tar.tar.gz', '/nested-tar.tar.gz/foo', '/nested-tar.tar.gz/foo/fighter']:
                assert recursivelyMounted.lookup(folder)
                assert recursivelyMounted.list(folder)
                assert recursivelyMounted.versions(folder) > 0

            for mountedFile in ['/nested-tar.tar.gz']:
                assert recursivelyMounted.versions(folder) > 0
                assert stat.S_ISREG(recursivelyMounted.lookup(mountedFile, fileVersion=1).mode)

            # assert recursivelyMounted.open(recursivelyMounted.lookup('/ufo_00/ufo')).read() == b'iriya\n'

    @staticmethod
    @pytest.mark.parametrize("recursive", [False])
    @pytest.mark.parametrize("maxRecursionDepth", range(7))
    def test_recursion_depth(parallelization, recursive, maxRecursionDepth):
        name = "triple-compressed-nested-tar"
        archivePath = f"tests/{name}.tgz.tgz.gz"
        options = {
            'clearIndexCache': True,
            'recursive': recursive,
            'recursionDepth': maxRecursionDepth,
            'parallelization': parallelization,
        }
        with copy_test_file(archivePath) as path, open_mount_source(path, **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)
            assert recursivelyMounted.list('/')

            maxDepth = 6
            recursionDepth = maxDepth if maxRecursionDepth is None else maxRecursionDepth

            def check_directory(fileInfo):
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)

            def check_file(fileInfo, size=None):
                assert fileInfo
                assert fileInfo.size == size
                assert not stat.S_ISDIR(fileInfo.mode)
                assert stat.S_ISREG(fileInfo.mode)

            def check_non_existing(fileInfo):
                assert not fileInfo

            # This was insane brainfuck... Off by one errors everywhere.
            # Check with: ratarmount -f -c --recursion-depth 1 tests/triple-compressed-nested-tar.tgz.tgz.gz mounted
            nestedRoot = f"/{name}.tgz.tgz/nested-tar.tar.gz"
            testPaths = {
                f"/{name}.tgz.tgz": {
                    0: lambda path: check_file(path, size=436),
                    1: lambda path: check_directory,
                },
                f"/{name}.tgz.tgz/{name}.tgz.tgz": {
                    1: lambda path: check_file(path, size=10240),
                    2: lambda path: check_directory,
                },
                nestedRoot: {
                    2: lambda path: check_file(path, size=291),
                    3: lambda path: check_directory,
                },
                # This file is only visible when only the gzip compression has been undone.
                # And the name is probably the one stored in gzip itself. It has support for that!
                # For the next recursion, this virtual file will be replaced by the actual contents.
                nestedRoot
                + "/nested-tar.tar": {
                    3: lambda path: check_file(path, size=20480),
                    4: check_non_existing,
                },
                nestedRoot
                + "/foo": {
                    4: lambda path: check_directory,
                },
                nestedRoot
                + "/foo/fighter": {
                    4: lambda path: check_directory,
                },
                nestedRoot
                + "/foo/fighter/ufo": {
                    4: lambda path: check_file(path, size=6),
                },
                nestedRoot
                + "/foo/lighter.tar": {
                    4: lambda path: check_file(path, size=10240),
                    5: lambda path: check_directory,
                },
                nestedRoot
                + "/foo/lighter.tar/fighter": {
                    5: lambda path: check_directory,
                },
                nestedRoot
                + "/foo/lighter.tar/fighter/bar": {
                    5: lambda path: check_file(path, size=10),
                },
            }

            # Check paths up to the allowed recursion depth
            for path, configurations in testPaths.items():
                fileInfo = recursivelyMounted.lookup(path)

                # Files should not exist when their actual depth is larger than the specified mount depth.
                if recursionDepth < min(configurations.keys()):
                    assert not recursivelyMounted.exists(path)
                    assert fileInfo is None
                    continue

                expectedDepth = recursionDepth if recursionDepth in configurations else max(configurations.keys())
                configurations[expectedDepth](fileInfo)
