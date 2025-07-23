# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import concurrent.futures
import io
import os
import stat
import subprocess
import sys
import tarfile
import tempfile

import pytest
import rapidgzip
from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar, SQLiteIndexedTarUserData  # noqa: E402
from ratarmountcore.utils import RatarmountError  # noqa: E402


@pytest.mark.parametrize("parallelization", [1, 2, 4])
class TestSQLiteIndexedTarParallelized:
    @staticmethod
    def _create_file(tarArchive, name, contents):
        tinfo = tarfile.TarInfo(name)
        tinfo.size = len(contents)
        tarArchive.addfile(tinfo, io.BytesIO(contents.encode()))

    @staticmethod
    def _make_folder(tarArchive, name):
        tinfo = tarfile.TarInfo(name)
        tinfo.type = tarfile.DIRTYPE
        tarArchive.addfile(tinfo, io.BytesIO())

    @staticmethod
    def test_context_manager(parallelization):
        with (
            copy_test_file("single-file.tar") as path,
            SQLiteIndexedTar(path, writeIndex=False, parallelization=parallelization) as indexedTar,
        ):
            assert indexedTar.list('/')
            assert indexedTar.lookup('/')

            fileInfo = indexedTar.lookup('/bar')
            assert fileInfo
            assert fileInfo.size == 4
            userdata = fileInfo.userdata[-1]
            assert isinstance(userdata, SQLiteIndexedTarUserData)
            assert userdata.recursiondepth == 0
            assert not userdata.isgenerated

            assert not indexedTar.lookup('../')
            assert not indexedTar.lookup('../bar')

    @staticmethod
    def test_tar_bz2_with_parallelization(parallelization):
        with (
            copy_test_file("2k-recursive-tars.tar.bz2") as path,
            SQLiteIndexedTar(
                path,
                clearIndexCache=True,
                recursive=False,
                parallelization=parallelization,
            ) as file,
        ):
            for folder in ['/', '/mimi']:
                assert file.lookup(folder)
                assert file.versions(folder) == 1
                assert file.list(folder)

            assert not file.list('/mimi/01995.tar')
            info = file.lookup('/mimi/01995.tar')
            assert info.userdata[0].offset == 21440512

            assert not file.list('/mimi/00105.tar')
            info = file.lookup('/mimi/00105.tar')
            assert info.userdata[0].offset == 1248256

    @staticmethod
    def test_recursive_tar_bz2_with_parallelization(parallelization):
        with (
            copy_test_file("2k-recursive-tars.tar.bz2") as path,
            SQLiteIndexedTar(
                path,
                clearIndexCache=True,
                recursive=True,
                parallelization=parallelization,
            ) as file,
        ):
            assert file.list('/')
            assert file.list('/mimi')

            assert file.list('/mimi/01995.tar')
            info = file.lookup('/mimi/01995.tar/foo')
            assert info.userdata[0].offset == 21441024

            assert file.list('/mimi/00105.tar')
            info = file.lookup('/mimi/00105.tar/foo')
            assert info.userdata[0].offset == 1248768

    @staticmethod
    @pytest.mark.parametrize("recursive", [False, True])
    @pytest.mark.parametrize("maxRecursionDepth", [None, 0, 1, 2, 3, 4, 5])
    def test_deep_recursive(parallelization, recursive, maxRecursionDepth):
        with (
            copy_test_file("packed-5-times.tar.gz") as path,
            SQLiteIndexedTar(
                path,
                clearIndexCache=True,
                recursive=recursive,
                recursionDepth=maxRecursionDepth,
                parallelization=parallelization,
            ) as mountSource,
        ):
            # packed-5-times.tar.gz -> /ufo_03.tar/ufo_02.tar/ufo_01.tar/ufo_00.tar/ufo
            maxDepth = 5  # 4 TAR archives + 1 compression layer
            recursionDepth = (maxDepth if recursive else 0) if maxRecursionDepth is None else maxRecursionDepth
            # Old default of undoing compression + TAR layer if nothing is specified.
            if not recursive and maxRecursionDepth is None:
                recursionDepth = 1

            assert mountSource.list('/')
            assert mountSource.lookup('/').userdata[-1] == SQLiteIndexedTarUserData(0, 0, False, False, True, 0)

            # Recursion depth:
            # file.tar -> / 0, /bar 0
            # nested-file.tar -> / 0, /bar.tar.version/1 0, /bar/ 1, /bar/foo 1
            # file.tar.gz -> / 0, /bar 1
            #    TODO: /file.tar 0  would be expected for completeness but it was not there in the past!
            # nested-file.tar -> / 0, /bar.tar.version/1 0, /bar/ 1, /bar/foo 1

            # The (generated) root has always recursion depth 0 because it will always be shown!
            assert mountSource.lookup('/').userdata[-1].recursiondepth == 0

            # Currently there is no way to get to this file when recursively mounted.
            # We would have to enable "/.versions/" to get older versions of the root folder or folders in general.
            if recursionDepth == 0:
                fileInfo = mountSource.lookup('/packed-5-times.tar')
                assert fileInfo
                userdata = fileInfo.userdata[-1]
                assert userdata.recursiondepth == 0
                assert stat.S_ISREG(fileInfo.mode)
                assert fileInfo.size == 51200

            # The recursion depth is 1 because of the gzip compression. Consider ufo_03.tar being inside
            # packaged-5-times.tar (without the .gz), then the recursion depth would have to be one less, i.e., 0!
            fileInfo = mountSource.lookup('/ufo_03.tar')
            if recursionDepth == 0:
                assert fileInfo is None
            elif recursionDepth == 1:
                assert fileInfo
                userdata = fileInfo.userdata[-1]
                assert userdata.recursiondepth == 1  # Not 0 because gzip compression counts as one layer of recursion.
                assert stat.S_ISREG(fileInfo.mode)
                assert not stat.S_ISDIR(fileInfo.mode)
                assert fileInfo.size == 40960
            else:
                assert fileInfo
                userdata = fileInfo.userdata[-1]
                assert userdata.recursiondepth == 2
                assert stat.S_ISDIR(fileInfo.mode)

                # Check that the older version is still reachable.
                fileInfo = mountSource.lookup('/ufo_03.tar', fileVersion=0 if recursionDepth == 0 else 1)
                assert fileInfo
                userdata = fileInfo.userdata[-1]
                assert userdata.recursiondepth == 1  # Not 0 because gzip compression counts as one layer of recursion.
                assert stat.S_ISREG(fileInfo.mode)
                assert not stat.S_ISDIR(fileInfo.mode)
                assert fileInfo.size == 40960

            def check_recursive_mount_point(path, depth):
                fileInfo = mountSource.lookup(path)
                if depth > recursionDepth:
                    return

                assert fileInfo
                userdata = fileInfo.userdata[-1]
                assert isinstance(userdata, SQLiteIndexedTarUserData)
                if depth <= recursionDepth:
                    assert stat.S_ISDIR(fileInfo.mode)
                    assert mountSource.list(path)
                else:
                    assert not stat.S_ISDIR(fileInfo.mode)

                assert userdata.recursiondepth == depth
                assert userdata.isgenerated
                assert userdata.istar

                if depth == maxDepth:
                    fileInfo = mountSource.lookup(path + '/ufo')
                    assert fileInfo
                    assert mountSource.open(fileInfo).read() == b'iriya\n'

            check_recursive_mount_point('/ufo_03.tar', 2)
            check_recursive_mount_point('/ufo_03.tar/ufo_02.tar', 3)
            check_recursive_mount_point('/ufo_03.tar/ufo_02.tar/ufo_01.tar', 4)
            check_recursive_mount_point('/ufo_03.tar/ufo_02.tar/ufo_01.tar/ufo_00.tar', 5)

    @staticmethod
    def test_compressed_tar(parallelization):
        with (
            copy_test_file("packed-5-times.tar.gz") as path,
            SQLiteIndexedTar(
                path,
                clearIndexCache=True,
                parallelization=parallelization,
            ) as mountSource,
        ):
            assert mountSource.list('/')

            # See test_deep_recursive for recursion depth discussion.
            assert mountSource.lookup('/').userdata[-1] == SQLiteIndexedTarUserData(0, 0, False, False, True, 0)
            fileInfo = mountSource.lookup('/ufo_03.tar')
            assert fileInfo
            assert stat.S_ISREG(fileInfo.mode)
            assert fileInfo.userdata[-1].recursiondepth == 1

    @staticmethod
    def test_index_creation_and_loading(parallelization):
        with tempfile.TemporaryDirectory() as tmpDirectory:
            oldCurrentWorkingDirectory = os.getcwd()

            # Try with a writable directory and a non-writable current working directory
            # because FUSE also changes to root after mounting and forking to background.
            for directory in [tmpDirectory, '/']:
                os.chdir(directory)
                try:
                    archiveName = 'simple.bz2'
                    indexPath = 'simple.custom.index'
                    if directory != tmpDirectory:
                        archiveName = os.path.join(tmpDirectory, archiveName)
                        indexPath = os.path.join(tmpDirectory, indexPath)

                    contents = b"Hello World!"
                    with bz2.open(archiveName, "wb") as bz2File:
                        bz2File.write(contents)

                    def test_index(tarFileName, fileObject, indexFilePath, contents=contents):
                        TestSQLiteIndexedTarParallelized._test_index_creation_and_loading(
                            tarFileName, fileObject, indexFilePath, contents, parallelization
                        )

                    # States for arguments:
                    #  - file name: 3 (None, Path, ':memory:')
                    #  - archiveName: Optional[str]
                    #  - fileObject: Optional[IO]
                    # => 3*2*2 = 12 cases

                    with pytest.raises(RatarmountError):
                        test_index(None, None, ':memory:')
                    with pytest.raises(RatarmountError):
                        test_index(None, None, indexPath)
                    with pytest.raises(RatarmountError):
                        test_index(None, None, None)

                    test_index(archiveName, None, ':memory:')
                    test_index(archiveName, None, indexPath)
                    test_index(archiveName, None, None)

                    with open(archiveName, "rb") as file:
                        test_index("tarFileName", file, ':memory:')
                        test_index("tarFileName", file, indexPath)
                        test_index("tarFileName", file, None)

                        test_index(None, file, ':memory:')
                        test_index(None, file, indexPath)
                        test_index(None, file, None)

                finally:
                    os.chdir(oldCurrentWorkingDirectory)

    @staticmethod
    def _test_index_creation_and_loading(tarFileName, fileObject, indexFilePath, contents, parallelization):
        if indexFilePath:
            assert not os.path.exists(indexFilePath) or os.stat(indexFilePath).st_size == 0
        oldFolderContents = os.listdir('.')

        # Create index
        with SQLiteIndexedTar(
            fileObject=fileObject,
            tarFileName=tarFileName,
            writeIndex=True,
            clearIndexCache=True,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
        ):
            pass

        # When opening a file object without a path or with path ':memory:',
        # no index should have been created anywhere!
        if (fileObject is not None and indexFilePath is None) or indexFilePath == ':memory:':
            assert oldFolderContents == os.listdir('.')
            return

        createdIndexFilePath = indexFilePath
        if not indexFilePath and not fileObject and tarFileName:
            createdIndexFilePath = tarFileName + '.index.sqlite'
        assert os.stat(createdIndexFilePath).st_size > 0

        # Read from index
        indexedFile = SQLiteIndexedTar(
            fileObject=fileObject,
            tarFileName=tarFileName,
            writeIndex=False,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        )

        objectName = '<file object>' if tarFileName is None else tarFileName
        expectedName = os.path.basename(tarFileName).rsplit('.', 1)[0] if fileObject is None else objectName

        folderList = indexedFile.list("/")
        assert isinstance(folderList, dict)
        if isinstance(folderList, dict):
            # https://github.com/PyCQA/pylint/issues/1162
            assert expectedName in folderList  # pylint: disable=unsupported-membership-test
            assert folderList[expectedName].size == len(contents)  # pylint: disable=unsubscriptable-object

        finfo = indexedFile.lookup("/" + expectedName)
        assert finfo.size == len(contents)
        assert indexedFile.read(finfo, size=len(contents), offset=0) == contents
        assert indexedFile.read(finfo, size=3, offset=3) == contents[3:6]

        if createdIndexFilePath:
            os.remove(createdIndexFilePath)

    @staticmethod
    def test_list_and_versions(parallelization):
        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpTarFile:
            with tarfile.open(name=tmpTarFile.name, mode="w:gz") as tarFile:
                create_file = TestSQLiteIndexedTarParallelized._create_file
                make_folder = TestSQLiteIndexedTarParallelized._make_folder

                create_file(tarFile, "./README.md", "hello world")
                make_folder(tarFile, "./src")
                create_file(tarFile, "./src/test.sh", "echo hi")
                make_folder(tarFile, "./dist")
                make_folder(tarFile, "./dist/a")
                make_folder(tarFile, "./dist/a/b")
                create_file(tarFile, "./dist/a/b/test2.sh", "echo two")

            with SQLiteIndexedTar(tmpTarFile.name, clearIndexCache=True, parallelization=parallelization) as indexedTar:
                folders = []
                files = []

                foldersToRecurse = ["/"]
                while foldersToRecurse:
                    folder = foldersToRecurse.pop()
                    folderContents = indexedTar.list(folder)
                    assert isinstance(folderContents, dict)
                    for name in folderContents:  # pylint: disable=not-an-iterable
                        path = os.path.join(folder, name)
                        fileInfo = indexedTar.lookup(path)
                        if not fileInfo:
                            continue

                        if stat.S_ISDIR(fileInfo.mode):
                            folders.append(path)
                            foldersToRecurse.append(path)
                        else:
                            files.append(path)

                assert set(folders) == {"/dist", "/dist/a", "/dist/a/b", "/src"}
                assert set(files) == {"/dist/a/b/test2.sh", "/src/test.sh", "/README.md"}

                for path in folders + files:
                    assert indexedTar.versions(path) == 1

    @staticmethod
    def test_open(parallelization):
        with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpTarFile:
            repeatCount = 10000

            with tarfile.open(name=tmpTarFile.name, mode="w:gz") as tarFile:
                create_file = TestSQLiteIndexedTarParallelized._create_file
                create_file(tarFile, "increasing.dat", "".join(["0123456789"] * repeatCount))
                create_file(tarFile, "decreasing.dat", "".join(["9876543210"] * repeatCount))

            with SQLiteIndexedTar(tmpTarFile.name, clearIndexCache=True, parallelization=parallelization) as indexedTar:
                iFile = indexedTar.open(indexedTar.lookup("/increasing.dat"))
                dFile = indexedTar.open(indexedTar.lookup("/decreasing.dat"))

                for i in range(repeatCount):
                    try:
                        assert iFile.read(10) == b"0123456789"
                        assert dFile.read(10) == b"9876543210"
                    except AssertionError as e:
                        print("Reading failed in iteration:", i)
                        raise e

    @staticmethod
    def test_multithreaded_reading(parallelization):
        parallelism = parallelization * 6  # Need a bit more parallelism to trigger bugs easier
        with (
            tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpTarFile,
            concurrent.futures.ThreadPoolExecutor(parallelism) as pool,
        ):
            repeatCount = 10000

            with tarfile.open(name=tmpTarFile.name, mode="w:gz") as tarFile:
                create_file = TestSQLiteIndexedTarParallelized._create_file
                create_file(tarFile, "increasing.dat", "".join(["0123456789"] * repeatCount))
                create_file(tarFile, "decreasing.dat", "".join(["9876543210"] * repeatCount))

            with SQLiteIndexedTar(tmpTarFile.name, clearIndexCache=True, parallelization=parallelization) as indexedTar:

                def read_sequences(file, useIncreasing):
                    for i in range(repeatCount):
                        try:
                            if useIncreasing:
                                assert file.read(10) == b"0123456789"
                            else:
                                assert file.read(10) == b"9876543210"
                        except AssertionError as e:
                            print("Reading failed in iteration:", i)
                            raise e
                    return True

                files = [
                    indexedTar.open(indexedTar.lookup(f"/{'in' if i % 2 == 0 else 'de'}creasing.dat"))
                    for i in range(parallelism)
                ]

                results = [pool.submit(read_sequences, files[i], i % 2 == 0) for i in range(parallelism)]
                for result in results:
                    result.result()

                for file in files:
                    file.close()

    @staticmethod
    def test_appending_to_small_archive(parallelization, tmpdir):
        create_file = TestSQLiteIndexedTarParallelized._create_file
        make_folder = TestSQLiteIndexedTarParallelized._make_folder

        # Create a simple small TAR
        tarPath = os.path.join(tmpdir, "foo.tar")
        with tarfile.open(name=tarPath, mode="w:") as tarFile:
            create_file(tarFile, "foo", "bar")

        # Create index
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=True,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/foo")
            assert not indexedTar.exists("/bar")
            assert not indexedTar.exists("/folder")

        # Append small file to TAR
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            create_file(tarFile, "bar", "foo")

        # Create index but only go over new files
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/foo")
            assert indexedTar.exists("/bar")
            assert not indexedTar.exists("/folder")

        # Append empty folder to TAR
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            make_folder(tarFile, "folder")

        # Create index but only go over new files
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/foo")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")

        # Append a sparse file
        sparsePath = os.path.join(tmpdir, "sparse")
        with open(sparsePath, "wb"):
            pass
        os.truncate(sparsePath, 1024 * 1024)
        # The tarfile module only has read support for sparse files, therefore use GNU tar to do it
        subprocess.run(["tar", "--append", "-f", tarPath, sparsePath], check=True)

        # Create index. Because of the sparse file at the end, it might be recreated from scratch.
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/foo")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")

        # Append small file to TAR after sparse file!
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            create_file(tarFile, "bar2", "foo")

        # Create index but only go over new files
        print("=== Update Index With New File After Sparse File ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/foo")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")
            assert indexedTar.exists("/bar2")

    @staticmethod
    def test_appending_to_large_archive(parallelization, tmpdir):
        create_file = TestSQLiteIndexedTarParallelized._create_file
        make_folder = TestSQLiteIndexedTarParallelized._make_folder

        # Create a TAR large in size as well as file count
        tarPath = os.path.join(tmpdir, "foo.tar")
        with (
            copy_test_file("tar-with-300-folders-with-1000-files-0B-files.tar.bz2") as path,
            rapidgzip.IndexedBzip2File(path) as file,
            open(tarPath, 'wb') as extracted,
        ):
            while True:
                data = file.read(1024 * 1024)
                if not data:
                    break
                extracted.write(data)

        # Create index
        print("\n=== Create Index ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=True,
            indexFilePath=indexFilePath,
            parallelization=os.cpu_count(),
            printDebug=3,
        ) as indexedTar:
            assert not indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/00000000000000000000000000000282/00000000000000000000000000000976")
            assert not indexedTar.exists("/bar")
            assert not indexedTar.exists("/folder")

        # Append small file to TAR
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            create_file(tarFile, "bar", "foo")

        # Create index but only go over new files
        print("\n=== Update Index With New File ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/00000000000000000000000000000282/00000000000000000000000000000976")
            assert indexedTar.exists("/bar")
            assert not indexedTar.exists("/folder")

        # Append empty folder to TAR
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            make_folder(tarFile, "folder")

        # Create index but only go over new files
        print("\n=== Update Index With New Folder ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/00000000000000000000000000000282/00000000000000000000000000000976")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")

        # Append a sparse file
        sparsePath = os.path.join(tmpdir, "sparse")
        with open(sparsePath, "wb"):
            pass
        os.truncate(sparsePath, 1024 * 1024)
        # The tarfile module only has read support for sparse files, therefore use GNU tar to do it
        subprocess.run(["tar", "--append", "-f", tarPath, sparsePath], check=True)

        # Create index. Because of the sparse file at the end, it might be recreated from scratch.
        print("\n=== Update Index With Sparse File ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
            printDebug=3,
        ) as indexedTar:
            assert indexedTar.hasBeenAppendedTo
            assert indexedTar.exists("/00000000000000000000000000000282/00000000000000000000000000000976")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")

        # Append small file to TAR after sparse file!
        with tarfile.open(name=tarPath, mode="a:") as tarFile:
            create_file(tarFile, "bar2", "foo")

        # Create index but only go over new files
        print("\n=== Update Index With New File After Sparse File ===")
        indexFilePath = os.path.join(tmpdir, "foo.tar.index")
        with SQLiteIndexedTar(
            tarFileName=tarPath,
            writeIndex=True,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
        ) as indexedTar:
            # assert indexedTar.hasBeenAppendedTo  # TODO
            assert indexedTar.exists("/00000000000000000000000000000282/00000000000000000000000000000976")
            assert indexedTar.exists("/bar")
            assert indexedTar.exists("/folder")
            assert indexedTar.exists("/bar2")
