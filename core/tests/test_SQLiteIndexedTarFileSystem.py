# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import sys
import tarfile
import tempfile
from pathlib import Path

import fsspec

try:
    import pandas as pd
except ImportError:
    pd = None  # type: ignore


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.SQLiteIndexedTarFsspec import SQLiteIndexedTarFileSystem  # noqa: E402


def find_test_file(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


def test_file_system():
    fs = SQLiteIndexedTarFileSystem(find_test_file('single-file.tar'))

    assert '/bar' in fs.ls("/", detail=False)
    assert '/bar' in [info['name'] for info in fs.ls("/", detail=True)]

    assert not fs.isfile("/")
    assert fs.isdir("/")
    assert fs.exists("/")

    assert fs.isfile("/bar")
    assert not fs.isdir("/bar")
    assert not fs.exists("/bar2")

    assert fs.cat("/bar") == b"foo\n"
    assert fs.cat("bar") == b"foo\n"

    with fs.open("bar") as file:
        assert file.read() == b"foo\n"


def test_listing():
    fs = SQLiteIndexedTarFileSystem(find_test_file('nested-tar.tar'))

    assert set(fs.find("/", maxdepth=9, withdirs=True)) == {'foo', 'foo/fighter', 'foo/fighter/ufo', 'foo/lighter.tar'}
    # Should not raise exceptions.
    print(fs.tree("/", recursion_limit=10))


def test_url_context_manager():
    with fsspec.open("ratar://bar::file://" + find_test_file('single-file.tar')) as file:
        assert file.read() == b"foo\n"


def test_url():
    openedFile = fsspec.open("ratar://bar::file://" + find_test_file('single-file.tar'))
    with openedFile as file:
        assert file.read() == b"foo\n"


def test_pandas():
    if pd is None:
        return

    with tempfile.TemporaryDirectory(suffix=".test.ratarmount") as folderPath:
        oldPath = os.getcwd()
        os.chdir(folderPath)
        try:
            Path("test.csv").write_bytes(b"1,2\n3,4")
            with tarfile.open("test-csv.tar", "w") as archive:
                archive.add("test.csv")

            # Pandas seems
            data = pd.read_csv("ratar://test.csv::file://test-csv.tar", compression=None, header=None)
            assert data.iloc[0, 1] == 2
        finally:
            os.chdir(oldPath)


if False:
    # I had problems with resource deallocation!
    # For Rapidgzip it becomes important because of the background threads.
    # I can only reproduce this bug when run in global namespace.
    # It always works without problems inside a function.
    # TODO I don't know how to fix this. Closing the file object in SQLiteIndexedTar.__del__
    #      would fix this particular error, but it would lead to other errors for recursive mounting
    #      and when using fsspec.open chained URLs...
    #      Only calling join_threads also does not work for some reason.
    #      Checking with sys.getrefcount and only closing it if it is the only one left also does not work
    #      because the refcount is 3 inside __del__ for some reason.
    #      Closing the file inside RapidgzipFile.__del__ also does not work because it results in the
    #      same error during that close call, i.e., it is already too late at that point. I'm not sure
    #      why it is too late there but not too late during the SQLiteIndexedTar destructor...
    #      Maybe there are also some cyclic dependencies?
    with tempfile.TemporaryDirectory(suffix=".test.ratarmount") as folderPath2:
        contents = os.urandom(96 * 1024 * 1024)

        tarPath = os.path.join(folderPath2, "random-data.tar.gz")
        with tarfile.open(name=tarPath, mode="w:gz") as tarArchive:
            # Must create a sufficiently large .tar.gz so that rapidgzip is actually used.
            # In the future this "has multiple chunks" rapidgzip test is to be removed and
            # this whole test becomes redundant.
            tinfo = tarfile.TarInfo("random-data")
            tinfo.size = len(contents)
            tarArchive.addfile(tinfo, io.BytesIO(contents))

        # Only global variables trigger the "Detected Python finalization from running rapidgzip thread." bug.
        # I am not sure why. Probably, because it gets garbage-collected later.
        globalOpenFile = fsspec.open("ratar://random-data::file://" + tarPath)
        with globalOpenFile as openedFile2:
            assert openedFile2.read() == contents

        # This is still some step the user has to do, but it cannot be avoided.
        # It might be helpful if fsspec had some kind of better resource management for filesystems though.
        del globalOpenFile
