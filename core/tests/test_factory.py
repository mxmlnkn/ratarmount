#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import openMountSource, ZipMountSource, SQLiteIndexedTar  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


class TestOpenMountSource:
    @staticmethod
    def test_joining_archive(tmpdir):
        compressed = bz2.compress(b"foobar")
        with open(os.path.join(tmpdir, "foo.001"), 'wb') as file:
            file.write(compressed[: len(compressed) // 2])
        with open(os.path.join(tmpdir, "foo.002"), 'wb') as file:
            file.write(compressed[len(compressed) // 2 :])

        with openMountSource(os.path.join(tmpdir, "foo.001")) as mountSource:
            fileInfo = mountSource.getFileInfo("/<file object>")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

        with openMountSource(os.path.join(tmpdir, "foo.002")) as mountSource:
            fileInfo = mountSource.getFileInfo("/<file object>")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_file(tmpdir):
        with open(os.path.join(tmpdir, "foo.001"), 'wb') as file:
            file.write(b"foo")
        with open(os.path.join(tmpdir, "foo.002"), 'wb') as file:
            file.write(b"bar")

        print(type(openMountSource(os.path.join(tmpdir, "foo.001"))))
        with openMountSource(os.path.join(tmpdir, "foo.001")) as mountSource:
            print("mountSource list:", mountSource.listDir("/"))
            fileInfo = mountSource.getFileInfo("/foo")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_files_exceeding_handle_limit(tmpdir):
        result = b''
        for i in range(1100):  # Default on my system is 1024
            with open(os.path.join(tmpdir, f"foo.{i:03}"), 'wb') as file:
                file.write(str(i).encode())
                result += str(i).encode()

        with openMountSource(os.path.join(tmpdir, "foo.005")) as mountSource:
            fileInfo = mountSource.getFileInfo("/foo")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == result

    @staticmethod
    def test_chimera_file():
        chimeraFilePath = findTestFile("chimera-tbz2-zip")
        indexPath = chimeraFilePath + ".index.sqlite"
        if os.path.exists(indexPath):
            os.remove(indexPath)

        # Check simple open and that index files are NOT created because they are too small.
        with openMountSource(
            chimeraFilePath, writeIndex=True, prioritizedBackends=['zipfile', 'rapidgzip']
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.listDir("/")
            assert files

            assert not os.path.exists(indexPath)

        # Same as above, but force index creation by lowering the file count threshold.
        with openMountSource(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['zipfile', 'rapidgzip'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.listDir("/")
            assert files

            fileInfo = mountSource.getFileInfo("/foo/fighter/ufo")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"iriya\n"

            assert os.path.exists(indexPath)

        # Check that everything works fine even if the index exists and the backend order is reversed.
        #
        # "Works fine" refers to a valid file hierarchy being shown and that files can be read without errors.
        # I'm not decided whether the next backend should be tried or whether the index should be recreated
        # by the first backend after an inconsistency has been noticed. The latter is easier to implement
        # and more consistent. I think, only after implementing storing the backend name into the index,
        # should the next backend be tried instead of it being overwritten and recreated.
        assert os.path.exists(indexPath)
        with openMountSource(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['rapidgzip', 'zipfile'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.listDir("/")
            assert files

            fileInfo = mountSource.getFileInfo("/foo/fighter/ufo")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"iriya\n"

            assert os.path.exists(indexPath)

        os.remove(indexPath)

        # Index file is always created for compressed files such as .tar.bz2
        with openMountSource(
            chimeraFilePath, writeIndex=True, prioritizedBackends=['rapidgzip', 'zipfile']
        ) as mountSource:
            assert isinstance(mountSource, SQLiteIndexedTar)
            files = mountSource.listDir("/")
            assert files

            fileInfo = mountSource.getFileInfo("/bar")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"foo\n"

            assert os.path.exists(indexPath)

        # Check that everything works fine even if the index exists and the backend order is reversed.
        assert os.path.exists(indexPath)
        with openMountSource(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['zipfile', 'rapidgzip'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, SQLiteIndexedTar)
            files = mountSource.listDir("/")
            print("files:", files)
            assert files

            fileInfo = mountSource.getFileInfo("/bar")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"foo\n"

            assert os.path.exists(indexPath)

        os.remove(indexPath)
