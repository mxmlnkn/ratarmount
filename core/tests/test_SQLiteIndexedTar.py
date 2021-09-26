#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402

from ratarmountcore import SQLiteIndexedTar  # noqa: E402


@pytest.mark.parametrize("parallelization", [1, 2, 4])
class TestSQLiteIndexedTarParallelized:
    @staticmethod
    def test_context_manager(parallelization):
        with SQLiteIndexedTar('tests/single-file.tar', writeIndex=False, parallelization=parallelization) as indexedTar:
            assert indexedTar.listDir('/')

    @staticmethod
    def test_tar_bz2_with_parallelization(parallelization):
        with SQLiteIndexedTar(
            "tests/2k-recursive-tars.tar.bz2",
            clearIndexCache=True,
            recursive=False,
            parallelization=parallelization,
        ) as file:
            assert file.listDir('/')
            assert file.listDir('/mimi')

            assert not file.listDir('/mimi/01995.tar')
            info = file.getFileInfo('/mimi/01995.tar')
            assert info.userdata[0].offset == 21440512

            assert not file.listDir('/mimi/00105.tar')
            info = file.getFileInfo('/mimi/00105.tar')
            assert info.userdata[0].offset == 1248256

    @staticmethod
    def test_recursive_tar_bz2_with_parallelization(parallelization):
        with SQLiteIndexedTar(
            "tests/2k-recursive-tars.tar.bz2",
            clearIndexCache=True,
            recursive=True,
            parallelization=parallelization,
        ) as file:
            assert file.listDir('/')
            assert file.listDir('/mimi')

            assert file.listDir('/mimi/01995.tar')
            info = file.getFileInfo('/mimi/01995.tar/foo')
            assert info.userdata[0].offset == 21441024

            assert file.listDir('/mimi/00105.tar')
            info = file.getFileInfo('/mimi/00105.tar/foo')
            assert info.userdata[0].offset == 1248768

    @staticmethod
    def test_index_creation_and_loading(parallelization):
        with tempfile.NamedTemporaryFile(suffix=".bz2") as tmpTarFile:
            contents = b"Hello World!"
            with bz2.open(tmpTarFile.name, "wb") as bz2File:
                bz2File.write(contents)

            testIndex = TestSQLiteIndexedTarParallelized._test_index_creation_and_loading

            with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmpIndexFile:
                testIndex(None, tmpTarFile.name, tmpIndexFile.name, contents, parallelization)

            with tempfile.NamedTemporaryFile(suffix=".sqlite") as tmpIndexFile:
                with open(tmpTarFile.name, "rb") as file:
                    testIndex(file, "tarFileName", tmpIndexFile.name, contents, parallelization)

    @staticmethod
    def _test_index_creation_and_loading(fileObject, tarFileName, indexFilePath, contents, parallelization):
        assert not os.path.exists(indexFilePath) or os.stat(indexFilePath).st_size == 0

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

        assert os.stat(indexFilePath).st_size > 0

        # Read from index
        indexedFile = SQLiteIndexedTar(
            fileObject=fileObject,
            tarFileName=tarFileName,
            writeIndex=False,
            clearIndexCache=False,
            indexFilePath=indexFilePath,
            parallelization=parallelization,
        )

        expected_name = os.path.basename(tarFileName).rsplit('.', 1)[0] if fileObject is None else tarFileName

        finfo = indexedFile._getFileInfo("/", listDir=True)
        assert expected_name in finfo
        assert finfo[expected_name].size == len(contents)

        finfo = indexedFile.getFileInfo("/" + expected_name)
        assert finfo.size == len(contents)
        assert indexedFile.read(finfo, size=len(contents), offset=0) == contents
        assert indexedFile.read(finfo, size=3, offset=3) == contents[3:6]
