#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402

from ratarmountcore import AutoMountLayer, openMountSource  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


@pytest.mark.parametrize("parallelization", [1, 2, 4])
class TestAutoMountLayer:
    @staticmethod
    def test_regex_mount_point_tar(parallelization):
        options = {
            'clearIndexCache': True,
            'recursive': True,
            'parallelization': parallelization,
            'transformRecursiveMountPoint': ('.*/([^/]*).tar', r'\1'),
        }

        with openMountSource(findTestFile("packed-100-times.tar.gz"), **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.listDir('/')
            assert recursivelyMounted.listDir('/ufo_12')
            assert recursivelyMounted.listDir('/ufo_00')
            assert recursivelyMounted.open(recursivelyMounted.getFileInfo('/ufo_00/ufo')).read() == b'iriya\n'

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
        with openMountSource(findTestFile("compressed-100-times.tar.gz"), **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.listDir('/')
            assert recursivelyMounted.listDir('/ufo_12')
            assert recursivelyMounted.listDir('/ufo_00')
            assert recursivelyMounted.open(recursivelyMounted.getFileInfo('/ufo_00/ufo')).read() == b'iriya\n'

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
        with openMountSource(findTestFile("compressed-100-times.gz"), **options) as mountSource:
            recursivelyMounted = AutoMountLayer(mountSource, **options)

            assert recursivelyMounted.listDir('/')
            assert recursivelyMounted.listDir('/ufo_12')
            assert recursivelyMounted.listDir('/ufo_00')
            fileInfo = recursivelyMounted.getFileInfo('/ufo_00/ufo')
            assert recursivelyMounted.open(fileInfo).read() == b'iriya\n'
