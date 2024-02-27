#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import DereferenceLayer, SQLiteIndexedTar  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


class TestDereferenceLayer:
    @staticmethod
    def test_simple_usage():
        with SQLiteIndexedTar(findTestFile('nested-symlinks.tar')) as mountSource:
            fileInfo = mountSource.getFileInfo('/foo/fighter/foo')
            assert fileInfo
            assert stat.S_ISLNK(fileInfo.mode)
            assert fileInfo.linkname == '../foo'

            dereferenced = DereferenceLayer(mountSource)

            #assert dereferenced.getFileInfo('/')
            #assert dereferenced.getFileInfo('/foo')
            #assert dereferenced.getFileInfo('/foo/fighter')
            assert dereferenced.getFileInfo('/foo/foo')
            fileInfo = dereferenced.getFileInfo('/foo/fighter/foo')
            assert fileInfo
            assert not stat.S_ISLNK(fileInfo.mode)
            with dereferenced.open(fileInfo) as file:
                assert file.read() == b"iriya\n"
