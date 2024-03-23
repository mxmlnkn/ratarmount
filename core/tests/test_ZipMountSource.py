#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import ZipMountSource  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


class TestZipMountSource:
    @staticmethod
    def test_simple_usage():
        with ZipMountSource(findTestFile('folder-symlink.zip')) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                assert mountSource.getFileInfo(folder)
                assert mountSource.fileVersions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/foo/fighter/ufo']:
                assert mountSource.getFileInfo(filePath)
                assert mountSource.fileVersions(filePath) == 1
                assert not mountSource.listDir(filePath)
                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'iriya\n'

            # Links are not resolved by the mount source but by FUSE, i.e., descending into a link to a folder
            # will not work. This behavior may change in the future.
            for linkPath in ['/foo/jet']:
                assert mountSource.getFileInfo(linkPath)
                assert mountSource.fileVersions(linkPath) == 1
                assert not mountSource.listDir(linkPath)
                fileInfo = mountSource.getFileInfo(linkPath)
                assert fileInfo.linkname == 'fighter'
                with mountSource.open(mountSource.getFileInfo(linkPath)) as file:
                    # Contents of symlink is the symlink destination itself.
                    # This behavior is not consistent with other MountSources and therefore subject to change!
                    assert file.read() == b'fighter'
