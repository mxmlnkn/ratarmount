# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

from helpers import copyTestFile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.py7zr import Py7zrMountSource, py7zr  # noqa: E402


class TestPy7zrMountSource:
    @staticmethod
    def test_password():
        if py7zr is None or not py7zr.__version__.startswith("1.0"):
            return

        with copyTestFile('encrypted-nested-tar.7z') as path, Py7zrMountSource(path, passwords=[b'foo']) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                fileInfo = mountSource.getFileInfo(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/foo/fighter/ufo']:
                fileInfo = mountSource.getFileInfo(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.listDir(filePath)
                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'iriya\n'
