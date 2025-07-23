# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.py7zr import Py7zrMountSource, py7zr  # noqa: E402


class TestPy7zrMountSource:
    @staticmethod
    def test_password():
        if py7zr is None or not getattr(py7zr, '__version__', '').startswith("1.0"):
            return

        with (
            copy_test_file('encrypted-nested-tar.7z') as path,
            Py7zrMountSource(path, passwords=[b'foo']) as mountSource,
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
