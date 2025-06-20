# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.SQLiteIndexMountSource import SQLiteIndexMountSource  # noqa: E402


class TestSQLiteIndexMountSource:
    @staticmethod
    def test_password():
        with copy_test_file("nested-tar.index.sqlite") as path, SQLiteIndexMountSource(path) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                fileInfo = mountSource.lookup(folder)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)
                assert mountSource.versions(folder) == 1
                assert mountSource.list(folder)

            for filePath in ['/foo/fighter/ufo', '/foo/lighter.tar']:
                fileInfo = mountSource.lookup(filePath)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)

                assert mountSource.versions(filePath) == 1
                assert not mountSource.list(filePath)

            # File contents cannot be read from a mounted index alone!

            # The 'transform' parameter is ignored because it is only for index creation, not index loading!
