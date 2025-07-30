# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import hashlib
import os
import stat
import sys

import pytest

try:
    import sqlcipher3
except ImportError:
    sqlcipher3 = None  # type:ignore

from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.sqlar import SQLARMountSource  # noqa: E402


class TestSQLARMountSource:
    @staticmethod
    @pytest.mark.parametrize('path', ['nested-tar.sqlar', 'nested-tar-compressed.sqlar', 'encrypted-nested-tar.sqlar'])
    @pytest.mark.parametrize('passwords', [None, [b"foo"], [b"'; DROP TABLE sqlar;", b"foo", b"c"]])
    def test_password(path, passwords):
        if 'encrypted' in path and (not sqlcipher3 or not passwords):
            return
        with copy_test_file(path) as tmpPath, SQLARMountSource(tmpPath, passwords=passwords) as mountSource:
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

            with mountSource.open(mountSource.lookup('/foo/fighter/ufo')) as file:
                assert file.read() == b'iriya\n'

            with mountSource.open(mountSource.lookup('/foo/lighter.tar')) as file:
                if 'encrypted' in path:
                    assert hashlib.md5(file.read()).hexdigest() == "2a06cc391128d74e685a6cb7cfe9f94d"
                else:
                    assert hashlib.md5(file.read()).hexdigest() == "4dfaddf7e55e48097d34e03936223a50"

    # TODO Does not use SQLiteIndex backend because it is already a SQLite database and it would seem redundant.
    #      Therefore 'transform' does not work.
