# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import hashlib
import io
import os
import stat
import sys

import pytest

try:
    import ext4
except ImportError:
    ext4 = None  # type:ignore

from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.ext4 import EXT4MountSource  # noqa: E402


class TestEXT4MountSource:
    @staticmethod
    @pytest.mark.parametrize('path', ['nested-tar-1M.ext4.bz2', 'nested-tar-10M.ext4.bz2'])
    def test_password(path):
        if not ext4:
            return
        with copy_test_file(path) as tmpPath, bz2.open(tmpPath, 'rb') as bz2File:
            tmpFileObject = io.BytesIO()
            tmpFileObject.write(bz2File.read())
            tmpFileObject.seek(0)

            mountSource = EXT4MountSource(tmpFileObject)
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
                assert hashlib.md5(file.read()).hexdigest() == "2a06cc391128d74e685a6cb7cfe9f94d"

    # TODO Does not use SQLiteIndex backend because it is already usably indexed and it would seem redundant.
    #      Therefore 'transform' does not work.
