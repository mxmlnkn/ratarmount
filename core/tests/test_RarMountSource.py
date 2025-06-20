# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import stat
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.rar import RarMountSource  # noqa: E402


def find_test_file(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


class TestRarMountSource:
    @staticmethod
    def test_simple_usage():
        with RarMountSource(find_test_file('folder-symlink.rar')) as mountSource:
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

            # Links are not resolved by the mount source but by FUSE, i.e., descending into a link to a folder
            # will not work. This behavior may change in the future.
            for linkPath in ['/foo/jet']:
                assert mountSource.lookup(linkPath)
                assert mountSource.versions(linkPath) == 1
                assert not mountSource.list(linkPath)
                with mountSource.open(mountSource.lookup(linkPath)) as file:
                    # Contents of symlink is the symlink destination itself.
                    assert file.read() == b'fighter'

    # TODO 'transform' does not work. Could be made to work easily when refactoring it to use SQLiteIndex
