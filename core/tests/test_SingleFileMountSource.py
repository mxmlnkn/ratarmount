# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name

import io
import os
import stat
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.compositing.singlefile import SingleFileMountSource  # noqa: E402


class TestSingleFileMountSource:
    @staticmethod
    @pytest.mark.parametrize('path', ["foo", "/foo", "/folder/../foo"])
    def test_single_file(path: str):
        contents = b"bar"
        ms = SingleFileMountSource(path, io.BytesIO(contents))

        # The mount source API expects normalized paths. Only the one-time constructor is lenient enough to
        # accept non-normalized paths.
        path = os.path.normpath('/' + path).lstrip('/')

        fileInfo = ms.lookup(path)
        assert fileInfo
        assert stat.S_ISREG(fileInfo.mode)
        assert not stat.S_ISDIR(fileInfo.mode)
        assert fileInfo.size == len(contents)

        with ms.open(fileInfo) as file:
            assert file.read() == contents
        assert ms.read(fileInfo, size=len(contents) + 1, offset=0) == contents

        splitPath = path.split('/')
        for i in range(len(splitPath)):
            subpath = '/'.join(['', *splitPath[:i]])
            for queryPath in (subpath, subpath.lstrip('/')):
                fileInfo = ms.lookup(queryPath)
                assert fileInfo
                assert stat.S_ISDIR(fileInfo.mode)
                assert not stat.S_ISREG(fileInfo.mode)

                files_mode = ms.list_mode(queryPath)
                assert files_mode
                assert len(list(files_mode)) == 1

                files = ms.list(queryPath)
                assert files
                assert len(list(files)) == 1
