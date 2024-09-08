#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import importlib
import io
import os
import struct
import sys

import pytest

from helpers import copyTestFile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.compressions import findSquashFSOffset  # noqa: E402
from ratarmountcore.SquashFSMountSource import SquashFSMountSource  # noqa: E402


compressionsToTest = []
if importlib.util.find_spec('PySquashfsImage'):
    compressionsToTest = ['no-compression', 'gzip', 'lzma', 'xz']
    if importlib.util.find_spec('lz4') is not None:
        compressionsToTest.append('lz4')
    if importlib.util.find_spec('lzo') is not None:
        compressionsToTest.append('lzo')
    if importlib.util.find_spec('zstandard') is not None:
        compressionsToTest.append('zstd')


class TestSquashfsMountSource:
    @staticmethod
    def test_find_magic_bytes():
        assert findSquashFSOffset(io.BytesIO()) < 0
        assert findSquashFSOffset(io.BytesIO(b"")) < 0
        assert findSquashFSOffset(io.BytesIO(b"a")) < 0
        assert findSquashFSOffset(io.BytesIO(b"ab")) < 0
        assert findSquashFSOffset(io.BytesIO(b"ab")) < 0
        assert findSquashFSOffset(io.BytesIO(b"foob")) < 0

        validHeader = b"hsqs" + struct.pack('<IIII', 0, 0, 4096, 0) + struct.pack('<HHHHHH', 0, 12, 0, 0, 4, 0)
        assert findSquashFSOffset(io.BytesIO(validHeader)) == 0
        assert findSquashFSOffset(io.BytesIO(b"0" + validHeader)) == 1
        assert findSquashFSOffset(io.BytesIO(b"0" * 1234 + validHeader)) == 1234
        assert findSquashFSOffset(io.BytesIO(b"0" * 1234 + validHeader + validHeader)) == 1234

    @staticmethod
    @pytest.mark.parametrize('compression', compressionsToTest)
    def test_simple_usage(compression):
        with copyTestFile(f'folder-symlink.{compression}.squashfs') as path, SquashFSMountSource(path) as mountSource:
            with open(path, 'rb') as file:
                assert findSquashFSOffset(file) == 0

            for folder in ['/', '/foo', '/foo/fighter']:
                assert mountSource.getFileInfo(folder)
                assert mountSource.fileVersions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/foo/fighter/ufo']:
                assert mountSource.getFileInfo(filePath)
                assert mountSource.fileVersions(filePath) == 1
                assert not mountSource.listDir(filePath)

                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read(1) == b'i'
                    assert file.read(5) == b'riya\n'

                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'iriya\n'
                    assert file.seek(0) == 0
                    assert file.read() == b'iriya\n'
                    assert file.seek(1) == 1
                    assert file.read() == b'riya\n'
                    assert file.seek(5) == 5
                    assert file.read() == b'\n'
                    assert file.seek(6) == 6
                    assert file.tell() == 6
                    assert file.read() == b''
                    assert file.tell() == 6
                    assert file.read(2) == b''
                    assert file.tell() == 6

            # Links are not resolved by the mount source but by FUSE, i.e., descending into a link to a folder
            # will not work. This behavior may change in the future.
            for linkPath in ['/foo/jet']:
                assert mountSource.getFileInfo(linkPath)
                assert mountSource.fileVersions(linkPath) == 1
                assert not mountSource.listDir(linkPath)
                fileInfo = mountSource.getFileInfo(linkPath)
                assert fileInfo.linkname == 'fighter'
