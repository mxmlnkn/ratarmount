#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import StenciledFile  # noqa: E402


testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write(testData)


class TestStenciledFile:
    @staticmethod
    def test_findStencil():
        stenciledFile = StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2), (4, 4), (1, 8), (0, 1)])
        expectedResults = [0, 0, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5]
        for offset, iExpectedStencil in enumerate(expectedResults):
            assert stenciledFile._findStencil(offset) == iExpectedStencil

    @staticmethod
    def test_single_stencil():
        assert StenciledFile(tmpFile, [(0, 1)]).read() == b"1"
        assert StenciledFile(tmpFile, [(0, 2)]).read() == b"12"
        assert StenciledFile(tmpFile, [(0, 3)]).read() == b"123"
        assert StenciledFile(tmpFile, [(0, len(testData))]).read() == testData

    @staticmethod
    def test_1B_stencils():
        assert StenciledFile(tmpFile, [(0, 1), (1, 1)]).read() == b"12"
        assert StenciledFile(tmpFile, [(0, 1), (2, 1)]).read() == b"13"
        assert StenciledFile(tmpFile, [(1, 1), (0, 1)]).read() == b"21"
        assert StenciledFile(tmpFile, [(0, 1), (1, 1), (2, 1)]).read() == b"123"
        assert StenciledFile(tmpFile, [(1, 1), (2, 1), (0, 1)]).read() == b"231"

    @staticmethod
    def test_2B_stencils():
        assert StenciledFile(tmpFile, [(0, 2), (1, 2)]).read() == b"1223"
        assert StenciledFile(tmpFile, [(0, 2), (2, 2)]).read() == b"1234"
        assert StenciledFile(tmpFile, [(1, 2), (0, 2)]).read() == b"2312"
        assert StenciledFile(tmpFile, [(0, 2), (1, 2), (2, 2)]).read() == b"122334"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read() == b"233412"

    @staticmethod
    def test_read_with_size():
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(0) == b""
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(1) == b"2"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(2) == b"23"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(3) == b"233"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(4) == b"2334"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(5) == b"23341"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(6) == b"233412"
        assert StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(7) == b"233412"

    @staticmethod
    def test_seek_and_tell():
        stenciledFile = StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)])
        for i in range(7):
            assert stenciledFile.tell() == i
            stenciledFile.read(1)
        for i in reversed(range(6)):
            assert stenciledFile.seek(-1, io.SEEK_CUR) == i
            assert stenciledFile.tell() == i
        assert stenciledFile.seek(0, io.SEEK_END) == 6
        assert stenciledFile.tell() == 6
        assert stenciledFile.seek(20, io.SEEK_END) == 26
        assert stenciledFile.tell() == 26
        assert stenciledFile.read(1) == b""
        assert stenciledFile.seek(-6, io.SEEK_END) == 0
        assert stenciledFile.read(1) == b"2"
