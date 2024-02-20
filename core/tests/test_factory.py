#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.factory import openMountSource  # noqa: E402


class TestOpenMountSource:
    @staticmethod
    def test_joining_archive(tmpdir):
        compressed = bz2.compress(b"foobar")
        with open(os.path.join(tmpdir, "foo.001"), 'wb') as file:
            file.write(compressed[: len(compressed) // 2])
        with open(os.path.join(tmpdir, "foo.002"), 'wb') as file:
            file.write(compressed[len(compressed) // 2 :])

        with openMountSource(os.path.join(tmpdir, "foo.001")) as mountSource:
            fileInfo = mountSource.getFileInfo("/<file object>")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

        with openMountSource(os.path.join(tmpdir, "foo.002")) as mountSource:
            fileInfo = mountSource.getFileInfo("/<file object>")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_file(tmpdir):
        with open(os.path.join(tmpdir, "foo.001"), 'wb') as file:
            file.write(b"foo")
        with open(os.path.join(tmpdir, "foo.002"), 'wb') as file:
            file.write(b"bar")

        print(type(openMountSource(os.path.join(tmpdir, "foo.001"))))
        with openMountSource(os.path.join(tmpdir, "foo.001")) as mountSource:
            print("mountSource list:", mountSource.listDir("/"))
            fileInfo = mountSource.getFileInfo("/foo")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_files_exceeding_handle_limit(tmpdir):
        result = b''
        for i in range(1100):  # Default on my system is 1024
            with open(os.path.join(tmpdir, f"foo.{i:03}"), 'wb') as file:
                file.write(str(i).encode())
                result += str(i).encode()

        with openMountSource(os.path.join(tmpdir, "foo.005")) as mountSource:
            fileInfo = mountSource.getFileInfo("/foo")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == result
