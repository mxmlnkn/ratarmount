#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import SQLiteIndex  # noqa: E402


class TestSQLiteIndexedTarParallelized:
    @staticmethod
    def test_normpath():
        normpath = SQLiteIndex.normpath
        assert normpath("/") == "/"
        assert normpath("//") == "/"
        assert normpath("///") == "/"

        assert normpath("a") == "/a"
        assert normpath("/a") == "/a"
        assert normpath("a/") == "/a"
        assert normpath("/a/") == "/a"
        assert normpath("//a//") == "/a"

        assert normpath(".") == "/"
        assert normpath("/.") == "/"
        assert normpath("./") == "/"
        assert normpath("/./") == "/"
        assert normpath("//.//") == "/"

        assert normpath("./././a/.././") == "/"
        assert normpath("../") == "/"
        assert normpath("../.././..") == "/"

    @staticmethod
    def test_query_normpath():
        normpath = SQLiteIndex._queryNormpath
        assert normpath("/") == "/"
        assert normpath("//") == "/"
        assert normpath("///") == "/"

        assert normpath("a") == "/a"
        assert normpath("/a") == "/a"
        assert normpath("a/") == "/a"
        assert normpath("/a/") == "/a"
        assert normpath("//a//") == "/a"

        assert normpath(".") == "/"
        assert normpath("/.") == "/"
        assert normpath("./") == "/"
        assert normpath("/./") == "/"
        assert normpath("//.//") == "/"

        assert normpath("./././a/.././") == "/"
        assert normpath("../") == "/.."
        assert normpath("../.././..") == "/../../.."
