#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import sqlite3
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import SQLiteBlobFile, SQLiteBlobsFile  # noqa: E402


testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write(testData)


randomTestData = os.urandom(128 * 1024)
randomTmpFile = tempfile.TemporaryFile()
randomTmpFile.write(randomTestData)


class TestLambdaReaderFile:
    @staticmethod
    def test_single_blob():
        db = sqlite3.connect(":memory:")
        db.execute('CREATE TABLE gzipindexes ( data BLOB )')
        db.execute('INSERT INTO gzipindexes VALUES (?)', (randomTestData,))

        expression = "SELECT {}data{} FROM gzipindexes WHERE ROWID == 1"

        assert SQLiteBlobFile.readBlobPart(db, expression, 0, 10) == randomTestData[0:10]
        assert SQLiteBlobFile.readBlobPart(db, expression, 300, 128) == randomTestData[300 : 300 + 128]
        assert SQLiteBlobFile.readBlobPart(db, expression, 1000, 1000) == randomTestData[1000:2000]
        assert SQLiteBlobFile.readBlobPart(db, expression, 0, len(randomTestData)) == randomTestData

        file = SQLiteBlobFile(db, expression)
        assert file.tell() == 0

        assert file.read(10) == randomTestData[0:10]
        assert file.tell() == 10

        assert file.seek(300) == 300
        assert file.tell() == 300
        assert file.read(128) == randomTestData[300 : 300 + 128]
        assert file.tell() == 300 + 128

        assert file.seek(1000) == 1000
        assert file.tell() == 1000
        assert file.read(1000) == randomTestData[1000:2000]
        assert file.tell() == 2000

        assert file.seek(0) == 0
        assert file.read() == randomTestData
        assert file.read() == b""

    @staticmethod
    def test_two_blobs():
        db = sqlite3.connect(":memory:")
        db.execute('CREATE TABLE gzipindexes ( data BLOB )')
        db.executemany('INSERT INTO gzipindexes VALUES (?)', ((b"bar",), (b"foo",)))

        file = SQLiteBlobsFile(db, 'gzipindexes', 'data')

        assert file.read(1) == b"b"
        assert file.tell() == 1

        assert file.seek(3) == 3
        assert file.tell() == 3

        assert file.read(2) == b"fo"
        assert file.tell() == 5

        assert file.seek(-4, io.SEEK_END) == 2
        assert file.tell() == 2

        assert file.read() == b"rfoo"
        assert file.tell() == 6

        assert file.read() == b""
        assert file.tell() == 6
