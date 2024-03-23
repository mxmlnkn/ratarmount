#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore import LibarchiveMountSource  # noqa: E402
from ratarmountcore.LibarchiveMountSource import IterableArchive  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


class TestLibarchiveMountSource:
    @staticmethod
    @pytest.mark.parametrize('compression', ['7z', 'rar', 'zip'])
    def test_simple_usage(compression):
        with LibarchiveMountSource(findTestFile('folder-symlink.' + compression)) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                assert mountSource.getFileInfo(folder)
                assert mountSource.fileVersions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/foo/fighter/ufo']:
                assert mountSource.getFileInfo(filePath)
                assert mountSource.fileVersions(filePath) == 1
                assert not mountSource.listDir(filePath)
                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'iriya\n'

            # Links are not resolved by the mount source but by FUSE, i.e., descending into a link to a folder
            # will not work. This behavior may change in the future.
            for linkPath in ['/foo/jet']:
                assert mountSource.getFileInfo(linkPath)
                assert mountSource.fileVersions(linkPath) == 1
                assert not mountSource.listDir(linkPath)
                fileInfo = mountSource.getFileInfo(linkPath)
                assert fileInfo.linkname == 'fighter'

    @staticmethod
    # 7z : libarchive.exception.ArchiveError: The file content is encrypted, but currently not supported
    #      (errno=-1, retcode=-30, archive_p=94443813387248)
    # RAR: libarchive.exception.ArchiveError: Unsupported block header size (was 4, max is 2)
    #      (errno=84, retcode=-30, archive_p=94443813892640)
    # Basically only ZIP has encryption support provided by libarchive, much less than I would have thought.
    # https://github.com/libarchive/libarchive/issues/579#issuecomment-118440525
    # @pytest.mark.parametrize("compression", ["7z", "rar", "zip"])
    @pytest.mark.parametrize('compression', ['zip'])
    def test_password(compression):
        with LibarchiveMountSource(
            findTestFile('encrypted-nested-tar.' + compression), passwords=['foo']
        ) as mountSource:
            for folder in ['/', '/foo', '/foo/fighter']:
                assert mountSource.getFileInfo(folder)
                assert mountSource.fileVersions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/foo/fighter/ufo']:
                assert mountSource.getFileInfo(filePath)
                assert mountSource.fileVersions(filePath) == 1
                assert not mountSource.listDir(filePath)
                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'iriya\n'

    @staticmethod
    @pytest.mark.parametrize('compression', ['bz2', 'gz', 'lrz', 'lz4', 'lzip', 'lzma', 'lzo', 'xz', 'Z', 'zst'])
    def test_stream_compressed(compression):
        with LibarchiveMountSource(findTestFile('simple.' + compression), passwords=['foo']) as mountSource:
            for folder in ['/']:
                assert mountSource.getFileInfo(folder)
                assert mountSource.fileVersions(folder) == 1
                assert mountSource.listDir(folder)

            for filePath in ['/simple']:
                assert mountSource.getFileInfo(filePath)
                assert mountSource.fileVersions(filePath) == 1
                assert not mountSource.listDir(filePath)
                with mountSource.open(mountSource.getFileInfo(filePath)) as file:
                    assert file.read() == b'foo fighter\n'
                    assert file.seek(4) == 4
                    assert file.read() == b'fighter\n'

    @staticmethod
    @pytest.mark.parametrize(
        'path,lineSize',
        [
            # libarchive bug: https://github.com/libarchive/libarchive/issues/2106
            # ('two-large-files-32Ki-lines-each-1024B.7z', 1024),
            ('two-large-files-32Ki-lines-each-1023B.7z', 1023),
        ],
    )
    def test_file_independence(path, lineSize):
        with LibarchiveMountSource(findTestFile(path)) as mountSource:
            with mountSource.open(mountSource.getFileInfo('zeros-32-MiB.txt')) as fileWithZeros:
                expectedZeros = b'0' * (lineSize - 1) + b'\n'
                assert fileWithZeros.read(lineSize) == expectedZeros
                assert fileWithZeros.tell() == lineSize
                assert fileWithZeros.seek(-lineSize, io.SEEK_END)
                actualZeros = fileWithZeros.read()
                assert len(actualZeros) == len(expectedZeros)
                assert actualZeros == expectedZeros

            with mountSource.open(mountSource.getFileInfo('zeros-32-MiB.txt')) as fileWithZeros, mountSource.open(
                mountSource.getFileInfo('spaces-32-MiB.txt')
            ) as fileWithSpaces:
                expectedSpaces = b' ' * (lineSize - 1) + b'\n'
                expectedZeros = b'0' * (lineSize - 1) + b'\n'

                assert fileWithSpaces.read(lineSize) == expectedSpaces
                assert fileWithSpaces.tell() == lineSize

                assert fileWithZeros.read(lineSize) == expectedZeros
                assert fileWithZeros.tell() == lineSize

                assert fileWithSpaces.tell() == lineSize

                assert fileWithSpaces.seek(-lineSize, io.SEEK_END)
                assert fileWithZeros.seek(-lineSize, io.SEEK_END)
                assert fileWithSpaces.read() == expectedSpaces
                assert fileWithZeros.read() == expectedZeros

                # Seek backwards inside of buffer

                assert fileWithSpaces.seek(-lineSize + 1, io.SEEK_END)
                assert fileWithZeros.seek(-lineSize + 1, io.SEEK_END)
                assert fileWithSpaces.read() == expectedSpaces[1:]
                assert fileWithZeros.read() == expectedZeros[1:]

                # Seek backwards outside of buffer

                assert fileWithSpaces.seek(1) == 1
                assert fileWithSpaces.read(lineSize - 1) == expectedSpaces[1:]
                assert fileWithSpaces.tell() == lineSize

                assert fileWithZeros.seek(1) == 1
                assert fileWithZeros.read(lineSize - 1) == expectedZeros[1:]
                assert fileWithZeros.tell() == lineSize

    @staticmethod
    def test_file_object_reader():
        memoryFile = io.BytesIO()
        with open(findTestFile('folder-symlink.zip'), 'rb') as file:
            memoryFile = io.BytesIO(file.read())
        with IterableArchive(memoryFile) as archive:
            for entry in archive:
                fileInfo = entry.convertToRow(0, lambda x: x)
                assert fileInfo
