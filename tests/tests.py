#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import sys
import gzip
import tarfile
import tempfile

if __name__ == '__main__' and __package__ is None:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import ratarmount
from ratarmount import FileInfo, SQLiteIndexedTar

ratarmount.printDebug = 2

testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write(testData)


print("Test StenciledFile._findStencil")
stenciledFile = ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2), (4, 4), (1, 8), (0, 1)])
expectedResults = [0, 0, 1, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5]
for offset, iExpectedStencil in enumerate(expectedResults):
    assert stenciledFile._findStencil(offset) == iExpectedStencil

print("Test StenciledFile with single stencil")

assert ratarmount.StenciledFile(tmpFile, [(0, 1)]).read() == b"1"
assert ratarmount.StenciledFile(tmpFile, [(0, 2)]).read() == b"12"
assert ratarmount.StenciledFile(tmpFile, [(0, 3)]).read() == b"123"
assert ratarmount.StenciledFile(tmpFile, [(0, len(testData))]).read() == testData


print("Test StenciledFile with stencils each sized 1 byte")

assert ratarmount.StenciledFile(tmpFile, [(0, 1), (1, 1)]).read() == b"12"
assert ratarmount.StenciledFile(tmpFile, [(0, 1), (2, 1)]).read() == b"13"
assert ratarmount.StenciledFile(tmpFile, [(1, 1), (0, 1)]).read() == b"21"
assert ratarmount.StenciledFile(tmpFile, [(0, 1), (1, 1), (2, 1)]).read() == b"123"
assert ratarmount.StenciledFile(tmpFile, [(1, 1), (2, 1), (0, 1)]).read() == b"231"

print("Test StenciledFile with stencils each sized 2 bytes")

assert ratarmount.StenciledFile(tmpFile, [(0, 2), (1, 2)]).read() == b"1223"
assert ratarmount.StenciledFile(tmpFile, [(0, 2), (2, 2)]).read() == b"1234"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (0, 2)]).read() == b"2312"
assert ratarmount.StenciledFile(tmpFile, [(0, 2), (1, 2), (2, 2)]).read() == b"122334"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read() == b"233412"

print("Test reading a fixed length of the StenciledFile")

assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(0) == b""
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(1) == b"2"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(2) == b"23"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(3) == b"233"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(4) == b"2334"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(5) == b"23341"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(6) == b"233412"
assert ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)]).read(7) == b"233412"

print("Test seek and tell")

stenciledFile = ratarmount.StenciledFile(tmpFile, [(1, 2), (2, 2), (0, 2)])
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


print("Test __enter__ and __exit__")

with SQLiteIndexedTar(os.path.join(os.path.dirname(__file__), 'single-file.tar'), writeIndex=False) as indexedTar:
    assert indexedTar.listDir('/')


print("\nTest creating and using an index with .tar.gz files with SQLiteIndexedTar")


def createFile(tarFile, name, contents):
    tinfo = tarfile.TarInfo(name)
    tinfo.size = len(contents)
    tarFile.addfile(tinfo, io.BytesIO(contents.encode()))


def makeFolder(tarFile, name):
    tinfo = tarfile.TarInfo(name)
    tinfo.type = tarfile.DIRTYPE
    tarFile.addfile(tinfo, io.BytesIO())


with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmpTarFile, tempfile.NamedTemporaryFile(
    suffix=".sqlite"
) as tmpIndexFile:
    with tarfile.open(name=tmpTarFile.name, mode="w:gz") as tarFile:
        createFile(tarFile, "./README.md", "hello world")
        makeFolder(tarFile, "./src")
        createFile(tarFile, "./src/test.sh", "echo hi")
        makeFolder(tarFile, "./dist")
        makeFolder(tarFile, "./dist/a")
        makeFolder(tarFile, "./dist/a/b")
        createFile(tarFile, "./dist/a/b/test2.sh", "echo two")

    print("Created temp tar:", tmpTarFile.name)

    testKwargs = {
        "file paths": {'fileObject': None, 'tarFileName': tmpTarFile.name},
        "file objects": {'fileObject': open(tmpTarFile.name, "rb"), 'tarFileName': "tarFileName"},
        "file objects with no fileno": {
            'fileObject': io.BytesIO(open(tmpTarFile.name, "rb").read()),
            'tarFileName': "tarFileName",
        },
    }

    for name, kwargs in testKwargs.items():
        print("\n== Test with {} ==".format(name))

        # Create index
        SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFileName=tmpIndexFile.name,
        )

        # Read from index
        indexedFile = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFileName=tmpIndexFile.name,
        )

        finfo = indexedFile.getFileInfo("/src/test.sh")
        assert finfo.type == tarfile.REGTYPE
        assert indexedFile.read(path="/src/test.sh", size=finfo.size, offset=0) == b"echo hi"

        finfo = indexedFile.getFileInfo("/dist/a")
        assert finfo.type == tarfile.DIRTYPE
        assert indexedFile.getFileInfo("/dist/a", listDir=True) == {
            'b': ratarmount.FileInfo(
                offsetheader=3584,
                offset=4096,
                size=0,
                mtime=0,
                mode=16804,
                type=b'5',
                linkname='',
                uid=0,
                gid=0,
                istar=0,
                issparse=0,
            )
        }
        assert indexedFile.getFileInfo("/", listDir=True) == {
            'README.md': FileInfo(
                offsetheader=0,
                offset=512,
                size=11,
                mtime=0,
                mode=33188,
                type=b'0',
                linkname='',
                uid=0,
                gid=0,
                istar=0,
                issparse=0,
            ),
            'dist': FileInfo(
                offsetheader=2560,
                offset=3072,
                size=0,
                mtime=0,
                mode=16804,
                type=b'5',
                linkname='',
                uid=0,
                gid=0,
                istar=0,
                issparse=0,
            ),
            'src': FileInfo(
                offsetheader=1024,
                offset=1536,
                size=0,
                mtime=0,
                mode=16804,
                type=b'5',
                linkname='',
                uid=0,
                gid=0,
                istar=0,
                issparse=0,
            ),
        }

        finfo = indexedFile.getFileInfo("/README.md")
        assert finfo.size == 11
        assert indexedFile.read("/README.md", size=11, offset=0) == b"hello world"
        assert indexedFile.read("/README.md", size=3, offset=3) == b"lo "


print("\nTest creating and using an index with .gz files with SQLiteIndexedTar")

with tempfile.NamedTemporaryFile(suffix=".gz") as tmpTarFile, tempfile.NamedTemporaryFile(
    suffix=".sqlite"
) as tmpIndexFile:
    with gzip.open(tmpTarFile.name, "wb") as f:
        f.write(b"hello world")

    testKwargs = {
        "file paths": dict(fileObject=None, tarFileName=tmpTarFile.name),
        "file objects": dict(fileObject=open(tmpTarFile.name, "rb"), tarFileName="tarFileName"),
        "file objects with no fileno": dict(
            fileObject=io.BytesIO(open(tmpTarFile.name, "rb").read()), tarFileName="tarFileName"
        ),
    }

    for name, kwargs in testKwargs.items():
        print("\n== Test with {} ==".format(name))

        # Create index
        SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFileName=tmpIndexFile.name,
        )

        # Read from index
        indexedFile = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFileName=tmpIndexFile.name,
        )

        expected_name = os.path.basename(tmpTarFile.name)[:-3] if kwargs["fileObject"] is None else "tarFileName"

        finfo = indexedFile.getFileInfo("/", listDir=True)
        assert expected_name in finfo
        assert finfo[expected_name].size == 11

        finfo = indexedFile.getFileInfo("/" + expected_name)
        assert finfo.size == 11
        assert indexedFile.read("/" + expected_name, size=11, offset=0) == b"hello world"
        assert indexedFile.read("/" + expected_name, size=3, offset=3) == b"lo "
