#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import stat
import sys
import tarfile
import tempfile
from typing import Dict

if __name__ == '__main__' or __package__ is not None:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core')))

from ratarmountcore import FileInfo, SQLiteIndexedTar, SQLiteIndexedTarUserData


print("\nTest creating and using an index with .tar.gz files with SQLiteIndexedTar")


def createFile(tarArchive, fileName, contents):
    tinfo = tarfile.TarInfo(fileName)
    tinfo.size = len(contents)
    tarArchive.addfile(tinfo, io.BytesIO(contents.encode()))


def makeFolder(tarArchive, folderName):
    tinfo = tarfile.TarInfo(folderName)
    tinfo.type = tarfile.DIRTYPE
    tarArchive.addfile(tinfo, io.BytesIO())


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

    testKwargs: Dict[str, Dict] = {
        "file paths": {'fileObject': None, 'tarFileName': tmpTarFile.name},
        "file objects": {'fileObject': open(tmpTarFile.name, "rb"), 'tarFileName': "tarFileName"},
        "file objects with no fileno": {
            'fileObject': io.BytesIO(open(tmpTarFile.name, "rb").read()),
            'tarFileName': "tarFileName",
        },
    }

    for name, kwargs in testKwargs.items():
        print(f"\n== Test with {name} ==")

        # Create index
        SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFilePath=tmpIndexFile.name,
            printDebug=3,
        )

        # Read from index
        indexedFile = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFilePath=tmpIndexFile.name,
            printDebug=3,
        )

        finfo = indexedFile.getFileInfo("/src/test.sh")
        assert stat.S_ISREG(finfo.mode)
        assert indexedFile.read(finfo, size=finfo.size, offset=0) == b"echo hi"

        finfo = indexedFile.getFileInfo("/dist/a")
        assert stat.S_ISDIR(finfo.mode)
        assert indexedFile.listDir("/dist/a") == {
            'b': FileInfo(
                size=0,
                mtime=0,
                mode=16804,
                linkname='',
                uid=0,
                gid=0,
                userdata=[
                    SQLiteIndexedTarUserData(
                        offsetheader=3584,
                        offset=4096,
                        istar=0,
                        issparse=0,
                    )
                ],
            )
        }

        assert indexedFile.listDir("/") == {
            'README.md': FileInfo(
                size=11,
                mtime=0,
                mode=33188,
                linkname='',
                uid=0,
                gid=0,
                userdata=[
                    SQLiteIndexedTarUserData(
                        offsetheader=0,
                        offset=512,
                        istar=0,
                        issparse=0,
                    )
                ],
            ),
            'dist': FileInfo(
                size=0,
                mtime=0,
                mode=16804,
                linkname='',
                uid=0,
                gid=0,
                userdata=[
                    SQLiteIndexedTarUserData(
                        offsetheader=2560,
                        offset=3072,
                        istar=0,
                        issparse=0,
                    )
                ],
            ),
            'src': FileInfo(
                size=0,
                mtime=0,
                mode=16804,
                linkname='',
                uid=0,
                gid=0,
                userdata=[
                    SQLiteIndexedTarUserData(
                        offsetheader=1024,
                        offset=1536,
                        istar=0,
                        issparse=0,
                    )
                ],
            ),
        }

        finfo = indexedFile.getFileInfo("/README.md")
        assert finfo.size == 11
        assert indexedFile.read(finfo, size=11, offset=0) == b"hello world"
        assert indexedFile.read(finfo, size=3, offset=3) == b"lo "
