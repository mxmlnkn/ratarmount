#!/usr/bin/env python3

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import stat
import sys
import tarfile

if __name__ == '__main__' or __package__ is not None:
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core')))

from ratarmountcore.mountsource import FileInfo
from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar, SQLiteIndexedTarUserData

print("\nTest creating and using an index with .tar.gz files with SQLiteIndexedTar")


def create_file(tarArchive, fileName, contents):
    tinfo = tarfile.TarInfo(fileName)
    tinfo.size = len(contents)
    tarArchive.addfile(tinfo, io.BytesIO(contents.encode()))


def make_folder(tarArchive, folderName):
    tinfo = tarfile.TarInfo(folderName)
    tinfo.type = tarfile.DIRTYPE
    tarArchive.addfile(tinfo, io.BytesIO())


def test_example(tmpdir):
    tar_path = os.path.join(tmpdir, "archive.tar.gz")
    index_path = tar_path + ".index.sqlite"
    with tarfile.open(name=tar_path, mode="w:gz") as tarFile:
        create_file(tarFile, "./README.md", "hello world")
        make_folder(tarFile, "./src")
        create_file(tarFile, "./src/test.sh", "echo hi")
        make_folder(tarFile, "./dist")
        make_folder(tarFile, "./dist/a")
        make_folder(tarFile, "./dist/a/b")
        create_file(tarFile, "./dist/a/b/test2.sh", "echo two")

    print("Created temp tar:", tar_path)

    testKwargs: dict[str, dict] = {
        "file paths": {'fileObject': None, 'tarFileName': tar_path},
        "file objects": {'fileObject': open(tar_path, "rb"), 'tarFileName': "tarFileName"},
        "file objects with no fileno": {
            'fileObject': io.BytesIO(open(tar_path, "rb").read()),
            'tarFileName': "tarFileName",
        },
    }

    for name, kwargs in testKwargs.items():
        print(f"\n== Test with {name} ==")

        # Create index
        with SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFilePath=index_path,
            printDebug=3,
        ):
            pass

        # Read from index
        indexedFile = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFilePath=index_path,
            printDebug=3,
        )

        finfo = indexedFile.lookup("/src/test.sh")
        assert stat.S_ISREG(finfo.mode)
        assert indexedFile.read(finfo, size=finfo.size, offset=0) == b"echo hi"

        finfo = indexedFile.lookup("/dist/a")
        assert stat.S_ISDIR(finfo.mode)
        assert indexedFile.list("/dist/a") == {
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
                        isgenerated=False,
                        recursiondepth=1,
                    )
                ],
            )
        }

        assert indexedFile.list("/") == {
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
                        isgenerated=False,
                        recursiondepth=1,
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
                        isgenerated=False,
                        recursiondepth=1,
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
                        isgenerated=False,
                        recursiondepth=1,
                    )
                ],
            ),
        }

        finfo = indexedFile.lookup("/README.md")
        assert finfo.size == 11
        assert indexedFile.read(finfo, size=11, offset=0) == b"hello world"
        assert indexedFile.read(finfo, size=3, offset=3) == b"lo "

        # Needs to be properly closed so that the index can be removed on Windows in the next loop iteration.
        indexedFile.close()
        # Second close should simply result in a no-op
        indexedFile.close()
