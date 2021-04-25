#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import sys
import gzip
import tarfile
import tempfile

if __name__ == '__main__' and __package__ is None:
    sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname(__file__) , '..' ) ) )

import ratarmount
from ratarmount import FileInfo, SQLiteIndexedTar

testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write( testData )


print( "Test StenciledFile._findStencil" )
stenciledFile = ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2),(4,4),(1,8),(0,1)] )
expectedResults = [ 0,0, 1,1, 2,2, 3,3,3,3, 4,4,4,4,4,4,4,4, 5 ]
for offset, iExpectedStencil in enumerate( expectedResults ):
    assert stenciledFile._findStencil( offset ) == iExpectedStencil

print( "Test StenciledFile with single stencil" )

assert ratarmount.StenciledFile( tmpFile, [(0,1)] ).read() == b"1"
assert ratarmount.StenciledFile( tmpFile, [(0,2)] ).read() == b"12"
assert ratarmount.StenciledFile( tmpFile, [(0,3)] ).read() == b"123"
assert ratarmount.StenciledFile( tmpFile, [(0,len( testData ) )] ).read() == testData


print( "Test StenciledFile with stencils each sized 1 byte" )

assert ratarmount.StenciledFile( tmpFile, [(0,1),(1,1)] ).read() == b"12"
assert ratarmount.StenciledFile( tmpFile, [(0,1),(2,1)] ).read() == b"13"
assert ratarmount.StenciledFile( tmpFile, [(1,1),(0,1)] ).read() == b"21"
assert ratarmount.StenciledFile( tmpFile, [(0,1),(1,1),(2,1)] ).read() == b"123"
assert ratarmount.StenciledFile( tmpFile, [(1,1),(2,1),(0,1)] ).read() == b"231"

print( "Test StenciledFile with stencils each sized 2 bytes" )

assert ratarmount.StenciledFile( tmpFile, [(0,2),(1,2)] ).read() == b"1223"
assert ratarmount.StenciledFile( tmpFile, [(0,2),(2,2)] ).read() == b"1234"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(0,2)] ).read() == b"2312"
assert ratarmount.StenciledFile( tmpFile, [(0,2),(1,2),(2,2)] ).read() == b"122334"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read() == b"233412"

print( "Test reading a fixed length of the StenciledFile" )

assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 0 ) == b""
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 1 ) == b"2"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 2 ) == b"23"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 3 ) == b"233"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 4 ) == b"2334"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 5 ) == b"23341"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 6 ) == b"233412"
assert ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read( 7 ) == b"233412"

print( "Test seek and tell" )

stenciledFile = ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] )
for i in range( 7 ):
    assert stenciledFile.tell() == i
    stenciledFile.read( 1 )
for i in reversed( range( 6 ) ):
    assert stenciledFile.seek( -1, io.SEEK_CUR ) == i
    assert stenciledFile.tell() == i
assert stenciledFile.seek( 0, io.SEEK_END ) == 6
assert stenciledFile.tell() == 6
assert stenciledFile.seek( 20, io.SEEK_END ) == 26
assert stenciledFile.tell() == 26
assert stenciledFile.read( 1 ) == b""
assert stenciledFile.seek( -6, io.SEEK_END ) == 0
assert stenciledFile.read( 1 ) == b"2"

print("Test creating and using an index with .tar.gz files with SQLiteIndexedTar")

def writestr(tf, name, contents):
    tinfo = tarfile.TarInfo(name)
    tinfo.size = len(contents)
    tf.addfile(tinfo, io.BytesIO(contents.encode()))

def writedir(tf, name):
    tinfo = tarfile.TarInfo(name)
    tinfo.type = tarfile.DIRTYPE
    tf.addfile(tinfo, io.BytesIO())

with tempfile.NamedTemporaryFile(
    suffix=".tar.gz"
) as tmp_tar_file, tempfile.NamedTemporaryFile(
    suffix=".sqlite"
) as tmp_index_file:
    with tarfile.open(name=tmp_tar_file.name, mode="w:gz") as tf:
        writestr(tf, "./README.md", "hello world")
        writedir(tf, "./src")
        writestr(tf, "./src/test.sh", "echo hi")
        writedir(tf, "./dist")
        writedir(tf, "./dist/a")
        writedir(tf, "./dist/a/b")
        writestr(tf, "./dist/a/b/test2.sh", "echo two")
    
    kwargs_set = {
        "\tTest with file paths": dict(fileObject=None, tarFileName=tmp_tar_file.name),
        "\tTest with file objects": dict(fileObject=open(tmp_tar_file.name, "rb"), tarFileName="tarFileName"),
        "\tTest with file objects with no fileno": dict(fileObject=io.BytesIO(open(tmp_tar_file.name, "rb").read()), tarFileName="tarFileName")
    }

    for name, kwargs in kwargs_set.items():
        print(name)
        # Create index
        SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFileName=tmp_index_file.name,
        )
        # Read from index
        indexed_file = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFileName=tmp_index_file.name,
        )
        finfo = indexed_file.getFileInfo("/src/test.sh")
        assert finfo.type == tarfile.REGTYPE
        assert indexed_file.read(path="/src/test.sh", size=finfo.size, offset=0) == b"echo hi"
        finfo = indexed_file.getFileInfo("/dist/a")
        assert finfo.type == tarfile.DIRTYPE
        assert indexed_file.getFileInfo("/dist/a", listDir=True) == {'b': ratarmount.FileInfo(offsetheader=3584, offset=4096, size=0, mtime=0, mode=16804, type=b'5', linkname='', uid=0, gid=0, istar=0, issparse=0)}
        assert indexed_file.getFileInfo("/", listDir=True) == {'README.md': FileInfo(offsetheader=0, offset=512, size=11, mtime=0, mode=33188, type=b'0', linkname='', uid=0, gid=0, istar=0, issparse=0), 'dist': FileInfo(offsetheader=2560, offset=3072, size=0, mtime=0, mode=16804, type=b'5', linkname='', uid=0, gid=0, istar=0, issparse=0), 'src': FileInfo(offsetheader=1024, offset=1536, size=0, mtime=0, mode=16804, type=b'5', linkname='', uid=0, gid=0, istar=0, issparse=0)}
        finfo = indexed_file.getFileInfo("/README.md")
        assert finfo.size == 11
        assert indexed_file.read("/README.md", size=11, offset=0) == b"hello world"
        assert indexed_file.read("/README.md", size=3, offset=3) == b"lo "

print("Test creating and using an index with .gz files with SQLiteIndexedTar")

with tempfile.NamedTemporaryFile(
    suffix=".gz"
) as tmp_tar_file, tempfile.NamedTemporaryFile(
    suffix=".sqlite"
) as tmp_index_file:
    with gzip.open(tmp_tar_file.name, "wb") as f:
        f.write(b"hello world")
    
    kwargs_set = {
        "\tTest with file paths": dict(fileObject=None, tarFileName=tmp_tar_file.name),
        "\tTest with file objects": dict(fileObject=open(tmp_tar_file.name, "rb"), tarFileName="tarFileName"),
        "\tTest with file objects with no fileno": dict(fileObject=io.BytesIO(open(tmp_tar_file.name, "rb").read()), tarFileName="tarFileName")
    }

    for name, kwargs in kwargs_set.items():
        print(name)
        # Create index
        SQLiteIndexedTar(
            **kwargs,
            writeIndex=True,
            clearIndexCache=True,
            indexFileName=tmp_index_file.name,
        )
        # Read from index
        indexed_file = SQLiteIndexedTar(
            **kwargs,
            writeIndex=False,
            clearIndexCache=False,
            indexFileName=tmp_index_file.name,
        )
        expected_name = os.path.basename(tmp_tar_file.name)[:-3] if kwargs["fileObject"] is None else "tarFileName"
        finfo = indexed_file.getFileInfo("/", listDir=True)
        assert expected_name in finfo
        assert finfo[expected_name].size == 11
        finfo = indexed_file.getFileInfo("/" + expected_name)
        assert finfo.size == 11
        assert indexed_file.read("/" + expected_name, size=11, offset=0) == b"hello world"
        assert indexed_file.read("/" + expected_name, size=3, offset=3) == b"lo "
