#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bz2
import hashlib
import os
import tempfile
import time

import bzip2

import numpy as np


def sha1_160( fileObject, bufferSize = 1024 * 1024 ):
    hasher = hashlib.sha1()
    for data in iter( lambda : fileObject.read( bufferSize ), b'' ):
        hasher.update( data )
    return hasher.digest()

def checkDecompression( rawFile, bz2File, bufferSize ):
    rawFile.seek( 0 )
    bz2File.seek( 0 )

    file = bzip2.SeekableBzip2( bz2File.fileno() )
    sha1 = sha1_160( file, bufferSize )
    sha2 = sha1_160( rawFile )

    #print( "SHA1:", sha1.hex(), sha2.hex() )
    assert sha1 == sha2

def checkSeek( rawFile, bz2File, seekPos ):
    bz2File.seek( seekPos )
    c1 = bz2File.read( 1 )

    rawFile.seek( seekPos )
    c2 = rawFile.read( 1 )

    if c1 != c2:
        print( "Char at pos", seekPos, "from sbzip2:", c1.hex(), "=?=", c2.hex(), "from raw file" )
        sb = bzip2.SeekableBzip2( "toybox-7bf68329eb3b.tar.bz2" )
        sb.read( seekPos )
        print( "Char when doing naive seek:", sb.read( 1 ).hex() )

    assert c1 == c2

def createRandomBz2( sizeInBytes, compresslevel = 9 ):
    randomData = os.urandom( sizeInBytes )

    rawFile = tempfile.TemporaryFile()
    rawFile.write( randomData )
    rawFile.seek( 0 );

    bz2File = tempfile.TemporaryFile()
    bz2File.write( bz2.compress( randomData, compresslevel ) )
    bz2File.seek( 0 )

    return rawFile, bz2File

def testDecompression():
    for size in [ 0, 1, 2, 3, 4, 5, 10, 20, 30, 100, 1000, 10000, 100000, 200000 ]:
        print( "Check BZip2 sized {} bytes".format( size ) )
        for compressionlevel in range( 1, 9 + 1 ):
            rawFile, bz2File = createRandomBz2( size, compressionlevel )
            for bufferSize in [ 1, 333, 500, 1024, 1024*1024, 64*1024*1024 ]:
                checkDecompression( rawFile, bz2File, bufferSize )

def testSeeking():
    for size in [ 1, 2, 3, 4, 5, 10, 20, 30, 100, 1000, 10000, 100000, 1000000 ]:
        seekPositions = np.append( np.random.randint( 0, size ), [ 0, size - 1 ] )
        for compressionlevel in range( 1, 9 + 1, 20 ):
            rawFile, bz2File = createRandomBz2( size, compressionlevel )
            sbzip2 = bzip2.SeekableBzip2( bz2File.fileno() )
            for seekPos in seekPositions:
                checkSeek( rawFile, sbzip2, seekPos )

if __name__ == '__main__':
    testSeeking()
    testDecompression()
