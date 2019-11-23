#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bz2
import hashlib
import os
import pprint
import subprocess
import tempfile
import time

import bzip2

import numpy as np


def sha1_160( fileObject, bufferSize = 1024 * 1024 ):
    hasher = hashlib.sha1()
    for data in iter( lambda : fileObject.read( bufferSize ), b'' ):
        hasher.update( data )
    return hasher.digest()

def checkDecompressionBytewise( rawFile, bz2File, bufferSize ):
    # Very slow for some reason! Only use this check if the checksum check fails
    rawFile.seek( 0 )
    bz2File.seek( 0 )

    decFile = bzip2.SeekableBzip2( bz2File.fileno() )

    while True:
        oldPos1 = rawFile.tell()
        oldPos2 = decFile.tell()

        data1 = rawFile.read( bufferSize )
        data2 = decFile.read( bufferSize )

        if data1 != data2:
            print( "Data at pos {} ({}) mismatches! After read at pos {} ({}).\nData:\n  {}\n  {}"
                   .format( oldPos1, oldPos2, rawFile.tell(), decFile.tell(), data1.hex(), data2.hex() ) )
            print( "Block offsets:" )
            pprint.pprint( decFile.blockOffsets() )

            bz2File.seek( 0 )
            file = open( "bugged-random.bz2", 'wb' )
            file.write( bz2File.read() )
            file.close()

            raise Exception( "Data mismatches!" )

def checkDecompression( rawFile, bz2File, bufferSize ):
    rawFile.seek( 0 )
    bz2File.seek( 0 )

    file = bzip2.SeekableBzip2( bz2File.fileno() )
    sha1 = sha1_160( file, bufferSize )
    sha2 = sha1_160( rawFile )

    if sha1 != sha2:
        print( "SHA1 mismatches:", sha1.hex(), sha2.hex() )
        print( "Checking bytewise ..." )
        checkDecompressionBytewise()
        assert False, "SHA1 mismatch"

def checkSeek( rawFile, bz2File, seekPos ):
    bz2File.seek( seekPos )
    c1 = bz2File.read( 1 )

    rawFile.seek( seekPos )
    c2 = rawFile.read( 1 )

    if c1 != c2:
        print( "Char at pos", seekPos, "from sbzip2:", c1.hex(), "=?=", c2.hex(), "from raw file" )

    assert c1 == c2

def createRandomBz2( sizeInBytes, compresslevel = 9, encoder = 'pybz2' ):
    randomData = os.urandom( sizeInBytes )

    rawFile = tempfile.TemporaryFile()
    rawFile.write( randomData )
    rawFile.seek( 0 );

    bz2File = tempfile.TemporaryFile()
    if encoder == 'pybz2':
        bz2File.write( bz2.compress( randomData, compresslevel ) )
    else:
        bz2File.write( subprocess.check_output( [ encoder, '-{}'.format( compresslevel ) ], input = randomData ) )
    bz2File.seek( 0 )

    return rawFile, bz2File

def testDecompression():
    for size in [ 1, 2, 3, 4, 5, 10, 20, 30, 100, 1000, 10000, 100000, 1000000, 2000000, 0 ]:
        print( "Check BZip2 sized {} bytes".format( size ) )
        for compressionlevel in range( 1, 9 + 1 ):
            for encoder in [ 'pbzip2', 'bzip2', 'pybz2' ]:
                #t0 = time.time()
                rawFile, bz2File = createRandomBz2( size, compressionlevel, encoder )
                #t1 = time.time()
                #print( "Creating compressed test file sized {} B raw and {} B compressed with {} took {:.3f} s"
                #       .format( os.fstat( rawFile.fileno() ).st_size, os.fstat( bz2File.fileno() ).st_size,
                #                encoder, t1 - t0 ) )
                # For some reason, creating a 0B file with pbzip2 takes 1s instead of ~2ms Oo?!
                for bufferSize in [ 128, 333, 500, 1024, 1024*1024, 64*1024*1024 ]:
                    try:
                        checkDecompression( rawFile, bz2File, bufferSize )
                    except Exception as e:
                        print( "Test for size {}, compression level {}, encoder {}, and buffer size {} failed"
                               .format( size, compressionlevel, encoder, bufferSize ) )
                        raise e

def testSeeking():
    for size in [ 1, 2, 3, 4, 5, 10, 20, 30, 100, 1000, 10000, 100000, 1000000, 10000000 ]:
        print( "Check seeking BZip2 sized {} bytes".format( size ) )
        seekPositions = np.append( np.random.randint( 0, size ), [ 0, size - 1 ] )
        for compressionlevel in range( 1, 9 + 1, 20 ):
            for encoder in [ 'pbzip2', 'bzip2', 'pybz2' ]:
                rawFile, bz2File = createRandomBz2( size, compressionlevel, encoder )
                sbzip2 = bzip2.SeekableBzip2( bz2File.fileno() )
                for seekPos in seekPositions:
                    try:
                        checkSeek( rawFile, sbzip2, seekPos )
                    except Exception as e:
                        print( "Test for size {}, compression level {}, encoder {}, and seek pos {} failed"
                               .format( size, compressionlevel, encoder, seekPos ) )
                        sb = bzip2.SeekableBzip2( bz2File )
                        sb.read( seekPos )
                        print( "Char when doing naive seek:", sb.read( 1 ).hex() )
                        raise e

if __name__ == '__main__':
    testDecompression()
    testSeeking()
