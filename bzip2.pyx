"""
Pyrex/C extension supporting `bx.misc.seekbzip2` (wrapping the low level
functions in `micro-bunzip.c`).
"""

from libc.stdlib cimport malloc, free
from libcpp.string cimport string
from libcpp cimport bool

import io
import os
import sys


cdef extern from "Python.h":
    char * PyString_AsString( object )
    object PyString_FromStringAndSize( char *, int )

cdef extern from "bzip2.h":
    ctypedef (unsigned long long int) size_t
    cppclass BZ2Reader:
        BZ2Reader( const string& ) except +
        bool finished() except +
        int fileno() except +
        void close() except +
        bool closed() except +
        void seek( size_t ) except +
        size_t tell() except +
        int read( int, char*, size_t ) except +


cdef class BZ2ReaderWrapper():
    cdef BZ2Reader* bz2reader

    def __init__( self, filename ):
        self.bz2reader = new BZ2Reader( filename.encode() )

    def close( self ):
        self.bz2reader.close()

    def closed( self ):
        return self.bz2reader.closed()

    def fileno( self ):
        return self.bz2reader.fileno()

    def read( self, size = -1 ):
        if size == 0 or self.bz2reader.finished():
            return b''

        cdef char* buffer
        if size > 0:
            buffer = <char*> malloc( size * sizeof( char ) )
            if not buffer:
                raise MemoryError()
            size = self.bz2reader.read( -1, buffer, size )
            try:
                result = <bytes> buffer[:size]
            finally:
                free( buffer )
            return result

        # iterate over small buffer and append to larger bytes
        #if size == -1:
        #    size = self.bz2reader.read( 1, NULL, -1 )
        #    print( "Read {} bytes form bz2".format( size ), file = sys.stderr )

        raise Exception( "Invalid size argument" )

    def seek( self, offset ):
        self.bz2reader.seek( offset )

    def tell( self ):
        return self.bz2reader.tell()

# Extra class because cdefs are not visible from otuside but cdef class can't inherit from io.BufferedIOBase
class SeekableBzip2( io.BufferedIOBase ):
    def __init__( self, filename ):
        self.bz2reader = BZ2ReaderWrapper( filename )
        self.name = filename
        self.mode = 'rb'

    def close( self ):
        self.bz2reader.close()

    def closed( self ):
        return self.bz2reader.closed()

    def fileno( self ):
        return self.bz2reader.fileno()

    def seekable( self ):
        return True

    def readable( self ):
        return True

    def writable( self ):
        return False

    def read( self, size = -1 ):
        return self.bz2reader.read( size )

    def seek( self, offset, whence = io.SEEK_SET ):
        if ( whence != io.SEEK_SET ):
            raise Exception( "not supported" )
        self.bz2reader.seek( offset )
        return offset

    def tell( self ):
        return self.bz2reader.tell()

    def peek( self, n = 0 ):
        raise Exception( "not supported" )

    def read1( self, size = -1 ):
        raise Exception( "not supported" )

    def readinto( self, b ):
        raise Exception( "not supported" )

    def readline( self, size = -1 ):
        raise Exception( "not supported" )

    def readlines( self, size = -1 ):
        raise Exception( "not supported" )

    def write( self, data ):
        raise Exception( "not supported" )

    def writelines( self, seq ):
        raise Exception( "not supported" )
