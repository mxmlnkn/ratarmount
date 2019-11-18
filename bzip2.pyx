"""
Pyrex/C extension supporting `bx.misc.seekbzip2` (wrapping the low level
functions in `micro-bunzip.c`).
"""

from libcpp.string cimport string


cdef extern from "Python.h":
    char * PyString_AsString( object )
    object PyString_FromStringAndSize( char *, int )

cdef extern from "bzip2.h":
    cppclass BZ2Reader:
        BZ2Reader( const string& ) except +

import sys
import os

cdef class BZip2Reader():
    cdef BZ2Reader* obj

    def __init__( self, filename ):
        self.obj = new BZ2Reader( filename.encode() )

    def __del__( self ):
        del self.obj
