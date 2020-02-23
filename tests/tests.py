#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import tempfile

if __name__ == '__main__' and __package__ is None:
    sys.path.insert( 0, os.path.abspath( os.path.join( os.path.dirname(__file__) , '..' ) ) )

import ratarmount

testData = b"1234567890"
tmpFile = tempfile.TemporaryFile()
tmpFile.write( testData )

assert( ratarmount.StenciledFile( tmpFile, [(0,1)] ).read() == b"1" )
assert( ratarmount.StenciledFile( tmpFile, [(0,2)] ).read() == b"12" )
assert( ratarmount.StenciledFile( tmpFile, [(0,3)] ).read() == b"123" )
assert( ratarmount.StenciledFile( tmpFile, [(0,len( testData ) )] ).read() == testData )

assert( ratarmount.StenciledFile( tmpFile, [(0,1),(1,1)] ).read() == b"12" )
assert( ratarmount.StenciledFile( tmpFile, [(0,1),(2,1)] ).read() == b"13" )
assert( ratarmount.StenciledFile( tmpFile, [(1,1),(0,1)] ).read() == b"21" )
assert( ratarmount.StenciledFile( tmpFile, [(0,1),(1,1),(2,1)] ).read() == b"123" )
assert( ratarmount.StenciledFile( tmpFile, [(1,1),(2,1),(0,1)] ).read() == b"231" )

assert( ratarmount.StenciledFile( tmpFile, [(0,2),(1,2)] ).read() == b"1223" )
assert( ratarmount.StenciledFile( tmpFile, [(0,2),(2,2)] ).read() == b"1234" )
assert( ratarmount.StenciledFile( tmpFile, [(1,2),(0,2)] ).read() == b"2312" )
assert( ratarmount.StenciledFile( tmpFile, [(0,2),(1,2),(2,2)] ).read() == b"122334" )
assert( ratarmount.StenciledFile( tmpFile, [(1,2),(2,2),(0,2)] ).read() == b"233412" )
