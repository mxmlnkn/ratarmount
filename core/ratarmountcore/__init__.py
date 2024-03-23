#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Ratarmount Core

This is the backend of ratarmount. It is intended to be used as a library.

This library offers an interface which is sufficient to work with FUSE.
This MountSource interface has methods for listing paths and getting file
metadata and contents.

The ratarmountcore library offers multiple implementations of the interface:

 - SQLiteIndexedTar: This is the oldest and most powerful implementation.
                     It supports fast access to files inside (compressed) TARs.
 - RarMountSource: An implementation for RARs using rarfile.
 - ZipMountSource: An implementation for ZIPs using zipfile.
 - FolderMountSource: An implementation taking an existing folder as input.
 - UnionMountSource: Takes multiple MountSource implementations and merges them.
 - SubvolumesMountSource: Takes multiple MountSource implementations and mounts
                          each in separate subfolders with specified names.
 - FileVersionLayer: Takes a MountSource as input, decodes the requested
                     paths, also accepting "<file>.version/<number>" paths,
                     and calls the methods of the MountSource with the given
                     file version.
 - AutoMountLayer: Takes one MountSource, goes over all its files and mounts
                   archives recursively in a similar manner to UnionMountSource.

The factory function 'open' opens one of the archive MountSource implementations
according to the file type.

Example:

    import ratarmountcore as rmc

    archive = rmc.open("foo.tar", recursive=True)
    archive.listDir("/")
    info = archive.getFileInfo("/bar")

    print "Contents of /bar:"
    with archive.open(info) as file:
        print(file.read())
"""

from .version import __version__

from .compressions import (
    TAR_COMPRESSION_FORMATS,
    ARCHIVE_FORMATS,
    findAvailableOpen,
    supportedCompressions,
    stripSuffixFromTarFile,
    checkForSplitFile,
    compressZstd,
    getGzipInfo,
)

from .utils import (
    RatarmountError,
    IndexNotOpenError,
    InvalidIndexError,
    CompressionError,
    overrides,
    distributionContainsFile,
    getModule,
    findModuleVersion,
)
from .StenciledFile import LambdaReaderFile, RawStenciledFile, StenciledFile, JoinedFile
from .SQLiteBlobFile import SQLiteBlobFile, SQLiteBlobsFile
from .BlockParallelReaders import BlockParallelReader, ParallelXZReader, ParallelZstdReader

from .MountSource import FileInfo, MountSource

from .FolderMountSource import FolderMountSource
from .LibarchiveMountSource import LibarchiveMountSource
from .RarMountSource import RarMountSource
from .ZipMountSource import ZipMountSource
from .SQLiteIndexedTar import SQLiteIndexedTar, SQLiteIndexedTarUserData
from .SQLiteIndex import SQLiteIndex

from .AutoMountLayer import AutoMountLayer
from .FileVersionLayer import FileVersionLayer
from .UnionMountSource import UnionMountSource
from .SubvolumesMountSource import SubvolumesMountSource

from .factory import openMountSource


open = openMountSource  # pylint: disable=redefined-builtin
