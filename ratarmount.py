#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import bisect
import bz2
import collections
import gzip
import io
import itertools
import json
import os
import pprint
import re
import sqlite3
import stat
import sys
import tarfile
import tempfile
import time
import traceback
from timeit import default_timer as timer

try:
    import indexed_bzip2
    from indexed_bzip2 import IndexedBzip2File
except ImportError:
    print( "[Warning] The indexed_bzip2 module was not found. Please install it to open bz2 compressed TAR files!" )

try:
    import indexed_gzip
    from indexed_gzip import IndexedGzipFile
except ImportError:
    print( "[Warning] The indexed_gzip module was not found. Please install it to open gzip compressed TAR files!" )

import fuse


__version__ = '0.6.1'

printDebug = 1

def overrides( parentClass ):
    def overrider( method ):
        assert method.__name__ in dir( parentClass )
        assert callable( getattr( parentClass, method.__name__ ) )
        return method
    return overrider

class ProgressBar:
    def __init__( self, maxValue ):
        self.maxValue = maxValue
        self.lastUpdateTime = time.time()
        self.lastUpdateValue = 0
        self.updateInterval = 2 # seconds
        self.creationTime = time.time()

    def update( self, value ):
        if self.lastUpdateTime is not None and ( time.time() - self.lastUpdateTime ) < self.updateInterval:
            return

        # Use whole interval since start to estimate time
        eta1 = int( ( time.time() - self.creationTime ) / value * ( self.maxValue - value ) )
        # Use only a shorter window interval to estimate time.
        # Accounts better for higher speeds in beginning, e.g., caused by caching effects.
        # However, this estimate might vary a lot while the other one stabilizes after some time!
        eta2 = int( ( time.time() - self.lastUpdateTime ) / ( value - self.lastUpdateValue ) * ( self.maxValue - value ) )
        print( "Currently at position {} of {} ({:.2f}%). "
               "Estimated time remaining with current rate: {} min {} s, with average rate: {} min {} s."
               .format( value, self.maxValue, value / self.maxValue * 100.,
                        eta2 // 60, eta2 % 60,
                        eta1 // 60, eta1 % 60 ),
               flush = True )

        self.lastUpdateTime = time.time()
        self.lastUpdateValue = value

class StenciledFile(io.BufferedIOBase):
    """A file abstraction layer giving a stenciled view to an underlying file."""

    def __init__(self, fileobj, stencils):
        """
        stencils: A list tuples specifying the offset and length of the underlying file to use.
                  The order of these tuples will be kept.
                  The offset must be non-negative and the size must be positive.

        Examples:
            stencil = [(5,7)]
                Makes a new 7B sized virtual file starting at offset 5 of fileobj.
            stencil = [(0,3),(5,3)]
                Make a new 6B sized virtual file containing bytes [0,1,2,5,6,7] of fileobj.
            stencil = [(0,3),(0,3)]
                Make a 6B size file containing the first 3B of fileobj twice concatenated together.
        """

        self.fileobj = fileobj
        self.offsets = [ x[0] for x in stencils ]
        self.sizes   = [ x[1] for x in stencils ]

        # Calculate cumulative sizes
        self.cumsizes = [ 0 ]
        for offset, size in stencils:
            assert offset >= 0
            assert size > 0
            self.cumsizes.append( self.cumsizes[-1] + size )

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek( 0 )

    def _findStencil( self, offset ):
        """
        Return index to stencil where offset belongs to. E.g., for stencils [(3,5),(8,2)], offsets 0 to
        and including 4 will still be inside stencil (3,5), i.e., index 0 will be returned. For offset 6,
        index 1 would be returned because it now is in the second contiguous region / stencil.
        """
        # bisect_left( value ) gives an index for a lower range: value < x for all x in list[0:i]
        # Because value >= 0 and list starts with 0 we can therefore be sure that the returned i>0
        # Consider the stencils [(11,2),(22,2),(33,2)] -> cumsizes [0,2,4,6]. Seek to offset 2 should seek to 22.
        assert offset >= 0
        i = bisect.bisect_left( self.cumsizes, offset + 1 ) - 1
        assert i >= 0
        return i

    @overrides(io.BufferedIOBase)
    def close(self):
        # Don't close the object given to us
        #self.fileobj.close()
        pass

    @overrides(io.BufferedIOBase)
    def fileno(self):
        return self.fileobj.fileno()

    @overrides(io.BufferedIOBase)
    def seekable(self):
        return self.fileobj.seekable()

    @overrides(io.BufferedIOBase)
    def readable(self):
        return self.fileobj.readable()

    @overrides(io.BufferedIOBase)
    def writable(self):
        return False

    @overrides(io.BufferedIOBase)
    def read(self, size=-1):
        if size == -1:
            size = self.cumsizes[-1] - self.offset

        # This loop works in a kind of leapfrog fashion. On each even loop iteration it seeks to the next stencil
        # and on each odd iteration it reads the data and increments the offset inside the stencil!
        result = b''
        i = self._findStencil( self.offset )
        while size > 0 and i < len( self.sizes ):
            # Read as much as requested or as much as the current contiguous region / stencil still contains
            readableSize = min( size, self.sizes[i] - ( self.offset - self.cumsizes[i] ) )
            if readableSize == 0:
                # Go to next stencil
                i += 1
                if i >= len( self.offsets ):
                    break
                self.fileobj.seek( self.offsets[i] )
            else:
                # Actually read data
                tmp = self.fileobj.read( readableSize )
                self.offset += len( tmp )
                result += tmp
                size -= readableSize
                # Now, either size is 0 or readableSize will be 0 in the next iteration

        return result

    @overrides(io.BufferedIOBase)
    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.cumsizes[-1] + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        if self.offset < 0:
            raise Exception("Trying to seek before the start of the file!")
        if self.offset >= self.cumsizes[-1]:
            return self.offset

        i = self._findStencil( self.offset )
        offsetInsideStencil = self.offset - self.cumsizes[i]
        assert offsetInsideStencil >= 0
        assert offsetInsideStencil < self.sizes[i]
        self.fileobj.seek( self.offsets[i] + offsetInsideStencil, io.SEEK_SET )

        return self.offset

    @overrides(io.BufferedIOBase)
    def tell(self):
        return self.offset


class SQLiteIndexedTar:
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    # Names must be identical to the SQLite column headers!
    FileInfo = collections.namedtuple( "FileInfo", "offsetheader offset size mtime mode type linkname uid gid istar issparse" )

    def __init__(
        self,
        tarFileName                = None,
        fileObject                 = None,
        writeIndex                 = False,
        clearIndexCache            = False,
        recursive                  = False,
        gzipSeekPointSpacing       = 4*1024*1024,
        encoding                   = tarfile.ENCODING,
        stripRecursiveTarExtension = False,
        ignoreZeros                = False,
        verifyModificationTime     = False,
    ):
        """
        tarFileName : Path to the TAR file to be opened. If not specified, a fileObject must be specified.
                      If only a fileObject is given, the created index can't be cached (efficiently).
        fileObject : A io.IOBase derived object. If not specified, tarFileName will be opened.
                     If it is an instance of IndexedBzip2File or IndexedGzipFile, then the offset
                     loading and storing from and to the SQLite database is managed automatically by this class.
        encoding : Will be forwarded to tarfile. Specifies how filenames inside the TAR are encoded.
        ignoreZeros : Will be forwarded to tarfile. Specifies to not only skip zero blocks but also blocks with
                      invalid data. Setting this to true can lead to some problems but is required to correctly
                      read concatenated tars.
        stripRecursiveTarExtension : If true and if recursive is also true, then a <file>.tar inside the current
                                     tar will be mounted at <file>/ instead of <file>.tar/.
        """
        # Version 0.1.0:
        #   - Initial version
        # Version 0.2.0:
        #   - Add sparse support and 'offsetheader' and 'issparse' columns to the SQLite database
        #   - Add TAR file size metadata in order to quickly check whether the TAR changed
        #   - Add 'offsetheader' to the primary key of the 'files' table so that files which were
        #     updated in the TAR can still be accessed if necessary.
        self.__version__ = '0.2.0'

        # stores which parent folders were last tried to add to database and therefore do exist
        self.parentFolderCache = []
        self.mountRecursively = recursive
        self.sqlConnection = None
        self.encoding = encoding
        self.stripRecursiveTarExtension = stripRecursiveTarExtension
        self.ignoreZeros = ignoreZeros
        self.verifyModificationTime = verifyModificationTime

        assert tarFileName or fileObject
        if not tarFileName:
            self.tarFileName = '<file object>'
            self.createIndex( fileObject )
            # return here because we can't find a save location without any identifying name
            return

        self.tarFileName = os.path.abspath( tarFileName )
        if not fileObject:
            fileObject = open( self.tarFileName, 'rb' )

        # rawFileObject : Only set when opening a compressed file and only kept to keep the
        #                 compressed file handle from being closed by the garbage collector.
        # tarFileObject : File object to the uncompressed (or decompressed) TAR file to read actual data out of.
        # compression   : Stores what kind of compression the originally specified TAR file uses.
        # isTar         : Can be false for the degenerated case of only a bz2 or gz file not containing a TAR
        self.tarFileObject, self.rawFileObject, self.compression, self.isTar = \
            SQLiteIndexedTar._openCompressedFile( fileObject, gzipSeekPointSpacing, encoding )

        # will be used for storing indexes if current path is read-only
        possibleIndexFilePaths = [
            self.tarFileName + ".index.sqlite",
            os.path.expanduser( os.path.join( "~", ".ratarmount",
                                              self.tarFileName.replace( "/", "_" ) + ".index.sqlite" ) )
        ]

        self.indexFileName = None
        if clearIndexCache:
            for indexPath in possibleIndexFilePaths:
                if os.path.isfile( indexPath ):
                    os.remove( indexPath )

        # Try to find an already existing index
        for indexPath in possibleIndexFilePaths:
            if self._tryLoadIndex( indexPath ):
                self.indexFileName = indexPath
                break
        if self.indexIsLoaded():
            self._loadOrStoreCompressionOffsets()
            return

        # Find a suitable (writable) location for the index database
        if writeIndex:
            for indexPath in possibleIndexFilePaths:
                try:
                    folder = os.path.dirname( indexPath )
                    os.makedirs( folder, exist_ok = True )

                    f = open( indexPath, 'wb' )
                    f.write( b'\0' * 1024 * 1024 )
                    f.close()
                    os.remove( indexPath )

                    self.indexFileName = indexPath
                    break
                except IOError:
                    if printDebug >= 2:
                        print( "Could not create file:", indexPath )

        self.createIndex( self.tarFileObject )
        self._loadOrStoreCompressionOffsets()

        self._storeTarMetadata()

        if printDebug >= 1 and writeIndex:
            # The 0-time is legacy for the automated tests
            print( "Writing out TAR index to", self.indexFileName, "took 0s",
                   "and is sized", os.stat( self.indexFileName ).st_size, "B" )

    def _storeTarMetadata( self ):
        """Adds some consistency meta information to recognize the need to update the cached TAR index"""

        metadataTable = """
            /* empty table whose sole existence specifies that we finished iterating the tar */
            CREATE TABLE "metadata" (
                "key"      VARCHAR(65535) NOT NULL, /* e.g. "tarsize" */
                "value"    VARCHAR(65535) NOT NULL  /* e.g. size in bytes as integer */
            );
        """

        try:
            tarStats = os.stat( self.tarFileName )
            self.sqlConnection.executescript( metadataTable )
            serializedTarStats = json.dumps( { attr : getattr( tarStats, attr )
                                               for attr in dir( tarStats ) if attr.startswith( 'st_' ) } )
            self.sqlConnection.execute( 'INSERT INTO "metadata" VALUES (?,?)', ( "tarstats", serializedTarStats ) )
            self.sqlConnection.commit()
        except Exception as exception:
            if printDebug >= 2:
                print( exception )
            print( "[Warning] There was an error when adding file metadata information." )
            print( "[Warning] Automatic detection of changed TAR files during index loading might not work." )

    def _openSqlDb( self, filePath ):
        self.sqlConnection = sqlite3.connect( filePath )
        self.sqlConnection.row_factory = sqlite3.Row
        self.sqlConnection.executescript( """
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
        """ )

    @staticmethod
    def _updateProgressBar( progressBar, fileobj ):
        try:
            if 'IndexedBzip2File' in globals() and isinstance( fileobj, IndexedBzip2File ):
                # Note that because bz2 works on a bitstream the tell_compressed returns the offset in bits
                progressBar.update( fileobj.tell_compressed() // 8 )
                return

            if hasattr( fileobj, 'fileobj' ):
                progressBar.update( fileobj.fileobj().tell() )
                return

            progressBar.update( fileobj.tell() )

        except:
            pass

    def createIndex( self, fileObject, progressBar = None, pathPrefix = '', streamOffset = 0 ):
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName, "..." )
        t0 = timer()

        # 1. If no SQL connection was given (by recursive call), open a new database file
        openedConnection = False
        if not self.indexIsLoaded():
            if printDebug >= 1:
                print( "Creating new SQLite index database at", self.indexFileName )

            createTables = """
                CREATE TABLE "files" (
                    "path"          VARCHAR(65535) NOT NULL,
                    "name"          VARCHAR(65535) NOT NULL,
                    "offsetheader"  INTEGER,  /* seek offset from TAR file where these file's contents resides */
                    "offset"        INTEGER,  /* seek offset from TAR file where these file's contents resides */
                    "size"          INTEGER,
                    "mtime"         INTEGER,
                    "mode"          INTEGER,
                    "type"          INTEGER,
                    "linkname"      VARCHAR(65535),
                    "uid"           INTEGER,
                    "gid"           INTEGER,
                    /* True for valid TAR files. Internally used to determine where to mount recursive TAR files. */
                    "istar"         BOOL   ,
                    "issparse"      BOOL   ,  /* for sparse files the file size refers to the expanded size! */
                    /* See SQL benchmarks for decision on the primary key.
                     * See also https://www.sqlite.org/optoverview.html
                     * (path,name) tuples might appear multiple times in a TAR if it got updated.
                     * In order to also be able to show older versions, we need to add
                     * the offsetheader column to the primary key. */
                    PRIMARY KEY (path,name,offsetheader)
                );
                /* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
                 * Therefore, it will not be sorted and inserting will be faster! */
                CREATE TABLE "filestmp" AS SELECT * FROM "files" WHERE 0;
                CREATE TABLE "parentfolders" (
                    "path"     VARCHAR(65535) NOT NULL,
                    "name"     VARCHAR(65535) NOT NULL,
                    PRIMARY KEY (path,name)
                );
            """

            openedConnection = True
            self._openSqlDb( self.indexFileName if self.indexFileName else ':memory:' )
            tables = self.sqlConnection.execute( 'SELECT name FROM sqlite_master WHERE type = "table";' )
            if set( [ "files", "filestmp", "parentfolders" ] ).intersection( set( [ t[0] for t in tables ] ) ):
                raise Exception( "[Error] The index file {} already seems to contain a table. "
                                 "Please specify --recreate-index." )
            self.sqlConnection.executescript( createTables )

        # 2. Open TAR file reader
        loadedTarFile = [] # Feign an empty TAR file if anything goes wrong
        try:
            streamed = bool( self.compression )
            # r: uses seeks to skip to the next file inside the TAR while r| doesn't do any seeks.
            # r| might be slower but for compressed files we have to go over all the data once anyways
            # and I had problems with seeks at this stage. Maybe they are gone now after the bz2 bugfix though.
            # Note that with ignore_zeros = True, no invalid header issues or similar will be raised even for
            # non TAR files!?
            # Using r: is buggy because the bz2 readers tell() is buggy and returns the number of total decoded bytes.
            # In seeking mode, tell() is used to initialize offset and offset_data so those are also bugged then!
            # This has been fixed starting from indexed_bzip2 1.1.2.
            if self.isTar:
                loadedTarFile = tarfile.open( fileobj      = fileObject,
                                              mode         = 'r|' if streamed else 'r:',
                                              ignore_zeros = self.ignoreZeros,
                                              encoding     = self.encoding )
        except tarfile.ReadError as exception:
            pass

        if progressBar is None:
            progressBar = ProgressBar( os.fstat( fileObject.fileno() ).st_size )

        # 3. Iterate over files inside TAR and add them to the database
        try:
          for tarInfo in loadedTarFile:
            loadedTarFile.members = []
            globalOffset = streamOffset + tarInfo.offset_data
            globalOffsetHeader = streamOffset + tarInfo.offset
            self._updateProgressBar( progressBar, fileObject )

            mode = tarInfo.mode
            if tarInfo.isdir() : mode |= stat.S_IFDIR
            if tarInfo.isfile(): mode |= stat.S_IFREG
            if tarInfo.issym() : mode |= stat.S_IFLNK
            if tarInfo.ischr() : mode |= stat.S_IFCHR
            if tarInfo.isfifo(): mode |= stat.S_IFIFO

            # Add a leading '/' as a convention where '/' represents the TAR root folder
            # Partly, done because fusepy specifies paths in a mounted directory like this
            # os.normpath does not delete duplicate '/' at beginning of string!
            fullPath = pathPrefix + "/" + os.path.normpath( tarInfo.name ).lstrip( '/' )

            # 4. Open contained TARs for recursive mounting
            tarExtension = '.tar'
            isTar = False
            if self.mountRecursively and tarInfo.isfile() and tarInfo.name.endswith( tarExtension ):
                if self.stripRecursiveTarExtension and len( tarExtension ) > 0 and fullPath.endswith( tarExtension ):
                    fullPath = fullPath[:-len( tarExtension )]

                oldPos = fileObject.tell()
                fileObject.seek( globalOffset )
                oldPrintName = self.tarFileName

                try:
                    self.tarFileName = tarInfo.name.lstrip( '/' ) # This is for output of the recursive call
                    # StenciledFile's tell returns the offset inside the file chunk instead of the global one,
                    # so we have to always communicate the offset of this chunk to the recursive call no matter
                    # whether tarfile has streaming access or seeking access!
                    tarFileObject = StenciledFile( fileObject, [ ( globalOffset, tarInfo.size ) ] )
                    self.createIndex( tarFileObject, progressBar, fullPath, globalOffset )

                    # if the TAR file contents could be read, we need to adjust the actual
                    # TAR file's metadata to be a directory instead of a file
                    mode = ( mode & 0o777 ) | stat.S_IFDIR
                    if mode & stat.S_IRUSR != 0: mode |= stat.S_IXUSR
                    if mode & stat.S_IRGRP != 0: mode |= stat.S_IXGRP
                    if mode & stat.S_IROTH != 0: mode |= stat.S_IXOTH
                    isTar = True

                except tarfile.ReadError:
                    None

                self.tarFileName = oldPrintName
                del tarFileObject
                fileObject.seek( oldPos )

            path, name = fullPath.rsplit( "/", 1 )
            fileInfo = (
                path               , # 0
                name               , # 1
                globalOffsetHeader , # 2
                globalOffset       , # 3
                tarInfo.size       , # 4
                tarInfo.mtime      , # 5
                mode               , # 6
                tarInfo.type       , # 7
                tarInfo.linkname   , # 8
                tarInfo.uid        , # 9
                tarInfo.gid        , # 10
                isTar              , # 11
                tarInfo.issparse() , # 12
            )
            self._setFileInfo( fileInfo )
        except tarfile.ReadError as e:
            if 'unexpected end of data' in str( e ):
                print( "[Warning] The TAR file is incomplete. Ratarmount will work but some files might be cut off. "
                       "If the TAR file size changes, ratarmount will recreate the index during the next mounting." )

        # Everything below should not be done in a recursive call of createIndex
        if streamOffset > 0:
            t1 = timer()
            if printDebug >= 1:
                print( "Creating offset dictionary for",
                       "<file object>" if self.tarFileName is None else self.tarFileName,
                       "took {:.2f}s".format( t1 - t0 ) )
            return

        # If no file is in the TAR, then it most likely indicates a possibly compressed non TAR file.
        # In that case add that itself to the file index. This won't work when called recursively,
        # so check stream offset.
        fileCount = self.sqlConnection.execute( 'SELECT COUNT(*) FROM "files";' ).fetchone()[0]
        if fileCount == 0:
            tarInfo = os.fstat( fileObject.fileno() )
            fname = os.path.basename( self.tarFileName )
            for suffix in [ '.gz', '.bz2', '.bzip2', '.gzip' ]:
                if fname.lower().endswith( suffix ) and len( fname ) > len( suffix ):
                    fname = fname[:-len( suffix )]
                    break

            # If the file object is actually an IndexedBzip2File or such, we can't directly use the file size
            # from os.stat and instead have to gather it from seek. Unfortunately, indexed_gzip does not support
            # io.SEEK_END even though it could as it has the index ...
            while fileObject.read( 1024*1024 ):
                self._updateProgressBar( progressBar, fileObject )
            fileSize = fileObject.tell()

            fileInfo = (
                ""                 , # 0 path
                fname              , # 1
                None               , # 2 header offset
                0                  , # 3 data offset
                fileSize           , # 4
                tarInfo.st_mtime   , # 5
                tarInfo.st_mode    , # 6
                None               , # 7 TAR file type. Don't care because it curerntly is unused and overlaps with mode
                None               , # 8 linkname
                tarInfo.st_uid     , # 9
                tarInfo.st_gid     , # 10
                False              , # 11 isTar
                False              , # 12 isSparse, don't care if it is actually sparse or not because it is not in TAR
            )
            self._setFileInfo( fileInfo )

        # All the code below is for database finalizing which should not be done in a recursive call of createIndex!
        if not openedConnection:
            return

        # 5. Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if printDebug >= 2:
            print( "Resorting files by path ..." )

        cleanupDatabase = """
            INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","name",rowid;
            DROP TABLE "filestmp";
            INSERT OR IGNORE INTO "files"
                /* path name offsetheader offset size mtime mode type linkname uid gid istar issparse */
                SELECT path,name,0,0,1,0,{},{},"",0,0,0,0
                FROM "parentfolders" ORDER BY "path","name";
            DROP TABLE "parentfolders";
        """.format( int( 0o555 | stat.S_IFDIR ), int( tarfile.DIRTYPE ) )
        self.sqlConnection.executescript( cleanupDatabase )

        # 6. Add Metadata
        metadataTables = """
            /* This table's sole existence specifies that we finished iterating the tar for older ratarmount versions */
            CREATE TABLE "versions" (
                "name"     VARCHAR(65535) NOT NULL, /* which component the version belongs to */
                "version"  VARCHAR(65535) NOT NULL, /* free form version string */
                /* Semantic Versioning 2.0.0 (semver.org) parts if they can be specified:
                 *   MAJOR version when you make incompatible API changes,
                 *   MINOR version when you add functionality in a backwards compatible manner, and
                 *   PATCH version when you make backwards compatible bug fixes. */
                "major"    INTEGER,
                "minor"    INTEGER,
                "patch"    INTEGER
            );
        """
        try:
            self.sqlConnection.executescript( metadataTables )
        except Exception as exception:
            if printDebug >= 2:
                print( exception )
            print( "[Warning] There was an error when adding metadata information. Index loading might not work." )

        try:
            def makeVersionRow( versionName, version ):
                versionNumbers = [ re.sub( '[^0-9]', '', x ) for x in version.split( '.' ) ]
                return ( versionName,
                         version,
                         versionNumbers[0] if len( versionNumbers ) > 0 else None,
                         versionNumbers[1] if len( versionNumbers ) > 1 else None,
                         versionNumbers[2] if len( versionNumbers ) > 2 else None, )

            versions = [ makeVersionRow( 'ratarmount', __version__ ),
                         makeVersionRow( 'index', self.__version__ ) ]

            if 'IndexedBzip2File' in globals() and isinstance( fileObject, IndexedBzip2File ):
                versions += [ makeVersionRow( 'indexed_bzip2', indexed_bzip2.__version__ ) ]

            if 'IndexedGzipFile' in globals() and isinstance( fileObject, IndexedGzipFile ):
                versions += [ makeVersionRow( 'indexed_gzip', indexed_gzip.__version__ ) ]

            self.sqlConnection.executemany( 'INSERT OR REPLACE INTO "versions" VALUES (?,?,?,?,?)', versions )
        except Exception as exception:
            print( "[Warning] There was an error when adding version information." )
            if printDebug >= 3:
                print( exception )

        self.sqlConnection.commit()

        t1 = timer()
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName,
                   "took {:.2f}s".format( t1 - t0 ) )

    def _rowToFileInfo( self, row ):
        return self.FileInfo(
            offset       = row['offset'],
            offsetheader = row['offsetheader'] if 'offsetheader' in row.keys() else 0,
            size         = row['size'],
            mtime        = row['mtime'],
            mode         = row['mode'],
            type         = row['type'],
            linkname     = row['linkname'],
            uid          = row['uid'],
            gid          = row['gid'],
            istar        = row['istar'],
            issparse     = row['issparse'] if 'issparse' in row.keys() else False
        )

    def getFileInfo( self, fullPath, listDir = False, listVersions = False, fileVersion = 0 ):
        """
        This is the heart of this class' public interface!

        path    : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        listDir : if True, return a dictionary for the given directory path: { fileName : FileInfo, ... }
                  if False, return simple FileInfo to given path (directory or file)
        fileVersion : If the TAR contains the same file path multiple times, by default only the last one is shown.
                      But with this argument other versions can be queried. Version 1 is the oldest one.
                      Version 0 translates to the most recent one for compatibility with tar --occurrence=<number>.
                      Version -1 translates to the second most recent, and so on.
                      For listDir=True, the file version makes no sense and is ignored!
                      So, even if a folder was overwritten by a file, which is already not well supported by tar,
                      then listDir for that path will still list all contents of the overwritten folder or folders,
                      no matter the specified version. The file system layer has to take care that a directory
                      listing is not even requeted in the first place if it is not a directory.
                      FUSE already does this by calling getattr for all parent folders in the specified path first.

        If path does not exist, always return None

        If listVersions is true, then return metadata for all versions of a file possibly appearing more than once
        in the TAR as a directory dictionary. listDir will then be ignored!
        """
        # @todo cache last listDir as most often a stat over all entries will soon follow

        assert isinstance( fileVersion, int )
        # also strips trailing '/' except for a single '/' and leading '/'
        fullPath = '/' + os.path.normpath( fullPath ).lstrip( '/' )

        if listVersions:
            path, name = fullPath.rsplit( '/', 1 )
            rows = self.sqlConnection.execute(
                'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?) ORDER BY "offsetheader" ASC',
                ( path, name )
            )
            result = { str( version + 1 ) : self._rowToFileInfo( row ) for version, row in enumerate( rows ) }
            return result

        if listDir:
            # For listing directory entries the file version can't be applied meaningfully at this abstraction layer.
            # E.g., should it affect the file version of the directory to list, or should it work on the listed files
            # instead and if so how exactly if there aren't the same versions for all files available, ...?
            # Or, are folders assumed to be overwritten by a new folder entry in a TAR or should they be union mounted?
            # If they should be union mounted, like is the case now, then the folder version only makes sense for
            # its attributes.
            rows = self.sqlConnection.execute( 'SELECT * FROM "files" WHERE "path" == (?)',
                                               ( fullPath.rstrip( '/' ), ) )
            dir = {}
            gotResults = False
            for row in rows:
                gotResults = True
                if row['name']:
                    dir[row['name']] = self._rowToFileInfo( row )
            return dir if gotResults else None

        path, name = fullPath.rsplit( '/', 1 )
        row = self.sqlConnection.execute(
            'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?) ORDER BY "offsetheader" {} LIMIT 1 OFFSET (?)'
            .format( 'DESC' if fileVersion is None or fileVersion <= 0 else 'ASC' ),
            ( path, name, 0 if fileVersion is None else fileVersion - 1 if fileVersion > 0 else fileVersion )
        ).fetchone()
        return self._rowToFileInfo( row ) if row else None

    def isDir( self, path ):
        return isinstance( self.getFileInfo( path, listDir = True ), dict )

    def _tryAddParentFolders( self, path ):
        # Add parent folders if they do not exist.
        # E.g.: path = '/a/b/c' -> paths = [('', 'a'), ('/a', 'b'), ('/a/b', 'c')]
        # Without the parentFolderCache, the additional INSERT statements increase the creation time
        # from 8.5s to 12s, so almost 50% slowdown for the 8MiB test TAR!
        paths = path.split( "/" )
        paths = [ p for p in ( ( "/".join( paths[:i] ), paths[i] ) for i in range( 1, len( paths ) ) )
                 if p not in self.parentFolderCache ]
        if not paths:
            return

        self.parentFolderCache += paths
        # Assuming files in the TAR are sorted by hierarchy, the maximum parent folder cache size
        # gives the maximum cacheable file nesting depth. High numbers lead to higher memory usage and lookup times.
        if len( self.parentFolderCache ) > 16:
            self.parentFolderCache = self.parentFolderCache[-8:]
        self.sqlConnection.executemany( 'INSERT OR IGNORE INTO "parentfolders" VALUES (?,?)',
                                        [ ( p[0], p[1] ) for p in paths ] )

    def _setFileInfo( self, row ):
        assert isinstance( row, tuple )
        try:
            self.sqlConnection.execute( 'INSERT OR REPLACE INTO "files" VALUES (' +
                                        ','.join( '?' * len( row ) ) + ');', row )
        except UnicodeEncodeError as e:
            print( "[Warning] Problem caused by file name encoding when trying to insert this row:", row )
            print( "[Warning] The file name will now be stored with the bad character being escaped" )
            print( "[Warning] instead of being correctly interpreted." )
            print( "[Warning] Please specify a suitable file name encoding using, e.g., --encoding iso-8859-1!" )
            print( "[Warning] A list of possible encodings can be found here:" )
            print( "[Warning] https://docs.python.org/3/library/codecs.html#standard-encodings" )

            checkedRow = []
            for x in list( row ): # check strings
                if isinstance( x, str ):
                    try:
                        x.encode()
                        checkedRow += [ x ]
                    except UnicodeEncodeError:
                        checkedRow += [ x.encode( self.encoding, 'surrogateescape' ) \
                                         .decode( self.encoding, 'backslashreplace' ) ]
                else:
                    checkedRow += [ x ]

            self.sqlConnection.execute( 'INSERT OR REPLACE INTO "files" VALUES (' +
                                        ','.join( '?' * len( row ) ) + ');', tuple( checkedRow ) )
            print( "[Warning] The escaped inserted row is now:", row )
            print()

        self._tryAddParentFolders( row[0] )

    def setFileInfo( self, fullPath, fileInfo ):
        """
        fullPath : the full path to the file with leading slash (/) for which to set the file info
        """
        assert self.sqlConnection
        assert fullPath[0] == "/"
        assert isinstance( fileInfo, self.FileInfo )

        # os.normpath does not delete duplicate '/' at beginning of string!
        path, name = fullPath.rsplit( "/", 1 )
        row = (
            path,
            name,
            fileInfo.offsetheader,
            fileInfo.offset,
            fileInfo.size,
            fileInfo.mtime,
            fileInfo.mode,
            fileInfo.type,
            fileInfo.linkname,
            fileInfo.uid,
            fileInfo.gid,
            fileInfo.istar,
            fileInfo.issparse,
        )
        self._setFileInfo( row )

    def indexIsLoaded( self ):
        if not self.sqlConnection:
            return False

        try:
            self.sqlConnection.execute( 'SELECT * FROM "files" WHERE 0 == 1;' )
        except sqlite3.OperationalError:
            self.sqlConnection = None
            return False

        return True

    def loadIndex( self, indexFileName ):
        """Loads the given index SQLite database and checks it for validity."""
        if self.indexIsLoaded():
            return

        t0 = time.time()
        self._openSqlDb( indexFileName )
        tables = [ x[0] for x in self.sqlConnection.execute( 'SELECT name FROM sqlite_master WHERE type="table"' ) ]
        versions = None
        try:
            rows = self.sqlConnection.execute( 'SELECT * FROM versions;' )
            versions = {}
            for row in rows:
                versions[row[0]] = ( row[2], row[3], row[4] )
        except:
            pass

        try:
            # Check indexes created with bugged bz2 decoder (bug existed when I did not store versions yet)
            if 'bzip2blocks' in tables and 'versions' not in tables:
                raise Exception( "The indexes created with version 0.3.0 through 0.3.3 for bzip2 compressed archives "
                                 "are very likely to be wrong because of a bzip2 decoder bug.\n"
                                 "Please delete the index or call ratarmount with the --recreate-index option!" )

            # Check for empty or incomplete indexes
            if 'files' not in tables:
                raise Exception( "SQLite index is empty" )

            if 'filestmp' in tables or 'parentfolders' in tables:
                raise Exception( "SQLite index is incomplete" )

            # Check for pre-sparse support indexes
            if 'versions' not in tables or 'index' not in versions or versions['index'][1] < 2:
                print( "[Warning] The found outdated index does not contain any sparse file information." )
                print( "[Warning] The index will also miss data about multiple versions of a file." )
                print( "[Warning] Please recreate the index if you have problems with those." )

            if 'metadata' in tables:
                values = dict( list( self.sqlConnection.execute( 'SELECT * FROM metadata;' ) ) )
                if 'tarstats' in values:
                    values = json.loads( values['tarstats'] )
                tarStats = os.stat( self.tarFileName )

                if hasattr( tarStats, "st_size" ) and 'st_size' in values \
                   and tarStats.st_size != values['st_size']:
                    raise Exception( "TAR file for this SQLite index has changed size from",
                                     values['st_size'], "to", tarStats.st_size)

                if self.verifyModificationTime \
                   and hasattr( tarStats, "st_mtime" ) \
                   and 'st_mtime' in values \
                   and tarStats.st_mtime != values['st_mtime']:
                    raise Exception( "The modification date for the TAR file", values['st_mtime'],
                                     "to this SQLite index has changed (" + str( tarStats.st_mtime ) + ")" )

        except Exception as e:
            # indexIsLoaded checks self.sqlConnection, so close it before returning because it was found to be faulty
            try:
                self.sqlConnection.close()
            except:
                pass
            self.sqlConnection = None

            raise e

        if printDebug >= 1:
            # Legacy output for automated tests
            print( "Loading offset dictionary from", indexFileName, "took {:.2f}s".format( time.time() - t0 ) )

    def _tryLoadIndex( self, indexFileName ):
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile( indexFileName ):
            return False

        try:
            self.loadIndex( indexFileName )
        except Exception as exception:
            if printDebug >= 3:
                traceback.print_exc()

            print( "[Warning] Could not load file '" + indexFileName  )
            print( "[Info] Exception:", exception )
            print( "[Info] Some likely reasons for not being able to load the index file:" )
            print( "[Info]   - The index file has incorrect read permissions" )
            print( "[Info]   - The index file is incomplete because ratarmount was killed during index creation" )
            print( "[Info]   - The index file was detected to contain errors because of known bugs of older versions" )
            print( "[Info]   - The index file got corrupted because of:" )
            print( "[Info]     - The program exited while it was still writing the index because of:" )
            print( "[Info]       - the user sent SIGINT to force the program to quit" )
            print( "[Info]       - an internal error occured while writing the index" )
            print( "[Info]       - the disk filled up while writing the index" )
            print( "[Info]     - Rare lowlevel corruptions caused by hardware failure" )

            print( "[Info] This might force a time-costly index recreation, so if it happens often\n"
                   "       and mounting is slow, try to find out why loading fails repeatedly,\n"
                   "       e.g., by opening an issue on the public github page." )

            try:
                os.remove( indexFileName )
            except OSError:
                print( "[Warning] Failed to remove corrupted old cached index file:", indexFileName )

        if printDebug >= 3 and self.indexIsLoaded():
            print( "Loaded index", indexFileName )

        return self.indexIsLoaded()

    @staticmethod
    def _detectCompression( name = None, fileobj = None, encoding = tarfile.ENCODING ):
        oldOffset = None
        if fileobj:
            assert fileobj.seekable()
            oldOffset = fileobj.tell()
            if name is None:
                name = fileobj.name
        isTar = False

        for compression in [ '', 'bz2', 'gz', 'xz' ]:
            try:
                # Simply opening a TAR file should be fast as only the header should be read!
                tarfile.open( name = name, fileobj = fileobj, mode = 'r:' + compression, encoding = encoding )
                isTar = True

                if compression == 'bz2' and 'IndexedBzip2File' not in globals():
                    raise Exception( "Can't open a bzip2 compressed TAR file '{}' without indexed_bzip2 module!"
                                     .format( name ) )
                elif compression == 'gz' and 'IndexedGzipFile' not in globals():
                    raise Exception( "Can't open a bzip2 compressed TAR file '{}' without indexed_gzip module!"
                                     .format( name ) )
                elif compression == 'xz':
                    raise Exception( "Can't open xz compressed TAR file '{}'!".format( name ) )

                if oldOffset is not None:
                    fileobj.seek( oldOffset )

                return isTar, compression

            except tarfile.ReadError as e:
                if oldOffset is not None:
                    fileobj.seek( oldOffset )

            try:
                if compression == 'bz2':
                    bz2.open( fileobj if fileobj else name ).read( 1 )
                    return isTar, compression

                if compression == 'gz':
                    gzip.open( fileobj if fileobj else name ).read( 1 )
                    return isTar, compression
            except:
                if oldOffset is not None:
                    fileobj.seek( oldOffset )

        raise Exception( "File '{}' does not seem to be a valid TAR file!".format( name ) )

    @staticmethod
    def _openCompressedFile( fileobj, gzipSeekPointSpacing, encoding ):
        """Opens a file possibly undoing the compression."""
        rawFile = None
        tarFile = fileobj
        isTar, compression = SQLiteIndexedTar._detectCompression( fileobj = tarFile, encoding = encoding )

        if compression == 'bz2':
            rawFile = tarFile # save so that garbage collector won't close it!
            tarFile = IndexedBzip2File( rawFile.fileno() )
        elif compression == 'gz':
            rawFile = tarFile # save so that garbage collector won't close it!
            # drop_handles keeps a file handle opening as is required to call tell() during decoding
            tarFile = IndexedGzipFile( fileobj = rawFile,
                                       drop_handles = False,
                                       spacing = gzipSeekPointSpacing )

        return tarFile, rawFile, compression, isTar

    def _loadOrStoreCompressionOffsets( self ):
        # This should be called after the TAR file index is complete (loaded or created).
        # If the TAR file index was created, then tarfile has iterated over the whole file once
        # and therefore completed the implicit compression offset creation.
        db = self.sqlConnection
        fileObject = self.tarFileObject

        if 'IndexedBzip2File' in globals() and isinstance( fileObject, IndexedBzip2File ):
            try:
                offsets = dict( db.execute( 'SELECT blockoffset,dataoffset FROM bzip2blocks;' ) )
                fileObject.set_block_offsets( offsets )
            except Exception as e:
                if printDebug >= 2:
                    print( "[Info] Could not load BZip2 Block offset data. Will create it from scratch." )

                tables = [ x[0] for x in db.execute( 'SELECT name FROM sqlite_master WHERE type="table";' ) ]
                if 'bzip2blocks' in tables:
                    db.execute( 'DROP TABLE bzip2blocks' )
                db.execute( 'CREATE TABLE bzip2blocks ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )' )
                db.executemany( 'INSERT INTO bzip2blocks VALUES (?,?)',
                                fileObject.block_offsets().items() )
                db.commit()
            return

        if 'IndexedGzipFile' in globals() and isinstance( fileObject, IndexedGzipFile ):
            # indexed_gzip index only has a file based API, so we need to write all the index data from the SQL
            # database out into a temporary file. For that, let's first try to use the same location as the SQLite
            # database because it should have sufficient writing rights and free disk space.
            gzindex = None
            for tmpDir in [ os.path.dirname( self.indexFileName ), None ]:
                # Try to export data from SQLite database. Note that no error checking against the existence of
                # gzipindex table is done because the exported data itself might also be wrong and we can't check
                # against this. Therefore, collate all error checking by catching exceptions.
                try:
                    gzindex = tempfile.mkstemp( dir = tmpDir )[1]
                    with open( gzindex, 'wb' ) as file:
                        file.write( db.execute( 'SELECT data FROM gzipindex' ).fetchone()[0] )
                except:
                    try:
                        os.remove( gzindex )
                    except:
                        pass
                    gzindex = None

            try:
                fileObject.import_index( filename = gzindex )
                return
            except:
                pass

            try:
                os.remove( gzindex )
            except:
                pass

            # Store the offsets into a temporary file and then into the SQLite database
            if printDebug >= 2:
                print( "[Info] Could not load GZip Block offset data. Will create it from scratch." )

            # Transparently force index to be built if not already done so. build_full_index was buggy for me.
            # Seeking from end not supported, so we have to read the whole data in in a loop
            while fileObject.read( 1024*1024 ):
                pass

            # The created index can unfortunately be pretty large and tmp might actually run out of memory!
            # Therefore, try different paths, starting with the location where the index resides.
            gzindex = None
            for tmpDir in [ os.path.dirname( self.indexFileName ), None ]:
                gzindex = tempfile.mkstemp( dir = tmpDir )[1]
                try:
                    fileObject.export_index( filename = gzindex )
                except indexed_gzip.ZranError:
                    try:
                        os.remove( gzindex )
                    except:
                        pass
                    gzindex = None

            if not gzindex or not os.path.isfile( gzindex ):
                print( "[Warning] The GZip index required for seeking could not be stored in a temporary file!" )
                print( "[Info] This might happen when you are out of space in your temporary file and at the" )
                print( "[Info] the index file location. The gzipindex size takes roughly 32kiB per 4MiB of" )
                print( "[Info] uncompressed(!) bytes (0.8% of the uncompressed data) by default." )
                raise Exception( "[Error] Could not initialize the GZip seek cache." )
            elif printDebug >= 2:
                print( "Exported GZip index size:", os.stat( gzindex ).st_size )

            # Store contents of temporary file into the SQLite database
            tables = [ x[0] for x in db.execute( 'SELECT name FROM sqlite_master WHERE type="table"' ) ]
            if 'gzipindex' in tables:
                db.execute( 'DROP TABLE gzipindex' )
            db.execute( 'CREATE TABLE gzipindex ( data BLOB )' )
            with open( gzindex, 'rb' ) as file:
                db.execute( 'INSERT INTO gzipindex VALUES (?)', ( file.read(), ) )
            db.commit()
            os.remove( gzindex )


class TarMount( fuse.Operations ):
    """
    This class implements the fusepy interface in order to create a mounted file system view
    to a TAR archive.
    Tasks of this class:
       - Changes all file permissions to read-only
       - Manage possibly multiple SQLiteIndexedTar objects and folder paths to be union mounted together
       - Forward access to mounted folders to the respective system calls
       - Resolve hard links returned by SQLiteIndexedTar
       - Get actual file contents either by directly reading from the TAR or by using StenciledFile and tarfile
       - Provide hidden folders as an interface to get older versions of updated files
    """

    __slots__ = (
        'mountSources',
        'rootFileInfo',
        'mountPoint',
        'mountPointFd',
        'mountPointWasCreated',
        'encoding',
    )

    def __init__( self, pathToMount, mountPoint = None, encoding = tarfile.ENCODING, **sqliteIndexedTarOptions ):
        self.encoding = encoding

        try:
            os.fspath( pathToMount )
            pathToMount = [ pathToMount ]
        except:
            pass

        self.mountSources = [ SQLiteIndexedTar( tarFile,
                                                writeIndex = True,
                                                encoding = self.encoding,
                                                **sqliteIndexedTarOptions )
                              if not os.path.isdir( tarFile ) else os.path.realpath( tarFile )
                              for tarFile in pathToMount ]

        # make the mount point read only and executable if readable, i.e., allow directory listing
        tarStats = os.stat( pathToMount[0] )
        # clear higher bits like S_IFREG and set the directory bit instead
        mountMode = ( tarStats.st_mode & 0o777 ) | stat.S_IFDIR
        if mountMode & stat.S_IRUSR != 0: mountMode |= stat.S_IXUSR
        if mountMode & stat.S_IRGRP != 0: mountMode |= stat.S_IXGRP
        if mountMode & stat.S_IROTH != 0: mountMode |= stat.S_IXOTH

        self.rootFileInfo = SQLiteIndexedTar.FileInfo(
            offset       = None             ,
            offsetheader = None             ,
            size         = tarStats.st_size ,
            mtime        = tarStats.st_mtime,
            mode         = mountMode        ,
            type         = tarfile.DIRTYPE  ,
            linkname     = ""               ,
            uid          = tarStats.st_uid  ,
            gid          = tarStats.st_gid  ,
            istar        = True             ,
            issparse     = False
        )

        # Create mount point if it does not exist
        self.mountPointWasCreated = False
        if mountPoint and not os.path.exists( mountPoint ):
            os.mkdir( mountPoint )
            self.mountPointWasCreated = True
        self.mountPoint = os.path.realpath( mountPoint )
        self.mountPointFd = os.open( self.mountPoint, os.O_RDONLY )

    def __del__( self ):
        try:
            if self.mountPointWasCreated:
                os.rmdir( self.mountPoint )
        except:
            pass

        os.close( self.mountPointFd )

    @staticmethod
    def _getFileInfoFromRealFile( filePath ):
        stats = os.lstat( filePath )
        return SQLiteIndexedTar.FileInfo(
            offset       = None          ,
            offsetheader = None          ,
            size         = stats.st_size ,
            mtime        = stats.st_mtime,
            mode         = stats.st_mode ,
            type         = None          , # I think this is completely unused and mostly contained in mode
            linkname     = os.readlink( filePath ) if os.path.islink( filePath ) else None,
            uid          = stats.st_uid  ,
            gid          = stats.st_gid  ,
            istar        = False         ,
            issparse     = False
        )

    def _getUnionMountFileInfo( self, filePath, fileVersion = 0 ):
        """Returns the file info from the last (most recent) mount source in mountSources, which contains that file"""

        if filePath == '/':
            return self.rootFileInfo, None

        # We need to keep the sign of the fileVersion in order to forward it to SQLiteIndexedTar.
        # When the requested version can't be found in a mount source, increment negative specified versions
        # by the amount of versions in that mount source or decrement the initially positive version.
        if fileVersion <= 0:
            for mountSource in reversed( self.mountSources ):
                if isinstance( mountSource, str ):
                    realFilePath = os.path.join( mountSource, filePath.lstrip( os.path.sep ) )
                    if os.path.lexists( realFilePath ):
                        if fileVersion == 0:
                            return self._getFileInfoFromRealFile( realFilePath ), \
                                   os.path.join( mountSource, filePath.lstrip( os.path.sep ) )
                        fileVersion += 1

                else:
                    fileInfo = mountSource.getFileInfo( filePath, listDir = False, fileVersion = fileVersion )
                    if isinstance( fileInfo, SQLiteIndexedTar.FileInfo ):
                        return fileInfo, mountSource
                    fileVersion += len( mountSource.getFileInfo( filePath, listVersions = True ) )

                if fileVersion > 0:
                    return None

            return None

        # fileVersion >= 1
        for mountSource in self.mountSources:
            if isinstance( mountSource, str ):
                realFilePath = os.path.join( mountSource, filePath.lstrip( os.path.sep ) )
                if os.path.lexists( realFilePath ):
                    if fileVersion == 1:
                        return self._getFileInfoFromRealFile( realFilePath ), \
                               os.path.join( mountSource, filePath.lstrip( os.path.sep ) )
                    fileVersion -= 1

            else:
                fileInfo = mountSource.getFileInfo( filePath, listDir = False, fileVersion = fileVersion )
                if isinstance( fileInfo, SQLiteIndexedTar.FileInfo ):
                    return fileInfo, mountSource
                fileVersion -= len( mountSource.getFileInfo( filePath, listVersions = True ) )

            if fileVersion < 1:
                return None

        return None

    def _decodeVersionsPathAPI( self, filePath ):
        """
        Do a loop over the parent path parts to resolve possible versions in parent folders.
        Note that multiple versions of a folder always are union mounted. So, for the path to a file
        inside those folders the exact version of a parent folder can simply be removed for lookup.
        Therefore, translate something like: /foo.version/3/bar.version/2/mimi.version/1 into
        /foo/bar/mimi.version/1
        This is possibly time-costly but requesting a different version from the most recent should
        be a rare occurence and FUSE also checks all parent parts before accessing a file so it
        might only slow down access by roughly factor 2.
        """

        # @todo make it work for files ending with '.versions'.
        # Currently, this feature would be hidden by those files. But, I think this should be quite rare.
        # I could allow arbitrary amounts of dots like '....versions' but then it wouldn't be discernible
        # for ...versions whether the versions of ..versions or .versions file was requested. I could add
        # a rule for the decision, like ...versions shows the versions of .versions and ....versions for
        # ..versions, however, all of this might require an awful lot of file existence checking.
        # My first idea was to use hidden subfolders for each file like path/to/file/.versions/1 but FUSE
        # checks the parents in a path that they are directories first, so getattr or readdir is not even
        # called for path/to/file/.versions if path/to/file is not a directory.
        # Another alternative might be one hidden folder at the root for a parallel file tree, like
        # /.versions/path/to/file/3 but that runs into similar problems when trying to specify the file
        # version or if a .versions root directory exists.

        filePathParts = filePath.lstrip( '/' ).split( '/' )
        filePath = ''
        pathIsVersions = False
        fileVersion = None # Not valid if None or parentIsVersions is True
        for part in filePathParts:
            # Skip over the exact version specified
            if pathIsVersions:
                try:
                    fileVersion = int( part )
                    assert str( fileVersion ) == part
                except:
                    return None
                pathIsVersions = False
                continue

            # Simply append normal existing folders
            tmpFilePath = '/'.join( [ filePath, part ] )
            if self._getUnionMountFileInfo( tmpFilePath ):
                filePath = tmpFilePath
                fileVersion = 0
                continue

            # If current path does not exist, check if it is a special versions path
            if part.endswith( '.versions' ) and len( part ) > len( '.versions' ):
                pathIsVersions = True
                filePath = tmpFilePath[:-len( '.versions' )]
                continue

            # Parent path does not exist and is not a versions path, so any subpaths also won't exist either
            return None

        return filePath, pathIsVersions, ( None if pathIsVersions else fileVersion )

    def _getFileInfo( self, filePath ):
        """Wrapper for _getUnionMountFileInfo, which also resolves special file version specifications in the path."""
        result = self._getUnionMountFileInfo( filePath )
        if result:
            return result

        # If no file was found, check if a special .versions folder to an existing file/folder was queried.
        result = self._decodeVersionsPathAPI( filePath )
        if not result:
            raise fuse.FuseOSError( fuse.errno.ENOENT )
        filePath, pathIsVersions, fileVersion = result

        # 2.) Check if the request was for the special .versions folder and return its contents or stats
        # At this point, filePath is assured to actually exist!
        if pathIsVersions:
            parentFileInfo, mountSource = self._getUnionMountFileInfo( filePath )
            return SQLiteIndexedTar.FileInfo(
                offset       = None                ,
                offsetheader = None                ,
                size         = 0                   ,
                mtime        = parentFileInfo.mtime,
                mode         = 0o777 | stat.S_IFDIR,
                type         = tarfile.DIRTYPE     ,
                linkname     = ""                  ,
                uid          = parentFileInfo.uid  ,
                gid          = parentFileInfo.gid  ,
                istar        = False               ,
                issparse     = False
            ), mountSource

        # 3.) At this point the request is for an actual version of a file or folder
        result = self._getUnionMountFileInfo( filePath, fileVersion = fileVersion )
        if result:
            return result

        raise fuse.FuseOSError( fuse.errno.ENOENT )

    def _getUnionMountListDir( self, folderPath ):
        """
        Returns the set of all folder contents over all mount sources or None if the path was found in none of them.
        """

        files = set()
        folderExists = False

        for mountSource in self.mountSources:
            if isinstance( mountSource, str ):
                realFolderPath = os.path.join( mountSource, folderPath.lstrip( os.path.sep ) )
                if os.path.isdir( realFolderPath ):
                    files = files.union( os.listdir( realFolderPath ) )
                    folderExists = True
            else:
                result = mountSource.getFileInfo( folderPath, listDir = True )
                if isinstance( result, dict ):
                    files = files.union( result.keys() )
                    folderExists = True

        return files if folderExists else None

    @overrides( fuse.Operations )
    def init( self, connection ):
        os.fchdir( self.mountPointFd )
        for i in range( len( self.mountSources ) ):
            if self.mountSources[i] == self.mountPoint:
                self.mountSources[i] = '.'

    @overrides( fuse.Operations )
    def getattr( self, path, fh = None ):
        fileInfo, _ = self._getFileInfo( path )

        # Dereference hard links
        if not stat.S_ISREG( fileInfo.mode ) and not stat.S_ISLNK( fileInfo.mode ) and fileInfo.linkname:
            targetLink = '/' + fileInfo.linkname.lstrip( '/' )
            if targetLink != path:
                return self.getattr( targetLink, fh )

        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = dict( ( "st_" + key, getattr( fileInfo, key ) ) for key in ( 'size', 'mtime', 'mode', 'uid', 'gid' ) )
        # signal that everything was mounted read-only
        statDict['st_mode'] &= ~( stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH )
        statDict['st_mtime'] = int( statDict['st_mtime'] )
        statDict['st_nlink'] = 1  # TODO: this is wrong for files with hardlinks

        # du by default sums disk usage (the number of blocks used by a file)
        # instead of file size directly. Tar files are usually a series of 512B
        # blocks, so we report a 1-block header + ceil(filesize / 512).
        statDict['st_blksize'] = 512
        statDict['st_blocks'] = 1 + ((fileInfo.size + 511) // 512)

        return statDict

    @overrides( fuse.Operations )
    def readdir( self, path, fh ):
        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        yield '.'
        yield '..'

        files = self._getUnionMountListDir( path )
        if files is not None:
            for key in files:
                yield key
            return

        # If no folder was found, check whether the special .versions folder was requested
        result = self._decodeVersionsPathAPI( path )
        if not result:
            return
        path, pathIsVersions, fileVersion = result

        if not pathIsVersions:
            files = self._getUnionMountListDir( path )
            if files is not None:
                for key in files:
                    yield key
            return

        # Print all available versions of the file at filePath as the contents of the special '.versions' folder
        version = 0
        for mountSource in self.mountSources:
            if isinstance( mountSource, str ):
                realPath = os.path.join( mountSource, path.lstrip( os.path.sep ) )
                if os.path.lexists( realPath ):
                    version += 1
                    yield str( version )
            else:
                result = mountSource.getFileInfo( path, listVersions = True )
                for x in result.keys():
                    version += 1
                    yield str( version )

    @overrides( fuse.Operations )
    def readlink( self, path ):
        fileInfo, _ = self._getFileInfo( path )
        return fileInfo.linkname

    @overrides( fuse.Operations )
    def read( self, path, length, offset, fh ):
        fileInfo, mountSource = self._getFileInfo( path )

        # Dereference hard links
        if not stat.S_ISREG( fileInfo.mode ) and not stat.S_ISLNK( fileInfo.mode ) and fileInfo.linkname:
            targetLink = '/' + fileInfo.linkname.lstrip( '/' )
            if targetLink != path:
                return self.read( targetLink, length, offset, fh )

        # Handle access to underlying non-empty folder
        if isinstance( mountSource, str ):
            f = open( mountSource, 'rb' )
            f.seek( offset )
            return f.read( length )

        if fileInfo.issparse:
            # The TAR file format is very simple. It's just a concatenation of TAR blocks. There is not even a
            # global header, only the TAR block headers. That's why we can simpley cut out the TAR block for
            # the sparse file using StenciledFile and then use tarfile on it to expand the sparse file correctly.
            tarBlockSize = fileInfo.offset - fileInfo.offsetheader + fileInfo.size
            tarSubFile = StenciledFile( mountSource.tarFileObject, [ ( fileInfo.offsetheader, tarBlockSize ) ] )
            tmpTarFile = tarfile.open( fileobj = tarSubFile, mode = 'r:', encoding = self.encoding )
            tmpFileObject = tmpTarFile.extractfile( next( iter( tmpTarFile ) ) )
            tmpFileObject.seek( offset, os.SEEK_SET )
            result = tmpFileObject.read( length )
            tmpTarFile.close()
            return result

        try:
            mountSource.tarFileObject.seek( fileInfo.offset + offset, os.SEEK_SET )
            return mountSource.tarFileObject.read( length )
        except RuntimeError as e:
            traceback.print_exc()
            print( "Caught exception when trying to read data from underlying TAR file! Returning errno.EIO." )
            raise fuse.FuseOSError( fuse.errno.EIO )


class TarFileType:
    """
    Similar to argparse.FileType but raises an exception if it is not a valid TAR file.
    """

    def __init__( self, mode = 'r', compressions = [ '' ], encoding = tarfile.ENCODING ):
        self.compressions = [ '' if c is None else c for c in compressions ]
        self.mode = mode
        self.encoding = encoding

    def __call__( self, tarFile ):
        if not os.path.exists( tarFile ):
            return

        for compression in self.compressions:
            try:
                tarfile.open( tarFile, mode = self.mode + ':' + compression, encoding = self.encoding )
                return ( tarFile, compression )
            except tarfile.ReadError:
                pass

            try:
                if compression == 'bz2':
                    bz2.open( tarFile ).read( 1 )
                    return ( tarFile, compression )
                if compression == 'gz':
                    gzip.open( tarFile ).read( 1 )
                    return ( tarFile, compression )
            except:
                pass

        msg = "Archive '{}' can't be opened!\n".format( tarFile )
        msg += "This might happen for xz compressed TAR archives, which currently is not supported.\n"
        if 'IndexedBzip2File' not in globals() or 'IndexedGzipFile' not in globals():
            msg += "If you are trying to open a bz2 or gzip compressed file make sure that you have the indexed_bzip2 "\
                   "and indexed_gzip modules installed."

        raise argparse.ArgumentTypeError( msg )

class CustomFormatter( argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter ):
    pass

def parseArgs( args = None ):
    parser = argparse.ArgumentParser(
        formatter_class = CustomFormatter,
        description = '''\
With ratarmount, you can:
  - Mount a TAR file to a folder for read-only access
  - Bind mount a folder to another folder for read-only access
  - Union mount a list of TARs and folders to a folder for read-only access
''',
        epilog = '''\
# Metadata Index Cache

In order to reduce the mounting time, the created index for random access
to files inside the tar will be saved to these locations in order. A lower
location will only be used if all upper locations can't be written to.

    1. <path to tar>.index.sqlite
    2. ~/.ratarmount/<path to tar: '/' -> '_'>.index.sqlite
       E.g., ~/.ratarmount/_media_cdrom_programm.tar.index.sqlite

# Bind Mounting

The mount sources can be TARs and/or folders.  Because of that, ratarmount
can also be used to bind mount folders read-only to another path similar to
"bindfs" and "mount --bind". So, for:
    ratarmount folder mountpoint
all files in folder will now be visible in mountpoint.

# Union Mounting

If multiple mount sources are specified, the sources on the right side will be
added to or update existing files from a mount source left of it. For example:
    ratarmount folder1 folder2 mountpoint
will make both, the files from folder1 and folder2, visible in mountpoint.
If a file exists in both multiple source, then the file from the rightmost
mount source will be used, which in the above example would be "folder2".

If you want to update / overwrite a folder with the contents of a given TAR,
you can specify the folder both as a mount source and as the mount point:
    ratarmount folder file.tar folder
The FUSE option -o nonempty will be automatically added if such a usage is
detected. If you instead want to update a TAR with a folder, you only have to
swap the two mount sources:
    ratarmount file.tar folder folder

# File versions

If a file exists multiple times in a TAR or in multiple mount sources, then
the hidden versions can be accessed through special <file>.versions folders.
For example, consider:
    ratarmount folder updated.tar mountpoint
and the file "foo" exists both in the folder and in two different versions
in "updated.tar". Then, you can list all three versions using:
    ls -la mountpoint/foo.versions/
        dr-xr-xr-x 2 user group     0 Apr 25 21:41 .
        dr-x------ 2 user group 10240 Apr 26 15:59 ..
        -r-x------ 2 user group   123 Apr 25 21:41 1
        -r-x------ 2 user group   256 Apr 25 21:53 2
        -r-x------ 2 user group  1024 Apr 25 22:13 3
In this example, the oldest version has only 123 bytes while the newest and
by default shown version has 1024 bytes. So, in order to look at the oldest
version, you can simply do:
    cat mountpoint/foo.versions/1
''' )

    parser.add_argument(
        '-f', '--foreground', action='store_true', default = False,
        help = 'Keeps the python program in foreground so it can print debug '
               'output when the mounted path is accessed.' )

    parser.add_argument(
        '-d', '--debug', type = int, default = 1,
        help = 'Sets the debugging level. Higher means more output. Currently, 3 is the highest.' )

    parser.add_argument(
        '-c', '--recreate-index', action='store_true', default = False,
        help = 'If specified, pre-existing .index files will be deleted and newly created.' )

    parser.add_argument(
        '-r', '--recursive', action='store_true', default = False,
        help = 'Mount TAR archives inside the mounted TAR recursively. '
               'Note that this only has an effect when creating an index. '
               'If an index already exists, then this option will be effectively ignored. '
               'Recreate the index if you want change the recursive mounting policy anyways.' )

    # Considerations for the default value:
    #   - seek times for the bz2 backend are between 0.01s and 0.1s
    #   - seek times for the gzip backend are roughly 1/10th compared to bz2 at a default spacing of 4MiB
    #     -> we could do a spacing of 40MiB (however the comparison are for another test archive, so it might not apply)
    #   - ungziping firefox 66 inflates the compressed size of 66MiB to 184MiB (~3 times more) and takes 1.4s on my PC
    #     -> to have a response time of 0.1s, it would require a spacing < 13MiB
    #   - the gzip index takes roughly 32kiB per seek point
    #   - the bzip2 index takes roughly 16B per 100-900kiB of compressed data
    #     -> for the gzip index to have the same space efficiency assuming a compression ratio of only 1,
    #        the spacing would have to be 1800MiB at which point it would become almost useless
    parser.add_argument(
        '-gs', '--gzip-seek-point-spacing', type = float, default = 16,
        help =
        'This only is applied when the index is first created or recreated with the -c option. '
        'The spacing given in MiB specifies the seek point distance in the uncompressed data. '
        'A distance of 16MiB means that archives smaller than 16MiB in uncompressed size will '
        'not benefit from faster seek times. A seek point takes roughly 32kiB. '
        'So, smaller distances lead to more responsive seeking but may explode the index size!' )

    parser.add_argument(
        '-p', '--prefix', type = str, default = '',
        help = '[deprecated] Use "-o modules=subdir,subdir=<prefix>" instead. '
               'This standard way utilizes FUSE itself and will also work for other FUSE '
               'applications. So, it is preferable even if a bit more verbose.'
               'The specified path to the folder inside the TAR will be mounted to root. '
               'This can be useful when the archive as created with absolute paths. '
               'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
               '-p /var/log/apt/ can be specified so that the mount target directory '
               '>directly< contains history.log.' )

    parser.add_argument(
        '-e', '--encoding', type = str, default = tarfile.ENCODING,
        help = 'Specify an input encoding used for file names among others in the TAR. '
               'This must be used when, e.g., trying to open a latin1 encoded TAR on an UTF-8 system. '
               'Possible encodings: https://docs.python.org/3/library/codecs.html#standard-encodings' )

    parser.add_argument(
        '-i', '--ignore-zeros', action = 'store_true',
        help = 'Ignore zeroed blocks in archive. Normally, two consecutive 512-blocks filled with zeroes mean EOF '
               'and ratarmount stops reading after encountering them. This option instructs it to read further and '
               'is useful when reading archives created with the -A option.' )

    parser.add_argument(
        '--verify-mtime', action = 'store_true',
        help = 'By default, only the TAR file size is checked to match the one in the found existing ratarmount index. '
               'If this option is specified, then also check the modification timestamp. But beware that the mtime '
               'might change during copying or downloading without the contents changing. So, this check might cause '
               'false positives.' )

    parser.add_argument(
        '-s', '--strip-recursive-tar-extension', action = 'store_true',
        help = 'If true, then recursively mounted TARs named <file>.tar will be mounted at <file>/. '
               'This might lead to folders of the same name being overwritten, so use with care. '
               'The index needs to be (re)created to apply this option!' )

    parser.add_argument(
        '-o', '--fuse', type = str, default = '',
        help = 'Comma separated FUSE options. See "man mount.fuse" for help. '
               'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ' )

    parser.add_argument(
        '-v', '--version', action='store_true', help = 'Print version string.' )

    parser.add_argument(
        'mount_source', nargs = '+',
        help = 'The path to the TAR archive to be mounted. '
               'If multiple archives and/or folders are specified, then they will be mounted as if the arguments '
               'coming first were updated with the contents of the archives or folders specified thereafter, '
               'i.e., the list of TARs and folders will be union mounted.' )
    parser.add_argument(
        'mount_point', nargs = '?',
        help = 'The path to a folder to mount the TAR contents into. '
               'If no mount path is specified, the TAR will be mounted to a folder of the same name '
               'but without a file extension.' )

    args = parser.parse_args( args )

    args.gzip_seek_point_spacing = args.gzip_seek_point_spacing * 1024 * 1024

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into the tarfilepaths namespace and we have to
    # manually separate them depending on the type.
    if os.path.isdir( args.mount_source[-1] ) or not os.path.exists( args.mount_source[-1] ):
        args.mount_point = args.mount_source[-1]
        args.mount_source = args.mount_source[:-1]
    if not args.mount_source:
        print( "[Error] You must at least specify one path to a valid TAR file or union mount source directory!" )
        exit( 1 )

    # Manually check that all specified TARs and folders exist
    compressions = [ '' ]
    if 'IndexedBzip2File' in globals():
        compressions += [ 'bz2' ]
    if 'IndexedGzipFile' in globals():
        compressions += [ 'gz' ]
    args.mount_source = [ TarFileType( mode = 'r', compressions = compressions, encoding = args.encoding )( tarFile )[0]
                          if not os.path.isdir( tarFile ) else os.path.realpath( tarFile )
                          for tarFile in args.mount_source ]

    # Automatically generate a default mount path
    if args.mount_point is None:
        tarPath = args.mount_source[0]
        for doubleExtension in [ '.tar.bz2', '.tar.gz' ]:
            if tarPath[-len( doubleExtension ):].lower() == doubleExtension.lower():
                args.mount_point = tarPath[:-len( doubleExtension )]
                break
        if not args.mount_point:
            args.mount_point = os.path.splitext( tarPath )[0]
    args.mount_point = os.path.abspath( args.mount_point )

    return args

def cli( args = None ):
    tmpArgs = sys.argv if args is None else args
    if '--version' in tmpArgs or '-v' in tmpArgs:
        print( "ratarmount", __version__ )
        return

    args = parseArgs( args )

    # Convert the comma separated list of key[=value] options into a dictionary for fusepy
    fusekwargs = dict( [ option.split( '=', 1 ) if '=' in option else ( option, True )
                       for option in args.fuse.split( ',' ) ] ) if args.fuse else {}
    if args.prefix:
        fusekwargs['modules'] = 'subdir'
        fusekwargs['subdir'] = args.prefix

    if args.mount_point in args.mount_source:
        fusekwargs['nonempty'] = True

    global printDebug
    printDebug = args.debug

    fuseOperationsObject = TarMount(
        pathToMount                = args.mount_source,
        clearIndexCache            = args.recreate_index,
        recursive                  = args.recursive,
        gzipSeekPointSpacing       = args.gzip_seek_point_spacing,
        mountPoint                 = args.mount_point,
        encoding                   = args.encoding,
        ignoreZeros                = args.ignore_zeros,
        verifyModificationTime     = args.verify_mtime,
        stripRecursiveTarExtension = args.strip_recursive_tar_extension,
    )

    fuse.FUSE( operations = fuseOperationsObject,
               mountpoint = args.mount_point,
               foreground = args.foreground,
               nothreads  = True, # Can't access SQLite database connection object from multiple threads
               **fusekwargs )

if __name__ == '__main__':
    cli( sys.argv[1:] )
