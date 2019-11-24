#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import collections
import io
import itertools
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

import fuse


printDebug = 1

def overrides( parentClass ):
    def overrider( method ):
        assert method.__name__ in dir( parentClass )
        return method
    return overrider


FileInfo = collections.namedtuple( "FileInfo", "offset size mtime mode type linkname uid gid istar" )


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

class SQLiteIndexedTar:
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    __slots__ = (
        'tarFileName',
        'mountRecursively',
        'indexFileName',
        'sqlConnection',
        'parentFolderCache', # stores which parent folders were last tried to add to database and therefore do exist
    )

    def __init__(
        self,
        tarFileName     = None, # either string path or file object
        fileObject      = None, # if not specified, tarFileName will be opened
        writeIndex      = False,
        clearIndexCache = False,
        recursive       = False,
    ):
        self.parentFolderCache = []
        self.mountRecursively = recursive
        self.sqlConnection = None

        assert tarFileName or fileObject
        if not tarFileName:
            self.tarFileName = '<file object>'
            self.createIndex( fileObject )
            # return here because we can't find a save location without any identifying name
            return
        self.tarFileName = os.path.normpath( tarFileName )

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

        if fileObject:
            self.createIndex( fileObject )
        else:
            with open( self.tarFileName, 'rb' ) as file:
                self.createIndex( file )

        if printDebug >= 1 and writeIndex:
            # The 0-time is legacy for the automated tests
            print( "Writing out TAR index to", self.indexFileName, "took 0s",
                   "and is sized", os.stat( self.indexFileName ).st_size, "B" )

    def _openSqlDb( self, filePath ):
        self.sqlConnection = sqlite3.connect( filePath )
        self.sqlConnection.row_factory = sqlite3.Row
        self.sqlConnection.executescript( """
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
        """ )

    def createIndex( self, fileObject, progressBar = None, pathPrefix = '', streamOffset = 0 ):
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName, "..." )
        t0 = timer()

        # 1. If no SQL connection was given (by recursive call), open a new database file
        openedConnection = False
        if not self.sqlConnection:
            if printDebug >= 1:
                print( "Creating new SQLite index database at", self.indexFileName )

            createTables = """
                CREATE TABLE "files" (
                    "path"     VARCHAR(65535) NOT NULL,
                    "name"     VARCHAR(65535) NOT NULL,
                    "offset"   INTEGER,  /* seek offset from TAR file where these file's contents resides */
                    "size"     INTEGER,
                    "mtime"    INTEGER,
                    "mode"     INTEGER,
                    "type"     INTEGER,
                    "linkname" VARCHAR(65535),
                    "uid"      INTEGER,
                    "gid"      INTEGER,
                    /* True for valid TAR files. Internally used to determine where to mount recursive TAR files. */
                    "istar"    BOOL,
                    PRIMARY KEY (path,name) /* see SQL benchmarks for decision on this */
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
        try:
            loadedTarFile = tarfile.open( fileobj = fileObject, mode = 'r|' )
        except tarfile.ReadError as exception:
            print( "Archive can't be opened! This might happen for compressed TAR archives, "
                   "which currently is not supported." )
            raise exception

        if progressBar is None:
            progressBar = ProgressBar( os.fstat( fileObject.fileno() ).st_size )

        # 3. Iterate over files inside TAR and add them to the database
        for tarInfo in loadedTarFile:
            loadedTarFile.members = []
            globalOffset = streamOffset + tarInfo.offset_data
            progressBar.update( globalOffset )

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
            isTar = False
            if self.mountRecursively and tarInfo.isfile() and tarInfo.name.endswith( ".tar" ):
                oldPos = fileObject.tell()
                fileObject.seek( globalOffset )

                oldPrintName = self.tarFileName
                try:
                    self.tarFileName = tarInfo.name.lstrip( '/' ) # This is for output of the recursive call
                    self.createIndex( fileObject, progressBar, fullPath, globalOffset )

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

                fileObject.seek( oldPos )

            path, name = fullPath.rsplit( "/", 1 )
            fileInfo = (
                path               , # 0
                name               , # 1
                globalOffset       , # 2
                tarInfo.size       , # 3
                tarInfo.mtime      , # 4
                mode               , # 5
                tarInfo.type       , # 6
                tarInfo.linkname   , # 7
                tarInfo.uid        , # 8
                tarInfo.gid        , # 9
                isTar              , # 10
            )
            self._setFileInfo( fullPath, fileInfo )

        # 5. Resort by (path,name). This one-time resort is faster than resorting on each INSERT (cache spill)
        if openedConnection:
            if printDebug >= 2:
                print( "Resorting files by path ..." )

            cleanupDatabase = """
                INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","name",rowid;
                DROP TABLE "filestmp";
                INSERT OR IGNORE INTO "files"
                    SELECT path,name,0,1,0,{},{},"",0,0,0
                    FROM "parentfolders" ORDER BY "path","name";
                DROP TABLE "parentfolders";
            """.format( int( 0o555 | stat.S_IFDIR ), int( tarfile.DIRTYPE ) )
            self.sqlConnection.executescript( cleanupDatabase )


        self.sqlConnection.commit()

        t1 = timer()
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName,
                   "took {:.2f}s".format( t1 - t0 ) )

    def getFileInfo( self, fullPath, listDir = False ):
        """
        This is the heart of this class' public interface!

        path    : full path to file where '/' denotes TAR's root, e.g., '/', or '/foo'
        listDir : if True, return a dictionary for the given directory path: { fileName : FileInfo, ... }
                  if False, return simple FileInfo to given path (directory or file)
        if path does not exist, always return None
        """
        # @todo cache last listDir as most ofthen a stat over all entries will soon follow

        # also strips trailing '/' except for a single '/' and leading '/'
        fullPath = '/' + os.path.normpath( fullPath ).lstrip( '/' )
        if listDir:
            rows = self.sqlConnection.execute( 'SELECT * FROM "files" WHERE "path" == (?)',
                                               ( fullPath.rstrip( '/' ), ) )
            dir = {}
            gotResults = False
            for row in rows:
                gotResults = True
                if row['name']:
                    dir[row['name']] = FileInfo( **dict( [ ( key, row[key] ) for key in FileInfo._fields ] ) )

            return dir if gotResults else None

        path, name = fullPath.rsplit( '/', 1 )
        row = self.sqlConnection.execute( 'SELECT * FROM "files" WHERE "path" == (?) AND "name" == (?)',
                                          ( path, name ) ).fetchone()
        return FileInfo( **dict( [ ( key, row[key] ) for key in FileInfo._fields ] ) ) if row else None

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

    def _setFileInfo( self, fullPath, row ):
        assert isinstance( row, tuple )
        self.sqlConnection.execute( 'INSERT OR REPLACE INTO "files" VALUES (?,?,?,?,?,?,?,?,?,?,?)', row )
        self._tryAddParentFolders( row[0] )

    def setFileInfo( self, fullPath, fileInfo ):
        """
        fullPath : the full path to the file with leading slash (/) for which to set the file info
        """
        assert self.sqlConnection
        assert fullPath[0] == "/"
        assert isinstance( fileInfo, FileInfo )

        # os.normpath does not delete duplicate '/' at beginning of string!
        path, name = fullPath.rsplit( "/", 1 )
        row = (
            path,
            name,
            fileInfo.offset,
            fileInfo.size,
            fileInfo.mtime,
            fileInfo.mode,
            fileInfo.type,
            fileInfo.linkname,
            fileInfo.uid,
            fileInfo.gid,
            fileInfo.istar,
        )
        self.sqlConnection.execute( 'INSERT OR REPLACE INTO "files" VALUES (?,?,?,?,?,?,?,?,?,?,?)', row )

    def indexIsLoaded( self ):
        if not self.sqlConnection:
            return False

        try:
            self.sqlConnection.execute( 'SELECT * FROM "files" WHERE 0 == 1;' )
        except Exception:
            return False

        return True

    def loadIndex( self, indexFileName ):
        if self.indexIsLoaded():
            return

        t0 = time.time()
        self._openSqlDb( indexFileName )

        if printDebug >= 1:
            # Legacy output for automated tests
            print( "Loading offset dictionary from", indexFileName, "took {:.2f}s".format( time.time() - t0 ) )

    def _tryLoadIndex( self, indexFileName ):
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile( indexFileName ):
            return False

        if os.path.getsize( indexFileName ) == 0:
            try:
                os.remove( indexFileName )
            except OSError:
                print( "[Warning] Failed to remove empty old cached index file:", indexFileName )

            return False

        try:
            self.loadIndex( indexFileName )
        except Exception:
            traceback.print_exc()
            print( "[Warning] Could not load file '" + indexFileName  )

            print( "[Info] Some likely reasons for not being able to load the index file:" )
            print( "[Info]   - The file has incorrect read permissions" )
            print( "[Info]   - The file got corrupted because of:" )
            print( "[Info]     - The program exited while it was still writing the index because of:" )
            print( "[Info]       - the user sent SIGINT to force the program to quit" )
            print( "[Info]       - an internal error occured while writing the index" )
            print( "[Info]       - the disk filled up while writing the index" )
            print( "[Info]     - Rare lowlevel corruptions caused by hardware failure" )

            print( "[Info] This might force a time-costly index recreation, so if it happens often and "
                   "mounting is slow, try to find out why loading fails repeatedly, "
                   "e.g., by opening an issue on the public github page." )

            try:
                os.remove( indexFileName )
            except OSError:
                print( "[Warning] Failed to remove corrupted old cached index file:", indexFileName )

        if printDebug >= 3 and self.indexIsLoaded():
            print( "Loaded index", indexFileName )
        return self.indexIsLoaded()


class IndexedTar:
    """
    This class reads once through the whole TAR archive and stores TAR file offsets
    for all contained files in an index to support fast seeking to a given file.
    """

    __slots__ = (
        'tarFileName',
        'fileIndex',
        'mountRecursively',
        'cacheFolder',
        'possibleIndexFilePaths',
        'indexFileName',
        'progressBar',
    )

    # these allowed backends also double as extensions for the index file to look for
    availableSerializationBackends = [
        'none',
        'pickle',
        'pickle2',
        'pickle3',
        'custom',
        'cbor',
        'msgpack',
        'rapidjson',
        'ujson',
        'simplejson'
    ]
    availableCompressions = [
        '', # no compression
        'lz4',
        'gz',
    ]

    def __init__( self,
                  pathToTar = None,
                  fileObject = None,
                  writeIndex = False,
                  clearIndexCache = False,
                  recursive = False,
                  serializationBackend = None,
                  progressBar = None ):
        self.progressBar = progressBar
        self.tarFileName = os.path.normpath( pathToTar )

        # Stores the file hierarchy in a dictionary with keys being either
        #  - the file and containing file metainformation
        #  - or keys being a folder name and containing a recursively defined dictionary.
        self.fileIndex = {}
        self.mountRecursively = recursive

        # will be used for storing indexes if current path is read-only
        self.cacheFolder = os.path.expanduser( "~/.ratarmount" )
        self.possibleIndexFilePaths = [
            self.tarFileName + ".index",
            self.cacheFolder + "/" + self.tarFileName.replace( "/", "_" ) + ".index"
        ]

        if not serializationBackend:
            serializationBackend = 'custom'

        if serializationBackend not in self.supportedIndexExtensions():
            print( "[Warning] Serialization backend '" + str( serializationBackend ) + "' not supported.",
                   "Defaulting to '" + serializationBackend + "'!" )
            print( "List of supported extensions / backends:", self.supportedIndexExtensions() )

            serializationBackend = 'custom'

        # this is the actual index file, which will be used in the end, and by default
        self.indexFileName = self.possibleIndexFilePaths[0] + "." + serializationBackend

        if clearIndexCache:
            for indexPath in self.possibleIndexFilePaths:
                for extension in self.supportedIndexExtensions():
                    indexPathWitExt = indexPath + "." + extension
                    if os.path.isfile( indexPathWitExt ):
                        os.remove( indexPathWitExt )

        if fileObject is not None:
            if writeIndex:
                print( "Can't write out index for file object input. Ignoring this option." )
            self.createIndex( fileObject )
        else:
            # first try loading the index for the given serialization backend
            if serializationBackend is not None:
                for indexPath in self.possibleIndexFilePaths:
                    if self.tryLoadIndex( indexPath + "." + serializationBackend ):
                        break

            # try loading the index from one of the pre-configured paths
            for indexPath in self.possibleIndexFilePaths:
                for extension in self.supportedIndexExtensions():
                    if self.tryLoadIndex( indexPath + "." + extension ):
                        break

            if not self.indexIsLoaded():
                with open( self.tarFileName, 'rb' ) as file:
                    self.createIndex( file )

                if writeIndex:
                    for indexPath in self.possibleIndexFilePaths:
                        indexPath += "." + serializationBackend

                        try:
                            folder = os.path.dirname( indexPath )
                            if not os.path.exists( folder ):
                                os.mkdir( folder )

                            f = open( indexPath, 'wb' )
                            f.close()
                            os.remove( indexPath )
                            self.indexFileName = indexPath

                            break
                        except IOError:
                            if printDebug >= 2:
                                print( "Could not create file:", indexPath )

                    try:
                        self.writeIndex( self.indexFileName )
                    except IOError:
                        print( "[Info] Could not write TAR index to file. ",
                               "Subsequent mounts might be slow!" )

    @staticmethod
    def supportedIndexExtensions():
        return [ '.'.join( combination ).strip( '.' )
                 for combination in itertools.product( IndexedTar.availableSerializationBackends,
                                                       IndexedTar.availableCompressions ) ]
    @staticmethod
    def dump( toDump, file ):
        import msgpack

        if isinstance( toDump, dict ):
            file.write( b'\x01' ) # magic code meaning "start dictionary object"

            for key, value in toDump.items():
                file.write( b'\x03' ) # magic code meaning "serialized key value pair"
                IndexedTar.dump( key, file )
                IndexedTar.dump( value, file )

            file.write( b'\x02' ) # magic code meaning "close dictionary object"

        elif isinstance( toDump, FileInfo ):
            serialized = msgpack.dumps( toDump )
            file.write( b'\x05' ) # magic code meaning "msgpack object"
            file.write( len( serialized ).to_bytes( 4, byteorder = 'little' ) )
            file.write( serialized )

        elif isinstance( toDump, str ):
            serialized = toDump.encode()
            file.write( b'\x04' ) # magic code meaning "string object"
            file.write( len( serialized ).to_bytes( 4, byteorder = 'little' ) )
            file.write( serialized )

        else:
            print( "Ignoring unsupported type to write:", toDump )

    @staticmethod
    def load( file ):
        import msgpack

        elementType = file.read( 1 )

        if elementType != b'\x01': # start of dictionary
            raise Exception( 'Custom TAR index loader: invalid file format' )

        result = {}

        dictElementType = file.read( 1 )
        while dictElementType:
            if dictElementType == b'\x02':
                break

            elif dictElementType == b'\x03':
                keyType = file.read( 1 )
                if keyType != b'\x04': # key must be string object
                    raise Exception( 'Custom TAR index loader: invalid file format' )
                size = int.from_bytes( file.read( 4 ), byteorder = 'little' )
                key = file.read( size ).decode()

                valueType = file.read( 1 )
                if valueType == b'\x05': # msgpack object
                    size = int.from_bytes( file.read( 4 ), byteorder = 'little' )
                    serialized = file.read( size )
                    value = FileInfo( *msgpack.loads( serialized ) )

                elif valueType == b'\x01': # dict object
                    file.seek( -1, io.SEEK_CUR )
                    value = IndexedTar.load( file )

                else:
                    raise Exception(
                        'Custom TAR index loader: invalid file format ' +
                        '(expected msgpack or dict but got' +
                        str( int.from_bytes( valueType, byteorder = 'little' ) ) + ')' )

                result[key] = value

            else:
                raise Exception(
                    'Custom TAR index loader: invalid file format ' +
                    '(expected end-of-dict or key-value pair but got' +
                    str( int.from_bytes( dictElementType, byteorder = 'little' ) ) + ')' )

            dictElementType = file.read( 1 )

        return result

    def getFileInfo( self, path, listDir = False ):
        # go down file hierarchy tree along the given path
        p = self.fileIndex
        for name in os.path.normpath( path ).split( os.sep ):
            if not name:
                continue
            if name not in p:
                return None
            p = p[name]

        def repackDeserializedNamedTuple( p ):
            if isinstance( p, list ) and len( p ) == len( FileInfo._fields ):
                return FileInfo( *p )

            if isinstance( p, dict ) and len( p ) == len( FileInfo._fields ) and \
                 'uid' in p and isinstance( p['uid'], int ):
                # a normal directory dict must only have dict or FileInfo values,
                # so if the value to the 'uid' key is an actual int,
                # then it is sure it is a deserialized FileInfo object and not a file named 'uid'
                print( "P ===", p )
                print( "FileInfo ===", FileInfo( **p ) )
                return FileInfo( **p )

            return p

        p = repackDeserializedNamedTuple( p )

        # if the directory contents are not to be printed and it is a directory,
        # return the "file" info of ".", which holds the directory metainformation
        if not listDir and isinstance( p, dict ):
            if '.' in p:
                p = p['.']
            else:
                return FileInfo(
                    offset   = 0, # not necessary for directory anyways
                    size     = 1, # might be misleading / non-conform
                    mtime    = 0,
                    mode     = 0o555 | stat.S_IFDIR,
                    type     = tarfile.DIRTYPE,
                    linkname = "",
                    uid      = 0,
                    gid      = 0,
                    istar    = False
                )

        return repackDeserializedNamedTuple( p )

    def isDir( self, path ):
        return isinstance( self.getFileInfo( path, listDir = True ), dict )

    def exists( self, path ):
        path = os.path.normpath( path )
        return self.isDir( path ) or isinstance( self.getFileInfo( path ), FileInfo )

    def setFileInfo( self, path, fileInfo ):
        """
        path: the full path to the file with leading slash (/) for which to set the file info
        """
        assert isinstance( fileInfo, FileInfo )

        pathHierarchy = os.path.normpath( path ).split( os.sep )
        if not pathHierarchy:
            return

        # go down file hierarchy tree along the given path
        p = self.fileIndex
        for name in pathHierarchy[:-1]:
            if not name:
                continue
            assert isinstance( p, dict )
            p = p.setdefault( name, {} ) # if parent folders of the file to add do not exist, add them

        # create a new key in the dictionary of the parent folder
        p.update( { pathHierarchy[-1] : fileInfo } )

    def setDirInfo( self, path, dirInfo, dirContents = {} ):
        """
        path: the full path to the file with leading slash (/) for which to set the folder info
        """
        assert isinstance( dirInfo, FileInfo )
        assert isinstance( dirContents, dict )

        pathHierarchy = os.path.normpath( path ).strip( os.sep ).split( os.sep )
        if not pathHierarchy:
            return

        # go down file hierarchy tree along the given path
        p = self.fileIndex
        for name in pathHierarchy[:-1]:
            if not name:
                continue
            assert isinstance( p, dict )
            p = p.setdefault( name, {} )

        # create a new key in the dictionary of the parent folder
        p.update( { pathHierarchy[-1] : dirContents } )
        p[pathHierarchy[-1]].update( { '.' : dirInfo } )

    def createIndex( self, fileObject ):
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName, "..." )
        t0 = timer()

        self.fileIndex = {}
        try:
            loadedTarFile = tarfile.open( fileobj = fileObject, mode = 'r:' )
        except tarfile.ReadError as exception:
            print( "Archive can't be opened! This might happen for compressed TAR archives, "
                   "which currently is not supported." )
            raise exception

        if self.progressBar is None and os.path.isfile( self.tarFileName ):
            self.progressBar = ProgressBar( os.stat( self.tarFileName ).st_size )

        for tarInfo in loadedTarFile:
            loadedTarFile.members = []
            if self.progressBar is not None:
                self.progressBar.update( tarInfo.offset_data )

            mode = tarInfo.mode
            if tarInfo.isdir() : mode |= stat.S_IFDIR
            if tarInfo.isfile(): mode |= stat.S_IFREG
            if tarInfo.issym() : mode |= stat.S_IFLNK
            if tarInfo.ischr() : mode |= stat.S_IFCHR
            if tarInfo.isfifo(): mode |= stat.S_IFIFO
            fileInfo = FileInfo(
                offset   = tarInfo.offset_data,
                size     = tarInfo.size       ,
                mtime    = tarInfo.mtime      ,
                mode     = mode               ,
                type     = tarInfo.type       ,
                linkname = tarInfo.linkname   ,
                uid      = tarInfo.uid        ,
                gid      = tarInfo.gid        ,
                istar    = False
            )

            # open contained tars for recursive mounting
            indexedTar = None
            if self.mountRecursively and tarInfo.isfile() and tarInfo.name.endswith( ".tar" ):
                oldPos = fileObject.tell()
                if oldPos != tarInfo.offset_data:
                    fileObject.seek( tarInfo.offset_data )
                indexedTar = IndexedTar( tarInfo.name,
                                         fileObject = fileObject,
                                         writeIndex = False,
                                         progressBar = self.progressBar )
                # might be especially necessary if the .tar is not actually a tar!
                fileObject.seek( fileObject.tell() )

            # Add a leading '/' as a convention where '/' represents the TAR root folder
            # Partly, done because fusepy specifies paths in a mounted directory like this
            path = os.path.normpath( "/" + tarInfo.name )

            # test whether the TAR file could be loaded and if so "mount" it recursively
            if indexedTar is not None and indexedTar.indexIsLoaded():
                # actually apply the recursive tar mounting
                mountMode = ( fileInfo.mode & 0o777 ) | stat.S_IFDIR
                if mountMode & stat.S_IRUSR != 0: mountMode |= stat.S_IXUSR
                if mountMode & stat.S_IRGRP != 0: mountMode |= stat.S_IXGRP
                if mountMode & stat.S_IROTH != 0: mountMode |= stat.S_IXOTH
                fileInfo = fileInfo._replace( mode = mountMode, istar = True )

                if self.exists( path ):
                    print( "[Warning]", path, "already exists in database and will be overwritten!" )

                # merge fileIndex from recursively loaded TAR into our Indexes
                self.setDirInfo( path, fileInfo, indexedTar.fileIndex )

            elif path != '/':
                # just a warning and check for the path already existing
                if self.exists( path ):
                    fileInfo = self.getFileInfo( path, listDir = False )
                    print( "[Warning]", path, "already exists in database and will be overwritten!" )

                # simply store the file or directory information from current TAR item
                if tarInfo.isdir():
                    self.setDirInfo( path, fileInfo, {} )
                else:
                    self.setFileInfo( path, fileInfo )

        t1 = timer()
        if printDebug >= 1:
            print( "Creating offset dictionary for",
                   "<file object>" if self.tarFileName is None else self.tarFileName,
                   "took {:.2f}s".format( t1 - t0 ) )

    def serializationBackendFromFileName( self, fileName ):
        splitName = fileName.split( '.' )

        if len( splitName ) > 2 and '.'.join( splitName[-2:] ) in self.supportedIndexExtensions():
            return '.'.join( splitName[-2:] )

        if splitName[-1] in self.supportedIndexExtensions():
            return splitName[-1]

        return None

    def indexIsLoaded( self ):
        return bool( self.fileIndex )

    def writeIndex( self, outFileName ):
        """
        outFileName: Full file name with backend extension.
                     Depending on the extension the serialization is chosen.
        """

        serializationBackend = self.serializationBackendFromFileName( outFileName )

        if printDebug >= 1:
            print( "Writing out TAR index using", serializationBackend, "to", outFileName, "..." )
        t0 = timer()

        fileMode = 'wt' if 'json' in serializationBackend else 'wb'

        if serializationBackend.endswith( '.lz4' ):
            import lz4.frame
            wrapperOpen = lambda x : lz4.frame.open( x, fileMode )
        elif serializationBackend.endswith( '.gz' ):
            import gzip
            wrapperOpen = lambda x : gzip.open( x, fileMode )
        else:
            wrapperOpen = lambda x : open( x, fileMode )
        serializationBackend = serializationBackend.split( '.' )[0]

        # libraries tested but not working:
        #  - marshal: can't serialize namedtuples
        #  - hickle: for some reason, creates files almost 64x larger and slower than pickle!?
        #  - yaml: almost a 10 times slower and more memory usage and deserializes everything including ints to string

        if serializationBackend == 'none':
            print( "Won't write out index file because backend 'none' was chosen. "
                   "Subsequent mounts might be slow!" )
            return

        with wrapperOpen( outFileName ) as outFile:
            if serializationBackend == 'pickle2':
                import pickle
                pickle.dump( self.fileIndex, outFile )
                pickle.dump( self.fileIndex, outFile, protocol = 2 )

            # default serialization because it has the fewest dependencies and because it was legacy default
            elif serializationBackend == 'pickle3' or \
                 serializationBackend == 'pickle' or \
                 serializationBackend is None:
                import pickle
                pickle.dump( self.fileIndex, outFile )
                pickle.dump( self.fileIndex, outFile, protocol = 3 ) # 3 is default protocol

            elif serializationBackend == 'simplejson':
                import simplejson
                simplejson.dump( self.fileIndex, outFile, namedtuple_as_object = True )

            elif serializationBackend == 'custom':
                IndexedTar.dump( self.fileIndex, outFile )

            elif serializationBackend in [ 'msgpack', 'cbor', 'rapidjson', 'ujson' ]:
                import importlib
                module = importlib.import_module( serializationBackend )
                getattr( module, 'dump' )( self.fileIndex, outFile )

            else:
                print( "Tried to save index with unsupported extension backend:", serializationBackend, "!" )

        t1 = timer()
        if printDebug >= 1:
            print( "Writing out TAR index to", outFileName, "took {:.2f}s".format( t1 - t0 ),
                   "and is sized", os.stat( outFileName ).st_size, "B" )

    def loadIndex( self, indexFileName ):
        if printDebug >= 1:
            print( "Loading offset dictionary from", indexFileName, "..." )
        t0 = timer()

        serializationBackend = self.serializationBackendFromFileName( indexFileName )

        fileMode = 'rt' if 'json' in serializationBackend else 'rb'

        if serializationBackend.endswith( '.lz4' ):
            import lz4.frame
            wrapperOpen = lambda x : lz4.frame.open( x, fileMode )
        elif serializationBackend.endswith( '.gz' ):
            import gzip
            wrapperOpen = lambda x : gzip.open( x, fileMode )
        else:
            wrapperOpen = lambda x : open( x, fileMode )
        serializationBackend = serializationBackend.split( '.' )[0]

        with wrapperOpen( indexFileName ) as indexFile:
            if serializationBackend in ( 'pickle2', 'pickle3', 'pickle' ):
                import pickle
                self.fileIndex = pickle.load( indexFile )

            elif serializationBackend == 'custom':
                self.fileIndex = IndexedTar.load( indexFile )

            elif serializationBackend == 'msgpack':
                import msgpack
                self.fileIndex = msgpack.load( indexFile, raw = False )

            elif serializationBackend == 'simplejson':
                import simplejson
                self.fileIndex = simplejson.load( indexFile, namedtuple_as_object = True )

            elif serializationBackend in [ 'cbor', 'rapidjson', 'ujson' ]:
                import importlib
                module = importlib.import_module( serializationBackend )
                self.fileIndex = getattr( module, 'load' )( indexFile )

            else:
                print( "Tried to load index path with unsupported serializationBackend:", serializationBackend, "!" )
                return

        if printDebug >= 2:
            def countDictEntries( d ):
                n = 0
                for value in d.values():
                    n += countDictEntries( value ) if isinstance( value, dict ) else 1
                return n
            print( "Files:", countDictEntries( self.fileIndex ) )

        t1 = timer()
        if printDebug >= 1:
            print( "Loading offset dictionary from", indexFileName, "took {:.2f}s".format( t1 - t0 ) )

    def tryLoadIndex( self, indexFileName ):
        """calls loadIndex if index is not loaded already and provides extensive error handling"""

        if self.indexIsLoaded():
            return True

        if not os.path.isfile( indexFileName ):
            return False

        if os.path.getsize( indexFileName ) == 0:
            try:
                os.remove( indexFileName )
            except OSError:
                print( "[Warning] Failed to remove empty old cached index file:", indexFileName )

            return False

        try:
            self.loadIndex( indexFileName )
        except Exception:
            self.fileIndex = None

            traceback.print_exc()
            print( "[Warning] Could not load file '" + indexFileName  )

            print( "[Info] Some likely reasons for not being able to load the index file:" )
            print( "[Info]   - Some dependencies are missing. Please isntall them with:" )
            print( "[Info]       pip3 --user -r requirements.txt" )
            print( "[Info]   - The file has incorrect read permissions" )
            print( "[Info]   - The file got corrupted because of:" )
            print( "[Info]     - The program exited while it was still writing the index because of:" )
            print( "[Info]       - the user sent SIGINT to force the program to quit" )
            print( "[Info]       - an internal error occured while writing the index" )
            print( "[Info]       - the disk filled up while writing the index" )
            print( "[Info]     - Rare lowlevel corruptions caused by hardware failure" )

            print( "[Info] This might force a time-costly index recreation, so if it happens often and "
                   "mounting is slow, try to find out why loading fails repeatedly, "
                   "e.g., by opening an issue on the public github page." )

            try:
                os.remove( indexFileName )
            except OSError:
                print( "[Warning] Failed to remove corrupted old cached index file:", indexFileName )

        return self.indexIsLoaded()


class TarMount( fuse.Operations ):
    """
    This class implements the fusepy interface in order to create a mounted file system view
    to a TAR archive.
    This class can and is relatively thin as it only has to create and manage an IndexedTar
    object and query it for directory or file contents.
    It also adds a layer over the file permissions as all files must be read-only even
    if the TAR reader reports the file as originally writable because no TAR write support
    is planned.
    """

    def __init__(
        self,
        pathToMount,
        clearIndexCache = False,
        recursive = False,
        serializationBackend = None,
        prefix = ''
    ):
        """
        prefix : Instead of mounting the TAR's root and showing all files a prefix can be specified.
                 For example '/bar' will only show all TAR files which are inside the '/bar' folder inside the TAR.
        """

        self.tarFile = open( pathToMount, 'rb' )

        # check for bzip2 compressed tar archive and add BZip2 reader if so
        magicBytes = self.tarFile.read( 10 )
        self.tarFile.seek( 0 ) # For some reason does not get propagated to underlying file descriptor and BZ2Reader?!
        self.tarFile.flush()
        type = None
        if magicBytes[0:3] == b"BZh" and magicBytes[4:10] == b"1AY&SY":
            from bzip2 import SeekableBzip2
            type = 'BZ2'
            self.rawFile = self.tarFile # save so that garbage collector won't close it!
            self.tarFile = SeekableBzip2( self.rawFile.fileno() )

        elif magicBytes[0:2] == b"\x1f\x8b":
            from indexed_gzip import IndexedGzipFile
            type = 'GZ'
            self.rawFile = self.tarFile # save so that garbage collector won't close it!
            self.tarFile = IndexedGzipFile( fileobj = self.rawFile )

        if type and serializationBackend != 'sqlite':
            print( "[Warning] Only the SQLite backend has .tar.bz2 and .tar.gz support, therefore will use that!" )
            serializationBackend = 'sqlite'

        if serializationBackend == 'sqlite':
            self.indexedTar = SQLiteIndexedTar(
                pathToMount,
                self.tarFile,
                writeIndex      = True,
                clearIndexCache = clearIndexCache,
                recursive       = recursive )

            # The index creation iterates over the whole file, so now we can query the block offsets gathered during
            if type == 'BZ2':
                db = self.indexedTar.sqlConnection
                try:
                    offsets = dict( db.execute( 'SELECT blockoffset,dataoffset FROM bzip2blocks' ) )
                    self.tarFile.setBlockOffsets( offsets )
                except Exception as e:
                    if printDebug >= 2:
                        print( "Could not load BZip2 Block offset data. Will create it from scratch." )

                    tables = [ x[0] for x in db.execute( 'SELECT name FROM sqlite_master WHERE type="table"' ) ]
                    if 'bzip2blocks' in tables:
                        db.execute( 'DROP TABLE bzip2blocks' )
                    db.execute( 'CREATE TABLE bzip2blocks ( blockoffset INTEGER PRIMARY KEY, dataoffset INTEGER )' )
                    db.executemany( 'INSERT INTO bzip2blocks VALUES (?,?)',
                                    self.tarFile.blockOffsets().items() )
                    db.commit()

            elif type == 'GZ':
                db = self.indexedTar.sqlConnection
                gzindex = tempfile.mkstemp()[1]
                try:
                    with open( gzindex, 'wb' ) as file:
                        file.write( db.execute( 'SELECT data FROM gzipindex' ).fetchone()[0] )
                    self.tarFile.import_index( filename = gzindex )
                except Exception as e:
                    if printDebug >= 2:
                        print( "Could not load GZip Block offset data. Will create it from scratch." )

                    # Transparently force index to be built if not already done so. build_full_index was buggy for me.
                    # Seeking from end not supported, so we have to read the whole data in in a loop
                    while self.tarFile.read( 1024*1024 ):
                        pass
                    self.tarFile.export_index( filename = gzindex )
                    if printDebug >= 2:
                        print( "Exported GZip index size:", os.stat( gzindex ).st_size )

                    tables = [ x[0] for x in db.execute( 'SELECT name FROM sqlite_master WHERE type="table"' ) ]
                    if 'gzipindex' in tables:
                        db.execute( 'DROP TABLE gzipindex' )
                    db.execute( 'CREATE TABLE gzipindex ( data BLOB )' )
                    with open( gzindex, 'rb' ) as file:
                        db.execute( 'INSERT INTO gzipindex VALUES (?)', ( file.read(), ) )
                    db.commit()
                os.remove( gzindex )

        else:
            self.indexedTar = IndexedTar(
                pathToMount,
                writeIndex           = True,
                clearIndexCache      = clearIndexCache,
                recursive            = recursive,
                serializationBackend = serializationBackend )

        if prefix:
            if not prefix.startswith( '/' ):
                prefix = '/' + prefix
            if not self.indexedTar.isDir( prefix ):
                prefix = ''
        self.prefix = prefix

        # make the mount point read only and executable if readable, i.e., allow directory listing
        # @todo In some cases, I even 2(!) '.' directories listed with ls -la!
        #       But without this, the mount directory is owned by root
        tarStats = os.stat( pathToMount )
        # clear higher bits like S_IFREG and set the directory bit instead
        mountMode = ( tarStats.st_mode & 0o777 ) | stat.S_IFDIR
        if mountMode & stat.S_IRUSR != 0: mountMode |= stat.S_IXUSR
        if mountMode & stat.S_IRGRP != 0: mountMode |= stat.S_IXGRP
        if mountMode & stat.S_IROTH != 0: mountMode |= stat.S_IXOTH
        rootFileInfo = FileInfo(
            offset   = 0                ,
            size     = tarStats.st_size ,
            mtime    = tarStats.st_mtime,
            mode     = mountMode        ,
            type     = tarfile.DIRTYPE  ,
            linkname = ""               ,
            uid      = tarStats.st_uid  ,
            gid      = tarStats.st_gid  ,
            istar    = True
        )

        if serializationBackend == 'sqlite':
            self.indexedTar.setFileInfo( '/' if not self.prefix else self.prefix, rootFileInfo )
        else:
            self.indexedTar.fileIndex[ self.prefix + '.' ] = rootFileInfo

            if printDebug >= 3:
                print( "Loaded File Index:" )
                pprint.pprint( self.indexedTar.fileIndex )

    @overrides( fuse.Operations )
    def getattr( self, path, fh = None ):
        if printDebug >= 2:
            print( "[getattr( path =", path, ", fh =", fh, ")] Enter" )

        fileInfo = self.indexedTar.getFileInfo( self.prefix + path, listDir = False )
        if not isinstance( fileInfo, FileInfo ):
            if printDebug >= 2:
                print( "Could not find path:", self.prefix + path )
            raise fuse.FuseOSError( fuse.errno.EROFS )

        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = dict( ( "st_" + key, getattr( fileInfo, key ) ) for key in ( 'size', 'mtime', 'mode', 'uid', 'gid' ) )
        # signal that everything was mounted read-only
        statDict['st_mode'] &= ~( stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH )
        statDict['st_mtime'] = int( statDict['st_mtime'] )
        statDict['st_nlink'] = 2

        if printDebug >= 2:
            print( "[getattr( path =", self.prefix + path, ", fh =", fh, ")] return:", statDict )

        return statDict

    @overrides( fuse.Operations )
    def readdir( self, path, fh ):
        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        yield '.'
        yield '..'

        dirInfo = self.indexedTar.getFileInfo( self.prefix + path, listDir = True )
        if printDebug >= 2:
            print( "[readdir( path =", self.prefix + path, ", fh =", fh, ")] return:",
                   dirInfo.keys() if dirInfo else None )

        if isinstance( dirInfo, dict ):
            for key in dirInfo.keys():
                yield key
        elif printDebug >= 2:
            print( "[readdir] Could not find path:", self.prefix + path )

    @overrides( fuse.Operations )
    def readlink( self, path ):
        if printDebug >= 2:
            print( "[readlink( path =", path, ")]" )

        fileInfo = self.indexedTar.getFileInfo( self.prefix + path )
        if not isinstance( fileInfo, FileInfo ):
            raise fuse.FuseOSError( fuse.errno.EROFS )

        pathname = fileInfo.linkname
        if pathname.startswith( "/" ):
            return os.path.relpath( pathname, "/" ) # @todo Not exactly sure what to return here

        return pathname

    @overrides( fuse.Operations )
    def read( self, path, length, offset, fh ):
        if printDebug >= 2:
            print( "[read( path =", path, ", length =", length, ", offset =", offset, ",fh =", fh, ")] path:", path )

        fileInfo = self.indexedTar.getFileInfo( self.prefix + path )
        if not isinstance( fileInfo, FileInfo ):
            raise fuse.FuseOSError( fuse.errno.EROFS )

        self.tarFile.seek( fileInfo.offset + offset, os.SEEK_SET )
        return self.tarFile.read( length )

def parseArgs( args = None ):
    parser = argparse.ArgumentParser(
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
        description = '''\
        If no mount path is specified, then the tar will be mounted to a folder of the same name but without a file extension.
        TAR files contained inside the tar and even TARs in TARs in TARs will be mounted recursively at folders of the same name barred the file extension '.tar'.

        In order to reduce the mounting time, the created index for random access to files inside the tar will be saved to <path to tar>.index.<backend>[.<compression]. If it can't be saved there, it will be saved in ~/.ratarmount/<path to tar: '/' -> '_'>.index.<backend>[.<compression].
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

    parser.add_argument(
        '-s', '--serialization-backend', type = str, default = 'sqlite',
        help =
        'Specify which library to use for writing out the TAR index. Supported keywords: (' +
        ','.join( IndexedTar.availableSerializationBackends + [ 'sqlite' ] ) + ')[.(' +
        ','.join( IndexedTar.availableCompressions ).strip( ',' ) + ')]' )

    parser.add_argument(
        '-p', '--prefix', type = str, default = '',
        help = 'The specified path to the folder inside the TAR will be mounted to root. '
               'This can be useful when the archive as created with absolute paths. '
               'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
               '-p /var/log/apt/ can be specified so that the mount target directory '
               '>directly< contains history.log.' )

    parser.add_argument(
        '--fuse', type = str, default = '',
        help = 'Comma separated FUSE options. See "man mount.fuse" for help. '
               'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ' )

    parser.add_argument(
        '-v', '--version', action='store_true', help = 'Print version string.' )

    parser.add_argument(
        'tarfilepath', metavar = 'tar-file-path',
        type = argparse.FileType( 'r' ), nargs = 1,
        help = 'The path to the TAR archive to be mounted.' )
    parser.add_argument(
        'mountpath', metavar = 'mount-path', nargs = '?',
        help = 'The path to a folder to mount the TAR contents into.' )

    return parser.parse_args( args )

def cli( args = None ):
    tmpArgs = sys.argv if args is None else args
    if '--version' in tmpArgs or '-v' in tmpArgs:
        print( "ratarmount 0.3.2" )
        return

    args = parseArgs( args )

    tarToMount = os.path.abspath( args.tarfilepath[0].name )

    type = None

    try:
        tarfile.open( tarToMount, mode = 'r:' )
        type = 'TAR'
    except tarfile.ReadError:
        None

    with open( tarToMount, 'rb' ) as file:
        magicBytes = file.peek( 10 )
        if magicBytes[0:3] == b"BZh" and magicBytes[4:10] == b"1AY&SY":
            type = 'BZ2'
        elif magicBytes[:2] == b'\x1f\x8b':
            type = 'GZ'

    if not type:
        print( "Archive", tarToMount, "can't be opened!",
               "This might happen for compressed TAR archives, which currently is not supported." )
        return 1

    fusekwargs = dict( [ option.split( '=', 1 ) if '=' in option else ( option, True )
                       for option in args.fuse.split( ',' ) ] ) if args.fuse else {}

    mountPath = args.mountpath
    if mountPath is None:
        for ext in [ '.tar', '.tar.bz2', '.tbz2', '.tar.gz', '.tgz' ]:
            if tarToMount[-len( ext ):].lower() == ext.lower():
                mountPath = tarToMount[:-len( ext )]
                break
        if not mountPath:
            mountPath = os.path.splitext( tarToMount )[0]

    mountPathWasCreated = False
    if not os.path.exists( mountPath ):
        os.mkdir( mountPath )

    global printDebug
    printDebug = args.debug

    fuseOperationsObject = TarMount(
        pathToMount          = tarToMount,
        clearIndexCache      = args.recreate_index,
        recursive            = args.recursive,
        serializationBackend = args.serialization_backend,
        prefix               = args.prefix )

    fuse.FUSE( operations = fuseOperationsObject,
               mountpoint = mountPath,
               foreground = args.foreground,
               nothreads  = args.serialization_backend == 'sqlite',
               **fusekwargs )

    if mountPathWasCreated and args.foreground:
        os.rmdir( mountPath )

    return 0

if __name__ == '__main__':
    cli( sys.argv )
