#!/usr/bin/env python3

import os, re, sys, stat, tarfile, pickle, fuse
from collections import namedtuple
from timeit import default_timer as timer


printDebug = 1

def overrides( parentClass ):
    def overrider( method ):
        assert( method.__name__ in dir( parentClass ) )
        return method
    return overrider


FileInfo = namedtuple( "FileInfo", "offset size mtime mode type linkname uid gid istar" )


class IndexedTar:
    """
    This class reads once through a whole TAR archive and stores TAR file offsets for all packed files
    in an index to support fast seeking to a given file.
    """

    def __init__( self, pathToTar = None, fileObject = None, writeIndex = False ):
        self.tarFileName = os.path.normpath( pathToTar )
        self.fileIndex = {}
        self.dirIndex = {}

        if fileObject is not None:
            if writeIndex:
                print( "Can't write out index for file object input. Ignoring this option." )
            self.createIndex( fileObject )
        else:
            self.indexFileName = self.tarFileName + ".index.pickle"
            self.cacheFolder = os.path.expanduser( "~/.ratarmount" )
            self.altIndexFileName = self.cacheFolder + "/" + self.tarFileName.replace( "/", "_" ) + ".index.pickle"

            for indexPath in [ self.indexFileName, self.altIndexFileName ]:
                if not self.dirIndex and not self.fileIndex and os.path.isfile( indexPath ):
                    if os.path.getsize( indexPath ) > 0:
                        self.loadIndex( indexPath )
                    else:
                        os.remove( indexPath )

            if not self.dirIndex and not self.fileIndex:
                with open( self.tarFileName, 'rb' ) as file:
                    self.createIndex( file )

                if writeIndex:
                    try:
                        try:
                            f = open( self.indexFileName, 'wb' )
                            f.close()
                        except IOError:
                            if not os.path.exists( self.cacheFolder ):
                                os.mkdir( self.cacheFolder )
                            self.indexFileName = self.altIndexFileName

                        self.writeIndex( self.indexFileName )
                    except IOError:
                        print( "[Info] Could not write TAR index to file. Subsequent mounts might be slow!" )

    def isDir( self, path ):
        return os.path.normpath( path ) in self.dirIndex

    def getDirInfo( self, path ):
        path = os.path.normpath( path )
        if path in self.dirIndex:
            return self.dirIndex[path]
        return None

    def getFileInfo( self, path, listDir = False ):
        path = os.path.normpath( path )
        if not listDir and path in self.dirIndex:
            return self.dirIndex[path]

        p = self.fileIndex
        for name in path.split( '/' ):
            if not name:
                continue

            if not name in p:
                return

            p = p[name]
        return p

    def exists( self, path ):
        path = os.path.normpath( path )
        return self.isDir( path ) or isinstance( self.getFileInfo( path ), FileInfo )

    @staticmethod
    def setFileInfo( fileIndex, path, fileInfo ):
        path = os.path.normpath( path )
        p = fileIndex
        for name in path.split( '/' )[:-1]:
            if not name:
                continue

            if not name in p:
                p = p.setdefault( name, {} )
            else:
                p = p[name]

        fileName = path.split( '/' )[-1]
        p.update( { fileName : fileInfo } )


    def createIndex( self, fileObject ):
        if printDebug >= 1:
            print( "Creating offset dictionary for", "<file object>" if self.tarFileName is None else self.tarFileName, "..." )
        t0 = timer()

        self.dirIndex = {}
        self.fileIndex = {}
        loadedTarFile = tarfile.open( fileobj = fileObject )
        for tarInfo in loadedTarFile:
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
            if tarInfo.isfile() and tarInfo.name.endswith( ".tar" ):
                oldPos = fileObject.tell()
                if oldPos != tarInfo.offset_data:
                    fileObject.seek( tarInfo.offset_data )
                indexedTar = IndexedTar( tarInfo.name, fileObject = fileObject, writeIndex = False )
                fileObject.seek( fileObject.tell() ) # might be especially necessary if the .tar is not actually a tar!

            # Add a leading '/' as a convention where '/' represents the TAR root folder
            # Partly, done because fusepy specifies paths in a mounted directory like this
            path = os.path.normpath( "/" + tarInfo.name )

            if indexedTar is not None and ( indexedTar.dirIndex or indexedTar.fileIndex ):
                # actually apply the recursive tar mounting
                extractedName = re.sub( r"\.tar$", "", path )
                if not self.exists( extractedName ):
                    path = extractedName

                mountMode = ( fileInfo.mode & 0o777 ) | stat.S_IFDIR
                if mountMode & stat.S_IRUSR != 0: mountMode |= stat.S_IXUSR
                if mountMode & stat.S_IRGRP != 0: mountMode |= stat.S_IXGRP
                if mountMode & stat.S_IROTH != 0: mountMode |= stat.S_IXOTH
                fileInfo = fileInfo._replace( mode = mountMode, istar = True )

                if self.exists( path ):
                    print( "[Warning]", path, "already exists in database and will be overwritten!" )

                for dir, info in indexedTar.dirIndex.items():
                    self.dirIndex[os.path.normpath( path + dir )] = info
                self.dirIndex[path] = fileInfo
                self.setFileInfo( self.fileIndex, path, indexedTar.fileIndex )
            else:
                if self.exists( path ):
                    fileInfo = self.getFileInfo( path )
                    if fileInfo.istar:
                        self.setFileInfo( self.fileIndex, path + ".tar", fileInfo )
                        # no need to delete old entry. Will be overwritten anyway
                        self.dirIndex[path + ".tar"] = self.dirIndex.pop( path )
                    else:
                        print( "[Warning]", path, "already exists in database and will be overwritten!" )

                if tarInfo.isdir():
                    self.dirIndex[path] = fileInfo
                    self.setFileInfo( self.fileIndex, path, {} )
                else:
                    self.setFileInfo( self.fileIndex, path, fileInfo )

        # add parent folders if they were left out in the tar info
        dirsToAdd = {}
        for path, info in self.dirIndex.items():
            while '/' in path:
                path = re.sub( r"/+[^/]*$", "", path )
                if path not in dirsToAdd and path not in self.dirIndex:
                    dirsToAdd[path] = FileInfo( info.offset, 0, info.mtime, 0o555 | stat.S_IFDIR, info.type,
                                                "", info.uid, info.gid, False )
        self.dirIndex.update( dirsToAdd )

        t1 = timer()
        if printDebug >= 1:
            print( "Creating offset dictionary for", "<file object>" if self.tarFileName is None else self.tarFileName, "took {:.2f}s".format( t1 - t0 ) )

    def writeIndex( self, outFileName ):
        with open( outFileName, 'wb' ) as outFile:
            pickle.dump( ( self.dirIndex, self.fileIndex ), outFile )

    def loadIndex( self, indexFileName ):
        if printDebug >= 1:
            print( "Loading offset dictionary from", indexFileName, "..." )
        t0 = timer()

        with open( indexFileName, 'rb' ) as indexFile:
            self.dirIndex, self.fileIndex = pickle.load( indexFile )

        if printDebug >= 2:
            def countDictEntries( d ):
                n = 0
                for key, value in d.items():
                    n += countDictEntries( value ) if type( value ) is dict else 1
                return n
            print( "Files:", countDictEntries( self.fileIndex ), "Dirs:", len( self.dirIndex ) )

        # add parent folders if they were left out in the tar info
        dirsToAdd = {}
        for path, info in self.dirIndex.items():
            while '/' in path:
                path = re.sub( r"/+[^/]*$", "", path )
                if path not in dirsToAdd and path not in self.dirIndex:
                    dirsToAdd[path] = FileInfo( info.offset, 0, info.mtime, 0o555 | stat.S_IFDIR, info.type,
                                                "", info.uid, info.gid, False )
        self.dirIndex.update( dirsToAdd )

        t1 = timer()
        if printDebug >= 1:
            print( "Loading offset dictionary from", indexFileName, "took {:.2f}s".format( t1 - t0 ) )


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

    def __init__( self, pathToMount ):
        self.tarFileName = pathToMount
        self.tarFile = open( self.tarFileName, 'rb' )
        self.indexedTar = IndexedTar( self.tarFileName, writeIndex = True )

        # make the mount point read only and executable if readable, i.e., allow directory listing
        tarStats = os.stat( self.tarFileName )
        # clear higher bits like S_IFREG and set the directory bit instead
        mountMode = ( tarStats.st_mode & 0o777 ) | stat.S_IFDIR
        if mountMode & stat.S_IRUSR != 0: mountMode |= stat.S_IXUSR
        if mountMode & stat.S_IRGRP != 0: mountMode |= stat.S_IXGRP
        if mountMode & stat.S_IROTH != 0: mountMode |= stat.S_IXOTH
        self.indexedTar.dirIndex[ "/" ] = fileInfo = FileInfo(
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

        if printDebug >= 3:
            print( "Loaded Dir Index:", self.indexedTar.dirIndex )
            print( "Loaded File Index:", self.indexedTar.fileIndex )

    @overrides( fuse.Operations )
    def getattr( self, path, fh = None ):
        if printDebug >= 2:
            print( "[getattr( path =", path, ", fh =", fh, ")]" )


        fileInfo = self.indexedTar.getFileInfo( path )
        if not isinstance( fileInfo, FileInfo ):
            if printDebug >= 2:
                print( "Could not find path:", path )
            raise fuse.FuseOSError( fuse.errno.EROFS )

        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = dict( ( "st_" + key, getattr( fileInfo, key ) ) for key in ( 'size', 'mtime', 'mode', 'uid', 'gid' ) )
        # signal that everything was mounted read-only
        statDict['st_mode'] &= ~( stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH )
        statDict['st_mtime'] = int( statDict['st_mtime'] )
        statDict['st_nlink'] = 2

        return statDict

    @overrides( fuse.Operations )
    def readdir( self, path, fh ):
        if printDebug >= 2:
            print( "[readdir( path =", path, ", fh =", fh, ")] getFileInfo.keys:",
                   self.indexedTar.getFileInfo( path, listDir = True ).keys() )

        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        yield '.'
        yield '..'

        for key in self.indexedTar.getFileInfo( path, listDir = True ).keys():
            yield key

    @overrides( fuse.Operations )
    def readlink( self, path ):
        fileInfo = self.indexedTar.getFileInfo( path )
        if not isinstance( fileInfo, FileInfo ):
            raise fuse.FuseOSError( fuse.errno.EROFS )

        pathname = fileInfo.linkname
        if pathname.startswith( "/" ):
            return os.path.relpath( pathname, self.root )
        else:
            return pathname

    @overrides( fuse.Operations )
    def read( self, path, length, offset, fh ):
        if printDebug >= 2:
            print( "[read( path =", path, ", length =", length, ", offset =", offset, ",fh =", fh, ")] path:", path )

        fileInfo = self.indexedTar.getFileInfo( path )
        if not isinstance( fileInfo, FileInfo ):
            raise fuse.FuseOSError( fuse.errno.EROFS )

        self.tarFile.seek( fileInfo.offset + offset, os.SEEK_SET )
        return self.tarFile.read( length )


if __name__ == '__main__':
    if len( sys.argv ) < 2 or "--help" in sys.argv or "-h" in sys.argv:
        print( """Usage:
    ratarmount <path to tar> [<mount path>]

If no mount path is specified, then the tar will be mounted to a folder of the same name but without a file extension.
TAR files contained inside the tar and even TARs in TARs in TARs will be mounted recursively at folders of the same name barred the file extension '.tar'.

In order to reduce the mounting time, the created index for random access to files inside the tar will be saved to <path to tar>.index.pickle. If it can't be saved there, it will be saved in ~/.ratarmount/<path to tar: '/' -> '_'>.index.pickle.
""" )

    tarToMount = sys.argv[1]
    mountPath = sys.argv[2] if len( sys.argv ) >= 3 else os.path.splitext( tarToMount )[0]
    mountPathWasCreated = False
    if not os.path.exists( mountPath ):
        os.mkdir( mountPath )

    foreground = False
    fuse.FUSE( operations = TarMount( pathToMount = tarToMount ), mountpoint = mountPath, foreground = foreground )
    if mountPathWasCreated and foreground:
        os.rmdir( mountPath )
