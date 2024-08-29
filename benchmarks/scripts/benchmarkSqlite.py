#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sqlite3
import sys
import time
import tempfile

import numpy as np
import matplotlib.pyplot as plt


fileNameLength = 256
schemas = [ 'CREATE TABLE "files" ( "path" VARCHAR(65535) PRIMARY KEY, "hash" INTEGER );',
            'CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" INTEGER PRIMARY KEY );',
            'CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" INTEGER, PRIMARY KEY (path,hash) );',
            'CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" VARCHAR(65535), PRIMARY KEY (path,hash) );' ]
labels = [ 'varchar primary key', 'integer primary key', 'varchar,integer primary key', 'varchar,varchar primary key' ]

def benchmarkCacheSizes( nFiles = 1000 * 1000 ):
    rowsPerInsert = 1000
    assert nFiles % rowsPerInsert == 0
    fname = f"sqlite cache size benchmark {nFiles // 1000}k files"

    cacheSizes = [ 2, 4, 16, 32, 64, 128, 256, 512 ]
    insertionTimes = []
    for cacheSize in cacheSizes:
        databaseFile = tempfile.mkstemp()[1]
        db = sqlite3.connect( databaseFile ) #'1m-names-test.sqlite3' )
        db.executescript( f"""
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
            PRAGMA CACHE_SIZE = -{cacheSize * 1000};
        """ )
        # use this schema as it represents the (path,name) primary key currently used in ratarmount
        db.execute( 'CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" VARCHAR(65535), PRIMARY KEY (path,hash) );' )

        ########### INSERT benchmark ###########
        t0InsertAll = time.time()
        for i in range( nFiles // rowsPerInsert ):
            rows = [ ( os.urandom( fileNameLength // 2 ).hex(), j )
                     for j in range( i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert ) ]
            db.executemany( 'INSERT INTO files VALUES (?,?)', rows )
        db.commit()
        t1InsertAll = time.time()
        print( "Inserting {} file names with {} characters and cache size {} took {:.3f} s".
               format( nFiles, fileNameLength, cacheSize, t1InsertAll - t0InsertAll ) )

        insertionTimes += [ t1InsertAll - t0InsertAll ]
        os.remove( databaseFile )

    fig = plt.figure()
    ax = fig.add_subplot( 111, xlabel = "SQL Cache Size / MB", ylabel = "Table Creation Time / s" )

    ax.plot( cacheSizes, insertionTimes, 'o' )

    ax.legend( loc = 'best' )
    fig.tight_layout()
    fig.savefig( fname + ".pdf" )
    fig.savefig( fname + ".png" )

def benchmarkCacheSizesSortAfter( nFiles = 1000 * 1000 ):
    rowsPerInsert = 1000
    assert nFiles % rowsPerInsert == 0
    fname = f"sqlite using intermediary table order by cache size benchmark {nFiles // 1000}k files"

    cacheSizes = [ 2, 4, 16, 32, 64, 128, 192, 256, 320, 384, 448, 512 ]
    insertionTimes = []
    for cacheSize in cacheSizes:
        databaseFile = tempfile.mkstemp()[1]
        db = sqlite3.connect( databaseFile ) #'1m-names-test.sqlite3' )
        db.executescript( f"""
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
            PRAGMA CACHE_SIZE = -{cacheSize * 1000};
        """ )
        # use this schema as it represents the (path,name) primary key currently used in ratarmount
        db.execute( """
            CREATE TABLE "files_tmp" (
                "id"   INTEGER PRIMARY KEY,
                "path" VARCHAR(65535),
                "name" VARCHAR(65535)
            );
        """ )

        ########### INSERT benchmark ###########
        t0InsertAll = time.time()
        for i in range( nFiles // rowsPerInsert ):
            rows = [ ( j, os.urandom( fileNameLength // 2 ).hex(), j )
                     for j in range( i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert ) ]
            db.executemany( 'INSERT INTO files_tmp VALUES (?,?,?)', rows )


        db.execute( """
            CREATE TABLE "files" (
                "path" VARCHAR(65535),
                "name" VARCHAR(65535),
                PRIMARY KEY (path,name)
            );
        """ )
        db.execute( 'INSERT INTO "files" (path,name) SELECT path,name FROM "files_tmp" ORDER BY path,name;' )
        db.execute( 'DROP TABLE "files_tmp"' )

        db.commit()
        t1InsertAll = time.time()

        print( "Inserting {} file names with {} characters and cache size {} took {:.3f} s".
               format( nFiles, fileNameLength, cacheSize, t1InsertAll - t0InsertAll ) )

        insertionTimes += [ t1InsertAll - t0InsertAll ]
        os.remove( databaseFile )

    fig = plt.figure()
    ax = fig.add_subplot( 111, xlabel = "SQL Cache Size / MB", ylabel = "Table Creation Time / s" )

    ax.plot( cacheSizes, insertionTimes, 'o' )

    fig.tight_layout()
    fig.savefig( fname + ".pdf" )
    fig.savefig( fname + ".png" )


benchmarkCacheSizesSortAfter( 1000 * 1000 )
#benchmarkCacheSizesSortAfter( 1000 * 1000 )
#benchmarkCacheSizes( 128 * 1000 )

plt.show()
exit()

def extractValuesFromLog( path ):
    result = []
    with open( path, 'rt' ) as file:
        blocks = file.read().split( '\n\n' )
        for block in blocks:
            if not block:
                continue

            timeRegexes = {
                'name'              : r'(CREATE TABLE .*)',
                'tinsert'           : r'Inserting ([0-9]+) file names with [0-9]+ characters took ([0-9.]+) s when excluding PRNG time',
                'tselectpath'       : r'Selecting ([0-9]+) paths took ([0-9.]+) s',
                'tselectpathstart'  : r'Selecting ([0-9]+) paths starting with took ([0-9.]+) s',
                'tselecthash'       : r'Selecting ([0-9]+) hashes took ([0-9.]+) s',
                'dbsize'            : r'SQL database size in bytes: ([0-9]+)',
            }

            namemap = dict( zip( schemas, labels ) )

            values = {}
            for key,timeRegex in timeRegexes.items():
                matches = list( filter( None, [ re.match( timeRegex, line.strip() ) for line in block.split( '\n' ) ] ) )[0]

                if key.startswith( 't' ):
                    values[key] = float( matches.group( 2 ) ) / float( matches.group( 1 ) )
                elif key == 'dbsize':
                    values[key] = int( matches.group( 1 ) )
                elif key == 'name':
                    if matches.group( 1 ) in namemap:
                        values[key] = namemap[matches.group( 1 )]
                    else:
                        values[key] = matches.group( 1 )
                else:
                    values[key] = matches.group( 1 )
                if 'tselect' in key:
                    values['nrowsselect'] = int( matches.group( 1 ) )
                if 'tinsert' in key:
                    values['nrowsinsert'] = int( matches.group( 1 ) )
            result += [ values ]

    return result

def plotSummary( logFiles ):
    values = []
    for logFile in logFiles:
        result = re.match( r'.*/sqlite primary key benchmark ([0-9]+)k files\.log', logFile )
        values += extractValuesFromLog( logFile )

    names = sorted( list( set( [ x['name'] for x in values ] ) ) )
    fig = plt.figure( figsize = (12,9) )
    for ikey, key in enumerate( [ 'tinsert', 'tselectpath', 'tselecthash', 'tselectpathstart' ] ):
        ax = fig.add_subplot( 2,2,1+ikey, xlabel = 'Number of Rows', ylabel = key + ' per row / s', xscale = 'log', yscale = 'log' )

        allx = []
        ally = []
        for name in names:
            # always use nrowsinsert as that is the database size and times are already per row
            xvalues = np.array( [ x['nrowsinsert'] for x in values if x['name'] == name ] )
            yvalues = np.array( [ x[key] for x in values if x['name'] == name ] )
            iSorted = np.argsort( xvalues )
            ax.plot( xvalues[iSorted], yvalues[iSorted], marker = 'o', label = name )
            allx += [ xvalues[iSorted] ]
            ally += [ yvalues[iSorted] ]
        minx = min( [ min( x ) for x in allx ] )
        maxx = max( [ max( x ) for x in allx ] )
        miny = min( [ min( x ) for x in ally ] )
        maxy = max( [ max( x ) for x in ally ] )
        x = 10 ** np.linspace( np.log10( minx ), np.log10( maxx ) )
        if key in [ 'tselectpathstart' ]:
            ax.plot( x, x / x[0] * miny, linestyle = '--', color = '0.5', label = 'linear scaling' )
            y = x**2 / x[-1]**2 * maxy
            ax.plot( x[y>miny], y[y>miny], linestyle = ':', color = '0.5', label = 'quadratic scaling' )
            y = x**3 / x[-1]**3 * maxy
            ax.plot( x[y>miny], y[y>miny], linestyle = '-', color = '0.5', label = 'cubic scaling' )
        if key in [ 'tselectpath', 'tselecthash' ]:
            ax.plot( x, x / x[0] * max( [ ys[0] for ys in ally ] ), linestyle = '--', color = '0.5', label = 'linear scaling' )

        if key in [ 'tselectpath', 'tselecthash' ]:
            ax.plot( x, np.log( x ) / np.log( x[0] ) * miny, linestyle = '-.', color = '0.5', label = 'logarithmic scaling' )

        if key in [ 'tinsert' ]:
            y = x / x[-1] * maxy
            ax.plot( x[y>miny], y[y>miny], linestyle = '--', color = '0.5', label = 'linear scaling' )
            ax.plot( x, np.log( x ) / np.log( x[0] ) * max( [ ys[0] for ys in ally ] ), linestyle = '-.', color = '0.5', label = 'logarithmic scaling' )

        ax.legend( loc = 'best' )

    fig.tight_layout()
    fig.savefig( "sqlite primary key benchmark times over row count.pdf" )
    fig.savefig( "sqlite primary key benchmark times over row count.png" )

    ############### size plot ###############

    fig = plt.figure( figsize = (8,6) )
    ax = fig.add_subplot( 111, xlabel = 'Number of Rows', ylabel = 'size per row / s', xscale = 'log', yscale = 'log' )

    allx = []
    ally = []
    for name in names:
        # always use nrowsinsert as that is the database size and times are already per row
        xvalues = np.array( [ x['nrowsinsert'] for x in values if x['name'] == name ] )
        yvalues = np.array( [ x['dbsize'] for x in values if x['name'] == name ] )
        iSorted = np.argsort( xvalues )
        ax.plot( xvalues[iSorted], yvalues[iSorted], marker = 'o', label = name )
        allx += [ list( xvalues[iSorted] ) ]
        ally += [ list( yvalues[iSorted] ) ]
    miny = min( [ min( x ) for x in ally ] )

    ax.plot( x, x / x[0] * miny, linestyle = '--', color = '0.5', label = 'linear scaling' )
    ax.legend( loc = 'best' )

    fig.tight_layout()
    fig.savefig( "sqlite primary key benchmark sizes over row count.pdf" )
    fig.savefig( "sqlite primary key benchmark sizes over row count.png" )

if len( sys.argv ) > 1 and os.path.isdir( sys.argv[1] ):
    logFiles = []
    folder = sys.argv[1]
    for fname in os.listdir( folder ):
        path = os.path.abspath( os.path.join( folder, fname ) )
        if fname.endswith( '.log' ) and os.path.isfile( path ):
            logFiles += [ path ]
    plotSummary( logFiles )
    plt.show()
    exit()

logFiles = []
#for nFiles in [ 3 * 1000, 10 * 1000, 32 * 1000, 100 * 1000,  320 * 1000,
#                1 * 1000 * 1000, 2 * 1000 * 1000, 4 * 1000 * 1000, 8 * 1000 * 1000, 16 * 1000 * 1000 ]:
for nFiles in [ 1000*1000 ]:
    rowsPerInsert = 1000
    assert nFiles % rowsPerInsert == 0

    nFilesSelect = 100
    if nFiles > 2 * 1000 * 1000:
        nFilesSelect = 4

    fname = f"sqlite primary key benchmark {nFiles // 1000}k files"

    fig = plt.figure( figsize = ( 12, 10 ) )
    axi = plt.subplot( 221, xscale = 'log', yscale = 'log', title = 'INSERT' )
    axs = plt.subplot( 222, xscale = 'log', yscale = 'log', title = 'SELECT PATH == x' )
    axsl = plt.subplot( 223, xscale = 'log', yscale = 'log', title = 'SELECT PATH LIKE x.%' )
    axsh = plt.subplot( 224, xscale = 'log', yscale = 'log', title = 'SELECT HASH == y' )

    logFile = open( fname + ".log", 'wt' )
    logFiles += [ logFile ]
    def log( line ):
        print( line )
        logFile.write( line + '\n' )
        logFile.flush()

    log( f"file name length: {fileNameLength}" )
    log( f"rows per insert: {rowsPerInsert}" )
    log( f"number of rows to insert: {nFiles}" )

    for iSchema, schema in enumerate( schemas ):
        log( schema )
        databaseFile = tempfile.mkstemp()[1]
        db = sqlite3.connect( databaseFile ) #'1m-names-test.sqlite3' )
        db.execute( schema )
        # benchmarks were done on 1M rows
        db.execute( 'PRAGMA LOCKING_MODE = EXCLUSIVE;' ) # only ~10% speedup but does not hurt
        #db.execute( 'PRAGMA TEMP_STORE = MEMORY;' ) # no real speedup but does hurt integrity!
        #db.execute( 'PRAGMA JOURNAL_MODE = OFF;' ) # no real speedup but does hurt integrity!
        db.execute( 'PRAGMA SYNCHRONOUS = OFF;' ) # ~10% speedup but does hurt integrity!
        # default (-2000): ~20s, 100: ~18s, 1000: ~18.6s, ~10000: 15s, ~100 000: ~8s
        # -100 000: ~12s, -400000 (~400MB): ~8s, -768000: ~8s
        # Benchmark with 4M rows: default(-2000): 114s, -512 000: ~70s
        # default page size: 4096 -> positive cache sizes are in page sizes, negative in kiB
        # I guess this normally is not that relevant because an increasing integer ID is used but
        # for this use case where it seems like on each transaction, the database gets resorted by
        # a string key, it might help to do the sorting less frequently and in memory on larger data.
        # Also, 512MB is alright for a trade-off. If you have a TAR with that much metadata in it,
        # you should have a system which can afford 512MB in the first place.
        db.execute( 'PRAGMA CACHE_SIZE = -512000;' )

        ########### INSERT benchmark ###########

        t0InsertAll = time.time()
        insertTimes = []
        for i in range( nFiles // rowsPerInsert ):
            rows = [ ( os.urandom( fileNameLength // 2 ).hex(), j )
                     for j in range( i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert ) ]
            t0 = time.time()
            db.executemany( 'INSERT INTO files VALUES (?,?)', rows )
            #db.commit() # data is written to disk automatically when it becomes too much
            t1 = time.time()
            #print( f"Inserting {rowsPerInsert} rows with {fileNameLength} character file names took {t1 - t0:.3f} s" )
            insertTimes += [ t1 - t0 ]

        t1InsertAll = time.time()
        log( f"Inserting {nFiles} file names with {fileNameLength} characters took {t1InsertAll - t0InsertAll:.3f} s" )

        t0Commit = time.time()
        db.commit()
        t1Commit = time.time()
        log( f"Commit took {t1Commit - t0Commit:.3f} s" )

        tTotalInsert = sum( insertTimes ) + t1Commit - t0Commit
        log( "Inserting {} file names with {} characters took {:.3f} s when excluding PRNG time".
             format( nFiles, fileNameLength, tTotalInsert ) )

        with open( f"{fname} {labels[iSchema]} insert.dat", 'wt' ) as dataFile:
            for t in insertTimes:
                dataFile.write( str( t ) + '\n' )

        axi.plot( insertTimes, linestyle = '', marker = '.',
                  label = f'{labels[iSchema]}, total time: {tTotalInsert:.3f}s' )

        ########### SELECT benchmarks ###########

        def benchmarkSelect( entity, sqlCommand, makeRow, ax ):
            t0Select = time.time()
            selectTimes = []
            for i in range( nFilesSelect ):
                t0 = time.time()
                db.execute( sqlCommand, makeRow( i ) ).fetchall()
                t1 = time.time()
                selectTimes += [ t1 - t0 ]
            t1Select = time.time()
            log( "Selecting {} {} took {:.3f} s".
                 format( nFilesSelect, entity, t1Select - t0Select) )

            tTotalSelectTime = sum( selectTimes )
            log( "Selecting {} {} took {:.3f} s excluding PRNG time".
                 format( nFilesSelect, entity, tTotalSelectTime ) )

            with open( f"{fname} {labels[iSchema]} select {entity}.dat", 'wt' ) as dataFile:
                for t in selectTimes:
                    dataFile.write( str( t ) + '\n' )

            ax.plot( selectTimes, linestyle = '', marker = '.',
                     label = f'labels[iSchema], total time: {tTotalSelectTime:.3f}s' )

        benchmarkSelect( 'paths', 'SELECT hash FROM files WHERE path == (?)',
                         lambda j : ( os.urandom( fileNameLength // 2 ).hex(), ), axs )

        benchmarkSelect( 'paths starting with', 'SELECT hash FROM files WHERE path LIKE (?)',
                         lambda j : ( os.urandom( fileNameLength // 2 ).hex() + '%', ), axsl )

        benchmarkSelect( 'hashes', 'SELECT path FROM files WHERE "hash" == (?);',
                         lambda j : ( np.random.randint( 0, nFiles ), ), axsh )

        ########### Cleanup ###########

        #db.execute( 'VACUUM' )  # does not help because we don't delete anything but still adds significant time overhead
        db.close()
        stats = os.stat( databaseFile )
        log( f"SQL database size in bytes: {stats.st_size}" )
        os.remove( databaseFile )

        log( "" )

    axi.legend( loc = 'best' )
    axs.legend( loc = 'best' )
    axsl.legend( loc = 'best' )
    axsh.legend( loc = 'best' )
    fig.tight_layout()
    fig.savefig( fname + ".pdf" )
    fig.savefig( fname + ".png" )
    plt.close( fig )

    logFile.close()

plotSummary( logFiles )
plt.show()


"""
Results:
  - SELECT query times are fairly constant
  - SELECT == on first primary key is ~100x faster
  - SELECT PATH LIKE x.% is always 100x slower because it can't bisect and has to check every row.
    Also the scaling changes from presumably logarithmic to linear.
  - SELECT on stringified ID is ~65% slower than comparing integers
  - INSERT time over row count seems to grow slower than power law, O(log(n))? as a bisect would suggest?
  - Primary key over two varchars is fast on lookup for the first but slow over the second
    (basically only the first can be used for bisecting, at least than all of the first a distinct!)
  - starting from roughly 1 million rows (~600MB for varchar primary key),
    a new timing regime for executemany appears, which is roughly 10x slower, this is presumably
    an automatically triggered disk-write when the cache becomes full.
  - The database with integer as primary key is less than half as large than varchar as primary key!
    Calling VACUUM does not help. It looks like SQL keeps a second list for the sorted lookup to an internal index.
    The overhead might not be so large if the primary key is not too large in comparison to other columns.
  - Increasing cache size from 2MB to 512MB, gives roughly 40% performance boost
  - The 10M rows test case is unproportionally slower than all others. Some limit seems to have been hit.
  - database size grows linearly
  - Insertion time per row seems to grow O(n) after 1M rows, i.e., the total database creation time grows with O(n^2)!

To Plot:
  - (file sizes/row count, total insertion time / row count, ...) over row count -> four lines one for each schema

Conclusions:
  - Looks like it is best to split the file path in 'path' and 'name' and use these two as primary key.
    the 8ms for one single lookup, would still result in 1s time for a directory listing of 100 files, so it'd be
    better to cache the last SELECT path == "folder" lookup, then everything should work fine.
    In the first place, the benchmarks above have relatively long file names unlike in practice and are 100% flat.
  - I don't know whether the O(n^2) creation time could be a trade-off, but for 16M files and
    PRIMARY KEY(VARCHAR,VARCHAR), it takes 918s ~15min.
    ImageNet has 14M files with quite a lot shorter file names (which probably is also a factor in speed!)
    and still takes 4h anyways, so this SQLite overhead might not be all that bad. But for archives
    with 100M files it might "better" to use the nested dictionary and the custom serialization
    as long as there is enough memory.
    ILSVRC has 2M files and takes 45min with the custom serialization whereas SQLite insertion of 2M takes 20s!

"""
