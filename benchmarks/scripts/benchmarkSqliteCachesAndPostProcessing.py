#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sqlite3
import time
import tempfile

import matplotlib.pyplot as plt


fileNameLength = 256


def benchmarkCacheSizes(nFiles):
    rowsPerInsert = 1000
    assert nFiles % rowsPerInsert == 0
    fname = f"sqlite cache size benchmark {nFiles // 1000}k files"

    cacheSizes = [2, 4, 16, 32, 64, 128, 256, 512]
    insertionTimes = []
    for cacheSize in cacheSizes:
        databaseFile = tempfile.mkstemp()[1]
        db = sqlite3.connect(databaseFile)  #'1m-names-test.sqlite3' )
        db.executescript(
            f"""
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
            PRAGMA CACHE_SIZE = -{cacheSize * 1000};
            """
        )
        # use this schema as it represents the (path,name) primary key currently used in ratarmount
        db.execute('CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" VARCHAR(65535), PRIMARY KEY (path,hash) );')

        ########### INSERT benchmark ###########
        t0InsertAll = time.time()
        for i in range(nFiles // rowsPerInsert):
            rows = [
                (os.urandom(fileNameLength // 2).hex(), j)
                for j in range(i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert)
            ]
            db.executemany('INSERT INTO files VALUES (?,?)', rows)
        db.commit()
        t1InsertAll = time.time()
        print(
            f"Inserting {nFiles} file names with {fileNameLength} characters and cache size {cacheSize} "
            + f"took {t1InsertAll - t0InsertAll:.3f} s"
        )

        insertionTimes += [t1InsertAll - t0InsertAll]
        os.remove(databaseFile)

    fig = plt.figure()
    ax = fig.add_subplot(111, xlabel="SQL Cache Size / MB", ylabel="Table Creation Time / s")

    ax.plot(cacheSizes, insertionTimes, 'o')

    ax.legend(loc='best')
    fig.tight_layout()
    fig.savefig(fname + ".pdf")
    fig.savefig(fname + ".png")


def benchmarkCacheSizesSortAfter(nFiles):
    rowsPerInsert = 1000
    assert nFiles % rowsPerInsert == 0
    fname = f"sqlite using intermediary table order by cache size benchmark {nFiles // 1000}k files"

    cacheSizes = [2, 4, 16, 32, 64, 128, 192, 256, 320, 384, 448, 512]
    insertionTimes = []
    for cacheSize in cacheSizes:
        databaseFile = tempfile.mkstemp()[1]
        db = sqlite3.connect(databaseFile)  #'1m-names-test.sqlite3' )
        db.executescript(
            f"""
            PRAGMA LOCKING_MODE = EXCLUSIVE;
            PRAGMA TEMP_STORE = MEMORY;
            PRAGMA JOURNAL_MODE = OFF;
            PRAGMA SYNCHRONOUS = OFF;
            PRAGMA CACHE_SIZE = -{cacheSize * 1000};
            """
        )
        # use this schema as it represents the (path,name) primary key currently used in ratarmount
        db.execute(
            """
            CREATE TABLE "files_tmp" (
                "id"   INTEGER PRIMARY KEY,
                "path" VARCHAR(65535),
                "name" VARCHAR(65535)
            );
            """
        )

        ########### INSERT benchmark ###########
        t0InsertAll = time.time()
        for i in range(nFiles // rowsPerInsert):
            rows = [
                (j, os.urandom(fileNameLength // 2).hex(), j)
                for j in range(i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert)
            ]
            db.executemany('INSERT INTO files_tmp VALUES (?,?,?)', rows)

        db.execute(
            """
            CREATE TABLE "files" (
                "path" VARCHAR(65535),
                "name" VARCHAR(65535),
                PRIMARY KEY (path,name)
            );
            """
        )
        db.execute('INSERT INTO "files" (path,name) SELECT path,name FROM "files_tmp" ORDER BY path,name;')
        db.execute('DROP TABLE "files_tmp"')

        db.commit()
        t1InsertAll = time.time()

        print(
            f"Inserting {nFiles} file names with {fileNameLength} characters and cache size {cacheSize} "
            f"took {t1InsertAll - t0InsertAll:.3f} s"
        )

        insertionTimes += [t1InsertAll - t0InsertAll]
        os.remove(databaseFile)

    fig = plt.figure()
    ax = fig.add_subplot(111, xlabel="SQL Cache Size / MB", ylabel="Table Creation Time / s")

    ax.plot(cacheSizes, insertionTimes, 'o')

    fig.tight_layout()
    fig.savefig(fname + ".pdf")
    fig.savefig(fname + ".png")


benchmarkCacheSizes(1_000_000)
benchmarkCacheSizesSortAfter(1_000_000)
plt.show()

"""
Inserting 1000000 file names with 256 characters

cache size 2 took 13.838 s
cache size 4 took 13.753 s
cache size 16 took 12.548 s
cache size 32 took 12.278 s
cache size 64 took 10.774 s
cache size 128 took 8.923 s
cache size 256 took 6.857 s
cache size 512 took 5.949 s

 -> Large cache sizes improve insertion times a lot, probably because of less frequent sorting being required.

Usage of files_tmp table to insert unsorted first and then sort it into a primary key table.

cache size 2 took 5.760 s
cache size 4 took 5.726 s
cache size 16 took 7.199 s
cache size 32 took 5.870 s
cache size 64 took 5.894 s
cache size 128 took 6.010 s
cache size 192 took 5.641 s
cache size 256 took 5.532 s
cache size 320 took 5.325 s
cache size 384 took 5.355 s
cache size 448 took 5.526 s
cache size 512 took 5.458 s

 -> Cache sizes do not matter anymore, this method is always faster!
 -> Presumably this is like changing from insertion sort to quicksort, i.e., from O(n^2) to O(n log(n)).
"""
