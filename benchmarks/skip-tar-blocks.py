#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import time

if __name__ == '__main__':
    block = 0
    entries = 0
    with open(sys.argv[1], 'rb') as tarFile:
        t0 = time.thread_time()

        while True:
            tarBlock = tarFile.read(512)
            if not tarBlock or tarBlock == b"\0" * 512:
                break

            entries += 1
            block += 1
            size = int(tarBlock[124:124+12].strip(b"\0"), 8)
            block += -(size // -512)  # ceiling division
            tarFile.seek(block * 512)

        t1 = time.thread_time()
        print(f"Skipping {entries} TAR entries took: {t1-t0}s")

"""
python3 skip-tar-blocks.py tar-with-1000-folders-with-1000-files-0B-files.tar

    Skipping 1001001 TAR entries took: 0.751045104s
    Skipping 1001001 TAR entries took: 0.770624965s
    Skipping 1001001 TAR entries took: 0.775764209s
    Skipping 1001001 TAR entries took: 0.760520796s

Manual benchmark results for:
    ./ratarmount.py -c -f tar-with-1000-folders-with-1000-files-0B-files.tar foo

    1429b4e / f63a5f6:
        37.83s 36.90s 37.59s 37.46s
    after time.thread_time performance improvements:
        34.85s 34.81s 34.73s 34.86s
    after caching 1M fileinfos and using executemany:
        27.75s 28.53s 28.47s 27.88s 28.31s
    after caching 1000 fileinfos and using executemany:
        27.85s 27.90s 27.86s 27.94s
    only iterating over tarfile without doing anything:
        19.86s 19.73s 19.74s 19.74s

  => There definitely is potential to improve the createIndex routine by parallelizing the tarfile iteration!

Benchmark timing method:
    t0 = time.time()
    for i in range(100000):
        t1 = time.time()
    print(t1-t0)

        0.14345788955688477

    t0 = time.thread_time()
    for i in range(100000):
        t1 = time.thread_time()
    print(t1-t0)

        0.07387778500000008
"""
