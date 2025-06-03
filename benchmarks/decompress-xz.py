#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gc
import lzma
import resource
import sys
import time

import guppy
import xz


def benchmark_python_xz():
    print("== Benchmark xz file decompression ==")

    h = guppy.hpy()
    result = None

    size = 0
    t0 = time.time()
    with xz.open(sys.argv[1], 'rb') as file:
        t1 = time.time()

        while True:
            readSize = len(file.read(32 * 1024 * 1024))
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 5:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
                gc.collect()
                # Note that this heap call would add a reference count to the file object...
                # if result is None:
                #    result = h.heap()
                # else:
                #    print((h.heap() - result))

        print(f"After finishing loop: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
        file.close()
        print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    print("file closed?", file.closed)
    t1 = time.time()

    print("File type:", file)
    print("File type:", type(file))
    print("File referred by:", gc.get_referrers(file))
    print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    del file
    print(f"After deleting file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    gc.collect()
    print(f"After garbage collection: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    if result is not None:
        print((h.heap() - result))
    print(f"Reading {size} B took: {t1 - t0}s")


def benchmark_reading():
    print("== Benchmark simple file reading ==")

    size = 0
    t0 = time.time()
    with open(sys.argv[1], 'rb') as file:
        t1 = time.time()

        while True:
            readSize = len(file.read(32 * 1024 * 1024))
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 0.1:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    t1 = time.time()

    print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    gc.collect()
    print(f"After garbage collection: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    print(f"Reading {size} B took: {t1 - t0}s")


def benchmark_lzma_compress():
    print("== Benchmark LZMA compression ==")

    size = 0
    t0 = time.time()
    with open(sys.argv[1], 'rb') as file:
        t1 = time.time()

        while True:
            data = file.read(32 * 1024 * 1024)
            compressed = lzma.compress(data)
            readSize = len(data)
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 5:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    t1 = time.time()

    print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    gc.collect()
    print(f"After garbage collection: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    print(f"Reading {size} B took: {t1 - t0}s")


def benchmark_lzma_decompress():
    print("== Benchmark LZMA compression + decompression ==")

    size = 0
    t0 = time.time()
    with open(sys.argv[1], 'rb') as file:
        t1 = time.time()

        while True:
            data = file.read(32 * 1024 * 1024)
            compressed = lzma.compress(data)
            data = lzma.decompress(compressed)
            readSize = len(data)
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 5:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    t1 = time.time()

    print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    gc.collect()
    print(f"After garbage collection: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    print(f"Reading {size} B took: {t1 - t0}s")


def benchmark_lzma_decompressor():
    print("== Benchmark LZMADecompressor ==")

    size = 0
    t0 = time.time()
    with open(sys.argv[1], 'rb') as file:
        t1 = time.time()

        while True:
            data = file.read(32 * 1024 * 1024)
            compressed = lzma.compress(data)
            decompressor = lzma.LZMADecompressor(format=lzma.FORMAT_XZ)
            data = decompressor.decompress(compressed)
            readSize = len(data)
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 5:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    t1 = time.time()

    print(f"After closing file: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    gc.collect()
    print(f"After garbage collection: {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")
    print(f"Reading {size} B took: {t1 - t0}s")


if __name__ == '__main__':
    print("xz version:", xz.__version__)
    # benchmark_reading()
    # benchmark_lzma_compress()
    # benchmark_lzma_decompress()
    # benchmark_lzma_decompressor()
    benchmark_python_xz()

"""
https://github.com/Rogdham/python-xz/issues/2

base64 /dev/urandom | head -c $(( 4*1024*1024*1024  )) > large
xz -T 0 --keep large
xz -l large.xz
    Strms  Blocks   Compressed Uncompressed  Ratio  Check   Filename
        1     171  3.148,1 MiB  4.096,0 MiB  0,769  CRC64   large.xz

python3 decompress-xz.py large.xz

    xz version: 0.2.0
    == Benchmark xz file decompression ==
    10s 171 MiB RSS
    21s 243 MiB RSS
    31s 323 MiB RSS
    41s 396 MiB RSS
    51s 468 MiB RSS
    61s 548 MiB RSS
    71s 621 MiB RSS
    82s 693 MiB RSS
    92s 773 MiB RSS
    102s 846 MiB RSS
    112s 918 MiB RSS
    123s 998 MiB RSS
    133s 1071 MiB RSS
    143s 1143 MiB RSS
    153s 1223 MiB RSS
    163s 1296 MiB RSS
    174s 1368 MiB RSS
    184s 1448 MiB RSS
    Reading 4294967296 B took: 186.66424465179443s

python3 decompress-xz.py small.xz

    xz version: 0.3.1
    == Benchmark simple file reading ==
    0s 42 MiB RSS
    0s 42 MiB RSS
    0s 42 MiB RSS
    0s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    1s 42 MiB RSS
    After closing file: 42 MiB RSS
    After garbage collection: 42 MiB RSS
    Reading 3300990440 B took: 1.475733995437622s

python3 decompress-xz.py small.xz

    xz version: 0.3.1
    == Benchmark LZMA compression ==
    12.20s 214 MiB RSS
    24.18s 261 MiB RSS
    36.03s 261 MiB RSS
    48.03s 261 MiB RSS
    60.09s 261 MiB RSS
    72.27s 261 MiB RSS
    84.22s 261 MiB RSS
    96.28s 261 MiB RSS
    108.20s 261 MiB RSS
    120.40s 261 MiB RSS
    132.59s 261 MiB RSS
    144.70s 261 MiB RSS
    147.78s 261 MiB RSS
    After closing file: 261 MiB RSS
    After garbage collection: 261 MiB RSS
    Reading 412603468 B took: 147.78389024734497s

python3 decompress-xz.py small.xz

    xz version: 0.3.1
    == Benchmark LZMA compression + decompression ==
    12.55s 214 MiB RSS
    25.03s 261 MiB RSS
    37.11s 261 MiB RSS
    50.34s 261 MiB RSS
    63.25s 261 MiB RSS
    75.79s 261 MiB RSS
    88.64s 261 MiB RSS
    101.26s 261 MiB RSS
    113.97s 261 MiB RSS
    126.59s 261 MiB RSS
    139.10s 261 MiB RSS
    151.64s 261 MiB RSS
    After closing file: 261 MiB RSS
    After garbage collection: 261 MiB RSS
    Reading 412603468 B took: 154.94597101211548s
"""
