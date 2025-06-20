#!/usr/bin/env python3

import concurrent.futures
import lzma
import os
import resource
import sys
import time

import indexed_zstd
import numpy as np
import xz
import zstandard
from ratarmountcore.BlockParallelReaders import ParallelXZReader, ParallelZstdReader


def benchmark_reading(fileObject):
    print(f"== Benchmark {fileObject} file decompression ==")

    size = 0
    t0 = time.time()
    with fileObject as file:
        t1 = time.time()

        while True:
            readSize = len(file.read(32 * 1024 * 1024))
            if readSize == 0:
                break
            size += readSize

            if time.time() - t1 > 5:
                t1 = time.time()
                print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

        file.close()

    t1 = time.time()
    print(f"Reading {size} B took: {t1 - t0:.3f}s")


def compare_reading(file, pfile):
    print("== Test file decompression ==")

    size = 0
    t0 = time.time()
    t1 = time.time()

    while True:
        readData = file.read(8 * 1024 * 1024)
        parallelReadData = pfile.read(len(readData))
        if readData != parallelReadData:
            print("inequal", len(readData), len(parallelReadData))
        assert readData == parallelReadData
        readSize = len(readData)
        if readSize == 0:
            break
        size += readSize

        if time.time() - t1 > 5:
            t1 = time.time()
            print(f"{t1 - t0:.2f}s {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024} MiB RSS")

    t1 = time.time()
    print(f"Reading {size} B took: {t1 - t0:.3f}s")


def test_zstd_seeking(filename):
    file = indexed_zstd.IndexedZstdFile(filename)
    for offset in file.block_offsets():
        file.seek(0)
        file.read(1)
        t0 = time.time()
        file.seek(offset)
        file.read(1)
        t1 = time.time()
        print(f"Seeking to {offset} took {t1 - t0:.3f}s")


def read_block(filename, offset, size):
    with indexed_zstd.IndexedZstdFile(filename) as file:
        file.seek(offset)
        return file.read(size)


def simple_parallel_zstd_reading(filename):
    parallelization = os.cpu_count()
    with concurrent.futures.ThreadPoolExecutor(parallelization) as pool:
        futures = []
        with indexed_zstd.IndexedZstdFile(filename) as file:
            offsets = np.array(list(file.block_offsets().values()))
        sizes = offsets[1:] - offsets[:-1]
        t0 = time.time()
        for offset, size in zip(offsets[:-1], sizes):
            futures.append(pool.submit(read_block, filename, offset, size))
            while len(futures) >= parallelization:
                futures.pop(0).result()
        t1 = time.time()
        print(f"Reading in parallel with a thread pool took {t1 - t0:.3f}s")


if __name__ == '__main__':
    for module in ('zstandard', 'indexed_zstd', 'ratarmountcore'):
        if hasattr(sys.modules[module], '__version__'):
            print(module, "version:", sys.modules[module].__version__)
    print()

    filename = sys.argv[1]
    if filename.endswith('.xz'):
        filename = filename[:-3]
    elif filename.endswith('.zst'):
        filename = filename[:-4]

    if os.path.isfile(filename + '.xz'):
        compare_reading(xz.open(filename + '.xz', 'rb'), ParallelXZReader(filename + '.xz', os.cpu_count()))
        benchmark_reading(xz.open(filename + '.xz', 'rb'))
        benchmark_reading(lzma.open(filename + '.xz', 'rb'))
        benchmark_reading(ParallelXZReader(filename + '.xz', os.cpu_count()))

    print()

    if os.path.isfile(filename + '.zst'):
        # simple_parallel_zstd_reading(filename + '.zst')
        # test_zstd_seeking(filename + '.zst')

        compare_reading(zstandard.open(filename + '.zst', 'rb'), ParallelZstdReader(filename + '.zst', os.cpu_count()))
        benchmark_reading(zstandard.open(filename + '.zst', 'rb'))
        benchmark_reading(indexed_zstd.IndexedZstdFile(filename + '.zst'))
        benchmark_reading(ParallelZstdReader(filename + '.zst', os.cpu_count()))
