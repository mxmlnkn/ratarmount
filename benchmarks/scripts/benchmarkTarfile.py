#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gc
import os
import pprint
import resource
import sys
import time


def byteSizeFormat(size, decimal_places=3):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        assert unit
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


def memoryUsage():
    statm_labels = ['size', 'resident', 'shared', 'text', 'lib', 'data', 'dirty pages']
    values = [int(x) * resource.getpagesize() for x in open(f'/proc/{os.getpid()}/statm', 'rt').read().split(' ')]
    return dict(zip(statm_labels, values))


def printMemDiff(mem0, mem1, action_message=None):
    if action_message:
        print(f"Memory change after '{action_message}'")
    memdiff = mem1.copy()
    for key, value in mem0.items():
        memdiff[key] -= value
    pprint.pprint(memdiff)
    print(f"Total size: {byteSizeFormat( mem1[ 'size' ] )} B for process {os.getpid()}")
    print()


class MemoryLogger:
    def __init__(self, quiet=False):
        self.quiet = quiet
        self.memlog = [("Initial Memory Usage", memoryUsage())]
        if not self.quiet:
            print(self.memlog[0][0])
            pprint.pprint(self.memlog[0][1])
            print(f"Total size: {byteSizeFormat( self.memlog[0][1][ 'size' ] )} B")
            print()

    def log(self, action_message=None):
        self.memlog += [(action_message, memoryUsage())]
        if not self.quiet:
            printMemDiff(self.memlog[-2][1], self.memlog[-1][1], action_message)


def benchmarkTarfile(filename):
    # Remember garbage collector objects
    before = {}
    for i in gc.get_objects():
        if type(i) in before:
            before[type(i)] += 1
        else:
            before[type(i)] = 1

    t0 = time.time()

    mem0 = memoryUsage()
    import tarfile

    mem1 = memoryUsage()
    printMemDiff(mem0, mem1, "Memory change after 'import tarfile'")

    with open(filename, 'rb') as file:
        loadedTarFile = tarfile.open(fileobj=file, mode='r:')
        count = 0
        for _fileinfo in loadedTarFile:
            count += 1

        print("Files in TAR", count)
        mem2 = memoryUsage()
        printMemDiff(mem1, mem2, 'iterate over TAR')

        loadedTarFile.members = []
        mem2b = memoryUsage()
        printMemDiff(mem2, mem2b, 'deleted tarfile members')

    mem3 = memoryUsage()
    printMemDiff(mem2, mem3, 'with open TAR file')

    t1 = time.time()
    print(f"Reading TAR took {t1 - t0:.2f} s")

    # Check garbage collector object states
    after = {}
    for i in gc.get_objects():
        if type(i) in after:
            after[type(i)] += 1
        else:
            after[type(i)] = 1

    # pprint.pprint( before )
    # pprint.pprint( after )
    pprint.pprint([(k, after.get(k, 0) - before.get(k, 0)) for k in after if after.get(k, 0) - before.get(k, 0)])


benchmarkTarfile(sys.argv[1])
