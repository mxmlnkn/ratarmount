#!/usr/bin/env python3

import gc
import os
import pprint
import resource
import sys
import time


def byte_size_format(size, decimal_places=3):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        assert unit
        if size < 1024.0:
            break
        size /= 1024.0
    return f"{size:.{decimal_places}f}{unit}"


def memory_usage():
    statm_labels = ['size', 'resident', 'shared', 'text', 'lib', 'data', 'dirty pages']
    values = [
        int(x) * resource.getpagesize() for x in open(f'/proc/{os.getpid()}/statm', encoding='utf-8').read().split(' ')
    ]
    return dict(zip(statm_labels, values))


def print_mem_diff(mem0, mem1, action_message=None):
    if action_message:
        print(f"Memory change after '{action_message}'")
    memdiff = mem1.copy()
    for key, value in mem0.items():
        memdiff[key] -= value
    pprint.pprint(memdiff)
    print(f"Total size: {byte_size_format( mem1[ 'size' ] )} B for process {os.getpid()}")
    print()


class MemoryLogger:
    def __init__(self, quiet=False):
        self.quiet = quiet
        self.memlog = [("Initial Memory Usage", memory_usage())]
        if not self.quiet:
            print(self.memlog[0][0])
            pprint.pprint(self.memlog[0][1])
            print(f"Total size: {byte_size_format( self.memlog[0][1][ 'size' ] )} B")
            print()

    def log(self, action_message=None):
        self.memlog += [(action_message, memory_usage())]
        if not self.quiet:
            print_mem_diff(self.memlog[-2][1], self.memlog[-1][1], action_message)


def benchmark_tarfile(filename):
    # Remember garbage collector objects
    before = {}
    for i in gc.get_objects():
        if type(i) in before:
            before[type(i)] += 1
        else:
            before[type(i)] = 1

    t0 = time.time()

    mem0 = memory_usage()
    import tarfile

    mem1 = memory_usage()
    print_mem_diff(mem0, mem1, "Memory change after 'import tarfile'")

    with open(filename, 'rb') as file:
        loadedTarFile = tarfile.open(fileobj=file, mode='r:')
        count = 0
        for _fileinfo in loadedTarFile:
            count += 1

        print("Files in TAR", count)
        mem2 = memory_usage()
        print_mem_diff(mem1, mem2, 'iterate over TAR')

        loadedTarFile.members = []
        mem2b = memory_usage()
        print_mem_diff(mem2, mem2b, 'deleted tarfile members')

    mem3 = memory_usage()
    print_mem_diff(mem2, mem3, 'with open TAR file')

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


benchmark_tarfile(sys.argv[1])
