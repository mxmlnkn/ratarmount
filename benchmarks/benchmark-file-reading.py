import time
import os
import sys
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

folder = sys.argv[1]

files = [os.path.join(r, f) for r, d, fs in os.walk(folder) for f in fs]


def read_file(path):
    with open(path, 'rb') as fp:
        return fp.read()


start = time.time()
with ProcessPoolExecutor(max_workers=3) as executor:
    list(executor.map(read_file, files))
elapsed = time.time() - start
print(f'Process-parallel: {elapsed:.3f} s')

start = time.time()
with ThreadPoolExecutor(max_workers=3) as executor:
    list(executor.map(read_file, files))
elapsed = time.time() - start
print(f'Threaded: {elapsed:.3f} s')


start = time.time()
for f in files:
    read_file(f)
elapsed = time.time() - start
print(f'Sequential: {elapsed:.3f} s')

start = time.time()
for f in files:
    Path(f).read_bytes()
elapsed = time.time() - start
print(f'Sequential pathlib: {elapsed:.3f} s')


# Sequential: 71.915 s          # Very first uncached run!
# Sequential: 5.155 s
# Sequential: 73.225 s          # After unmount and remounting to clear (file) system caches!
#
# Sequential pathlib: 5.703 s
# Threaded (3x): 1.920 s
# Threaded (3x): 71.532 s            # After unmount and remounting
# Process-parallel (3x): 2.208 s
# Process-parallel (3x): 71.438 s    # After unmount and remounting
#
# -> Seems like parallelization helps nothing in uncached situations :(
