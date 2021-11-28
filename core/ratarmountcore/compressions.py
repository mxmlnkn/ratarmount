#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import collections
import sys

try:
    import indexed_bzip2
except ImportError:
    indexed_bzip2 = None

try:
    import indexed_gzip
except ImportError:
    indexed_gzip = None

try:
    import indexed_zstd
except ImportError:
    indexed_zstd = None

try:
    import lzmaffi
except ImportError:
    lzmaffi = None

try:
    import xz
except ImportError:
    if 'xz' not in sys.modules:
        # For some reason, only this import triggers mypy. All the others are fine.
        # Should be something like Optional[Module] but there is no Module type.
        xz = None  # type: ignore

try:
    import rarfile
except ImportError:
    rarfile = None


# The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
# Therefore disable ZIP support there!
# I don't see it documented, instead I tested different Python versions with Docker.
if sys.version_info[1] > 6:
    import zipfile
else:
    zipfile = None


# Defining lambdas does not yet check the names of entities used inside the lambda!
CompressionInfo = collections.namedtuple(
    'CompressionInfo', ['suffixes', 'doubleSuffixes', 'moduleName', 'checkHeader', 'open']
)


supportedCompressions = {
    'bz2': CompressionInfo(
        ['bz2', 'bzip2'],
        ['tb2', 'tbz', 'tbz2', 'tz2'],
        'indexed_bzip2',
        lambda x: (x.read(4)[:3] == b'BZh' and x.read(6) == (0x314159265359).to_bytes(6, 'big')),
        lambda x: indexed_bzip2.open(x),
    ),
    'gz': CompressionInfo(
        ['gz', 'gzip'],
        ['taz', 'tgz'],
        'indexed_gzip',
        lambda x: x.read(2) == b'\x1F\x8B',
        lambda x: indexed_gzip.IndexedGzipFile(fileobj=x),
    ),
    'rar': CompressionInfo(
        ['rar'],
        [],
        'rarfile',
        lambda x: x.read(6) == b'Rar!\x1A\x07',
        lambda x: rarfile.RarFile(x),
    ),
    'xz': CompressionInfo(
        ['xz'],
        ['txz'],
        'lzmaffi' if 'lzmaffi' in sys.modules else 'xz',
        lambda x: x.read(6) == b"\xFD7zXZ\x00",
        (lambda x: lzmaffi.open(x)) if 'lzmaffi' in sys.modules else (lambda x: xz.open(x)),
    ),
    'zip': CompressionInfo(
        ['zip'],
        [],
        'zipfile',
        lambda x: x.read(2) == b'PK',
        lambda x: zipfile.ZipFile(x),
    ),
    'zst': CompressionInfo(
        ['zst', 'zstd'],
        ['tzst'],
        'indexed_zstd',
        lambda x: x.read(4) == (0xFD2FB528).to_bytes(4, 'little'),
        lambda x: indexed_zstd.IndexedZstdFile(x.fileno()),
    ),
}


def stripSuffixFromCompressedFile(path: str) -> str:
    """Strips compression suffixes like .bz2, .gz, ..."""
    for compression in supportedCompressions.values():
        for suffix in compression.suffixes:
            if path.lower().endswith('.' + suffix.lower()):
                return path[: -(len(suffix) + 1)]

    return path


def stripSuffixFromTarFile(path: str) -> str:
    """Strips extensions like .tar.gz or .gz or .tgz, ..."""
    # 1. Try for conflated suffixes first
    for compression in supportedCompressions.values():
        for suffix in compression.doubleSuffixes + ['t' + s for s in compression.suffixes]:
            if path.lower().endswith('.' + suffix.lower()):
                return path[: -(len(suffix) + 1)]

    # 2. Remove compression suffixes
    path = stripSuffixFromCompressedFile(path)

    # 3. Remove .tar if we are left with it after the compression suffix removal
    if path.lower().endswith('.tar'):
        path = path[:-4]

    return path
