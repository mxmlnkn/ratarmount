#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import collections
import concurrent.futures
import os
import struct
import sys
from typing import Callable, Dict, IO, Iterable, List, Optional, Tuple

from .utils import isLatinAlpha, isLatinDigit, isLatinHexAlpha, formatNumber, ALPHA, DIGITS, HEX

try:
    import indexed_bzip2
except ImportError:
    indexed_bzip2 = None  # type: ignore

try:
    import indexed_gzip
except ImportError:
    indexed_gzip = None  # type: ignore

try:
    import indexed_zstd
except ImportError:
    indexed_zstd = None  # type: ignore

try:
    import lzmaffi
except ImportError:
    lzmaffi = None  # type: ignore

try:
    import xz
except ImportError:
    if 'xz' not in sys.modules:
        # For some reason, only this import triggers mypy. All the others are fine.
        # Should be something like Optional[Module] but there is no Module type.
        xz = None  # type: ignore

try:
    import rapidgzip
except ImportError:
    rapidgzip = None  # type: ignore

try:
    import rarfile
except ImportError:
    rarfile = None  # type: ignore

try:
    import zstandard
except ImportError:
    zstandard = None  # type: ignore


# The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
# Therefore disable ZIP support there!
# I don't see it documented, instead, I tested different Python versions with Docker.
if sys.version_info[0] == 3 and sys.version_info[1] > 6:
    import zipfile
else:
    zipfile = None


CompressionModuleInfo = collections.namedtuple('CompressionModuleInfo', ['name', 'open'])
# Defining lambdas does not yet check the names of entities used inside the lambda!
# "modules" contains a list of CompressionModuleInfo for modules that are available.
# Those appearing first in this list have priority.
CompressionInfo = collections.namedtuple('CompressionInfo', ['suffixes', 'doubleSuffixes', 'modules', 'checkHeader'])


TAR_COMPRESSION_FORMATS: Dict[str, CompressionInfo] = {
    'bz2': CompressionInfo(
        ['bz2', 'bzip2'],
        ['tb2', 'tbz', 'tbz2', 'tz2'],
        [CompressionModuleInfo('indexed_bzip2', lambda x: indexed_bzip2.open(x))],  # type: ignore
        lambda x: (x.read(4)[:3] == b'BZh' and x.read(6) == (0x314159265359).to_bytes(6, 'big')),
    ),
    'gz': CompressionInfo(
        ['gz', 'gzip'],
        ['taz', 'tgz'],
        [
            CompressionModuleInfo('rapidgzip', lambda x: rapidgzip.RapidgzipFile(x)),
            CompressionModuleInfo('indexed_gzip', lambda x: indexed_gzip.IndexedGzipFile(fileobj=x)),
        ],
        lambda x: x.read(2) == b'\x1F\x8B',
    ),
    'xz': CompressionInfo(
        ['xz'],
        ['txz'],
        # Prioritize xz over lzmaffi
        [
            CompressionModuleInfo('xz', lambda x: xz.open(x)),
            CompressionModuleInfo('lzmaffi', lambda x: lzmaffi.open(x)),
        ],
        lambda x: x.read(6) == b"\xFD7zXZ\x00",
    ),
    'zst': CompressionInfo(
        ['zst', 'zstd'],
        ['tzst'],
        [CompressionModuleInfo('indexed_zstd', lambda x: indexed_zstd.IndexedZstdFile(x.fileno()))],
        lambda x: x.read(4) == (0xFD2FB528).to_bytes(4, 'little'),
    ),
}


def isRarFile(fileObject) -> bool:
    # @see https://www.rarlab.com/technote.htm#rarsign
    # > RAR 5.0 signature consists of 8 bytes: 0x52 0x61 0x72 0x21 0x1A 0x07 0x01 0x00.
    # > You need to search for this signature in supposed archive from beginning and up to maximum SFX module size.
    # > Just for comparison this is RAR 4.x 7 byte length signature: 0x52 0x61 0x72 0x21 0x1A 0x07 0x00.
    # > Self-extracting module (SFX)
    # > Any data preceding the archive signature. Self-extracting module size and contents is not defined.
    # > At the moment of writing this documentation RAR assumes the maximum SFX module size to not exceed 1 MB,
    # > but this value can be increased in the future.
    oldPosition = fileObject.tell()
    if fileObject.read(6) == b'Rar!\x1A\x07':
        return True
    if 'rarfile' in sys.modules:
        fileObject.seek(oldPosition)
        fileObject.seek(oldPosition)
        if rarfile.is_rarfile_sfx(fileObject):
            return True
    return False


ARCHIVE_FORMATS: Dict[str, CompressionInfo] = {
    'rar': CompressionInfo(
        ['rar'],
        [],
        [CompressionModuleInfo('rarfile', lambda x: rarfile.RarFile(x))],
        isRarFile,
    ),
    'zip': CompressionInfo(
        ['zip'],
        [],
        [CompressionModuleInfo('zipfile', lambda x: zipfile.ZipFile(x))],
        lambda x: x.read(2) == b'PK',
    ),
}


supportedCompressions = {**TAR_COMPRESSION_FORMATS, **ARCHIVE_FORMATS}


def findAvailableOpen(compression: str, prioritizedBackends: Optional[List[str]] = None) -> Optional[Callable]:
    if compression not in supportedCompressions:
        return None

    modules = supportedCompressions[compression].modules
    if prioritizedBackends:
        for moduleName in prioritizedBackends:
            if moduleName in sys.modules:
                for module in modules:
                    if module.name == moduleName and module.open:
                        return module.open

    for module in modules:
        if module.name in sys.modules:
            return module.open

    return None


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


def hasMatchingAlphabets(a: str, b: str):
    return (
        (isLatinAlpha(a) and isLatinAlpha(b))
        or (isLatinDigit(a) and isLatinDigit(b))
        or (isLatinHexAlpha(a) and isLatinHexAlpha(b))
    )


def checkForSequence(extensions: Iterable[str], numberFormatter: Callable[[int], str]) -> List[str]:
    suffixSequence: List[str] = []
    i = 0
    suffixLength = len(numberFormatter(0))

    while True:
        suffix = numberFormatter(i)
        if suffix in extensions:
            suffixSequence.append(suffix)
        elif i > 0 or len(suffix) != suffixLength:
            # We allow the extensions to start with 0 or 1.
            # So, even if the zeroth does not exist, do not break, instead also test 1.
            break
        i += 1

    return suffixSequence


def checkForSplitFile(path: str) -> Optional[Tuple[List[str], str]]:
    """
    Returns the paths to all files belonging to the split and a string identifying the format.
    The latter is one of: '', 'x', 'a' to specify the numbering system: decimal, hexadecimal, alphabetical.
    The width and starting number can be determined from the extension of the first file path,
    which is returned in the first member of the tuple.
    """

    # Check for split files. Note that GNU coreutils' split 8.32 by default creates files in the form:
    #   xaa, xab, xac, ..., xba, ...
    # However, for now, don't support those. At least a dot should be used to reduce misidentification!
    # Split supports not only alphabetical, but also decimal, and hexadecimal numbering schemes and the
    # latter, both, start at 0, which is also quite unusual.
    # Most files I encountered in the wild used decimal suffixes starting with name.001.
    # Split can be customized a lot with these options:
    #   split [OPTION]... [FILE [PREFIX]]
    #   --suffix-length
    #   --additional-suffix=SUFFIX
    #   --numeric-suffixes[=FROM]
    #   --hex-suffixes[=FROM]
    #   --suffix-length
    # -> It seems like there is no way to specify alphabetical counting to begin at something other than a, aa, ...
    # To avoid false positives, these restrictions to split's options are made:
    #   - FROM is 0 or 1. Anything else is too aberrant to be supported.
    #   - PREFIX ends with a dot (.).

    # These character tests are necessary because Python's built-in isalpha, isdigit and such
    # all also return true for a lot of Unicode alternatives like for the Thai zero.

    folder, filename = os.path.split(os.path.realpath(path))  # Get file extensions
    if '.' not in filename:
        return None
    basename, extension = filename.rsplit('.', maxsplit=1)

    # Collect all other files in the folder that might belong to the same split.
    extensions = [name[len(basename) + 1 :] for name in os.listdir(folder) if name.startswith(basename + '.')]
    extensions = [e for e in extensions if hasMatchingAlphabets(e, extension)]
    if not extensions:
        return None
    assert extension in extensions

    # Note that even if something consists only of letters or only digits it still might be hexadecimal encoding!
    maxFormatSpecifier = ''
    maxExtensions: List[str] = []
    for formatSpecifier, baseDigits in [('a', ALPHA), ('0', DIGITS), ('x', HEX)]:
        extensionSequence = checkForSequence(
            extensions, lambda i, baseDigits=baseDigits: formatNumber(i, baseDigits, len(extension))  # type: ignore
        )
        if len(extensionSequence) > len(maxExtensions):
            maxFormatSpecifier = formatSpecifier
            maxExtensions = extensionSequence

    if maxFormatSpecifier and len(maxExtensions) > 1:
        paths = [path.rsplit('.', maxsplit=1)[0] + '.' + extension for extension in maxExtensions]
        return paths, maxFormatSpecifier

    return None


def _compressZstd(data):
    return zstandard.ZstdCompressor().compress(data)


def compressZstd(filePath: str, outputFilePath: str, frameSize: int, parallelization: Optional[int] = None):
    """
    Compresses filePath into outputFilePath with one zstandard frame for each frameSize chunk of uncompressed data.
    """
    if not parallelization:
        parallelization = os.cpu_count()
        assert parallelization is not None, "Cannot automatically determine CPU count!"

    with open(filePath, 'rb') as file, open(
        outputFilePath, 'wb'
    ) as compressedFile, concurrent.futures.ThreadPoolExecutor(parallelization) as pool:
        results = []
        while True:
            toCompress = file.read(frameSize)
            if not toCompress:
                break
            results.append(pool.submit(_compressZstd, toCompress))
            while len(results) >= parallelization:
                compressedData = results.pop(0).result()
                compressedFile.write(compressedData)

        while results:
            compressedFile.write(results.pop(0).result())


def getGzipInfo(fileobj: IO[bytes]) -> Optional[Tuple[str, int]]:
    id1, id2, compression, flags, mtime, _, _ = struct.unpack('<BBBBLBB', fileobj.read(10))
    if id1 != 0x1F or id2 != 0x8B or compression != 0x08:
        return None

    if flags & (1 << 2) != 0:
        fileobj.read(struct.unpack('<U', fileobj.read(2))[0])

    if flags & (1 << 3) != 0:
        name = b''
        c = fileobj.read(1)
        while c != b'\0':
            name += c
            c = fileobj.read(1)
        return name.decode(), mtime

    return None
