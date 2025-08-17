import concurrent.futures
import dataclasses
import itertools
import logging
import os
import platform
import struct
import sys
from collections.abc import Iterable, Sequence
from typing import IO, Any, Callable, Optional, cast

from .BlockParallelReaders import ParallelXZReader
from .formats import ARCHIVE_FORMATS, COMPRESSION_FORMATS, FID, FileFormatID, might_be_format
from .utils import (
    ALPHA,
    DIGITS,
    HEX,
    CompressionError,
    format_number,
    is_latin_alpha,
    is_latin_digit,
    is_latin_hex_alpha,
    is_on_slow_drive,
)

logger = logging.getLogger(__name__)

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
    import zstandard
except ImportError:
    zstandard = None  # type: ignore


try:
    # Must be imported because find_available_backend checks for it to be in sys.modules!
    # Although, I'm unsure whether it gets implicitly added to sys.modules below when importing file_reader.
    # OSError can happen when dependencies are missing, e.g., libicuuc.so.74.
    import libarchive  # pylint: disable=unused-import
except Exception:
    libarchive = None  # type: ignore

try:
    from libarchive import file_reader as libarchive_file_reader
except Exception:

    def libarchive_file_reader(path):
        raise ImportError("Please install python-libarchive-c with: pip install libarchive-c")


TAR_CONTRACTED_EXTENSIONS: dict[FileFormatID, list[str]] = {
    FID.BZIP2: ['tb2', 'tbz', 'tbz2', 'tz2'],
    FID.GZIP: ['taz', 'tgz'],
    FID.XZ: ['txz'],
    FID.ZSTANDARD: ['tzst'],
}


@dataclasses.dataclass
class CompressionBackendInfo:
    # Opens a file object from a path or file object and additional options (kwargs).
    open: Callable[..., IO[bytes]]
    # Supported file formats. These are for quick checks and prioritization based on file extension.
    formats: set[FileFormatID]
    # If a format is suspected e.g. by extension or by a non-module dependent format check,
    # the modules listed here are checked and the module package name can be suggested to be installed.
    # Tuple: (module name, package name)
    requiredModules: list[tuple[str, str]]
    delegatedArchiveBackend: str


COMPRESSION_BACKENDS: dict[str, CompressionBackendInfo] = {
    'rapidgzip-bzip2': CompressionBackendInfo(
        (lambda x, parallelization=0: rapidgzip.IndexedBzip2File(x, parallelization=parallelization)),
        {FID.BZIP2},
        [('rapidgzip', 'rapidgzip')],
        'tarfile',
    ),
    'rapidgzip': CompressionBackendInfo(
        (lambda x, parallelization=1: rapidgzip.RapidgzipFile(x, parallelization=parallelization)),
        {FID.GZIP, FID.ZLIB},
        [('rapidgzip', 'rapidgzip')],
        'tarfile',
    ),
    'indexed_gzip': CompressionBackendInfo(
        (lambda x, parallelization=1: indexed_gzip.IndexedGzipFile(fileobj=x)),
        {FID.GZIP},
        [('indexed_gzip', 'indexed_gzip')],
        'tarfile',
    ),
    # Prioritize xz over lzmaffi
    'xz': CompressionBackendInfo(
        (lambda x, parallelization=1: cast(IO[bytes], xz.open(x))), {FID.XZ}, [('xz', 'python-xz')], 'tarfile'
    ),
    'lzmaffi': CompressionBackendInfo(
        (lambda x, parallelization=1: lzmaffi.open(x)), {FID.XZ}, [('lzmaffi', 'lzmaffi')], 'tarfile'
    ),
    'indexed_zstd': CompressionBackendInfo(
        (lambda x, parallelization=1: indexed_zstd.IndexedZstdFile(x.fileno())),
        {FID.ZSTANDARD},
        [('indexed_zstd', 'indexed_zstd')],
        'tarfile',
    ),
    'libarchive': CompressionBackendInfo(
        # TODO Does not work because libarchive_file_reader is some kind of context manager.
        #      This only affects detect_compression for undoing the compression in SQLiteIndexedTar,
        #      so not that important for now.
        (lambda x, parallelization=1: libarchive_file_reader(x)),
        {
            # Also supported by other backends, which should be used first.
            FID.BZIP2,
            FID.GZIP,
            FID.XZ,
            FID.ZSTANDARD,
            # Compression formats
            FID.GRZIP,
            FID.LRZIP,
            FID.LZ4,
            FID.LZIP,
            FID.LZMA,
            FID.LZOP,
            FID.RPM,
            FID.UU,
            FID.Z,
        },
        [('libarchive', 'libarchive-c')],
        'libarchive',
    ),
}


def find_available_backend(
    compression: FileFormatID,
    enabledBackends: Optional[Sequence[str]] = None,
    prioritizedBackends: Optional[Sequence[str]] = None,
) -> Optional[CompressionBackendInfo]:
    if prioritizedBackends is None:
        prioritizedBackends = []

    matchingBackends = [
        backend
        for backend, info in COMPRESSION_BACKENDS.items()
        if (enabledBackends is None or backend in enabledBackends) and compression in info.formats
    ]

    for backendName in prioritizedBackends:
        if backendName not in matchingBackends or backendName not in COMPRESSION_BACKENDS:
            continue
        backend = COMPRESSION_BACKENDS[backendName]
        if all(module in sys.modules for module, _ in backend.requiredModules):
            return backend

    for backendName in matchingBackends:
        if backendName in prioritizedBackends or backendName not in COMPRESSION_BACKENDS:
            continue
        backend = COMPRESSION_BACKENDS[backendName]
        if all(module in sys.modules for module, _ in backend.requiredModules):
            return backend

    return None


def strip_suffix_from_compressed_file(path: str) -> str:
    """Strips compression suffixes like .bz2, .gz, ..."""
    for formatInfo in COMPRESSION_FORMATS.values():
        for extension in formatInfo.extensions:
            if path.lower().endswith('.' + extension.lower()):
                return path[: -(len(extension) + 1)]
    return path


def strip_suffix_from_archive(path: str) -> str:
    """Strips extensions like .tar.gz or .gz or .tgz, .rar, .zip ..."""
    extensions = itertools.chain(
        (e for extensions in TAR_CONTRACTED_EXTENSIONS.values() for e in extensions),
        ('t' + e for formatInfo in COMPRESSION_FORMATS.values() for e in formatInfo.extensions),
        ('tar.' + e for formatInfo in COMPRESSION_FORMATS.values() for e in formatInfo.extensions),
        (e for formatInfo in COMPRESSION_FORMATS.values() for e in formatInfo.extensions),
        (e for formatInfo in ARCHIVE_FORMATS.values() for e in formatInfo.extensions),
    )
    for extension in extensions:
        if path.lower().endswith('.' + extension.lower()):
            return path[: -(len(extension) + 1)]
    return path


def has_matching_alphabets(a: str, b: str):
    return (
        (is_latin_alpha(a) and is_latin_alpha(b))
        or (is_latin_digit(a) and is_latin_digit(b))
        or (is_latin_hex_alpha(a) and is_latin_hex_alpha(b))
    )


def check_for_sequence(extensions: Iterable[str], numberFormatter: Callable[[int], str]) -> list[str]:
    suffixSequence: list[str] = []
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


def check_for_split_file_in(path: str, candidateNames: Iterable[str]) -> Optional[tuple[list[str], str]]:
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

    filename = os.path.split(os.path.realpath(path))[1]  # Get file extensions
    if '.' not in filename:
        return None
    basename, extension = filename.rsplit('.', maxsplit=1)

    # Collect all other files in the folder that might belong to the same split.
    extensions = [name[len(basename) + 1 :] for name in candidateNames if name.startswith(basename + '.')]
    extensions = [e for e in extensions if has_matching_alphabets(e, extension)]
    if not extensions:
        return None
    assert extension in extensions

    # Note that even if something consists only of letters or only digits it still might be hexadecimal encoding!
    maxFormatSpecifier = ''
    maxExtensions: list[str] = []
    for formatSpecifier, baseDigits in [('a', ALPHA), ('0', DIGITS), ('x', HEX)]:
        extensionSequence = check_for_sequence(
            extensions, lambda i, baseDigits=baseDigits: format_number(i, baseDigits, len(extension))  # type: ignore
        )
        if len(extensionSequence) > len(maxExtensions):
            maxFormatSpecifier = formatSpecifier
            maxExtensions = extensionSequence

    if maxFormatSpecifier and len(maxExtensions) > 1:
        paths = [path.rsplit('.', maxsplit=1)[0] + '.' + extension for extension in maxExtensions]
        return paths, maxFormatSpecifier

    return None


def check_for_split_file_in_folder(path: str) -> Optional[tuple[list[str], str]]:
    try:
        return check_for_split_file_in(path, os.listdir(os.path.dirname(path) or '.'))
    except FileNotFoundError:
        pass
    return None


def _compress_zstd(data):
    return zstandard.ZstdCompressor().compress(data)


def compress_zstd(filePath: str, outputFilePath: str, frameSize: int, parallelization: Optional[int] = None):
    """
    Compresses filePath into outputFilePath with one zstandard frame for each frameSize chunk of uncompressed data.
    """
    if not parallelization:
        parallelization = os.cpu_count()
        assert parallelization is not None, "Cannot automatically determine CPU count!"

    with (
        open(filePath, 'rb') as file,
        open(outputFilePath, 'wb') as compressedFile,
        concurrent.futures.ThreadPoolExecutor(parallelization) as pool,
    ):
        results = []
        while toCompress := file.read(frameSize):
            results.append(pool.submit(_compress_zstd, toCompress))
            while len(results) >= parallelization:
                compressedData = results.pop(0).result()
                compressedFile.write(compressedData)

        while results:
            compressedFile.write(results.pop(0).result())


def get_gzip_info(fileobj: IO[bytes]) -> Optional[tuple[str, int]]:
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


def detect_compression(
    fileobj: IO[bytes], prioritizedBackends: Optional[Sequence[str]] = None
) -> Optional[FileFormatID]:
    # isinstance(fileobj, io.IOBase) does not work for everything, e.g., for paramiko.sftp_file.SFTPFile
    # because it does not inherit from io.IOBase. Therefore, do duck-typing and test for required methods.
    expectedMethods = ['seekable', 'seek', 'read', 'tell']
    isNotFileObject = any(not hasattr(fileobj, method) for method in expectedMethods)
    if isNotFileObject or not fileobj.seekable():
        logger.info(
            "Cannot detect compression for given Python object %s "
            "because it does not look like a file object or is not seekable (%s).",
            fileobj,
            None if isNotFileObject else fileobj.seekable(),
        )
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Object attributes: %s", dir(fileobj))
            for name in ['readable', 'seekable', 'writable', 'closed', 'tell']:
                method = getattr(fileobj, name, None)
                if method is not None:
                    logger.debug("  fileobj.%s: %s", name, method() if callable(method) else method)
        return None

    oldOffset = fileobj.tell()
    for compressionId in COMPRESSION_FORMATS:
        if not might_be_format(fileobj, compressionId):
            continue

        backend = find_available_backend(compressionId, prioritizedBackends=prioritizedBackends)
        # If no appropriate module exists, then don't do any further checks.
        if not backend:
            logger.warning(
                "A given file with magic bytes for %s could not be opened because "
                "no appropriate Python module could be loaded. Are some dependencies missing? To install "
                "ratarmountcore with all dependencies do: python3 -m pip install --user ratarmountcore[full]",
                compressionId.name,
            )
            return None

        try:
            # Disable parallelization because it may lead to unnecessary expensive prefetching.
            # We only need to read 1 byte anyway to verify correctness.
            compressedFileobj = backend.open(fileobj, parallelization=1)

            # Reading 1B from a single-frame zst file might require decompressing it fully in order
            # to get uncompressed file size! Avoid that. The magic bytes should suffice mostly.
            # TODO: Make indexed_zstd not require the uncompressed size for the read call.
            if compressionId != FID.ZSTANDARD:
                compressedFileobj.read(1)
            compressedFileobj.close()
            fileobj.seek(oldOffset)
            return compressionId
        except Exception as exception:
            logger.info(
                "A given file with magic bytes for %s could not be opened because: %s",
                compressionId,
                exception,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )
            fileobj.seek(oldOffset)

    return None


def use_rapidgzip(
    fileobj: IO[bytes],
    gzipSeekPointSpacing: int = 16 * 1024 * 1024,
    prioritizedBackends: Optional[Sequence[str]] = None,
) -> bool:
    if fileobj is None:
        return False

    if 'rapidgzip' not in sys.modules:
        logger.warning("Cannot use rapidgzip for access to gzip file because it is not installed. Try:")
        logger.warning("    python3 -m pip install --user rapidgzip")
        return False

    # Check whether indexed_gzip might have a higher priority than rapidgzip if both are listed.
    if (
        prioritizedBackends
        and 'indexed_gzip' in prioritizedBackends
        and (
            (
                'rapidgzip' in prioritizedBackends
                and prioritizedBackends.index('indexed_gzip') < prioritizedBackends.index('rapidgzip')
            )
            or 'rapidgzip' not in prioritizedBackends
        )
    ):
        # Low index have higher priority (because normally the list would be checked from lowest indexes).
        return False

    # Only allow mounting of real files. Rapidgzip does work with Python file objects but we don't want to
    # mount recursive archives all with the parallel gzip decoder because then the cores would be oversubscribed!
    # Similarly, small files would result in being wholly cached into memory, which probably isn't what the user
    # had intended by using ratarmount?
    isRealFile = hasattr(fileobj, 'name') and fileobj.name and os.path.isfile(fileobj.name)
    hasMultipleChunks = isRealFile and os.stat(fileobj.name).st_size >= 4 * gzipSeekPointSpacing
    if not hasMultipleChunks:
        if logger.isEnabledFor(logging.INFO):
            logger.info("Do not reopen with rapidgzip backend because:")
            if not isRealFile:
                logger.info(
                    " - the file to open is a recursive file, which limits the usability of parallel decompression."
                )
            if not hasMultipleChunks:
                logger.info(" - is too small to qualify for parallel decompression.")
        return False

    return True


def open_compressed_file(
    fileobj: IO[bytes],
    gzipSeekPointSpacing: int = 16 * 1024 * 1024,
    parallelizations: Optional[dict[str, int]] = None,
    enabledBackends: Optional[Sequence[str]] = None,
    prioritizedBackends: Optional[Sequence[str]] = None,
) -> tuple[Any, Optional[IO[bytes]], Optional[FileFormatID]]:
    """
    Opens a file possibly undoing the compression.
    Returns (tar_file_obj, raw_file_obj, compression).
    raw_file_obj will be none if compression is None.
    """
    compression = detect_compression(fileobj, prioritizedBackends=prioritizedBackends)
    logger.debug("Detected compression %s for file object: %s", compression, fileobj)
    if not compression:
        return fileobj, None, compression

    matchingBackends = [
        backend
        for backend, info in COMPRESSION_BACKENDS.items()
        if (enabledBackends is None or backend in enabledBackends) and compression in info.formats
    ]
    if not matchingBackends:
        return fileobj, None, compression

    backend = find_available_backend(
        compression, enabledBackends=enabledBackends, prioritizedBackends=prioritizedBackends
    )
    if not backend:
        packages = [
            package for backend in matchingBackends for _, package in COMPRESSION_BACKENDS[backend].requiredModules
        ]
        raise CompressionError(
            f"Cannot open a {compression} compressed TAR file '{fileobj.name}' "
            f"without any of these packages: {packages}"
        )

    parallelization = 1
    if parallelizations is None:
        parallelizations = {}

    if compression == FID.GZIP:
        if use_rapidgzip(fileobj, gzipSeekPointSpacing=gzipSeekPointSpacing, prioritizedBackends=prioritizedBackends):
            isRealFile = hasattr(fileobj, 'name') and fileobj.name and os.path.isfile(fileobj.name)
            parallelization = (
                1
                if isRealFile and is_on_slow_drive(fileobj.name)
                else parallelizations.get(
                    'rapidgzip-gzip', parallelizations.get('rapidgzip', parallelizations.get('', 1))
                )
            )
            logger.debug(
                "Parallelization to use for rapidgzip backend: %d, slow drive detected: %s",
                parallelization,
                is_on_slow_drive(fileobj.name),
            )
            decompressedFileObject = rapidgzip.RapidgzipFile(
                fileobj,
                parallelization=parallelization,
                verbose=logger.isEnabledFor(logging.INFO),
                chunk_size=gzipSeekPointSpacing,
            )
        else:
            # The buffer size must be much larger than the spacing or else there will be large performance penalties
            # even for reading sequentially, see https://github.com/pauldmccarthy/indexed_gzip/issues/89
            # Use 4x spacing because each raw read seeks from the last index point even if the position did not
            # change since the last read call. On average, this incurs an overhead of spacing / 2. For 3x spacing,
            # thisoverhead would be 1/6 = 17%, which should be negligible. The increased memory-usage is not an
            # issue because internally many buffers are allocated with 4 * spacing size.
            bufferSize = max(3 * 1024 * 1024, 3 * gzipSeekPointSpacing)
            # drop_handles keeps a file handle opening as is required to call tell() during decoding
            decompressedFileObject = indexed_gzip.IndexedGzipFile(
                fileobj=fileobj, drop_handles=False, spacing=gzipSeekPointSpacing, buffer_size=bufferSize
            )
    elif compression == FID.BZIP2:
        parallelization = parallelizations.get('rapidgzip-bzip2', parallelizations.get('', 1))
        decompressedFileObject = rapidgzip.IndexedBzip2File(fileobj, parallelization=parallelization)  # type: ignore
    elif (
        compression == FID.XZ
        and xz
        and parallelizations.get('xz', parallelizations.get('', 1)) != 1
        and hasattr(fileobj, 'name')
        and os.path.isfile(fileobj.name)
        and platform.system() == 'Linux'
    ):
        block_boundaries = getattr(fileobj, 'block_boundaries', [])
        decompressedFileObject = backend.open(fileobj)
        if block_boundaries and len(block_boundaries) > 1:
            parallelization = parallelizations.get('xz', parallelizations.get('', 1))
            decompressedFileObject.close()
            decompressedFileObject = ParallelXZReader(fileobj.name, parallelization=parallelization)
    else:
        decompressedFileObject = backend.open(fileobj)

    logger.debug(
        "Undid %s file compression by using: %s with parallelization=%d",
        compression,
        type(decompressedFileObject).__name__,
        parallelization,
    )

    return decompressedFileObject, fileobj, compression
