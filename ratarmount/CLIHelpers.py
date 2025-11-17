import argparse
import contextlib
import importlib
import json
import logging
import os
import sys
from typing import Any

from ratarmountcore.compressions import COMPRESSION_BACKENDS, check_for_split_file_in_folder
from ratarmountcore.formats import FileFormatID, detect_formats
from ratarmountcore.mountsource.archives import ARCHIVE_BACKENDS
from ratarmountcore.utils import determine_recursion_depth, is_random

try:
    import fsspec
except ImportError:
    fsspec = None  # type: ignore

try:
    import sqlcipher3
except ImportError:
    sqlcipher3 = None  # type: ignore


logger = logging.getLogger(__name__)


def check_input_file_type(path: str) -> str:
    """Raises an exception if it is not an accepted archive format else returns the real path."""

    splitURI = path.split('://')
    if len(splitURI) > 1:
        if fsspec is None:
            raise argparse.ArgumentTypeError("Detected an URI, but fsspec was not found. Try: pip install fsspec.")
        if not all(protocol in fsspec.available_protocols() for protocol in splitURI[0].split('::')):
            raise argparse.ArgumentTypeError(
                f"URI: {path} uses an unknown protocol. Protocols known by fsspec are: "
                + ', '.join(fsspec.available_protocols())
            )
        return path

    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"File '{path}' is not a file!")
    path = os.path.realpath(path)

    result = check_for_split_file_in_folder(path)
    if result:
        return result[0][0]

    with open(path, 'rb') as fileobj:
        formats = detect_formats(fileobj)
        # SQLAR will always appear because the encrypted version has no magic bytes. (Subject to change)
        # Formats which have no magic bytes and require modules for checking will also always appear, e.g., EXT4,
        # however, those should filtered when looking for backends supporting those.
        if FileFormatID.SQLAR in formats:
            formats.remove(FileFormatID.SQLAR)

        if FileFormatID.ZSTANDARD in formats:
            try:
                zstdFile = COMPRESSION_BACKENDS['indexed_std'].open(fileobj)

                # Determining if there are many frames in zstd is O(1) with is_multiframe
                is_multiframe = getattr(zstdFile, 'is_multiframe', None)
                if is_multiframe and not is_multiframe() and os.stat(path).st_size > 1024 * 1024:
                    logger.warning(
                        "The specified file '%s' is compressed using zstd but only contains one zstd frame."
                        "This makes it impossible to use true seeking! Please (re)compress your TAR using multiple "
                        "frames in order for ratarmount to do be able to do fast seeking to requested files. "
                        "Else, each file access will decompress the whole TAR from the beginning!",
                        path,
                    )
                    logger.warning("You can try out t2sz for creating such archives:")
                    logger.warning("    https://github.com/martinellimarco/t2sz")
                    logger.warning("Here you can find a simple bash script demonstrating how to do this:")
                    logger.warning("    https://github.com/mxmlnkn/ratarmount#xz-and-zst-files")
                    logger.warning("")
            except Exception:
                pass

        # 1. Find any working backend for any of the possible formats.
        for backend in list(ARCHIVE_BACKENDS.values()) + list(COMPRESSION_BACKENDS.values()):
            if not formats.intersection(backend.formats):
                continue

            # Try importing required modules in case something went wrong there.
            # Normally this should be done in ratarmountcore.archives and ratarmountcore.compressions.
            # Do not yet return errors because another backend could work.
            for module, _ in backend.requiredModules:
                if module not in sys.modules:
                    with contextlib.suppress(Exception):
                        importlib.import_module(module)

            if all(module in sys.modules for module, _ in backend.requiredModules):
                return path

        # 2. Check for some obscure archive formats.
        supportedCompressions = {
            fid
            for backend in list(ARCHIVE_BACKENDS.values()) + list(COMPRESSION_BACKENDS.values())
            for fid in backend.formats
            if all(module in sys.modules for module, _ in backend.requiredModules)
        }
        if not supportedCompressions.intersection(formats):
            if sqlcipher3 is not None and path.lower().endswith(".sqlar") and is_random(fileobj.read(4096)):
                return path

            if logger.isEnabledFor(logging.WARNING):
                logger.warning(
                    "Archive '%s' (format: %s) cannot be opened. Supported compressions: %s",
                    path,
                    sorted(fid.name for fid in formats),
                    sorted(fid.name for fid in supportedCompressions),
                )

            raise argparse.ArgumentTypeError(f"Archive '{path}' cannot be opened!")

        # 2. Try importing possible modules again and print helpful error messages if there is an error.
        for backend in list(ARCHIVE_BACKENDS.values()) + list(COMPRESSION_BACKENDS.values()):
            intersectingFormats = sorted(fid.name for fid in backend.formats)
            if not intersectingFormats:
                continue

            for module, package in backend.requiredModules:
                if module in sys.modules or not package:
                    continue

                try:
                    importlib.import_module(module)
                except ModuleNotFoundError as exception:
                    raise argparse.ArgumentTypeError(
                        f"Cannot open a {','.join(intersectingFormats)} archive '{fileobj.name}' "
                        f"without module: {module}. Try: pip install {package}"
                    ) from exception
                except Exception as exception:
                    if module == 'libarchive':
                        logger.warning("It seems that the libarchive backend is not available. Try installing it with:")
                        logger.warning(" - apt install libarchive13")
                        logger.warning(" - yum install libarchive")
                    raise argparse.ArgumentTypeError(
                        f"Cannot open a {','.join(intersectingFormats)} archive '{fileobj.name}' "
                        f"without module: {module}. Importing the module raised an exception: {exception}"
                    ) from exception

    # TODO test error messages
    return path


def parse_parallelization(parallelization: str) -> dict[str, int]:
    parallelizations = (
        {'': parallelization} if parallelization.isdigit() else dict(kv.split(':') for kv in parallelization.split(','))
    )
    result: dict[str, int] = {}
    defaultParallelization = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()
    if defaultParallelization is None:
        defaultParallelization = 1

    for backend, parallelizationString in parallelizations.items():
        # isdigit does will be false if there is a minus sign, which is what we want.
        if not parallelizationString.isdigit():
            raise argparse.ArgumentTypeError(
                f"Parallelization must be non-negative number but got {parallelizationString} for {backend}!"
            )
        result[backend] = defaultParallelization if int(parallelizationString) == 0 else int(parallelizationString)

    if '' not in result:
        result[''] = defaultParallelization
    return result


def process_trivial_parsed_arguments(args):
    """
    Checks and post-processes 'trivial' arguments, i.e., those that do not depend on others or require
    filesystem access for checks. Ergo, it does not has to check for is_inside_fuse_context.
    """

    for path in args.mount_source:
        if args.mount_source.count(path) > 1:
            raise argparse.ArgumentTypeError(f"Path may not appear multiple times at different locations: {path}")

    args.gzipSeekPointSpacing = int(args.gzip_seek_point_spacing * 1024 * 1024)

    if (args.strip_recursive_tar_extension or args.transform_recursive_mount_point) and determine_recursion_depth(
        recursive=args.recursive, recursion_depth=args.recursion_depth
    ) <= 0:
        logger.warning(
            "The options --strip-recursive-tar-extension and --transform-recursive-mount-point only have an "
            "effect when used with recursive mounting."
        )

    if args.transform_recursive_mount_point:
        args.transform_recursive_mount_point = tuple(args.transform_recursive_mount_point)

    # Sanitize different ways to specify passwords into a simple list
    # Better initialize it before calling check_mount_source, which might use args.passwords in the future.
    args.passwords = []
    if args.password:
        args.passwords.append(args.password.encode())

    # Preprocess the --index-folders list as a string argument
    if args.index_folders and isinstance(args.index_folders, str):
        if args.index_folders[0] == '[':
            args.index_folders = json.loads(args.index_folders)
        elif ',' in args.index_folders:
            args.index_folders = args.index_folders.split(',')
        else:
            args.index_folders = [args.index_folders]

    args.parallelizations = parse_parallelization(args.parallelization)

    # Clean backend list
    args.prioritizedBackends = (
        [backend for backendString in args.use_backend for backend in backendString.split(',')][::-1]
        if args.use_backend
        else []
    )


def parsed_args_to_options(args) -> dict[str, Any]:
    # fmt: off
    return {
        'pathToMount'                  : args.mount_source,
        'clearIndexCache'              : bool(args.recreate_index),
        'recursive'                    : bool(args.recursive),
        'recursionDepth'               : args.recursion_depth,
        'gzipSeekPointSpacing'         : int(args.gzipSeekPointSpacing),
        'mountPoint'                   : args.mount_point,
        'encoding'                     : args.encoding,
        'ignoreZeros'                  : bool(args.ignore_zeros),
        'verifyModificationTime'       : bool(args.verify_mtime),
        'stripRecursiveTarExtension'   : args.strip_recursive_tar_extension,
        'indexFilePath'                : args.index_file,
        'indexFolders'                 : args.index_folders,
        'lazyMounting'                 : bool(args.lazy),
        'passwords'                    : list(args.passwords),
        'parallelizations'             : args.parallelizations,
        'isGnuIncremental'             : args.gnu_incremental,
        'writeOverlay'                 : args.write_overlay,
        'transformRecursiveMountPoint' : args.transform_recursive_mount_point,
        'transform'                    : args.transform,
        'prioritizedBackends'          : args.prioritizedBackends,
        'disableUnionMount'            : args.disable_union_mount,
        'maxCacheDepth'                : args.union_mount_cache_max_depth,
        'maxCacheEntries'              : args.union_mount_cache_max_entries,
        'maxSecondsToCache'            : args.union_mount_cache_timeout,
        'indexMinimumFileCount'        : args.index_minimum_file_count,
        'logFile'                      : args.log_file,
        'enableFileVersions'           : args.file_versions,
        'controlInterface'             : args.control_interface,
        'writeIndex'                   : True,
        'mount'                        : args.mount,
    }
    # fmt: on
