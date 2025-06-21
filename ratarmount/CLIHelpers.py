import argparse
import contextlib
import importlib
import os
import sys

from ratarmountcore.compressions import COMPRESSION_BACKENDS, check_for_split_file
from ratarmountcore.formats import FileFormatID, detect_formats
from ratarmountcore.mountsource.archives import ARCHIVE_BACKENDS
from ratarmountcore.utils import is_random

try:
    import fsspec
except ImportError:
    fsspec = None  # type: ignore

try:
    import sqlcipher3
except ImportError:
    sqlcipher3 = None  # type: ignore


def check_input_file_type(path: str, printDebug: int = 0) -> str:
    """Raises an exception if it is not an accepted archive format else returns the real path."""

    splitURI = path.split('://')
    if len(splitURI) > 1:
        protocol = splitURI[0]
        if fsspec is None:
            raise argparse.ArgumentTypeError("Detected an URI, but fsspec was not found. Try: pip install fsspec.")
        if protocol not in fsspec.available_protocols():
            raise argparse.ArgumentTypeError(
                f"URI: {path} uses an unknown protocol. Protocols known by fsspec are: "
                + ', '.join(fsspec.available_protocols())
            )
        return path

    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"File '{path}' is not a file!")
    path = os.path.realpath(path)

    result = check_for_split_file(path)
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
                    print(f"[Warning] The specified file '{path}'")
                    print("[Warning] is compressed using zstd but only contains one zstd frame. This makes it ")
                    print("[Warning] impossible to use true seeking! Please (re)compress your TAR using multiple ")
                    print("[Warning] frames in order for ratarmount to do be able to do fast seeking to requested ")
                    print("[Warning] files. Else, each file access will decompress the whole TAR from the beginning!")
                    print("[Warning] You can try out t2sz for creating such archives:")
                    print("[Warning] https://github.com/martinellimarco/t2sz")
                    print("[Warning] Here you can find a simple bash script demonstrating how to do this:")
                    print("[Warning] https://github.com/mxmlnkn/ratarmount#xz-and-zst-files")
                    print()
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

            if printDebug >= 2:
                print(f"Archive '{path}' (format: {sorted(fid.name for fid in formats)}) cannot be opened!")

            if printDebug >= 1:
                print("[Info] Supported compressions:", sorted(fid.name for fid in supportedCompressions))

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
                        print(
                            "[Warning] It seems that the libarchive backend is not available. Try installing it with:"
                        )
                        print("[Warning]  - apt install libarchive13")
                        print("[Warning]  - yum install libarchive")
                    raise argparse.ArgumentTypeError(
                        f"Cannot open a {','.join(intersectingFormats)} archive '{fileobj.name}' "
                        f"without module: {module}. Importing the module raised an exception: {exception}"
                    ) from exception

    # TODO test error messages
    return path
