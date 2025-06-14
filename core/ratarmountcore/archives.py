#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dataclasses
import traceback

from typing import Any, Callable, Dict, IO, List, Optional, Set, Tuple, Union

from .formats import FileFormatID

from .MountSource import MountSource
from .ASARMountSource import ASARMountSource
from .EXT4MountSource import EXT4MountSource
from .FATMountSource import FATMountSource
from .Py7zrMountSource import Py7zrMountSource
from .RarMountSource import RarMountSource
from .SQLiteIndexMountSource import SQLiteIndexMountSource
from .SQLiteIndexedTar import SQLiteIndexedTar
from .SQLARMountSource import SQLARMountSource
from .SquashFSMountSource import SquashFSMountSource
from .ZipMountSource import ZipMountSource
from .LibarchiveMountSource import LibarchiveMountSource

try:
    import libarchive
except (ImportError, AttributeError):
    libarchive = None  # type: ignore

FID = FileFormatID


def _openTarMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> MountSource:
    if isinstance(fileOrPath, str):
        if 'tarFileName' in options:
            copiedOptions = options.copy()
            del copiedOptions['tarFileName']
            return SQLiteIndexedTar(fileOrPath, **copiedOptions)
        return SQLiteIndexedTar(fileOrPath, **options)
    return SQLiteIndexedTar(fileObject=fileOrPath, **options)


def _openLibarchiveMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    if libarchive is None:
        return None

    printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

    try:
        if printDebug >= 2:
            print("[Info] Trying to open archive with libarchive backend.")
        return LibarchiveMountSource(fileOrPath, **options)
    except Exception as exception:
        if printDebug >= 2:
            print("[Info] Checking for libarchive file raised an exception:", exception)
        if printDebug >= 3:
            traceback.print_exc()
    finally:
        try:
            if hasattr(fileOrPath, 'seek'):
                fileOrPath.seek(0)  # type: ignore
        except Exception as exception:
            if printDebug >= 1:
                print("[Info] seek(0) raised an exception:", exception)
            if printDebug >= 2:
                traceback.print_exc()
    return None


@dataclasses.dataclass
class ArchiveBackendInfo:
    # Opens a MountSource from a path or file object and additional options (kwargs).
    # Note that the MountSource derived classes themselves (or rather their __init__) are fitting callables!
    open: Callable[[Any], Optional[MountSource]]
    # Supported file formats. These are for quick checks and prioritization based on file extension.
    formats: Set[FileFormatID]
    # If a format is suspected e.g. by extension or by a non-module dependent format check,
    # the modules listed here are checked and the module package name can be suggested to be installed.
    # Tuple: (module name, package name)
    requiredModules: List[Tuple[str, str]]


# Map of backends to their respective open-function. The order implies a priority.
# The priority is overwritten by the associated file formats for a backend, which is used to look up
# file extensions via ARCHIVE_FORMATS. Furthermore, the backend is only tried if any of the associated
# file format checkers returns True!
# The keys are the backend names the user can specify with --backends or via prioritizedBackends arguments.
ARCHIVE_BACKENDS: Dict[str, ArchiveBackendInfo] = {
    "rarfile": ArchiveBackendInfo(RarMountSource, {FID.RAR}, [('rarfile', 'rarfile')]),
    "tarfile": ArchiveBackendInfo(
        _openTarMountSource, {FID.TAR, FID.GZIP, FID.BZIP2, FID.XZ, FID.ZSTANDARD}, [('tarfile', '')]
    ),
    "zipfile": ArchiveBackendInfo(ZipMountSource, {FID.ZIP}, [('zipfile', '')]),
    "PySquashfsImage": ArchiveBackendInfo(
        SquashFSMountSource, {FID.SQUASHFS}, [('PySquashfsImage', 'PySquashfsImage')]
    ),
    # Formats are handily listed either in libarchive:
    # https://github.com/libarchive/libarchive/blob/6110e9c82d8ba830c3440f36b990483ceaaea52c/libarchive/
    #   archive_read_support_format_all.c#L32
    # Or in python-libarchive-c:
    # https://github.com/Changaco/python-libarchive-c/blob/5f7008d876103bac84c40905d00bb6b5afbab91a/libarchive/
    #   ffi.py#L243
    # Supported by other backends: tar, rar, zip
    # Not supported because it has no magic identification and therefore is only trouble: lha, mtree
    # http://fileformats.archiveteam.org/wiki/LHA
    # > LHA can be identified with high accuracy, but doing so can be laborious,
    # > due to the lack of a signature, and other complicating factors.
    "libarchive": ArchiveBackendInfo(
        _openLibarchiveMountSource,
        {
            FID.AR,
            FID.CAB,
            FID.XAR,
            FID.CPIO,
            FID.ISO9660,
            FID.WARC,
            # py7zr cannot handle symbolic links, therefore prefer libarchive!
            # But, libarchive does not support decrpytion.
            FID.SEVEN_ZIP,
            # Archive formats supported by other backends with higher precedence
            FID.RAR,
            FID.TAR,
            FID.ZIP,
            # Compression formats
            FID.BZIP2,
            FID.GZIP,
            FID.XZ,
            FID.ZSTANDARD,
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
    ),
    "py7zr": ArchiveBackendInfo(Py7zrMountSource, {FID.SEVEN_ZIP}, [('py7zr', 'py7zr')]),
    "pyfatfs": ArchiveBackendInfo(FATMountSource, {FID.FAT}, [('pyfatfs', 'pyfatfs')]),
    "ext4": ArchiveBackendInfo(EXT4MountSource, {FID.EXT4}, [('ext4', 'ext4')]),
    "asar": ArchiveBackendInfo(ASARMountSource, {FID.ASAR}, []),
    "sqlar": ArchiveBackendInfo(SQLARMountSource, {FID.SQLAR}, []),
    "RatarmountIndex": ArchiveBackendInfo(SQLiteIndexMountSource, {FID.RATARMOUNT_INDEX}, []),
}
