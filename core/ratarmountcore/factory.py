#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import traceback

from typing import IO, Optional, Union

from .compressions import checkForSplitFile, libarchive, rarfile, TAR_COMPRESSION_FORMATS, zipfile
from .utils import CompressionError, RatarmountError
from .MountSource import MountSource
from .FolderMountSource import FolderMountSource
from .RarMountSource import RarMountSource
from .SingleFileMountSource import SingleFileMountSource
from .SQLiteIndexedTar import SQLiteIndexedTar
from .StenciledFile import JoinedFileFromFactory
from .ZipMountSource import ZipMountSource
from .LibarchiveMountSource import LibarchiveMountSource


def _openRarMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        if rarfile is not None and rarfile.is_rarfile_sfx(fileOrPath):
            return RarMountSource(fileOrPath, **options)
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore
    return None


def _openTarMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        if isinstance(fileOrPath, str):
            return SQLiteIndexedTar(fileOrPath, **options)
        return SQLiteIndexedTar(fileObject=fileOrPath, **options)
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore


def _openZipMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        if zipfile is not None and zipfile is not None:
            # is_zipfile might yields some false positives, but those should then raise exceptions, which
            # are caught, so it should be fine. See: https://bugs.python.org/issue42096
            if zipfile.is_zipfile(fileOrPath):
                mountSource = ZipMountSource(fileOrPath, **options)
                return mountSource
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore
    return None


def _openLibarchiveMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    if libarchive is None:
        return None

    printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

    try:
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
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore
    return None


_BACKENDS = {
    "rarfile": _openRarMountSource,
    "tarfile": _openTarMountSource,
    "zipfile": _openZipMountSource,
    "libarchive": _openLibarchiveMountSource,
}


def openMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> MountSource:
    printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

    joinedFileName = ''
    if isinstance(fileOrPath, str):
        if not os.path.exists(fileOrPath):
            raise RatarmountError(f"Mount source does not exist: {fileOrPath}")

        if os.path.isdir(fileOrPath):
            return FolderMountSource('.' if fileOrPath == '.' else os.path.realpath(fileOrPath))

        splitFileResult = checkForSplitFile(fileOrPath)
        if splitFileResult:
            filesToJoin = splitFileResult[0]
            joinedFileName = os.path.basename(filesToJoin[0]).rsplit('.', maxsplit=1)[0]
            if 'indexFilePath' not in options or not options['indexFilePath']:
                options['indexFilePath'] = filesToJoin[0] + ".index.sqlite"
            # https://docs.python.org/3/faq/programming.html
            # > Why do lambdas defined in a loop with different values all return the same result?
            fileOrPath = JoinedFileFromFactory(
                [(lambda file=file: open(file, 'rb')) for file in filesToJoin]  # type: ignore
            )

    prioritizedBackends = options.get("prioritizedBackends", [])
    triedBackends = set()
    tarCompressionBackends = [module.name for _, info in TAR_COMPRESSION_FORMATS.items() for module in info.modules]

    for name in prioritizedBackends + list(_BACKENDS.keys()):
        if name in tarCompressionBackends:
            name = "tarfile"
        if name in triedBackends:
            continue
        triedBackends.add(name)
        if name not in _BACKENDS:
            if printDebug >= 1:
                print(f"[Info] Skipping unknown compression backend: {name}")
            continue

        try:
            if printDebug >= 3:
                print(f"[Info] Try to open with {name}")
            result = _BACKENDS[name](fileOrPath, **options)
            if result:
                if printDebug >= 2:
                    print(f"[Info] Opened archive with {name} backend.")
                return result
        except Exception as exception:
            if printDebug >= 2:
                print(f"[Info] Trying to open with {name} raised an exception:", exception)
            if printDebug >= 3:
                traceback.print_exc()

    if joinedFileName and not isinstance(fileOrPath, str):
        return SingleFileMountSource(joinedFileName, fileOrPath)

    raise CompressionError(f"Archive to open ({str(fileOrPath)}) has unrecognized format!")
