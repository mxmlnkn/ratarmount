#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=no-member,abstract-method
# Disable pylint errors. See https://github.com/fsspec/filesystem_spec/issues/1678

import http
import os
import re
import stat
import sys
import traceback
import warnings

from typing import IO, Optional, Union

from .compressions import (
    checkForSplitFile,
    libarchive,
    PySquashfsImage,
    pyfatfs,
    rarfile,
    TAR_COMPRESSION_FORMATS,
    zipfile,
)
from .compressions import isSquashFS
from .utils import CompressionError, RatarmountError
from .MountSource import MountSource
from .FATMountSource import FATMountSource
from .FolderMountSource import FolderMountSource
from .FSSpecMountSource import FSSpecMountSource
from .GitMountSource import GitMountSource
from .RarMountSource import RarMountSource
from .SingleFileMountSource import SingleFileMountSource
from .SQLiteIndexedTar import SQLiteIndexedTar
from .SquashFSMountSource import SquashFSMountSource
from .StenciledFile import JoinedFileFromFactory
from .ZipMountSource import ZipMountSource
from .LibarchiveMountSource import LibarchiveMountSource

try:
    import fsspec
    import fsspec.utils
    import fsspec.implementations.http
except ImportError:
    fsspec = None  # type: ignore

try:
    from sshfs import SSHFileSystem

    class FixedSSHFileSystem(SSHFileSystem):
        protocols = ["sftp", "ssh", "scp"]
        cachable = False

        def open(self, *args, **kwargs):
            # Note that asycnssh SSHFile does/did not implement seekable correctly!
            # https://github.com/fsspec/sshfs/pull/50
            result = super().open(*args, **kwargs)
            result.seekable = lambda: True  # type:ignore
            return result

except ImportError:
    FixedSSHFileSystem = None  # type: ignore

try:
    from webdav4.fsspec import WebdavFileSystem
except ImportError:
    WebdavFileSystem = None  # type: ignore

try:
    from dropboxdrivefs import DropboxDriveFileSystem

    class FixedDropboxDriveFileSystem(DropboxDriveFileSystem):
        def info(self, url, **kwargs):
            if url == ('/', ''):
                return {'size': 0, 'name': '/', 'type': 'directory'}
            return super().info(url, **kwargs)

except ImportError:
    FixedDropboxDriveFileSystem = None  # type: ignore


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
            if 'tarFileName' in options:
                copiedOptions = options.copy()
                del copiedOptions['tarFileName']
                return SQLiteIndexedTar(fileOrPath, **copiedOptions)
            return SQLiteIndexedTar(fileOrPath, **options)
        return SQLiteIndexedTar(fileObject=fileOrPath, **options)
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore


def _openZipMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        if zipfile is not None:
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


def _openPySquashfsImage(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        # Better to check file type here because I am unsure about what the MountSource semantic should be
        # regarding file object closing when it raises an exception in the constructor.
        if not isinstance(fileOrPath, str) and not isSquashFS(fileOrPath):
            return None

        if PySquashfsImage is not None:
            return SquashFSMountSource(fileOrPath, **options)
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore
    return None


def _openFATImage(fileOrPath: Union[str, IO[bytes]], **options) -> Optional[MountSource]:
    try:
        if pyfatfs is not None:
            return FATMountSource(fileOrPath, **options)
    finally:
        if hasattr(fileOrPath, 'seek'):
            fileOrPath.seek(0)  # type: ignore
    return None


_BACKENDS = {
    "rarfile": _openRarMountSource,
    "tarfile": _openTarMountSource,
    "zipfile": _openZipMountSource,
    "pysquashfsimage": _openPySquashfsImage,
    "libarchive": _openLibarchiveMountSource,
    "pyfatfs": _openFATImage,
}


def _openGitMountSource(url: str) -> Union[MountSource, IO[bytes], str]:
    splitURI = url.split('://', 1)
    if len(splitURI) <= 1 or splitURI[0] != 'git':
        raise RatarmountError("Expected URL starting with git://")

    if not GitMountSource.enabled:
        raise RatarmountError(
            "Detected git:// URL but GitMountSource could not be loaded. Please ensure that pygit2 is installed."
        )

    splitRepositoryPath = splitURI[1].split(':', 1)
    repositoryPath = splitRepositoryPath[0] if len(splitRepositoryPath) > 1 else None
    remainder = splitRepositoryPath[-1]

    splitReference = remainder.split('@', 1)
    reference = splitReference[0] if len(splitReference) > 1 else None
    pathInsideRepository = splitReference[-1]

    mountSource = GitMountSource(repositoryPath, reference=reference)
    if pathInsideRepository:
        fileInfo = mountSource.getFileInfo(pathInsideRepository)
        if not fileInfo:
            raise RatarmountError(
                f"The path {pathInsideRepository} in the git repository specified via '{url}' does not exist!"
            )

        if stat.S_ISDIR(fileInfo.mode):
            mountSource.prefix = pathInsideRepository
        else:
            # In the future it might be necessary to extend the lifetime of mountSource by adding it as
            # a member of the opened file, but not right now.
            return mountSource.open(fileInfo)

    return mountSource


def _openSSHFSMountSource(url: str) -> Union[MountSource, IO[bytes], str]:
    if FixedSSHFileSystem is None:
        raise RatarmountError("Cannot open with sshfs module because it seems to not be installed!")

    # Note that fsspec.implementations.ssh did not use ~/.ssh/config!
    # That's one of the many reasons why fsspec/sshfs based on asyncssh is used instead of paramiko.
    fs = FixedSSHFileSystem(**FixedSSHFileSystem._get_kwargs_from_urls(url))  # pytype: disable=attribute-error

    # Remove one leading / in order to add support for relative paths. E.g.:
    #   ssh://127.0.0.1/relative/path
    #   ssh://127.0.0.1//home/user/relative/path
    path = fsspec.utils.infer_storage_options(url)['path']
    if path.startswith("/"):
        path = path[1:]
    if not path:
        path = "."

    if not fs.exists(path):
        raise RatarmountError(f"Cannot open URL: {url} because the remote path: {path} does not exist!")
    # Note that the resulting file object has a .fs member for correct lifetime tracking.
    return fs.open(path) if fs.isfile(path) else FSSpecMountSource(fs, path)


def tryOpenURL(url, printDebug: int) -> Union[MountSource, IO[bytes], str]:
    splitURI = url.split('://', 1)
    protocol = splitURI[0] if len(splitURI) > 1 else ''
    if not protocol:
        raise RatarmountError(f"Expected to be called with URL containing :// but got: {url}")

    if printDebug >= 3:
        print(f"[Info] Try to open URL: {url}")

    if protocol == 'file':
        return splitURI[1]

    if protocol == 'git':
        return _openGitMountSource(url)

    if FixedSSHFileSystem is not None and protocol in FixedSSHFileSystem.protocols:
        return _openSSHFSMountSource(url)

    if not fsspec:
        raise RatarmountError(
            "An fsspec URL was detected but fsspec is not installed. Install it with: pip install ratarmount[fsspec]"
        )

    if printDebug >= 3:
        print("[Info] Try to open with fsspec")

    if protocol == 'ftp' and sys.version_info < (3, 9):
        url_to_fs = fsspec.url_to_fs if hasattr(fsspec, 'url_to_fs') else fsspec.core.url_to_fs
        # Suppress warning about (default!) encoding not being supported for Python<3.9 -.-.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fileSystem, path = url_to_fs(url)
    elif protocol == 'webdav':
        # WebDAV needs special handling because we need to decide between HTTP and HTTPS and because of:
        # https://github.com/skshetry/webdav4/issues/197
        if not WebdavFileSystem:
            raise RatarmountError(f"Install the webdav4 Python package to mount {protocol}://.")

        matchedURI = re.match("(?:([^:/]*):([^@/]*)@)?([^/]*)(.*)", splitURI[1])
        if not matchedURI:
            raise RatarmountError(
                "Failed to match WebDAV URI of the format webdav://[user:password@]host[:port][/path]\n"
                "If your user name or password contains special characters such as ':/@', then use the environment "
                "variables WEBDAV_USER and WEBDAV_PASSWORD to specify them."
            )
        username, password, baseURL, path = matchedURI.groups()
        if path is None:
            path = ""
        if username is None and 'WEBDAV_USER' in os.environ:
            username = os.environ.get('WEBDAV_USER')
        if password is None and 'WEBDAV_PASSWORD' in os.environ:
            password = os.environ.get('WEBDAV_PASSWORD')
        auth = None if username is None or password is None else (username, password)

        def checkForHTTPS(url):
            try:
                connection = http.client.HTTPSConnection(url, timeout=2)
                connection.request("HEAD", "/")
                return bool(connection.getresponse())
            except Exception as exception:
                if printDebug >= 3:
                    print("[Info] Determined WebDAV URL to not use HTTP instead HTTPS because of:", exception)
                return False

        transportProtocol = "https" if checkForHTTPS(baseURL) else "http"
        fileSystem = WebdavFileSystem(f"{transportProtocol}://{baseURL}", auth=auth)
    elif protocol == 'dropbox':
        # Dropbox needs special handling because there is no way to specify the token and because
        # there are some obnoxius intricacies regarding ls and stat of the root folder.
        if FixedDropboxDriveFileSystem is None:
            raise RatarmountError(f"Install the dropboxdrivefs Python package to mount {protocol}://.")

        dropboxToken = os.environ.get('DROPBOX_TOKEN', None)
        if not dropboxToken:
            raise RatarmountError(
                "Please set the DROPBOX_TOKEN environment variable to mount dropbox:// URLs. "
                "Please refer to the ratarmount online ReadMe or to the DropBox documentation for creating a token."
            )

        fileSystem = FixedDropboxDriveFileSystem(token=dropboxToken)
        path = splitURI[1]
        # Dropbox requires all paths to start with /, so simply add it
        # instead of making each user run into this problem.
        if path and not path.startswith('/'):
            path = '/' + path
        # Dropbox also does not like trailing / -.-. God is it super finicky.
        # dropbox.exceptions.ApiError: ApiError('12345', GetMetadataError('path', LookupError('malformed_path', None)))
        path = path.rstrip('/')
    else:
        url_to_fs = fsspec.url_to_fs if hasattr(fsspec, 'url_to_fs') else fsspec.core.url_to_fs
        fileSystem, path = url_to_fs(url)

    if printDebug >= 3:
        print("[Info] Opened filesystem:", fileSystem)

    # Note that http:// URLs are always files. Folders are only regex-parsed HTML files!
    # By checking with isdir instead of isfile, we give isdir a higher precedence.
    # Also note that isdir downloads the whole file!
    # https://github.com/fsspec/filesystem_spec/issues/1707
    if isinstance(fileSystem, fsspec.implementations.http.HTTPFileSystem):
        info = fileSystem.info(path)
        if info.get('mimetype', None) == 'text/html' and fileSystem.isdir(path):
            return FSSpecMountSource(fileSystem, path)
    elif fileSystem.isdir(path):
        return FSSpecMountSource(fileSystem, path)

    if not fileSystem.exists(path):
        raise RatarmountError(f"Opening URL {url} failed because path {path} does not exist on remote!")

    # This open call can fail with FileNotFoundError, IsADirectoryError, and probably others.
    result = fileSystem.open(path)  # pylint: disable=no-member

    # Avoid resource leaks, e.g., when the seek check fails.
    oldDel = getattr(result, '__del__', None)

    def newDel():
        if callable(oldDel):
            oldDel()
        result.close()

    result.__del__ = newDel

    # Check that seeking works. May fail when, e.g., the HTTP server does not support range requests.
    # Use https://github.com/danvk/RangeHTTPServer for testing purposes because
    # "python3 -m http.server 9000" does not have range support. Use "python3 -m RangeHTTPServer 9000".
    result.seek(1)
    result.read(1)
    result.seek(0)

    return result


def openMountSource(fileOrPath: Union[str, IO[bytes]], **options) -> MountSource:
    printDebug = int(options.get("printDebug", 0)) if isinstance(options.get("printDebug", 0), int) else 0

    if isinstance(fileOrPath, str) and '://' in fileOrPath:
        openedURL = tryOpenURL(fileOrPath, printDebug=printDebug)

        # If the URL pointed to a folder, return a MountSource, else open the returned file object as an archive.
        if isinstance(openedURL, MountSource):
            return openedURL

        # Add tarFileName argument so that mounting a TAR file via SSH can create a properly named index
        # file inside ~/.cache/ratarmount.
        if not isinstance(openedURL, str) and 'tarFileName' not in options:
            options['tarFileName'] = fileOrPath

        fileOrPath = openedURL

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
