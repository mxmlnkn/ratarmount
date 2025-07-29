# pylint: disable=no-member,abstract-method
# Disable pylint errors. See https://github.com/fsspec/filesystem_spec/issues/1678

import http
import logging
import os
import re
import stat
import sys
import warnings
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Union

from ratarmountcore.compressions import COMPRESSION_BACKENDS, check_for_split_file_in_folder
from ratarmountcore.formats import FILE_FORMATS, FileFormatID
from ratarmountcore.StenciledFile import JoinedFileFromFactory
from ratarmountcore.utils import CompressionError, RatarmountError

from . import MountSource
from .archives import ARCHIVE_BACKENDS
from .compositing.singlefile import SingleFileMountSource
from .formats.folder import FolderMountSource
from .formats.fsspec import FSSpecMountSource
from .formats.git import GitMountSource

try:
    import fsspec
    import fsspec.implementations.http
    import fsspec.utils
except ImportError:
    fsspec = None  # type: ignore

try:
    from sshfs import SSHFileSystem

    class FixedSSHFileSystem(SSHFileSystem):
        protocols = ("sftp", "ssh", "scp")
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


logger = logging.getLogger(__name__)


def _open_git_mount_source(url: str) -> Union[MountSource, IO[bytes], str]:
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
        fileInfo = mountSource.lookup(pathInsideRepository)
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


def _open_sshfs_mount_source(url: str) -> Union[MountSource, IO[bytes], str]:
    if FixedSSHFileSystem is None:
        raise RatarmountError("Cannot open with sshfs module because it seems to not be installed!")

    # Note that fsspec.implementations.ssh did not use ~/.ssh/config!
    # That's one of the many reasons why fsspec/sshfs based on asyncssh is used instead of paramiko.
    fs = FixedSSHFileSystem(**FixedSSHFileSystem._get_kwargs_from_urls(url))  # pytype: disable=attribute-error

    # Remove one leading / in order to add support for relative paths. E.g.:
    #   ssh://127.0.0.1/relative/path
    #   ssh://127.0.0.1//home/user/relative/path
    path = fsspec.utils.infer_storage_options(url)['path']
    path = path.removeprefix("/")
    if not path:
        path = "."

    if not fs.exists(path):
        raise RatarmountError(f"Cannot open URL: {url} because the remote path: {path} does not exist!")
    # Note that the resulting file object has a .fs member for correct lifetime tracking.
    return fs.open(path) if fs.isfile(path) else FSSpecMountSource(fs, path)


def try_open_url(url) -> Union[MountSource, IO[bytes], str]:
    splitURI = url.split('://', 1)
    protocol = splitURI[0] if len(splitURI) > 1 else ''
    if not protocol:
        raise RatarmountError(f"Expected to be called with URL containing :// but got: {url}")

    logger.debug("Try to open URL: %s", url)

    if protocol == 'file':
        return splitURI[1]

    if protocol == 'git':
        return _open_git_mount_source(url)

    if FixedSSHFileSystem is not None and protocol in FixedSSHFileSystem.protocols:
        return _open_sshfs_mount_source(url)

    if not fsspec:
        raise RatarmountError(
            "An fsspec URL was detected but fsspec is not installed. Install it with: pip install ratarmount[fsspec]"
        )

    logger.debug("Try to open with fsspec")

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

        def check_for_https(url):
            try:
                connection = http.client.HTTPSConnection(url, timeout=2)
                connection.request("HEAD", "/")
                return bool(connection.getresponse())
            except Exception as exception:
                logger.debug("Determined WebDAV URL to not use HTTP instead HTTPS because of: %s", exception)
                return False

        transportProtocol = "https" if check_for_https(baseURL) else "http"
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

    logger.debug("Opened filesystem: %s", fileSystem)

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

    def new_del():
        if callable(oldDel):
            oldDel()
        result.close()

    result.__del__ = new_del

    # Check that seeking works. May fail when, e.g., the HTTP server does not support range requests.
    # Use https://github.com/danvk/RangeHTTPServer for testing purposes because
    # "python3 -m http.server 9000" does not have range support. Use "python3 -m RangeHTTPServer 9000".
    result.seek(1)
    result.read(1)
    result.seek(0)

    return result


def _matches_extension(fileName: str, formats: Iterable[FileFormatID]) -> bool:
    return any(
        fileName.lower().endswith('.' + extension.lower())
        for formatId in formats
        for extension in FILE_FORMATS[formatId].extensions
    )


def find_backends_by_extension(fileName: str) -> list[str]:
    return [backend for backend, info in ARCHIVE_BACKENDS.items() if _matches_extension(fileName, info.formats)] + [
        info.delegatedArchiveBackend
        for _, info in COMPRESSION_BACKENDS.items()
        if _matches_extension(fileName, info.formats)
    ]


def open_mount_source(fileOrPath: Union[str, IO[bytes], os.PathLike], **options) -> MountSource:
    if isinstance(fileOrPath, str) and '://' in fileOrPath:
        openedURL = try_open_url(fileOrPath)

        # If the URL pointed to a folder, return a MountSource, else open the returned file object as an archive.
        if isinstance(openedURL, MountSource):
            return openedURL

        # Add tarFileName argument so that mounting a TAR file via SSH can create a properly named index
        # file inside ~/.cache/ratarmount.
        if not isinstance(openedURL, str) and 'tarFileName' not in options:
            options['tarFileName'] = fileOrPath

        fileOrPath = openedURL

    autoPrioritizedBackends: list[str] = []
    joinedFileName = ''
    if isinstance(fileOrPath, (str, os.PathLike)):
        path = Path(fileOrPath)
        if not path.exists():
            raise RatarmountError(f"Mount source does not exist: {fileOrPath!s}")

        if path.is_dir():
            return FolderMountSource('.' if str(fileOrPath) == '.' else path.resolve())

        splitFileResult = check_for_split_file_in_folder(str(fileOrPath))
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
        else:
            fileOrPath = str(fileOrPath)
            autoPrioritizedBackends = find_backends_by_extension(fileOrPath)
    else:
        autoPrioritizedBackends = find_backends_by_extension(options.get('tarFileName', ''))

    prioritizedBackends = options.get("prioritizedBackends", [])
    triedBackends = set()
    # Map user-specified backend prioritization such as rapidgzip, indexed_bzip2, ... to tarfile,
    # which actually undoes those.
    mapToArchiveBackend: dict[str, str] = {
        backend: info.delegatedArchiveBackend for backend, info in COMPRESSION_BACKENDS.items()
    }

    for name in prioritizedBackends + autoPrioritizedBackends + list(ARCHIVE_BACKENDS.keys()):
        name = mapToArchiveBackend.get(name, name)
        if name in triedBackends:
            continue
        triedBackends.add(name)
        if name not in ARCHIVE_BACKENDS:
            logger.warning("Skipping unknown archive backend: %s", name)
            continue

        try:
            logger.debug("Try to open with: %s", name)
            result = ARCHIVE_BACKENDS[name].open(fileOrPath, **options)
            if result:
                logger.info("Opened archive with %s backend.", name)
                return result
        except Exception as exception:
            logger.info(
                "Trying to open with %s raised an exception: %s",
                name,
                exception,
                exc_info=logger.isEnabledFor(logging.DEBUG),
            )

            try:
                if hasattr(fileOrPath, 'seek'):
                    fileOrPath.seek(0)  # type: ignore
            except Exception as seekException:
                logger.warning(
                    "seek(0) raised an exception: %s", seekException, exc_info=logger.isEnabledFor(logging.DEBUG)
                )

    if joinedFileName and not isinstance(fileOrPath, (str, os.PathLike)):
        return SingleFileMountSource(joinedFileName, fileOrPath)

    raise CompressionError(f"Archive to open ({fileOrPath!s}) has unrecognized format!")
