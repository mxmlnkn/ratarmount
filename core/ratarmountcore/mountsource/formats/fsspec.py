import os
import stat
import time
import urllib
from collections.abc import Iterable
from typing import IO, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.utils import overrides

try:
    import fsspec
    import fsspec.core
except ImportError:
    fsspec = None  # type: ignore

try:
    from fsspec.implementations.http import HTTPFileSystem
except ImportError:
    HTTPFileSystem = None  # type: ignore

try:
    from fsspec.implementations.github import GithubFileSystem
except ImportError:
    GithubFileSystem = None  # type: ignore

try:
    from webdav4.fsspec import WebdavFileSystem
except ImportError:
    WebdavFileSystem = None  # type: ignore

try:
    from dropboxdrivefs import DropboxDriveFileSystem
except ImportError:
    DropboxDriveFileSystem = None  # type: ignore


class FSSpecMountSource(MountSource):
    """
    Generic wrapper around fsspec-based filesystems.
    At least as "generic" as it gets given that many details are inconsistent between the implementations.
    Note also that many implementations are rather experimental, utterly slow, or unstable.
    """

    # TODO implement some of the most rudimentarily implemented filesystems myself instead of via fsspec.
    # wc -l 'fsspec/implementations/'*.py | sort -n
    #      0 fsspec/implementations/__init__.py
    #     58 fsspec/implementations/data.py
    #     75 fsspec/implementations/cache_mapper.py
    #    124 fsspec/implementations/jupyter.py
    #    124 fsspec/implementations/tar.py              -> SQLiteIndexedTar
    #    127 fsspec/implementations/git.py              -> TODO
    #    152 fsspec/implementations/dask.py
    #    176 fsspec/implementations/zip.py              -> ZipMountSource
    #    180 fsspec/implementations/sftp.py             -> fsspec/sshfs
    #    213 fsspec/implementations/libarchive.py       -> LibarchiveMountSource
    #    232 fsspec/implementations/cache_metadata.py
    #    239 fsspec/implementations/github.py
    #    303 fsspec/implementations/memory.py
    #    304 fsspec/implementations/arrow.py
    #    372 fsspec/implementations/dirfs.py            -> FolderMountSource + chdir
    #    395 fsspec/implementations/ftp.py
    #    416 fsspec/implementations/smb.py
    #    467 fsspec/implementations/dbfs.py
    #    471 fsspec/implementations/local.py            -> FolderMountSource
    #    484 fsspec/implementations/webhdfs.py
    #    872 fsspec/implementations/http.py
    #    929 fsspec/implementations/cached.py
    #   1173 fsspec/implementations/reference.py
    # I guess git is the most obvious candidate because it is the most interesting and most barebone implementation.

    # pylint: disable=unused-argument
    def __init__(self, urlOrFS, prefix: Optional[str] = None, **options) -> None:
        """
        urlOrFS : Take a URL or an already opened fsspec Filesystem object.
                  Note that this might take an AbstractFileSystem-derived object in the future.
        """
        if isinstance(urlOrFS, fsspec.AbstractFileSystem):
            fs = urlOrFS
        elif isinstance(urlOrFS, str):
            url_to_fs = fsspec.url_to_fs if hasattr(fsspec, 'url_to_fs') else fsspec.core.url_to_fs
            fs, path = url_to_fs(urlOrFS)
            if prefix is None:
                prefix = path
        else:
            raise ValueError("First argument must be an URL or inherit from fsspec.AbstractFileSystem!")
        self.fileSystem: fsspec.AbstractFileSystem = fs
        self.rootFileInfo = create_root_file_info(userdata=["/"])

        # The fsspec filesystems are not uniform! http:// expects the arguments to isdir with prefixed
        # protocol while other filesystem implementations are fine with only the path.
        #  - https://github.com/ray-project/ray/issues/26423#issuecomment-1179561181
        #  - https://github.com/fsspec/filesystem_spec/issues/1713
        #  - https://github.com/skshetry/webdav4/issues/198
        self._pathsRequireQuoting = HTTPFileSystem is not None and isinstance(self.fileSystem, HTTPFileSystem)
        if WebdavFileSystem:
            self._pathsRequireQuoting = self._pathsRequireQuoting or isinstance(self.fileSystem, WebdavFileSystem)
        self.prefix = prefix.rstrip("/") if prefix and prefix.strip("/") and self.fileSystem.isdir(prefix) else None
        self._pathsWithoutLeadingSlash = GithubFileSystem is not None and isinstance(self.fileSystem, GithubFileSystem)

    def _get_path(self, path: str) -> str:
        if self._pathsRequireQuoting:
            path = urllib.parse.quote(path)
        if self.prefix:
            if not path or path == "/":
                return self.prefix
            return self.prefix.rstrip("/") + "/" + path.lstrip("/")
        if self._pathsWithoutLeadingSlash:
            return path.lstrip("/")
        # The fsspec TAR implementation is verbatim. The path must exactly match the one in the archive, even
        # if the archive path is not normalized. E.g., compare with these:
        #  - tests/nested-symlinks.tar -> leading /. Did not work because
        #  - tests/nested-tar.tar -> no leading / only works with this fix
        #  - tests/single-file-with-leading-dot-slash.tar -> not working at all.
        #       We cannot test all possible denormalized paths when a normalized path is requested via FUSE.
        if not self.fileSystem.lexists(path) and self.fileSystem.lexists(path.lstrip("/")):
            path = path.lstrip("/")
        return path

    @staticmethod
    def _get_mode(entry) -> int:
        return 0o555 | (stat.S_IFDIR if entry.get('type', '') == 'directory' else stat.S_IFREG)

    @staticmethod
    def _get_modification_time(entry) -> Union[int, float]:
        # There is no standardized API for the modification time:
        # https://github.com/fsspec/filesystem_spec/issues/1680#issuecomment-2368750882
        #
        # sshfs.SSHF: 'mtime': datetime.datetime(2020, 3, 23, 20, 15, 34)
        # fsspec.implementations.git.GitFileSystem: Nothing with listdir(details=True)!
        # fsspec.implementations.ftp.FTPFileSystem: 'modify': '20241004165129'
        mtime = entry.get('mtime', None)
        if mtime is not None:
            return mtime.timestamp() if hasattr(mtime, 'timestamp') else mtime
        modify = entry.get('modify', None)
        if isinstance(modify, str):
            return time.mktime(time.strptime(modify, "%Y%m%d%H%M%S"))
        return 0

    @staticmethod
    def _convert_to_file_info(entry, path) -> FileInfo:
        # TODO fsspec does not have an API to get symbolic link targets!
        #      They kinda work only like hardlinks.
        # https://github.com/fsspec/filesystem_spec/issues/1679
        # https://github.com/fsspec/filesystem_spec/issues/1680
        size = entry.get('size', 0)
        # fmt: off
        return FileInfo(
            size     = size or 0,
            mtime    = FSSpecMountSource._get_modification_time(entry),
            mode     = FSSpecMountSource._get_mode(entry),
            linkname = "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [path],
        )
        # fmt: on

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return self.fileSystem.lexists(self._get_path(path))

    def _list(self, path: str, onlyMode: bool) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        path = self._get_path(path)

        if path == '/' and DropboxDriveFileSystem and isinstance(self.fileSystem, DropboxDriveFileSystem):
            # We need to work around this obnoxious error:
            # dropbox.exceptions.BadInputError: BadInputError(
            #   '12345', 'Error in call to API function "files/list_folder":
            #    request body: path: Specify the root folder as an empty string rather than as "/".')
            # On the other hand, all paths must start with / or else they will not be found...
            path = ""

        result = self.fileSystem.listdir(path, detail=True)
        if not result:
            return []
        if isinstance(result[0], str):
            return result

        # Examples for listdir return values:
        #
        # sshfs.SSHF: [
        #   {'size': 8, 'type': 'link', 'gid': 0, 'uid': 0, 'time': datetime.datetime(2024, 10, 3, 19, 32, 42),
        #    'mtime': datetime.datetime(2020, 3, 23, 20, 15, 34), 'permissions': 41471, 'name': '/sbin'},
        #   {'size': 4096, 'type': 'directory', 'gid': 0, 'uid': 0, 'time': datetime.datetime(2024, 9, 25, 19, 45, 31),
        #    'mtime': datetime.datetime(2023, 7, 22, 11, 32, 1), 'permissions': 16877, 'name': '/var'}
        #   {'size': 134217728, 'type': 'file', 'gid': 0, 'uid': 0, 'time': datetime.datetime(2024, 9, 25, 19, 45, 30),
        #    'mtime': datetime.datetime(2021, 6, 16, 19, 26, 38), 'permissions': 33188, 'name': '/swapfile'}
        # -> "name" contains the absolute path to each file (also tested with subfolders)!
        # fsspec.implementations.git.GitFileSystem: [
        #   {'type': 'file', 'name': '.gitattributes', 'hex': '2a396079050e5847b7c995642ed07a7c8591bde9',
        #    'mode': '100644', 'size': 363},
        #   {'type': 'directory', 'name': '.github', 'hex': 'c8ab28a6ded46c96fa33a96a9d6d0b53dfe815de',
        #    'mode': '40000', 'size': 0},
        #   [{'type': 'directory', 'name': '.github/workflows', 'hex': 'b1b9b9b0d1ca1210f823195238e8fe71829fae42',
        #     'mode': '40000', 'size': 0}]
        # -> "name" is absolute path but without leading slash
        # fsspec.implementations.ftp.FTPFileSystem: [
        #   {'modify': '20241004165129', 'perm': 'el', 'size': 0, 'type': 'directory',
        #    'unique': 'fd01ga9f7f6', 'name': '/.git'},
        #   {'modify': '20240602192724', 'perm': 'r', 'size': 363, 'type': 'file',
        #    'unique': 'fd01g2de4e2', 'name': '/.gitattributes'},
        # fsspec.implementations.http.HTTPFileSystem: [
        #   {'name': 'http://127.0.0.1:8000/?S=D', 'size': None, 'type': 'file'},
        #   {'name': 'http://127.0.0.1:8000/benchmarks/', 'size': None, 'type': 'directory'},
        #   {'name': 'http://127.0.0.1:8000/benchmark-sshfs-block_size.py', 'size': None, 'type': 'file'},
        # -> For some reason, the name always has to include the full URL for the request and result.
        # -> There are some HTTP server artifacts such as "?S=D", which are links for changing the sorting...
        prefixToStrip = path.lstrip('/')
        result = {
            (
                entry['name'].strip('/')[len(prefixToStrip) :].strip('/')
                if entry['name'].strip('/').startswith(prefixToStrip)
                else entry['name']
            ): (
                FSSpecMountSource._get_mode(entry)
                if onlyMode
                else FSSpecMountSource._convert_to_file_info(entry, entry['name'])
            )
            for entry in result
        }

        # For some dumb reason, only the TAR filesystem, returns '/' for some subfolders.
        # It happens with nested-symlinks.tar, probably because the subfolder as its own entry in the TAR:
        #     drwx------ user/user  0 2020-04-10 10:46 /foo/
        #     lrwxrwxrwx user/user  0 2020-04-10 10:46 /foo/ufo -> /foo/foo
        #     -rwx------ user/user  6 2020-04-09 17:59 /foo/foo
        #     drwx------ user/user  0 2020-04-09 17:57 /foo/fighter/
        #     lrwxrwxrwx user/user  0 2020-04-09 16:53 /foo/fighter/foo -> ../foo
        #     lrwxrwxrwx user/user  0 2020-04-09 16:52 /foo/fighter/python -> /usr/bin/python
        #     lrwxrwxrwx user/user  0 2020-04-09 18:01 /foo/iriya -> ../iriya
        if '' in result:
            del result['']

        # For HTTPFileSystem, we need to filter out the entries for sorting.
        # For WebDAV we do not even need to unquote! We get unquoted file names with ls!
        if isinstance(self.fileSystem, fsspec.implementations.http.HTTPFileSystem):
            return {
                urllib.parse.unquote(name): info for name, info in result.items() if not name.startswith(('?', '#'))
            }

        return result

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self._list(path, onlyMode=False)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self._list(path, onlyMode=True)

    def _lookup_http(self, path: str) -> Optional[FileInfo]:
        path = self._get_path(path)

        # Avoid aiohttp.client_exceptions.ClientResponseError: 404, message='Not Found'
        if not self.fileSystem.lexists(path):
            return None

        # fs.info will always return the given path to be file because it counts it as an HTML file ...
        # isdir works somewhat better, but it downloads the whole file!
        # https://github.com/fsspec/filesystem_spec/issues/1707
        # Therefore, only call it if the mimetype indicates an HTML file.
        # In the future it might be best to call listdir on the parent path to detect whether it is a folder or file.
        info = self.fileSystem.info(path)
        if info.get('mimetype', None) == 'text/html' and self.fileSystem.isdir(path):
            return FSSpecMountSource._convert_to_file_info({'type': 'directory'}, path)
        return FSSpecMountSource._convert_to_file_info(info, path)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if isinstance(self.fileSystem, fsspec.implementations.http.HTTPFileSystem):
            return self._lookup_http(path)

        path = self._get_path(path)
        if path == '/' or not path:
            # We need to handle this specially because some filesystems, at least ssshfs.SSHFileSystem,
            # do not support 'info' on '/' and will cause an exception:
            #
            # Traceback (most recent call last):
            #   sshfs/utils.py", line 27, in wrapper
            #     return await func(*args, **kwargs)
            #   sshfs/spec.py", line 145, in _info
            #     attributes = await channel.stat(path)
            #   asyncssh/sftp.py", line 4616, in stat
            #     return await self._handler.stat(path, flags,
            #   asyncssh/sftp.py", line 2713, in stat
            #     return cast(SFTPAttrs,  await self._make_request(
            #   asyncssh/sftp.py", line 2468, in _make_request
            #     result = self._packet_handlers[resptype](self, resp)
            #   asyncssh/sftp.py", line 2484, in _process_status
            #     raise exc
            # asyncssh.sftp.SFTPNoSuchFile: No such file
            #
            # Dropbox also does not like this:
            #
            # dropbox.exceptions.BadInputError: BadInputError('12345',
            #   'Error in call to API function "files/get_metadata":
            #   request body: path: The root folder is unsupported.')
            return self.rootFileInfo.clone()

        if not self.fileSystem.lexists(path):
            return None
        return FSSpecMountSource._convert_to_file_info(self.fileSystem.info(path), path)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        return self.fileSystem.open(path, block_size=buffering if buffering >= 0 else None)

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        if hasattr(self.fileSystem, '__exit__'):
            self.fileSystem.__exit__(exception_type, exception_value, exception_traceback)
