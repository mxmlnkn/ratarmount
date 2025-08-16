import io
import os
import stat
from collections.abc import Iterable
from typing import IO, Any, Optional, Union

try:
    import pygit2
except ImportError:
    pygit2 = None  # type: ignore

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import overrides


class GitMountSource(MountSource):
    """
    Reimplementation from scratch of the very barebones implementation inside fsspec
    because it is slow and "older" versions did not work with pygit2 1.15.

    https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/git.py
    https://github.com/fsspec/filesystem_spec/issues/1708
    """

    enabled = pygit2 is not None

    # pylint: disable=unused-argument
    def __init__(self, path: Optional[str] = None, reference: Optional[str] = None, **kwargs):
        self.repository = pygit2.Repository(path or os.getcwd())
        self.reference = reference or self._get_default_reference(self.repository)
        commit, reference = self.repository.resolve_refish(self.reference)  # type: ignore
        self.tree = commit.tree
        self.commitTime = self.repository[self.repository.head.target].commit_time  # type: ignore
        self.prefix = ""

    @staticmethod
    def _get_default_reference(repository):
        if 'init.defaultBranch' in repository.config:
            return repository.config['init.defaultBranch']

        # Try to find checked out branch.
        for branch in repository.branches:
            if repository.branches[branch].is_head():
                return branch

        for branch in ['master', 'main']:
            if branch in repository.branches:
                return branch

        return 'master'

    def _look_up_path(self, path: str) -> Optional[Any]:
        tree: Any = self.tree
        for name in self.prefix.split("/") + path.split("/"):
            if name and isinstance(tree, pygit2.Tree):
                if name not in tree:
                    return None
                tree = tree[name]
        return tree

    @staticmethod
    def _convert_to_file_mode(obj):
        if obj.filemode == pygit2.enums.FileMode.LINK:
            return 0o555 | stat.S_IFLNK
        return 0o555 | (stat.S_IFDIR if isinstance(obj, pygit2.Tree) else stat.S_IFREG)

    def _convert_to_file_info(self, obj, path: str):
        # fmt: off
        return FileInfo(
            size     = obj.size if hasattr(obj, 'size') else 0,
            mtime    = self.commitTime,
            mode     = GitMountSource._convert_to_file_mode(obj),
            linkname = obj.data.decode() if obj.filemode == pygit2.enums.FileMode.LINK else "",
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
        return self._look_up_path(path) is not None

    def _list(self, path: str, onlyMode: bool) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        tree = self._look_up_path(path)
        if not isinstance(tree, pygit2.Tree):
            return None
        return {
            obj.name: (
                GitMountSource._convert_to_file_mode(obj)
                if onlyMode
                else self._convert_to_file_info(obj, path + '/' + obj.name)
            )
            for obj in tree
            if obj.name
        }

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self._list(path, onlyMode=False)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self._list(path, onlyMode=True)

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        obj = self._look_up_path(path)
        return None if obj is None else self._convert_to_file_info(obj, path)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        if not isinstance(path, str):
            raise TypeError("Expected str path in userdata!")
        # TODO Avoid high memory usage for very large files.
        #      Check whether pygit2 even has a kind of streaming API for file contents.
        blob = self._look_up_path(path)
        if blob:
            return io.BytesIO(blob.data)
        raise FileNotFoundError(path)

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass
