#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import os
import stat
from typing import Dict, Iterable, IO, Optional, Union

try:
    import pygit2
except ImportError:
    pygit2 = None  # type: ignore

from .MountSource import FileInfo, MountSource
from .utils import overrides


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
        self.repository = pygit2.Repository(path if path else os.getcwd())
        self.reference = reference if reference else self._getDefaultReference(self.repository)
        commit, reference = self.repository.resolve_refish(self.reference)
        self.tree = commit.tree
        self.commitTime = self.repository[self.repository.head.target].commit_time
        self.prefix = ""

    @staticmethod
    def _getDefaultReference(repository):
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

    def _lookUpPath(self, path: str):
        tree = self.tree
        for name in self.prefix.split("/") + path.split("/"):
            if name and isinstance(tree, pygit2.Tree):
                if name not in tree:
                    return None
                tree = tree[name]
        return tree

    @staticmethod
    def _convertToFileMode(obj):
        if obj.filemode == pygit2.enums.FileMode.LINK:
            return 0o555 | stat.S_IFLNK
        return 0o555 | (stat.S_IFDIR if isinstance(obj, pygit2.Tree) else stat.S_IFREG)

    def _convertToFileInfo(self, obj, path: str):
        return FileInfo(
            # fmt: off
            size     = obj.size if hasattr(obj, 'size') else 0,
            mtime    = self.commitTime,
            mode     = GitMountSource._convertToFileMode(obj),
            linkname = obj.data.decode() if obj.filemode == pygit2.enums.FileMode.LINK else "",
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [path],
            # fmt: on
        )

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        return self._lookUpPath(path) is not None

    def _listDir(self, path: str, onlyMode: bool) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        tree = self._lookUpPath(path)
        if not isinstance(tree, pygit2.Tree):
            return None
        return {
            obj.name: (
                GitMountSource._convertToFileMode(obj)
                if onlyMode
                else self._convertToFileInfo(obj, path + '/' + obj.name)
            )
            for obj in tree
        }

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self._listDir(path, onlyMode=False)

    @overrides(MountSource)
    def listDirModeOnly(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        return self._listDir(path, onlyMode=True)

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        obj = self._lookUpPath(path)
        return None if obj is None else self._convertToFileInfo(obj, path)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return 1

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        path = fileInfo.userdata[-1]
        assert isinstance(path, str)
        # TODO Avoid high memory usage for very large files.
        #      Check whether pygit2 even has a kind of streaming API for file contents.
        return io.BytesIO(self._lookUpPath(path).data)

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass
