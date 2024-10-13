#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import stat
from typing import Optional

from fsspec.spec import AbstractFileSystem

from .MountSource import MountSource


class MountSourceFileSystem(AbstractFileSystem):
    """A thin adaptor from the MountSource interface to the fsspec AbstractFileSystem interface."""

    cachable = False

    def __init__(self, mountSource: MountSource, **kwargs):
        super().__init__(**kwargs)
        self.mountSource = mountSource

    @classmethod
    def _stripProtocol(cls, path):
        return path[-len(cls.protocol) - 3] if path.startswith(cls.protocol + '://') else path

    @staticmethod
    def _fileInfoToDict(name, fileInfo):
        # obj.name and obj.filemode are None for the root tree!
        is_dir = isinstance(obj, pygit2.Tree)
        return {
            "type": "directory" if stat.S_ISDIR(fileInfo.mode) else "file",
            "name": name,
            "mode": f"{fileInfo.mode:o}",
            "size": fileInfo.size,
        }

    def ls(self, path, detail=True, ref=None, **kwargs):
        strippedPath = self._stripProtocol(path)
        if detail:
            result = self.mountSource.listDir(strippedPath)
            if result is None:
                raise FileNotFoundError(path)
            if not isinstance(result, dict):
                result = {name: self.mountSource.getFileInfo(name) for name in result}
            return [self._fileInfoToDict(name, info) for name, info in result.items() if info is not None]

        result = self.mountSource.listDirModeOnly(strippedPath)
        if result is None:
            raise FileNotFoundError(path)
        return list(result.keys()) if isinstance(result, dict) else result

    def info(self, path, ref=None, **kwargs):
        result = self.mountSource.getFileInfo(self._stripProtocol(path))
        if result is None:
            raise FileNotFoundError(path)
        return self._fileInfoToDict(result)

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        **kwargs,
    ):
        if mode != "rb":
            raise ValueError("Only binary reading is supported!")
        return self.mountSource(self._stripProtocol(path))


class SQLiteIndexedTarFileSystem(AbstractFileSystem):
    """Browse the files of a (compressed) TAR archive quickly."""

    protocol = "ratar"

    def __init__(self, tarFileName: Optional[str] = None, fileObject: Optional[IO[bytes]] = None, **kwargs):
        """Refer to SQLiteIndexedTar for all supported arguments and options."""
        super().__init__(self, SQLiteIndexedTar(tarFileName, fileObject, **kwargs))
