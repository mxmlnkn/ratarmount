#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=abstract-method

from typing import Any, Callable, Dict, Iterable, Optional, Union

from .MountSource import FileInfo, MountSource
from .SQLiteIndex import SQLiteIndex
from .utils import overrides


class SQLiteIndexMountSource(MountSource):
    def __init__(
        self, index: SQLiteIndex, clearIndexCache: bool, checkMetadata: Optional[Callable[[Dict[str, Any]], None]]
    ) -> None:
        self.index = index
        if clearIndexCache:
            self.index.clearIndexes()
        self.index.openExisting(checkMetadata=checkMetadata)

    def __enter__(self):
        return self

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.index.close()

    @overrides(MountSource)
    def isImmutable(self) -> bool:
        return True

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.index.getFileInfo(path, fileVersion=fileVersion)

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        return self.index.listDir(path)

    @overrides(MountSource)
    def listDirModeOnly(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        return self.index.listDirModeOnly(path)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        fileVersions = self.index.fileVersions(path)
        return len(fileVersions) if isinstance(fileVersions, dict) else 0
