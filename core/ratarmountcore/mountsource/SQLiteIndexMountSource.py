# pylint: disable=abstract-method

import shutil
import tempfile
from typing import IO, Any, Callable, Dict, Iterable, List, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import RatarmountError, overrides


class SQLiteIndexMountSource(MountSource):
    def __init__(
        self,
        index: Union[SQLiteIndex, str, IO[bytes]],
        clearIndexCache: bool = False,
        checkMetadata: Optional[Callable[[Dict[str, Any]], None]] = None,
        printDebug: int = 0,
        **_,
    ) -> None:
        self.indexFilePath = ""

        if isinstance(index, SQLiteIndex):
            self.index = index
            if clearIndexCache:
                self.index.clearIndexes()
            self.index.openExisting(checkMetadata=checkMetadata)
        else:
            # Open existing index without any corresponding archive, i.e., file open will not work!
            if isinstance(index, str):
                with open(index, 'rb') as file:
                    SQLiteIndexMountSource._quickCheckFile(file, index)
                self.indexFilePath = index
            else:
                SQLiteIndexMountSource._quickCheckFile(index, "File object")

                # Copy to a temporary file because sqlite cannot work with Python file objects. This can be wasteful!
                index.seek(0)
                self._temporaryFile = tempfile.NamedTemporaryFile(suffix=".ratarmount.index.sqlite", delete=True)
                shutil.copyfileobj(index, self._temporaryFile.file)  # type: ignore
                self._temporaryFile.file.flush()

                self.indexFilePath = self._temporaryFile.name

            # Encoding is only used for setFileInfos, so we are fine not forwarding it.
            self.index = SQLiteIndex(indexFilePath=self.indexFilePath, indexFolders=[], printDebug=printDebug)
            self.index.openExisting(checkMetadata=checkMetadata, readOnly=True)
            if not self.index.indexIsLoaded():
                raise RatarmountError(f"Specified file {self.indexFilePath} is not a valid Ratarmount index.")

    @staticmethod
    def _quickCheckFile(fileObject: IO[bytes], name: str) -> None:
        try:
            if fileObject.read(len(SQLiteIndex.MAGIC_BYTES)) == SQLiteIndex.MAGIC_BYTES:
                return
        finally:
            fileObject.seek(0)

        raise RatarmountError(name + " is not an ratarmount index file.")

    @staticmethod
    def _checkDatabase(connection) -> bool:
        # May throw when sqlar does not exist or it is encrypted without the correct key being specified.
        result = connection.execute("SELECT name FROM sqlar LIMIT 1;").fetchone()
        return result and result[0]

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
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        raise NotImplementedError

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

    @overrides(MountSource)
    def listxattr(self, fileInfo: FileInfo) -> List[str]:
        return self.index.listxattr(fileInfo)

    @overrides(MountSource)
    def getxattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        return self.index.getxattr(fileInfo, key)
