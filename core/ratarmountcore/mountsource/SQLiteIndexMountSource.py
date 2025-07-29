# pylint: disable=abstract-method

import builtins
import shutil
import tempfile
from collections.abc import Iterable
from typing import IO, Any, Callable, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import RatarmountError, overrides


class SQLiteIndexMountSource(MountSource):
    def __init__(
        self,
        index: Union[SQLiteIndex, str, IO[bytes]],
        clearIndexCache: bool = False,
        checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None,
        **_,
    ) -> None:
        self.indexFilePath = ""

        if isinstance(index, SQLiteIndex):
            self.index = index
            if clearIndexCache:
                self.index.clear_indexes()
            self.index.open_existing(checkMetadata=checkMetadata)
        else:
            # Open existing index without any corresponding archive, i.e., file open will not work!
            if isinstance(index, str):
                with open(index, 'rb') as file:
                    SQLiteIndexMountSource._quick_check_file(file, index)
                self.indexFilePath = index
            else:
                SQLiteIndexMountSource._quick_check_file(index, "File object")

                # Copy to a temporary file because sqlite cannot work with Python file objects. This can be wasteful!
                index.seek(0)
                self._temporaryFile = tempfile.NamedTemporaryFile(suffix=".ratarmount.index.sqlite", delete=True)
                shutil.copyfileobj(index, self._temporaryFile.file)  # type: ignore
                self._temporaryFile.file.flush()

                self.indexFilePath = self._temporaryFile.name

            # Encoding is only used for set_file_infos, so we are fine not forwarding it.
            self.index = SQLiteIndex(indexFilePath=self.indexFilePath, indexFolders=[], deleteInvalidIndexes=False)
            self.index.open_existing(checkMetadata=checkMetadata, readOnly=True)
            if not self.index.index_is_loaded():
                raise RatarmountError(f"Specified file {self.indexFilePath} is not a valid Ratarmount index.")

    @staticmethod
    def _quick_check_file(fileObject: IO[bytes], name: str) -> None:
        try:
            if fileObject.read(len(SQLiteIndex.MAGIC_BYTES)) == SQLiteIndex.MAGIC_BYTES:
                return
        finally:
            fileObject.seek(0)

        raise RatarmountError(name + " is not an ratarmount index file.")

    @staticmethod
    def _check_database(connection) -> bool:
        # May throw when sqlar does not exist or it is encrypted without the correct key being specified.
        result = connection.execute("SELECT name FROM sqlar LIMIT 1;").fetchone()
        return result and result[0]

    def __enter__(self):
        return self

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.index.close()

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.index.lookup(path, fileVersion=fileVersion)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        raise NotImplementedError

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return self.index.list(path)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        return self.index.list_mode(path)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        versions = self.index.versions(path)
        return len(versions) if isinstance(versions, dict) else 0

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        return self.index.list_xattr(fileInfo)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        return self.index.get_xattr(fileInfo, key)
