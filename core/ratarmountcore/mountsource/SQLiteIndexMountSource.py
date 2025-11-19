# pylint: disable=abstract-method

import builtins
import re
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
        *,  # force all parameters after to be keyword-only
        clearIndexCache: bool = False,
        checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None,
        transform: Optional[tuple[str, str]] = None,
        writeIndex: bool = False,
        **_,
    ) -> None:
        """
        clearIndexCache
            If true, then check all possible index file locations for the given tarFileName/fileObject
            combination and delete them. This also implicitly forces a recreation of the index.
        writeIndex
            If true, then the sidecar index file will be written to a suitable location.
            Will be ignored if indexFilePath is ':memory:' or if only fileObject is specified
            but not tarFileName.
        """
        self.indexFilePath = ""
        self.transformPattern = transform
        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )
        self.writeIndex = writeIndex

        # Initialize index
        if isinstance(index, SQLiteIndex):
            self.index = index
            if clearIndexCache:
                self.index.clear_indexes()
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

        # Initialize members before using checkMetadata because it might to try to use those.
        self.archiveFilePath = self.index.archiveFilePath
        self.encoding = self.index.encoding

        # Try to load existing index.
        if isinstance(index, SQLiteIndex):
            self.index.open_existing(checkMetadata=checkMetadata)
        else:
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
