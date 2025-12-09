# pylint: disable=abstract-method

import builtins
import json
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
        index: Optional[Union[str, IO[bytes]]] = None,
        *,  # force all parameters after to be keyword-only
        clearIndexCache: bool = False,
        checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None,
        transform: Optional[tuple[str, str]] = None,
        writeIndex: bool = False,
        verifyModificationTime: bool = False,
        indexMinimumFileCount: int = 1000,
        **options,
    ) -> None:
        """
        clearIndexCache
            If true, then check all possible index file locations for the given tarFileName/fileObject
            combination and delete them. This also implicitly forces a recreation of the index.
        writeIndex
            If true, then the sidecar index file will be written to a suitable location.
            Will be ignored if indexFilePath is ':memory:' or if only fileObject is specified
            but not tarFileName.
        verifyModificationTime
            If true, then the index will be recreated automatically if the TAR archive has a more
            recent modification time than the index file.
        """
        self.indexFilePath = ""
        self.transformPattern = transform
        self.transform = (
            (lambda x: re.sub(self.transformPattern[0], self.transformPattern[1], x))
            if isinstance(self.transformPattern, (tuple, list)) and len(self.transformPattern) == 2
            else (lambda x: x)
        )
        self.writeIndex = writeIndex
        self.verifyModificationTime = verifyModificationTime
        self.options = options

        # Initialize index
        if index is None:
            self.index = SQLiteIndex(indexMinimumFileCount=indexMinimumFileCount, **options)
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
        if index is None:
            self.index.open_existing(checkMetadata=checkMetadata or self._check_metadata)
        else:
            self.index.open_existing(checkMetadata=checkMetadata or self._check_metadata_dummy, readOnly=True)
            if not self.index.index_is_loaded():
                raise RatarmountError(f"Specified file {self.indexFilePath} is not a valid Ratarmount index.")

    def _store_default_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern']
        argumentsMetadata = json.dumps(
            {argument: getattr(self, argument) for argument in argumentsToSave if hasattr(self, argument)}
        )
        self.index.store_metadata(argumentsMetadata)

    def _check_metadata_dummy(self, metadata: dict[str, Any]) -> None:
        pass

    def _check_metadata(self, metadata: dict[str, Any]) -> None:
        """Raises an exception if the metadata mismatches so much that the index has to be treated as incompatible."""
        SQLiteIndex.check_archive_stats(self.archiveFilePath, metadata, self.verifyModificationTime)

        if 'arguments' in metadata:
            SQLiteIndex.check_metadata_arguments(
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern']
            )

        if 'backendName' not in metadata:
            self.index.try_to_open_first_file(lambda path: self.open(self.lookup(path)))

    def _finalize_index(
        self,
        create_index: Callable[[], None],
        *,  # force all parameters after to be keyword-only
        store_metadata: Optional[Callable[[], None]] = None,
        isFileObject: Optional[bool] = None,
    ):
        """
        metadata
            Should either be a list of attributes on 'self' that should be stored or a callable that stores
            metadata by calling self.index.store_metadata. If it is None a default selection of attributes
            will be saved.
        """
        self.index.finalize_index(
            create_index=create_index,
            store_metadata=store_metadata if callable(store_metadata) else self._store_default_metadata,
            isFileObject=isFileObject,
            writeIndex=self.writeIndex,
        )

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
        self.close()

    def close(self):
        if index := getattr(self, 'index', None):
            index.close()

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
