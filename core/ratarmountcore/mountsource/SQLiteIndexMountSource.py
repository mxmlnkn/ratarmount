# pylint: disable=abstract-method

import builtins
import json
import logging
import re
import shutil
import tempfile
from collections.abc import Iterable
from typing import IO, Any, Callable, Optional, Union

from ratarmountcore.hashing import compute_hashes
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.ProgressBar import ProgressBar
from ratarmountcore.SQLiteIndex import SQLiteIndex
from ratarmountcore.utils import RatarmountError, overrides

logger = logging.getLogger(__name__)


class SQLiteIndexMountSource(MountSource):
    def __init__(
        self,
        index: Optional[Union[str, IO[bytes]]] = None,
        *,  # force all parameters after to be keyword-only
        clearIndexCache: bool = False,
        checkMetadata: Optional[Callable[[dict[str, Any]], None]] = None,
        transform: Optional[tuple[str, str]] = None,
        hashes: Optional[list[str]] = None,
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
        self.hashes = sorted(set(hashes or []))

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
            self.index = SQLiteIndex(indexFilePath=self.indexFilePath, indexFolders=[], readOnly=True)

        # Initialize members before using checkMetadata because it might to try to use those.
        self.archiveFilePath = self.index.archiveFilePath
        self.encoding = self.index.encoding

        # Try to load existing index.
        if index is None:
            self.index.open_existing(checkMetadata=checkMetadata or self._check_metadata)
        else:
            self.index.open_existing(checkMetadata=checkMetadata or self._check_metadata_dummy)
            if not self.index.index_is_loaded():
                raise RatarmountError(f"Specified file {self.indexFilePath} is not a valid Ratarmount index.")

    def _store_default_metadata(self) -> None:
        argumentsToSave = ['encoding', 'transformPattern', 'hashes']
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
                json.loads(metadata['arguments']), self, argumentsToCheck=['encoding', 'transformPattern', 'hashes']
            )

        if 'backendName' not in metadata:
            self.index.try_to_open_first_file(lambda path: self.open(self.lookup(path)))

    def _compute_and_store_hashes(self) -> None:
        if not self.hashes:
            return

        # We want to find all rows in "files" that are missing any of the self.hashes in "xattrs".
        # This makes it possible to interrupt the slow checksumming and resume it later!
        required_xattrs = [f"user.hash.{h}" for h in self.hashes]
        # The LEFT JOIN "attaches" the xattrs rows onto the left ("files") rows.
        # All "files" rows are returned. If 1+ xattr row(s) got matched with a file row (by the offsetheader),
        # then 1+ rows will be returned. The file row contents will be duplicated for each separate xattr row.
        # Example result: SELECT * FROM files LEFT JOIN xattrs ON files.offsetheader == xattrs.offsetheader;
        #     /|0|0||1|0.0|33216|0||1000|1000|0|0|0|0|0|user.hash.crc32|f4dbdf21
        #     /|0|0||1|0.0|33216|0||1000|1000|0|0|0|0|0|user.hash.smplayer|0000000000000031
        #     /|1|1||1|0.0|33216|0||1000|1000|0|0|0|0|1|user.hash.crc32|83dcefb7
        #     /|1|1||1|0.0|33216|0||1000|1000|0|0|0|0|1|user.hash.smplayer|0000000000000032
        #     /|10|2||1|0.0|33216|0||1000|1000|0|0|0|0|2|user.hash.crc32|f4dbdf21
        #     ...
        #     /|99992|162301||1|0.0|33216|0||1000|1000|0|0|0|0|||
        #     /|99991|162300||1|0.0|33216|0||1000|1000|0|0|0|0|||
        #     /|99990|162299||1|0.0|33216|0||1000|1000|0|0|0|0|||
        #  -> This is for a single folder with ~163k files containing 1 B each simply named with a number.
        #     It is tests/tar-with-300-folders-with-1000-files-1B-files.tar.bz2 not fully extracted.
        # After that LEFT JOIN, we need to use GROUP BY again to restore the "files" primary key and then
        # count the number of distinct xattrs keys (already filtered to only be the hashes of interest) per grouped
        # (primary) key triplet.
        # HAVING filters >after< an aggregation (GROUP BY) whereas WHERE filters >before< aggregations.
        subquery = f'''(
            SELECT files.*
            FROM "files"
            LEFT JOIN xattrs
                   ON xattrs.offsetheader = files.offsetheader
                  AND xattrs.key IN ({",".join("?" for _ in required_xattrs)})
            WHERE (mode & 0xF000) == 0x8000
              AND files.offsetheader IS NOT NULL
              AND NOT isgenerated
            GROUP BY files.path, files.name, files.offsetheader
            HAVING COUNT(DISTINCT xattrs.key) < ?
        )'''

        totals = (
            self.index.get_connection()
            .execute(f'SELECT SUM(size) FROM {subquery};', (*required_xattrs, len(required_xattrs)))
            .fetchone()
        )
        totalBytes = int(totals[0]) if totals and totals[0] is not None else 0

        # Simply go over all file rows instead of expensive and complicated recursive tree traversal.
        rows = self.index.get_connection().execute(
            f'SELECT * FROM {subquery} ORDER BY "offsetheader" ASC;', (*required_xattrs, len(required_xattrs))
        )
        xattrs: list[tuple[int, str, bytes]] = []
        hashedBytes = 0
        with ProgressBar(totalBytes, description="Checksumming", showRate=True) as progressBar:

            def chunk_update(byteCount):
                progressBar.update(hashedBytes + byteCount)

            for row in rows:
                fileInfo = self.index._row_to_file_info(row)  # pylint: disable=protected-access
                if not fileInfo.userdata:
                    continue
                userData = fileInfo.userdata[-1]

                try:
                    with self.open(fileInfo) as fileObject:
                        computed = compute_hashes(
                            fileObject, fileInfo.size, self.hashes, progress_callback=chunk_update
                        )
                except Exception as exception:
                    logger.warning(
                        "Failed to compute hashes for indexed file %s/%s: %s",
                        row['path'].rstrip('/'),
                        row['name'],
                        exception,
                        exc_info=logger.isEnabledFor(logging.DEBUG),
                    )
                    continue
                finally:
                    hashedBytes += fileInfo.size

                xattrs += [
                    (userData.offsetheader, f"user.hash.{name}", value.encode('utf-8'))
                    for name, value in computed.items()
                ]
                if len(xattrs) >= 1000:
                    self.index.setxattrs(xattrs)
                    xattrs.clear()

            progressBar.update(totalBytes)

        if xattrs:
            self.index.setxattrs(xattrs)

    def _finalize_index(
        self,
        create_index: Callable[[], None],
        *,  # force all parameters after to be keyword-only
        store_metadata: Optional[Callable[[], None]] = None,
        isFileObject: Optional[bool] = None,
    ):
        callable_store_metadata = store_metadata if callable(store_metadata) else self._store_default_metadata
        if self.index.index_is_loaded():
            # index_is_loaded checks for completed 'files' (stats etc.) table.
            # It does not check regarding xattrs completenes. We need to check whether some hashes
            # should still be computed.
            self._compute_and_store_hashes()
            callable_store_metadata()
            self.index.reload_index_read_only()
            return

        def create_index_and_post_process(index=self.index):
            create_index()
            # Finalize the 'files' table from 'filestmp' and 'parentfolders' so that
            # the by magnitudes costlier checksumming can be interrupted and resumed.
            index.finalize()
            self._compute_and_store_hashes()

        self.index.finalize_index(
            create_index=create_index_and_post_process,
            store_metadata=callable_store_metadata,
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
