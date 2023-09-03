#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import sqlite3

from typing import cast, IO, Optional

from .utils import overrides
from .StenciledFile import JoinedFile, LambdaReaderFile


class SQLiteBlobFile(LambdaReaderFile):
    """Provides a memory-efficient file object interface to a single large blob inside an SQLite table"""

    def __init__(self, connection: sqlite3.Connection, expression: str, size: Optional[int] = None):
        """
        expression: Should yield a single blob when executed. And should also contain format string placeholders {}
                    before and after the column name.
        """

        self.connection = connection
        self.expression = expression

        super().__init__(
            (lambda offset, size: SQLiteBlobFile.readBlobPart(self.connection, expression, offset, size)),
            self.connection.execute(self.expression.format("LENGTH(", ")")).fetchone()[0] if size is None else size,
        )

    @staticmethod
    def readBlobPart(connection: sqlite3.Connection, expression: str, offset: int, size: int):
        # Note that SQLite offsets begin counting at 1 unlike any other programming language except maybe for Fortran
        return connection.execute(expression.format("substr(", ",?,?)"), (offset + 1, size)).fetchone()[0]


class SQLiteBlobsFile(JoinedFile):
    """Provides a memory-efficient file object interface to multiple large blobs inside an SQLite table"""

    def __init__(
        self, connection: sqlite3.Connection, table: str, column: str, buffer_size: int = io.DEFAULT_BUFFER_SIZE
    ):
        super().__init__(
            [
                cast(
                    IO,
                    SQLiteBlobFile(
                        connection, f"SELECT {{}}{column}{{}} FROM {table} WHERE ROWID == {rowid}", size=size
                    ),
                )
                for rowid, size in connection.execute(f"SELECT ROWID,LENGTH({column}) FROM {table} ORDER BY ROWID")
            ],
            buffer_size=buffer_size,
        )


class WriteSQLiteBlobs(io.RawIOBase):
    def __init__(self, connection: sqlite3.Connection, table: str, blob_size: int = io.DEFAULT_BUFFER_SIZE) -> None:
        io.RawIOBase.__init__(self)
        self.connection = connection
        self.table = table
        self.blob_size = blob_size
        self.blob = io.BytesIO()
        self._size = 0

    def _flushBlob(self):
        if self.blob.tell() > 0:
            self.connection.execute(f'INSERT INTO {self.table} VALUES (?)', (self.blob.getbuffer(),))
        self.blob = io.BytesIO()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self._flushBlob()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self._flushBlob()

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def write(self, buffer) -> int:
        freeBytesInBlob = self.blob_size - self.blob.tell()
        writtenCount = 0

        if len(buffer) < freeBytesInBlob:
            writtenCount += self.blob.write(buffer)
        else:
            writtenCount += self.blob.write(buffer[:freeBytesInBlob])
            self._flushBlob()
            writtenCount += self.blob.write(buffer[freeBytesInBlob:])

        self._size += writtenCount

        if writtenCount != len(buffer):
            raise RuntimeError("Failed to write all of the given data out!")

        return len(buffer)

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        # We are always at SEEK_END because no real seeking is implemented.
        if (whence in [io.SEEK_CUR, io.SEEK_END] and offset == 0) or (whence == io.SEEK_SET and offset == self.tell()):
            return self._size
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self._size
