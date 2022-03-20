#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io
import sqlite3

from typing import cast, IO, Optional

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
