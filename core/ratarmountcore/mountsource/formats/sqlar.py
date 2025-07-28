import os
import shutil
import sqlite3
import sys
import tempfile
import urllib.parse
from collections.abc import Iterable, Sequence
from typing import IO, Any, Optional, Union

from ratarmountcore.formats import FileFormatID, replace_format_check
from ratarmountcore.mountsource import FileInfo, MountSource, create_root_file_info
from ratarmountcore.SQLiteBlobFile import SQLiteBlobFile
from ratarmountcore.StenciledFile import LambdaReaderFile
from ratarmountcore.utils import overrides

try:
    import rapidgzip
except ImportError:
    rapidgzip = None

# Password protection requires the paid SEE version of SQLite or sqlcipher.
#  - https://www.sqlite.org/see/doc/trunk/www/readme.wiki
#  - https://github.com/sqlcipher/sqlcipher
#
# Overview of Python bindings:
#  - https://github.com/rigglemania/pysqlcipher3
#    Last update: 2 years ago
#    > This project is no longer being actively maintained. Security vulnerabilities may exist in this code.
#    > Use at your own risk.
#    > This library is a fork of pysqlcipher targeted for use with Python 3, although support for Python 2
#    > is still maintained
#    - https://pypi.org/project/pysqlcipher3
#
#  - https://github.com/coleifer/pysqlite3
#    Last commit: 2 months ago
#    > This library takes the SQLite module from Python 3 and packages it as a separately-installable module.
#    > This may be useful for creating SQLite modules capable of working with other versions of SQLite
#    > (via the amalgamation option).
#
#  - https://github.com/coleifer/sqlcipher3
#    Last commit: 8 months ago
#    > This library takes pysqlite3 and makes some small modifications so it is suitable for use
#    > with sqlcipher (sqlite with encryption).
#    - https://pypi.org/project/sqlcipher3/
#    - https://pypi.org/project/sqlcipher3-binary/
#      - Only has 8 wheels, namely only manylinux x86_64 for each CPython version 3.6 to 3.13.
#        No support for Windows, MacOS, Power, ARM, ...
#
#    - https://github.com/laggykiller/sqlcipher3
#      Fork. Last commit: 2 months ago, 23 stars
#      > This is a fork of sqlcipher3 which adds github action for creating wheels for Windows, MacOS and Linux.
#      > The unofficial wheels from this fork are uploaded to sqlcipher3-wheels on pypi, while the official
#      > wheels are sqlcipher3-binary.
#      See issue on fork base "PR for building wheels and other improvements"
#      - https://github.com/coleifer/sqlcipher3/issues/24
#      - https://pypi.org/project/sqlcipher3-wheels/
#
# The SQLAR (SQLite Archiver) format is fortunately pretty simple. It's very similar to SQLiteIndex used for
# ratarmount but it also stores the file contents and has some fewer columns for metadata.
#
# https://www.sqlite.org/sqlar.html
# https://sqlite.org/sqlar/doc/trunk/README.md
#
# CREATE TABLE sqlar(
#     name TEXT PRIMARY KEY,  -- name of the file
#     mode INT,               -- access permissions
#     mtime INT,              -- last modification time
#     sz INT,                 -- original file size
#     data BLOB               -- compressed content
# );
#
# > Both directories and empty files have sqlar.sz==0. Directories can be distinguished from empty files
# > because directories have sqlar.data IS NULL. The file is compressed if length(sqlar.blob)<sqlar.sz
# > and is stored as plaintext if length(sqlar.blob)==sqlar.sz.
# >
# > Symbolic links have sqlar.sz set to -1, and the link target stored as a text value in the sqlar.data field.
# >
# > SQLAR uses the "zlib format" for compression.
#
# Based on the code, "name" is assumed to not start with a leading slash.
#
# Unfortunately, it has only a single "name" column instead of splitting the file path into folder and name.
# This requires linear scans of the database in order to list all files of a folder. I find that unfortunate.
# This was one of the things I
#
# One might think that there should be no file versions because the file name(path) is the primary key,
# but using denormal paths such as a, /a, //a, ./a, and so on, one could hack file versions into this format.
# Then again, the original tool woult not support looking this up because of its globbing rule, so maybe
# let's simply ignore that this could happen. The format is not very s
#
# You can even download the SQLite source code as an sqlar archive:
#  - https://www.sqlite.org/src/dir?ci=trunk
#  - https://sqlite.org/src/sqlar/sqlite.sqlar?r=release

try:
    # This package is required by many other packages, so it is almost safe to require it:
    # Required-by: asyncssh, azure-identity, azure-storage-blob, fido2, gajim, msal, oci, omemo-dr, pyOpenSSL, pyspnego,
    #              smbprotocol, types-paramiko, types-pyOpenSSL, types-redis
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
    from sqlcipher3 import dbapi2 as sqlcipher3  # type:ignore

    # No way to detect encrypted SQLAR.
    replace_format_check(FileFormatID.SQLAR, lambda x: True)
except ImportError:
    # The cryptography imports can fail pretty badly and it does not seem to be catchable :(
    #
    #    core/tests/test_formats.py:13: in <module>
    #        import ratarmountcore.mountsource.archives  # noqa: E402, F401
    #        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    #    core/ratarmountcore/mountsource/archives.py:14: in <module>
    #        from .formats.sqlar import SQLARMountSource
    #    core/ratarmountcore/mountsource/formats/sqlar.py:99: in <module>
    #        from cryptography.hazmat.primitives import hashes
    #    /usr/lib/python3/dist-packages/cryptography/hazmat/primitives/hashes.py:10: in <module>
    #        from cryptography.hazmat.bindings._rust import openssl as rust_openssl
    #    E   pyo3_runtime.PanicException: Python API call failed
    #    ------------------------- Captured stderr -------------------------
    #    ModuleNotFoundError: No module named '_cffi_backend'
    #    thread '<unnamed>' panicked at /usr/share/cargo/registry/pyo3-0.20.2/src/err/mod.rs:788:5:
    #    Python API call failed
    #    note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
    #
    # In my case, pytest pointed to the Python 3.11 version even though my system-default is 3.12.
    # Using python3 -m pytest instead of pytest fixed the error. (Re)installing cffi and cryptography in 3.11
    # might also have worked.
    sqlcipher3 = None  # type:ignore
    PBKDF2HMAC = None  # type:ignore
    default_backend = None  # type:ignore


class FixedSqliteBlob(LambdaReaderFile):
    """
    Rapidgzip expects a seek method, but it does not exist for sqlite3.Blob :'(
    This wrapper adds a seekable method among many others.
    https://docs.python.org/3/library/sqlite3.html#sqlite3.Blob
    """

    def __init__(self, blob):
        self.blob = blob

        def pread(offset: int, size: int) -> bytes:
            self.blob.seek(offset)
            return self.blob.read(size)

        super().__init__(pread, len(self.blob))


class SQLARMountSource(MountSource):
    _SQLITE_FILEINFO_COLUMNS = "rowid, mode, mtime, sz, CASE WHEN sz=-1 THEN data ELSE '' END"
    _MAGIC_BYTES = b'SQLite format 3\x00'

    def __init__(
        self, fileOrPath: Union[str, IO[bytes]], passwords: Optional[Sequence[bytes]] = None, **options
    ) -> None:
        self._temporaryFilePath: Optional[Any] = None

        magicBytes = b""
        if isinstance(fileOrPath, str):
            with open(fileOrPath, 'rb') as file:
                magicBytes = SQLARMountSource._quick_check_file(file, fileOrPath, passwords)
            self.indexFilePath = fileOrPath
        else:
            magicBytes = SQLARMountSource._quick_check_file(fileOrPath, "File object", passwords)

            # Copy to a temporary file because sqlite cannot work with Python file objects. This can be wasteful!
            fileOrPath.seek(0)
            self._temporaryFile = tempfile.NamedTemporaryFile(suffix=".ratarmount.sqlar", delete=True)
            shutil.copyfileobj(fileOrPath, self._temporaryFile.file)  # type: ignore
            self._temporaryFile.file.flush()

            self.indexFilePath = self._temporaryFile.name

        uriPath = f"file:{urllib.parse.quote(self.indexFilePath)}?mode=ro"
        connection: Optional[Any] = None
        try:
            # check_same_thread=False can be used because it is read-only and allows to enable FUSE multithreading.
            connection = sqlite3.connect(uriPath, uri=True, check_same_thread=False)
            SQLARMountSource._check_database(connection)
        except sqlite3.DatabaseError:
            if connection:
                connection.close()
                connection = None
            if passwords and sqlcipher3 is not None and PBKDF2HMAC is not None and len(magicBytes) >= 16:
                connection = self._find_password(uriPath, passwords, salt=magicBytes[:16])
            if not connection:
                raise
        assert connection
        self.connection = connection

        self.options = options

        # Check for denormal names and store a normalized list if necessary.
        self._files: dict[str, int] = {}
        if any(
            os.path.normpath(name.lstrip('/')) != name for name, in self.connection.execute("SELECT name FROM sqlar;")
        ):
            self._files = {
                os.path.normpath(name.lstrip('/')): rowid
                for name, rowid in self.connection.execute("SELECT name,rowid FROM sqlar ORDER BY rowid;")
            }

    @staticmethod
    def _quick_check_file(fileObject: IO[bytes], name: str, passwords: Optional[Sequence[bytes]]) -> bytes:
        try:
            magicBytes = fileObject.read(len(SQLARMountSource._MAGIC_BYTES))
        finally:
            fileObject.seek(0)

        if magicBytes == SQLARMountSource._MAGIC_BYTES or (sqlcipher3 is not None and passwords):
            return magicBytes
        message = name + " is not an SQLAR file."
        if sqlcipher3 is None and passwords:
            message += " Passwords have been ignored because sqlcipher3 and cryptography are not installed."
            message += " Please install with 'pip install sqlcipher3-binary cryptography' or 'pip install sqlcipher3-wheels cryptography'."
        if sqlcipher3 is not None and not passwords:
            message += " Please specify passwords if it is an encrypted SQLAR."
        raise ValueError(message)

    @staticmethod
    def _check_database(connection) -> bool:
        # May throw when sqlar does not exist or it is encrypted without the correct key being specified.
        result = connection.execute("SELECT name FROM sqlar LIMIT 1;").fetchone()
        return result and result[0]

    @staticmethod
    def _find_password(uriPath: str, passwords: Sequence[bytes], salt: bytes):
        for password in passwords:
            # Do the key derivation manually in order to support all characters in passwords, even " and ;.
            # https://stackoverflow.com/a/79657272
            # https://www.zetetic.net/sqlcipher/sqlcipher-api/#PRAGMA_key
            # https://www.zetetic.net/sqlcipher/design/
            keyDerivation = PBKDF2HMAC(
                algorithm=hashes.SHA512(),
                # 256-bit key for 256-bit AES in CBC mode
                length=32,
                # The salt is stored in the first 16 bytes.
                salt=salt,
                # This is the current default. Older versions may have used fewer iterations.
                # It can also be specified with 'PRAGMA kdf_iter'.
                iterations=256_000,
                backend=default_backend(),
            )

            try:
                connection = sqlcipher3.connect(uriPath, uri=True, check_same_thread=False)  # pylint:disable=no-member
                # https://stackoverflow.com/questions/77406316/
                #   how-do-you-safely-pass-values-to-sqlite-pragma-statements-in-python
                # Single quotes after the key= do not work!
                connection.execute(f"""PRAGMA key="x'{keyDerivation.derive(password).hex()}'";""")
                # Unfortunately sqlcipher3 prints lots of output on a wrong password:
                #   2025-06-07 20:42:22.736: ERROR CORE sqlcipher_page_cipher: hmac check failed for pgno=1
                #   2025-06-07 20:42:22.736: ERROR CORE sqlite3Codec: error decrypting page 1 data: 1
                #   2025-06-07 20:42:22.736: ERROR CORE sqlcipher_codec_ctx_set_error 1
                SQLARMountSource._check_database(connection)
                return connection
            except sqlcipher3.DatabaseError:  # pylint:disable=no-member
                pass
        return None

    @staticmethod
    def _convert_to_file_info(rowid: int, mode: int, mtime: int, size: int, linkname: str) -> FileInfo:
        # fmt: off
        return FileInfo(
            size     = max(0, int(size)),
            mtime    = int(mtime),
            mode     = mode,
            linkname = linkname,
            uid      = os.getuid(),
            gid      = os.getgid(),
            userdata = [rowid],
        )
        # fmt: on

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        return True

    def _list_names(self, path: str) -> Optional[Iterable[str]]:
        if not self._files:
            return None

        path = path.lstrip('/')
        if path and not path.endswith('/'):
            path = path + '/'

        # Use list instead of set to keep the order.
        names = []
        for name in self._files:
            if not name.startswith(path):
                continue
            name = name[len(path) :].split('/', maxsplit=1)[0]
            if name not in names:
                names.append(name)
        return names

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        if self._files:
            return self._list_names(path)

        pathGlob = "*" if path == "/" else path.strip("/") + "/*"
        return {
            name: SQLARMountSource._convert_to_file_info(rowid, mode, mtime, size, linkname)
            for name, rowid, mode, mtime, size, linkname in self.connection.execute(
                f"SELECT substr(name,(?)),{SQLARMountSource._SQLITE_FILEINFO_COLUMNS} FROM sqlar "
                "WHERE name GLOB (?) AND substr(name,(?)) NOT GLOB '*/*'",
                (len(pathGlob), pathGlob, len(pathGlob)),
            )
        }

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], dict[str, int]]]:
        if self._files:
            return self._list_names(path)

        pathGlob = "*" if path == "/" else path.strip("/") + "/*"
        return dict(
            self.connection.execute(
                "SELECT substr(name,(?)), mode FROM sqlar WHERE name GLOB (?) AND substr(name,(?)) NOT GLOB '*/*'",
                (len(pathGlob), pathGlob, len(pathGlob)),
            )
        )

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        if path == "/":
            return create_root_file_info([-1])

        if self._files:
            path = path.lstrip('/')
            # We assume here that all parent folders to files appear in the table.
            if path not in self._files:
                return None
            rowid = self._files[path]

            result = self.connection.execute(
                f"SELECT {SQLARMountSource._SQLITE_FILEINFO_COLUMNS} FROM sqlar WHERE rowid=(?)", (rowid,)
            ).fetchone()
        else:
            result = self.connection.execute(
                f"SELECT {SQLARMountSource._SQLITE_FILEINFO_COLUMNS} FROM sqlar WHERE name=(?)", (path.strip("/"),)
            ).fetchone()

        return self._convert_to_file_info(*result) if result else None

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        assert fileInfo.userdata
        rowid = fileInfo.userdata[-1]
        assert isinstance(rowid, int)

        result = self.connection.execute("SELECT sz,LENGTH(data) FROM sqlar WHERE rowid == (?);", (rowid,)).fetchone()
        if not result:
            raise FileNotFoundError(f"[SQLARMountSource] rowid: {rowid}")

        # Simply because we can, already try to use the new Python 3.11 sqlite3.Connection.blobopen helper!
        # Does not work for sqlcipher3 0.5.4!
        blob: Any = None
        if sys.version_info >= (3, 11) and hasattr(self.connection, 'blobopen'):
            blob = FixedSqliteBlob(self.connection.blobopen("sqlar", "data", rowid, readonly=True))
        else:
            blob = SQLiteBlobFile(
                self.connection, f"SELECT {{}}data{{}} FROM sqlar WHERE ROWID == {rowid}", size=int(result[1])
            )
        return blob if result[0] == result[1] else rapidgzip.open(blob)

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.connection.close()
        if self._temporaryFilePath:
            self._temporaryFilePath.close()
