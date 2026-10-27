# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import bz2
import logging
import os
import shutil
import socket
import ssl
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from helpers import change_working_directory, find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.factory import _webdav_transport_protocol, open_mount_source
from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar
from ratarmountcore.mountsource.formats.zip import ZipMountSource
from ratarmountcore.utils import RatarmountError


@pytest.mark.order(-1)
@pytest.mark.parametrize("transform_path", [str, Path])
class TestOpenMountSource:
    @staticmethod
    def test_open_with_name_only(transform_path):
        # Specifying only a name, implying the current directory, triggered a bug because os.path.dirnmae("name")
        # returns an empty string and os.listdir raises FileNotFoundError on an empty string.
        # This should not be triggered via ratarmount because it uses os.realpath for all inputs,
        # but funnily enough, this got triggered by the AUR test, which directly calls open_mount_source!
        path, name = os.path.split(find_test_file("single-file.tar"))
        with change_working_directory(path), open_mount_source(transform_path(name)) as mountSource:
            assert mountSource.list("")

    @staticmethod
    def test_joining_archive(tmp_path, transform_path):
        compressed = bz2.compress(b"foobar")
        (tmp_path / "foo.001").write_bytes(compressed[: len(compressed) // 2])
        (tmp_path / "foo.002").write_bytes(compressed[len(compressed) // 2 :])

        for path in [tmp_path / "foo.001", tmp_path / "foo.002"]:
            with open_mount_source(transform_path(path)) as mountSource:
                fileInfo = mountSource.lookup("/<file object>")
                assert fileInfo
                assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_file(tmp_path, transform_path):
        (tmp_path / "foo.001").write_bytes(b"foo")
        (tmp_path / "foo.002").write_bytes(b"bar")

        for path in [tmp_path / "foo.001", tmp_path / "foo.002"]:
            with open_mount_source(transform_path(path)) as mountSource:
                fileInfo = mountSource.lookup("/foo")
                assert fileInfo
                assert mountSource.open(fileInfo).read() == b"foobar"

    @staticmethod
    def test_joining_files_exceeding_handle_limit(tmp_path, transform_path):
        result = b''
        for i in range(1100):  # Default on my system is 1024
            (tmp_path / f"foo.{i:03}").write_bytes(str(i).encode())
            result += str(i).encode()

        with open_mount_source(transform_path(tmp_path / "foo.005")) as mountSource:
            fileInfo = mountSource.lookup("/foo")
            assert fileInfo
            assert mountSource.open(fileInfo).read() == result

    @staticmethod
    def test_chimera_file(transform_path):
        chimeraFilePath = transform_path(Path(find_test_file("chimera-tbz2-zip")))
        indexPath = Path(str(chimeraFilePath) + ".index.sqlite")
        if indexPath.exists():
            indexPath.unlink()

        # Check simple open and that index files are NOT created because they are too small.
        with open_mount_source(
            chimeraFilePath, writeIndex=True, prioritizedBackends=['zipfile', 'rapidgzip']
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.list("/")
            assert files

            assert not indexPath.exists()

        # Same as above, but force index creation by lowering the file count threshold.
        with open_mount_source(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['zipfile', 'rapidgzip'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.list("/")
            assert files

            fileInfo = mountSource.lookup("/foo/fighter/ufo")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"iriya\n"

            assert indexPath.exists()

        # Check that everything works fine even if the index exists and the backend order is reversed.
        #
        # "Works fine" refers to a valid file hierarchy being shown and that files can be read without errors.
        # I'm not decided whether the next backend should be tried or whether the index should be recreated
        # by the first backend after an inconsistency has been noticed. The latter is easier to implement
        # and more consistent. I think, only after implementing storing the backend name into the index,
        # should the next backend be tried instead of it being overwritten and recreated.
        assert indexPath.exists()
        with open_mount_source(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['rapidgzip', 'zipfile'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, ZipMountSource)
            files = mountSource.list("/")
            assert files

            fileInfo = mountSource.lookup("/foo/fighter/ufo")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"iriya\n"

            assert indexPath.exists()

        indexPath.unlink()

        # Index file is always created for compressed files such as .tar.bz2
        with open_mount_source(
            chimeraFilePath, writeIndex=True, prioritizedBackends=['rapidgzip', 'zipfile']
        ) as mountSource:
            assert isinstance(mountSource, SQLiteIndexedTar)
            files = mountSource.list("/")
            assert files

            fileInfo = mountSource.lookup("/bar")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"foo\n"

            assert indexPath.exists()

        # Check that everything works fine even if the index exists and the backend order is reversed.
        assert indexPath.exists()
        with open_mount_source(
            chimeraFilePath,
            writeIndex=True,
            prioritizedBackends=['zipfile', 'rapidgzip'],
            indexMinimumFileCount=0,
        ) as mountSource:
            assert isinstance(mountSource, SQLiteIndexedTar)
            files = mountSource.list("/")
            print("files:", files)
            assert files

            fileInfo = mountSource.lookup("/bar")
            assert fileInfo
            with mountSource.open(fileInfo) as file:
                assert file.read() == b"foo\n"

            assert indexPath.exists()

        indexPath.unlink()


class TestWebDAVTransportProtocol:
    @staticmethod
    def _fail_probe_with(monkeypatch, exception):
        class FailingConnection:
            def __init__(self, *_args, **_kwargs):
                pass

            def request(self, *_args, **_kwargs):
                raise exception

            def close(self):
                pass

        monkeypatch.setattr("ratarmountcore.mountsource.factory.http.client.HTTPSConnection", FailingConnection)

    @staticmethod
    def test_webdavs_is_https_without_probing(monkeypatch):
        TestWebDAVTransportProtocol._fail_probe_with(monkeypatch, AssertionError("must not probe"))
        assert _webdav_transport_protocol("webdavs", "127.0.0.1:1") == "https"

    @staticmethod
    @pytest.mark.parametrize("exception", [ConnectionRefusedError(), TimeoutError(), ssl.SSLError("wrong version")])
    def test_webdav_falls_back_to_http_with_a_warning(monkeypatch, caplog, exception):
        TestWebDAVTransportProtocol._fail_probe_with(monkeypatch, exception)
        with caplog.at_level(logging.WARNING):
            assert _webdav_transport_protocol("webdav", "127.0.0.1:1") == "http"
        assert "webdavs://" in caplog.text

    @staticmethod
    def test_webdav_does_not_fall_back_on_an_invalid_certificate(monkeypatch):
        TestWebDAVTransportProtocol._fail_probe_with(monkeypatch, ssl.SSLCertVerificationError("self-signed"))
        with pytest.raises(RatarmountError, match="invalid"):
            _webdav_transport_protocol("webdav", "127.0.0.1:1")

    @staticmethod
    def test_webdav_does_not_fall_back_for_a_real_self_signed_server(tmp_path):
        if not shutil.which("openssl"):
            pytest.skip("openssl is required to create a self-signed certificate")
        # fmt: off
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-keyout", str(tmp_path / "key.pem"), "-out", str(tmp_path / "cert.pem")
            ],
            check=True,
            capture_output=True,
        )
        # fmt: on
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(tmp_path / "cert.pem"), str(tmp_path / "key.pem"))
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen()

            def serve():
                try:
                    with context.wrap_socket(server.accept()[0], server_side=True) as connection:
                        connection.recv(1)
                except (OSError, ssl.SSLError):
                    pass  # The client aborts the handshake because it does not trust the certificate.

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            with pytest.raises(RatarmountError, match="invalid"):
                _webdav_transport_protocol("webdav", f"127.0.0.1:{server.getsockname()[1]}")
            thread.join(timeout=5)
