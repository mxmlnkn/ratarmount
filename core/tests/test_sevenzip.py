# pylint: disable=wrong-import-order,wrong-import-position,protected-access
"""Tests for the custom 7z backend (parse, store, solid, streaming seek, AES, recursion, indexes)."""

import os
import stat
import sys
import tempfile
from pathlib import Path

import pytest

from helpers import copy_test_file, find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ratarmountcore.mountsource.compositing.automount import AutoMountLayer
from ratarmountcore.mountsource.factory import open_mount_source
from ratarmountcore.mountsource.formats.sevenzip import SevenZipMountSource
from ratarmountcore.sevenzip import (
    METHOD_AES,
    METHOD_COPY,
    METHOD_LZMA2,
    SevenZipError,
    StreamingFolderDecoder,
    decompress_folder,
    parse_7z_archive,
)


def _has_7z_cli() -> bool:
    from shutil import which

    return bool(which("7z") or which("7zz") or which("7za"))


def _7z_bin() -> str:
    from shutil import which

    for name in ("7z", "7zz", "7za"):
        path = which(name)
        if path:
            return path
    raise RuntimeError("7z CLI not found")


def _require_fixture(name: str) -> str:
    path = find_test_file(name)
    if not os.path.isfile(path):
        pytest.skip(f"Missing fixture {name}")
    return path


# ---------------------------------------------------------------------------
# M1: header parse + real pack offsets
# ---------------------------------------------------------------------------


class TestSevenZipParser:
    @pytest.mark.parametrize(
        "name",
        [
            "file-in-non-existing-folder.7z",
            "folder-symlink.7z",
            "nested-with-symlink.7z",
            "zip.7z",
            "two-large-files-32Ki-lines-each-1024B.7z",
            "store-copy-two-files.7z",
            "lzma2-two-files-and-medium.7z",
            "encrypted-hello.7z",
            "nested-inner-hello.7z",
            "nested-encrypted-inner.7z",
            "double-compressed-nested-tar.tar.7z.7z",
            "encrypted-nested-tar.7z",
        ],
    )
    def test_parse_fixture(self, name):
        path = _require_fixture(name)
        with open(path, "rb") as file:
            info = parse_7z_archive(file)
        assert info.files, name
        assert all(e.path for e in info.files), name
        for entry in info.files:
            if entry.folder_index is not None and entry.size > 0:
                assert entry.pack_offset >= 32
                assert entry.pack_size > 0

    def test_parse_file_in_non_existing_folder(self):
        with open(_require_fixture("file-in-non-existing-folder.7z"), "rb") as file:
            info = parse_7z_archive(file)
        assert len(info.files) == 1
        entry = info.files[0]
        assert entry.path == "foo2/ufo"
        assert entry.size == 6
        assert entry.pack_offset == 32
        assert entry.unpack_offset == 0
        assert entry.methods == (METHOD_LZMA2,)

    def test_parse_solid_two_large_files(self):
        with open(_require_fixture("two-large-files-32Ki-lines-each-1024B.7z"), "rb") as file:
            info = parse_7z_archive(file)
        assert info.solid
        assert len(info.files) == 2
        assert info.files[0].path == "spaces-32-MiB.txt"
        assert info.files[1].path == "zeros-32-MiB.txt"
        assert info.files[0].pack_offset == info.files[1].pack_offset
        assert info.files[1].unpack_offset == info.files[0].size

    def test_parse_store_copy_has_copy_method(self):
        with open(_require_fixture("store-copy-two-files.7z"), "rb") as file:
            info = parse_7z_archive(file)
        data_files = [e for e in info.files if e.size > 0]
        assert len(data_files) == 2
        assert all(e.methods == (METHOD_COPY,) for e in data_files)

    def test_parse_encrypted_has_aes(self):
        with open(_require_fixture("encrypted-hello.7z"), "rb") as file:
            info = parse_7z_archive(file)
        assert any(folder.is_encrypted() for folder in info.folders)
        entry = next(e for e in info.files if e.size > 0)
        assert entry.methods[0] == METHOD_AES

    def test_reject_non_7z(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b"not a 7z file")
            tmp.flush()
            with open(tmp.name, "rb") as file, pytest.raises(SevenZipError):
                parse_7z_archive(file)

    def test_parse_from_file_object_not_only_path(self):
        path = _require_fixture("store-copy-two-files.7z")
        with open(path, "rb") as file:
            info = parse_7z_archive(file)
        assert len([e for e in info.files if e.size > 0]) == 2


# ---------------------------------------------------------------------------
# M2/M3: open store, compressed, solid, symlinks
# ---------------------------------------------------------------------------


class TestSevenZipMountSource:
    def test_read_simple_member(self):
        with (
            copy_test_file("file-in-non-existing-folder.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as mount_source,
        ):
            file_info = mount_source.lookup("/foo2/ufo")
            assert file_info is not None
            assert not stat.S_ISDIR(file_info.mode)
            with mount_source.open(file_info) as file:
                assert file.read() == b"iriya\n"

    def test_store_copy_random_access(self):
        with (
            copy_test_file("store-copy-two-files.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/a.txt")) as file:
                data = file.read()
                assert data.startswith(b"AAA")
                assert data.endswith(b"AAA")
                file.seek(3)
                assert file.read(4) == b"aaaa"
            with mount_source.open(mount_source.lookup("/b.txt")) as file:
                data = file.read()
                assert data.startswith(b"BBB")
                file.seek(3)
                assert file.read(4) == b"bbbb"

    def test_lzma2_medium_member_seek(self):
        with (
            copy_test_file("lzma2-two-files-and-medium.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:", sevenZipSmallFolderThreshold=0) as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/a.txt")) as file:
                assert file.read().startswith(b"AAA")
            medium = mount_source.lookup("/medium.bin")
            assert medium is not None
            assert medium.size == 2 * 1024 * 1024
            with mount_source.open(medium) as file:
                assert file.read(16) == b"X" * 16
                file.seek(medium.size // 2)
                assert file.read(16) == b"X" * 16
                file.seek(medium.size - 16)
                assert file.read() == b"X" * 16
                file.seek(0)
                assert file.read(16) == b"X" * 16

    def test_folder_symlink_listing_and_read(self):
        with (
            copy_test_file("folder-symlink.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as mount_source,
        ):
            for folder in ("/", "/foo", "/foo/fighter"):
                info = mount_source.lookup(folder)
                assert info is not None, folder
                assert stat.S_ISDIR(info.mode), folder

            ufo = mount_source.lookup("/foo/fighter/ufo")
            assert ufo is not None
            with mount_source.open(ufo) as file:
                assert file.read() == b"iriya\n"

            jet = mount_source.lookup("/foo/jet")
            assert jet is not None
            assert stat.S_ISLNK(jet.mode)
            assert jet.linkname == "fighter"

    def test_nested_with_symlink_reads(self):
        with (
            copy_test_file("nested-with-symlink.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/foo/fighter/ufo")) as file:
                assert file.read() == b"iriya\n"
            saucer = mount_source.lookup("/foo/fighter/saucer")
            assert saucer is not None
            assert stat.S_ISLNK(saucer.mode)
            assert saucer.linkname == "ufo"
            lighter = mount_source.lookup("/foo/lighter.tar")
            assert lighter is not None
            assert lighter.size == 10240
            with mount_source.open(lighter) as file:
                file.seek(257)
                assert file.read(5) == b"ustar"

    def test_seek_within_member(self):
        with (
            copy_test_file("file-in-non-existing-folder.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/foo2/ufo")) as file:
                assert file.seekable()
                assert file.read(2) == b"ir"
                assert file.seek(0) == 0
                assert file.read() == b"iriya\n"
                assert file.seek(3) == 3
                assert file.read() == b"ya\n"

    def test_solid_large_files_prefix(self):
        path = _require_fixture("two-large-files-32Ki-lines-each-1024B.7z")
        with SevenZipMountSource(path, indexFilePath=":memory:") as mount_source:
            spaces = mount_source.lookup("/spaces-32-MiB.txt")
            zeros = mount_source.lookup("/zeros-32-MiB.txt")
            assert spaces is not None and zeros is not None
            assert spaces.size == 32 * 1024 * 1024
            assert zeros.size == 32 * 1024 * 1024
            with mount_source.open(spaces) as file:
                head = file.read(1024)
                assert len(head) == 1024
                file.seek(spaces.size - 1024)
                tail = file.read()
                assert len(tail) == 1024
            with mount_source.open(zeros) as file:
                head = file.read(1024)
                assert len(head) == 1024
                file.seek(0)
                assert file.read(1024) == head

    def test_factory_prefers_sevenzip_backend(self):
        with copy_test_file("store-copy-two-files.7z") as path:
            mount_source = open_mount_source(path, indexFilePath=":memory:")
            try:
                assert type(mount_source).__name__ == "SevenZipMountSource"
            finally:
                mount_source.close()

    def test_open_from_file_object(self):
        path = _require_fixture("store-copy-two-files.7z")
        with open(path, "rb") as file_obj:
            with SevenZipMountSource(file_obj, indexFilePath=":memory:") as mount_source:
                assert mount_source.open(mount_source.lookup("/a.txt")).read().startswith(b"AAA")


# ---------------------------------------------------------------------------
# M4: streaming content-level seek
# ---------------------------------------------------------------------------


class TestSevenZipStreamingSeek:
    def test_streaming_content_seek_large_solid(self):
        path = _require_fixture("two-large-files-32Ki-lines-each-1024B.7z")
        with SevenZipMountSource(
            path,
            indexFilePath=":memory:",
            sevenZipSmallFolderThreshold=0,
            sevenZipChunkSize=1024 * 1024,
            sevenZipMaxCachedChunks=8,
        ) as mount_source:
            spaces = mount_source.lookup("/spaces-32-MiB.txt")
            zeros = mount_source.lookup("/zeros-32-MiB.txt")
            assert spaces is not None and zeros is not None

            with mount_source.open(spaces) as file:
                assert file.seekable()
                start = file.read(4096)
                assert len(start) == 4096
                mid = spaces.size // 2
                assert file.seek(mid) == mid
                mid_data = file.read(4096)
                assert len(mid_data) == 4096
                assert file.seek(spaces.size - 4096) == spaces.size - 4096
                end_data = file.read(4096)
                assert len(end_data) == 4096
                assert file.seek(0) == 0
                assert file.read(4096) == start

            with mount_source.open(zeros) as file:
                z_start = file.read(4096)
                assert len(z_start) == 4096
                file.seek(zeros.size // 2)
                assert len(file.read(4096)) == 4096
                file.seek(0)
                assert file.read(4096) == z_start

            with mount_source.open(spaces) as file:
                file.seek(100)
                assert len(file.read(50)) == 50

            decoder = next(iter(mount_source._streamDecoders.values()))
            assert decoder.cached_chunk_count <= 8

    def test_streaming_matches_full_decompress(self):
        path = _require_fixture("two-large-files-32Ki-lines-each-1024B.7z")
        with open(path, "rb") as archive:
            info = parse_7z_archive(archive)
            entry = info.files[0]
            folder = info.folders[0]
            archive.seek(entry.pack_offset)
            packed = archive.read(entry.pack_size)

        full = decompress_folder(folder, packed)
        dec = StreamingFolderDecoder(folder, packed, chunk_size=256 * 1024, max_cached_chunks=4)
        for start, length in ((0, 1000), (10_000, 500), (entry.size - 200, 200), (entry.size // 3, 8192)):
            assert dec.read_range(start, length) == full[start : start + length]

    def test_streaming_medium_lzma2_fixture(self):
        with (
            copy_test_file("lzma2-two-files-and-medium.7z") as path,
            SevenZipMountSource(
                path,
                indexFilePath=":memory:",
                sevenZipSmallFolderThreshold=0,
                sevenZipChunkSize=64 * 1024,
                sevenZipMaxCachedChunks=4,
            ) as mount_source,
        ):
            medium = mount_source.lookup("/medium.bin")
            with mount_source.open(medium) as file:
                assert file.read(100) == b"X" * 100
                file.seek(1_000_000)
                assert file.read(100) == b"X" * 100
            decoder = next(iter(mount_source._streamDecoders.values()))
            assert decoder.cached_chunk_count <= 4
            # First-touch should not have required full 2 MiB+ decode if only mid was needed earlier,
            # but after both ranges it may have decoded through ~1 MiB.
            assert decoder.decoded_through >= 1_000_000


# ---------------------------------------------------------------------------
# M5: AES encryption
# ---------------------------------------------------------------------------


class TestSevenZipEncryption:
    def test_encrypted_hello_fixture(self):
        with (
            copy_test_file("encrypted-hello.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:", passwords=["secret"]) as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/secret.txt")) as file:
                assert file.read() == b"secret content\n"

    def test_encrypted_content_with_password(self):
        with (
            copy_test_file("encrypted-nested-tar.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:", passwords=[b"foo"]) as mount_source,
        ):
            for folder in ("/", "/foo", "/foo/fighter"):
                info = mount_source.lookup(folder)
                assert info is not None
                assert stat.S_ISDIR(info.mode)

            ufo = mount_source.lookup("/foo/fighter/ufo")
            assert ufo is not None
            with mount_source.open(ufo) as file:
                assert file.read() == b"iriya\n"

            lighter = mount_source.lookup("/foo/lighter.tar")
            assert lighter is not None
            assert lighter.size == 10240
            with mount_source.open(lighter) as file:
                file.seek(257)
                assert file.read(5) == b"ustar"
                file.seek(0)
                head = file.read(8)
                assert len(head) == 8
                assert head != b"iriya\n"

    def test_encrypted_wrong_password(self):
        with copy_test_file("encrypted-hello.7z") as path, pytest.raises(SevenZipError, match="password"):
            SevenZipMountSource(path, indexFilePath=":memory:", passwords=[b"not-the-password"])

    def test_encrypted_missing_password(self):
        with copy_test_file("encrypted-hello.7z") as path, pytest.raises(SevenZipError, match="encrypted"):
            SevenZipMountSource(path, indexFilePath=":memory:")

    def test_encrypted_string_password(self):
        with (
            copy_test_file("encrypted-nested-tar.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:", passwords=["foo"]) as mount_source,
        ):
            with mount_source.open(mount_source.lookup("/foo/fighter/ufo")) as file:
                assert file.read() == b"iriya\n"

    def test_factory_prefers_sevenzip_for_encrypted(self):
        with copy_test_file("encrypted-hello.7z") as path:
            mount_source = open_mount_source(path, indexFilePath=":memory:", passwords=["secret"])
            try:
                assert type(mount_source).__name__ == "SevenZipMountSource"
                with mount_source.open(mount_source.lookup("/secret.txt")) as file:
                    assert file.read() == b"secret content\n"
            finally:
                mount_source.close()


# ---------------------------------------------------------------------------
# Recursive 7z-in-7z (manual + AutoMountLayer)
# ---------------------------------------------------------------------------


class TestSevenZipRecursive:
    def test_manual_nested_open_from_member_file_object(self):
        with (
            copy_test_file("nested-inner-hello.7z") as path,
            SevenZipMountSource(path, indexFilePath=":memory:") as outer,
        ):
            inner_info = outer.lookup("/inner-hello.7z")
            assert inner_info is not None
            with outer.open(inner_info) as inner_file:
                magic = inner_file.read(6)
                assert magic == b"7z\xbc\xaf'\x1c"
                inner_file.seek(0)
                with SevenZipMountSource(inner_file, indexFilePath=":memory:") as inner:
                    assert type(inner).__name__ == "SevenZipMountSource"
                    with inner.open(inner.lookup("/hello.txt")) as file:
                        assert file.read() == b"hello from nested\n"

    def test_automount_nested_inner_hello(self):
        options = {"recursive": True, "indexFilePath": ":memory:", "clearIndexCache": True}
        with copy_test_file("nested-inner-hello.7z") as path:
            base = open_mount_source(path, **options)
            try:
                assert type(base).__name__ == "SevenZipMountSource"
                auto = AutoMountLayer(base, **options)
                # Outer member mounts as directory of its contents
                listing = auto.list("/inner-hello.7z")
                assert listing is not None
                assert "hello.txt" in (listing.keys() if isinstance(listing, dict) else listing)

                hello = auto.lookup("/inner-hello.7z/hello.txt")
                assert hello is not None
                with auto.open(hello) as file:
                    assert file.read() == b"hello from nested\n"

                assert "/inner-hello.7z" in auto.mounted
            finally:
                base.close()

    def test_automount_double_compressed_nested_tar_7z_7z(self):
        """Existing fixture: .tar.7z.7z → nested-tar.tar.7z → nested-tar.tar → ufo."""
        options = {"recursive": True, "indexFilePath": ":memory:", "clearIndexCache": True}
        with copy_test_file("double-compressed-nested-tar.tar.7z.7z") as path:
            base = open_mount_source(path, **options)
            try:
                assert type(base).__name__ == "SevenZipMountSource"
                auto = AutoMountLayer(base, **options)

                deep = "/nested-tar.tar.7z/nested-tar.tar/foo/fighter/ufo"
                file_info = auto.lookup(deep)
                assert file_info is not None, deep
                with auto.open(file_info) as file:
                    assert file.read() == b"iriya\n"

                assert "/nested-tar.tar.7z" in auto.mounted
                assert "/nested-tar.tar.7z/nested-tar.tar" in auto.mounted
            finally:
                base.close()

    def test_automount_nested_encrypted_inner(self):
        options = {
            "recursive": True,
            "indexFilePath": ":memory:",
            "clearIndexCache": True,
            "passwords": ["innerpw"],
        }
        with copy_test_file("nested-encrypted-inner.7z") as path:
            base = open_mount_source(path, **options)
            try:
                auto = AutoMountLayer(base, **options)
                payload = auto.lookup("/inner-encrypted.7z/payload.txt")
                assert payload is not None
                with auto.open(payload) as file:
                    assert file.read() == b"secret content\n"
            finally:
                base.close()


# ---------------------------------------------------------------------------
# M6: on-disk persistent indexes
# ---------------------------------------------------------------------------


class TestSevenZipPersistentIndex:
    def test_write_and_reload_index(self, tmp_path: Path):
        with copy_test_file("store-copy-two-files.7z") as archive:
            index_path = str(tmp_path / "store.index.sqlite")
            with SevenZipMountSource(
                archive,
                indexFilePath=index_path,
                writeIndex=True,
                indexMinimumFileCount=0,
            ) as mount_source:
                assert mount_source.open(mount_source.lookup("/a.txt")).read().startswith(b"AAA")
            assert os.path.isfile(index_path)
            assert os.path.getsize(index_path) > 0

            with SevenZipMountSource(
                archive,
                indexFilePath=index_path,
                writeIndex=False,
                indexMinimumFileCount=0,
            ) as mount_source:
                assert mount_source.open(mount_source.lookup("/b.txt")).read().startswith(b"BBB")

    def test_index_survives_encrypted_archive(self, tmp_path: Path):
        with copy_test_file("encrypted-hello.7z") as archive:
            index_path = str(tmp_path / "enc.index.sqlite")
            with SevenZipMountSource(
                archive,
                indexFilePath=index_path,
                writeIndex=True,
                indexMinimumFileCount=0,
                passwords=["secret"],
            ) as mount_source:
                assert mount_source.open(mount_source.lookup("/secret.txt")).read() == b"secret content\n"

            with SevenZipMountSource(
                archive,
                indexFilePath=index_path,
                writeIndex=False,
                passwords=["secret"],
            ) as mount_source:
                assert mount_source.open(mount_source.lookup("/secret.txt")).read() == b"secret content\n"


# ---------------------------------------------------------------------------
# Optional: generate with system 7z CLI (non-solid store)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _has_7z_cli(), reason="7z CLI required to generate non-solid store archives")
class TestSevenZipCliGenerated:
    def test_nonsolid_store_cli(self, tmp_path: Path):
        import subprocess

        src = tmp_path / "src"
        src.mkdir()
        (src / "a.txt").write_bytes(b"AAA" + b"a" * 1000 + b"AAA")
        (src / "b.txt").write_bytes(b"BBB" + b"b" * 1000 + b"BBB")
        archive = tmp_path / "store.7z"
        subprocess.check_call(
            [_7z_bin(), "a", "-t7z", "-mx=0", "-ms=off", str(archive), str(src / "a.txt"), str(src / "b.txt")],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        with open(archive, "rb") as file:
            info = parse_7z_archive(file)
        assert all(e.methods == (METHOD_COPY,) for e in info.files if not e.is_dir)
        assert not info.solid

        with SevenZipMountSource(str(archive), indexFilePath=":memory:") as mount_source:
            with mount_source.open(mount_source.lookup("/a.txt")) as file:
                assert file.read() == (src / "a.txt").read_bytes()
                file.seek(3)
                assert file.read(4) == b"aaaa"
            with mount_source.open(mount_source.lookup("/b.txt")) as file:
                assert file.read() == (src / "b.txt").read_bytes()
