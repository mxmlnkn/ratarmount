"""Regression tests for multi-nested solid LZMA2 7z support bundles."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
from pathlib import Path

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from helpers import find_test_file

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "nested-7z-multi"
OUTER_ARCHIVE = FIXTURE_DIR / "nested-multi-support.7z"
MANIFEST = FIXTURE_DIR / "manifest.json"

RECURSIVE_EXTENSIONS = [
    "/archive",
    "/disk",
    "/split",
    "/compressed/-",
    ".raw/-",
    ".log.gz/-",
]


def _require_fixture() -> None:
    if not OUTER_ARCHIVE.is_file() or not MANIFEST.is_file():
        pytest.skip(
            "nested multi fixture missing; run: node core/scripts/build-nested-7z-fixture.js"
        )


@pytest.fixture(scope="module")
def manifest() -> dict:
    _require_fixture()
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def nested_mount_tree():
    _require_fixture()
    from ratarmountcore.mountsource.compositing.automount import AutoMountLayer
    from ratarmountcore.mountsource.factory import open_mount_source

    options = {
        "writeIndex": True,
        "indexFilePath": ":memory:",
        "recursive": True,
        "recursiveExtensions": RECURSIVE_EXTENSIONS,
    }
    outer = open_mount_source(str(OUTER_ARCHIVE), **options)
    layered = AutoMountLayer(outer, **options)
    try:
        yield layered
    finally:
        layered.__exit__(None, None, None)
        outer.close()


def _read_member(layer, path: str) -> bytes:
    info = layer.lookup(path)
    assert info is not None, f"missing path {path!r}"
    return layer.read(info, info.size, 0)


def _inner_root(layer, suffix: str) -> str:
    matches = [name for name in layer.list("/") if suffix in name]
    assert len(matches) == 1, f"expected one embedded archive matching {suffix!r}, got {matches!r}"
    return f"/{matches[0]}"


class TestNestedMultiFixtureMetadata:
    def test_manifest_lists_multiple_inner_archives(self, manifest: dict) -> None:
        inners = manifest["inner_archives"]
        assert len(inners) >= 4
        ids = {item["id"] for item in inners}
        assert ids == {"cm-primary", "cm-secondary", "ra", "slave"}

    def test_secondary_has_many_staged_files(self, manifest: dict) -> None:
        secondary = next(item for item in manifest["inner_archives"] if item["id"] == "cm-secondary")
        assert secondary["staged_file_count"] >= 50


class TestNestedMultiMountReads:
    def test_all_embedded_archives_are_mounted(self, nested_mount_tree, manifest: dict) -> None:
        listed = set(nested_mount_tree.list("/"))
        for inner in manifest["inner_archives"]:
            assert inner["name"] in listed
            assert nested_mount_tree.is_dir(f"/{inner['name']}")

    def test_outer_index_stays_small(self) -> None:
        from ratarmountcore.mountsource.factory import open_mount_source

        index_path = Path(tempfile.mktemp(suffix=".index.sqlite"))
        try:
            open_mount_source(
                str(OUTER_ARCHIVE),
                writeIndex=True,
                indexFilePath=str(index_path),
                recursive=True,
                recursiveExtensions=RECURSIVE_EXTENSIONS,
            ).close()
            count = sqlite3.connect(index_path).execute("SELECT COUNT(*) FROM files").fetchone()[0]
            assert count <= 20, f"outer index bloated to {count} rows"
        finally:
            index_path.unlink(missing_ok=True)

    @pytest.mark.parametrize(
        ("inner_suffix", "member_path"),
        [
            ("CM-Primary", "cmd-top.txt"),
            ("CM-Secondary", "home/afa/.fa-distribution-logs.history"),
            ("CM-Secondary", "home/afa/.fa-distribution-logs.history.20"),
            ("CM-Secondary", "home/afa/monitor.log.10"),
            ("RA--10.0.0.3", "home/afa/.fa/performance.json"),
            ("Slave--10.0.0.4", "home/afa/.fa-auto-remove.log.3"),
        ],
    )
    def test_read_probe_members(self, nested_mount_tree, inner_suffix: str, member_path: str) -> None:
        root = _inner_root(nested_mount_tree, inner_suffix)
        data = _read_member(nested_mount_tree, f"{root}/{member_path}")
        assert len(data) > 0

    def test_secondary_exposes_full_file_tree(self, nested_mount_tree) -> None:
        root = _inner_root(nested_mount_tree, "CM-Secondary")
        afa = nested_mount_tree.list(f"{root}/home/afa")
        assert len(afa) >= 50, f"expected full CM-Secondary tree, got {len(afa)} entries"

    def test_lzma2_repeat_read_uses_decoder(self, nested_mount_tree) -> None:
        from ratarmountcore.mountsource.formats.sevenzip import SevenZipMountSource

        root = _inner_root(nested_mount_tree, "CM-Secondary")
        path = f"{root}/home/afa/.fa-distribution-logs.history"
        info = nested_mount_tree.lookup(path)
        assert info is not None
        mount = nested_mount_tree.mounted[root.rstrip("/")].mountSource
        assert isinstance(mount, SevenZipMountSource)
        entry = next(item for item in mount._archive.files if item.path == "home/afa/.fa-distribution-logs.history")
        decoder = mount._get_stream_decoder(entry)
        first = decoder.read_range(entry.unpack_offset, 4096)
        second = decoder.read_range(entry.unpack_offset, 4096)
        assert first == second
        assert len(first) == 4096


class TestNestedMultiSpoolCache:
    def test_nested_spool_cache_used_for_large_inner(self, tmp_path, monkeypatch) -> None:
        """Replicates incomplete /tmp spool failures using validated on-disk cache."""
        from ratarmountcore.mountsource.formats import sevenzip as sevenzip_fmt
        from ratarmountcore.mountsource.formats.sevenzip import SevenZipMountSource, nested_spool_cache_key
        from ratarmountcore.sevenzip import parse_7z_archive

        _require_fixture()
        secondary = next(item for item in json.loads(MANIFEST.read_text(encoding="utf-8"))["inner_archives"] if item["id"] == "cm-secondary")
        inner_name = secondary["name"]

        with open(OUTER_ARCHIVE, "rb") as outer_file:
            outer = parse_7z_archive(outer_file)
        inner_entry = next(item for item in outer.files if item.path == inner_name and not item.is_dir)
        with open(OUTER_ARCHIVE, "rb") as outer_file:
            outer_file.seek(inner_entry.pack_offset)
            inner_payload = outer_file.read(inner_entry.pack_size)

        monkeypatch.setenv("RATARMOUNT_NESTED_7Z_CACHE", str(tmp_path))
        monkeypatch.setattr(sevenzip_fmt, "_NESTED_SPOOL_THRESHOLD", max(1024, len(inner_payload) // 2))

        reader = _TrackedReader(inner_payload)
        ms = SevenZipMountSource(
            reader,
            indexFilePath=":memory:",
            tarFileName=inner_name,
        )
        try:
            cache_path = tmp_path / f"{nested_spool_cache_key(inner_name, len(inner_payload))}.7z"
            assert cache_path.is_file()
            assert ms._spool_cached is True
            assert ms._spool_path == str(cache_path)
            assert reader.read_calls >= 1
            entry = next(
                item for item in ms._archive.files if item.path == "home/afa/.fa-distribution-logs.history.20"
            )
            decoder = ms._get_stream_decoder(entry)
            payload = decoder.read_range(entry.unpack_offset, 4096)
            assert len(payload) == 4096
        finally:
            ms.close()

    def test_parallel_inner_spool_from_outer_members(self, tmp_path, monkeypatch) -> None:
        """Parallel nested opens mirror mount-time indexing across embedded archives."""
        from ratarmountcore.mountsource.formats import sevenzip as sevenzip_fmt
        from ratarmountcore.mountsource.formats.sevenzip import SevenZipMountSource
        from ratarmountcore.sevenzip import parse_7z_archive

        _require_fixture()
        manifest_data = json.loads(MANIFEST.read_text(encoding="utf-8"))
        with open(OUTER_ARCHIVE, "rb") as outer_file:
            outer = parse_7z_archive(outer_file)

        payloads: list[tuple[str, bytes]] = []
        for inner in manifest_data["inner_archives"]:
            entry = next(item for item in outer.files if item.path == inner["name"] and not item.is_dir)
            with open(OUTER_ARCHIVE, "rb") as outer_file:
                outer_file.seek(entry.pack_offset)
                payloads.append((inner["name"], outer_file.read(entry.pack_size)))

        monkeypatch.setenv("RATARMOUNT_NESTED_7Z_CACHE", str(tmp_path))
        monkeypatch.setattr(sevenzip_fmt, "_NESTED_SPOOL_THRESHOLD", 512)

        results: dict[str, int] = {}
        errors: list[BaseException] = []

        def worker(name: str, payload: bytes, member_path: str) -> None:
            try:
                ms = SevenZipMountSource(
                    _TrackedReader(payload),
                    indexFilePath=":memory:",
                    tarFileName=name,
                )
                try:
                    entry = next(item for item in ms._archive.files if item.path == member_path)
                    decoder = ms._get_stream_decoder(entry)
                    data = decoder.read_range(entry.unpack_offset, min(4096, entry.size))
                    results[name] = len(data)
                finally:
                    ms.close()
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        probes = {
            "fixture-support-zip--CM-Primary--10.0.0.1--2026-01-01--00-00-00.7z": "cmd-top.txt",
            "fixture-support-zip--CM-Secondary--10.0.0.2--2026-01-01--00-00-00.7z": "home/afa/monitor.log.10",
            "fixture-support-zip--RA--10.0.0.3--2026-01-01--00-00-00.7z": "home/afa/.fa/performance.json",
            "fixture-support-zip--Slave--10.0.0.4--2026-01-01--00-00-00.7z": "home/afa/.fa-auto-remove.log.3",
        }
        threads = [
            threading.Thread(
                target=worker,
                args=(name, payload, probes[name]),
                daemon=True,
            )
            for name, payload in payloads
            if name in probes
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)
        assert not errors, errors
        assert len(results) == len(probes)
        assert all(size > 0 for size in results.values())


class _TrackedReader:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._pos = 0
        self.read_calls = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            self._pos = offset
        elif whence == os.SEEK_CUR:
            self._pos += offset
        elif whence == os.SEEK_END:
            self._pos = len(self._payload) + offset
        else:
            raise ValueError(whence)
        self._pos = max(0, min(self._pos, len(self._payload)))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        self.read_calls += 1
        if size < 0:
            size = len(self._payload) - self._pos
        end = min(self._pos + size, len(self._payload))
        out = self._payload[self._pos : end]
        self._pos = end
        return out
