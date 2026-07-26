"""Tests for nested 7z spool cache and validation."""

from __future__ import annotations

import io
import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ratarmountcore.mountsource.formats import sevenzip as sevenzip_fmt
from ratarmountcore.mountsource.formats.sevenzip import (
    SevenZipMountSource,
    nested_spool_cache_key,
    prune_orphan_temp_spools,
    validate_nested_spool_file,
)
from ratarmountcore.sevenzip import SevenZipError


def test_nested_spool_cache_key_is_stable() -> None:
    key_a = nested_spool_cache_key("inner.7z", 123456)
    key_b = nested_spool_cache_key("inner.7z", 123456)
    key_c = nested_spool_cache_key("inner.7z", 123457)
    assert key_a == key_b
    assert key_a != key_c


def test_validate_nested_spool_file_accepts_complete_7z(tmp_path, monkeypatch) -> None:
    payload = sevenzip_fmt._NESTED_SPOOL_MAGIC + b"payload"
    path = tmp_path / "good.7z"
    path.write_bytes(payload)
    monkeypatch.setattr(
        sevenzip_fmt,
        "parse_7z_archive",
        lambda _file: type("Archive", (), {"files": [object()] * 60})(),
    )
    assert validate_nested_spool_file(path, len(payload))


def test_validate_nested_spool_file_rejects_few_entries(tmp_path, monkeypatch) -> None:
    payload = sevenzip_fmt._NESTED_SPOOL_MAGIC + (b"x" * (101 * 1024 * 1024))
    path = tmp_path / "big.7z"
    path.write_bytes(payload)
    monkeypatch.setattr(
        sevenzip_fmt,
        "parse_7z_archive",
        lambda _file: type("Archive", (), {"files": [object()] * 10})(),
    )
    assert not validate_nested_spool_file(path, len(payload))


def test_validate_nested_spool_file_rejects_wrong_size(tmp_path) -> None:
    path = tmp_path / "short.7z"
    path.write_bytes(sevenzip_fmt._NESTED_SPOOL_MAGIC + b"x" * 10)
    assert not validate_nested_spool_file(path, 999)


def test_validate_nested_spool_file_rejects_bad_magic(tmp_path) -> None:
    path = tmp_path / "bad.7z"
    path.write_bytes(b"not-a-7z-archive")
    assert not validate_nested_spool_file(path, len(path.read_bytes()))


def test_prune_orphan_temp_spools(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(sevenzip_fmt.tempfile, "gettempdir", lambda: str(tmp_path))
    orphan = tmp_path / "ratarmount-7z-deadbeef.7z"
    orphan.write_bytes(b"partial")
    keep = tmp_path / "other.7z"
    keep.write_bytes(b"keep")
    assert prune_orphan_temp_spools() == 1
    assert not orphan.exists()
    assert keep.exists()


def test_nested_spool_reuses_valid_cache(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RATARMOUNT_NESTED_7Z_CACHE", str(tmp_path))
    monkeypatch.setattr(sevenzip_fmt, "_NESTED_SPOOL_THRESHOLD", 8)

    payload = sevenzip_fmt._NESTED_SPOOL_MAGIC + b"0123456789"
    cache_path = tmp_path / f"{nested_spool_cache_key('inner.7z', len(payload))}.7z"
    cache_path.write_bytes(payload)

    reader = _TrackedReader(io.BytesIO(payload), threading.Lock())
    monkeypatch.setattr(
        sevenzip_fmt,
        "parse_7z_archive",
        lambda _file: type("Archive", (), {"files": [], "folders": []})(),
    )
    monkeypatch.setattr(
        sevenzip_fmt,
        "validate_nested_spool_file",
        lambda path, size: path == cache_path and size == len(payload),
    )

    ms = SevenZipMountSource(
        reader,
        indexFilePath=":memory:",
        tarFileName="inner.7z",
    )
    try:
        assert ms._spool_cached is True
        assert ms._spool_path == str(cache_path)
        assert reader.read_calls == 0
        assert ms.fileObject.read(len(payload)) == payload
    finally:
        ms.close()


def test_nested_spool_rejects_truncated_stream(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("RATARMOUNT_NESTED_7Z_CACHE", str(tmp_path))
    monkeypatch.setattr(sevenzip_fmt, "_NESTED_SPOOL_THRESHOLD", 8)
    monkeypatch.setattr(
        sevenzip_fmt,
        "parse_7z_archive",
        lambda _file: type("Archive", (), {"files": [], "folders": []})(),
    )

    payload = sevenzip_fmt._NESTED_SPOOL_MAGIC + b"0123456789"
    short_stream = _ShortReader(payload[: len(payload) - 3], reported_size=len(payload))
    with pytest.raises(SevenZipError, match="Short read while spooling"):
        SevenZipMountSource(
            short_stream,
            indexFilePath=":memory:",
            tarFileName="inner.7z",
        ).close()


class _TrackedReader(io.RawIOBase):
    def __init__(self, buffer: io.BytesIO, lock: threading.Lock):
        self._buffer = buffer
        self._lock = lock
        self.read_calls = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        with self._lock:
            return self._buffer.seek(offset, whence)

    def tell(self) -> int:
        with self._lock:
            return self._buffer.tell()

    def read(self, size: int = -1) -> bytes:
        with self._lock:
            self.read_calls += 1
            return self._buffer.read(size)


class _ShortReader(io.RawIOBase):
    def __init__(self, data: bytes, *, reported_size: int):
        self._data = data
        self._reported_size = reported_size
        self._pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            self._pos = offset
        elif whence == io.SEEK_CUR:
            self._pos += offset
        elif whence == io.SEEK_END:
            self._pos = self._reported_size + offset
        else:
            raise ValueError(whence)
        self._pos = max(0, min(self._pos, self._reported_size))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def read(self, size: int = -1) -> bytes:
        if self._pos >= len(self._data):
            return b""
        if size < 0:
            size = len(self._data) - self._pos
        end = min(self._pos + size, len(self._data))
        out = self._data[self._pos : end]
        self._pos = end
        return out
