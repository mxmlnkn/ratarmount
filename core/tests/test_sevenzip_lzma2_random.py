"""Tests for LZMA2 chunk-indexed random access in ratarmountcore."""

from __future__ import annotations

import lzma
import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ratarmountcore.sevenzip import (
    Coder,
    Folder,
    Lzma2RandomAccessDecoder,
    METHOD_LZMA2,
    StreamingFolderDecoder,
    create_folder_decoder,
    index_lzma2_chunks,
)


def _make_lzma2_folder(packed: bytes, unpack_size: int) -> Folder:
    return Folder(
        coders=[Coder(method=METHOD_LZMA2, properties=None)],
        unpack_sizes=[unpack_size],
    )


def test_index_lzma2_chunks_matches_full_decompress() -> None:
    data = bytes(range(256)) * 50000
    packed = lzma.compress(
        data,
        format=lzma.FORMAT_RAW,
        filters=[{"id": lzma.FILTER_LZMA2, "preset": 1}],
    )
    chunks = index_lzma2_chunks(packed)
    assert chunks
    assert sum(chunk.unpacked_size for chunk in chunks) == len(data)


def test_lzma2_random_access_matches_sequential() -> None:
    data = bytes(range(256)) * 50000
    packed = lzma.compress(
        data,
        format=lzma.FORMAT_RAW,
        filters=[{"id": lzma.FILTER_LZMA2, "preset": 1}],
    )
    folder = _make_lzma2_folder(packed, len(data))
    for start in (0, len(data) // 2, len(data) - 4096):
        random_decoder = Lzma2RandomAccessDecoder(folder, packed)
        sequential = StreamingFolderDecoder(folder, packed)
        assert random_decoder.read_range(start, 4096) == sequential.read_range(start, 4096)


def test_create_folder_decoder_selects_lzma2_random_access() -> None:
    data = b"hello world " * 1000
    packed = lzma.compress(
        data,
        format=lzma.FORMAT_RAW,
        filters=[{"id": lzma.FILTER_LZMA2, "preset": 0}],
    )
    folder = _make_lzma2_folder(packed, len(data))
    decoder = create_folder_decoder(folder, packed)
    assert isinstance(decoder, Lzma2RandomAccessDecoder)
    assert decoder.read_range(100, 10) == data[100:110]


def test_lzma2_chain_decode_uses_folder_filter() -> None:
    """Chain decode must use the folder LZMA2 filter, not per-chunk props."""
    from ratarmountcore.sevenzip import _lzma2_chunk_filter, _lzma_filter_from_coder

    data = bytes(range(256)) * 120000
    packed = lzma.compress(
        data,
        format=lzma.FORMAT_RAW,
        filters=[{"id": lzma.FILTER_LZMA2, "preset": 1, "dict_size": 1 << 20}],
    )
    folder = _make_lzma2_folder(packed, len(data))
    default = _lzma_filter_from_coder(folder.coders[0])
    chunks = index_lzma2_chunks(packed)
    dependent = next(i for i, c in enumerate(chunks) if c.is_lzma and not c.independent)
    chain_start = Lzma2RandomAccessDecoder._chain_start(chunks, dependent)
    start = chunks[chain_start]
    chunk_filter = _lzma2_chunk_filter(start, packed, default)
    if chunk_filter == default:
        pytest.skip("test archive has no per-chunk property divergence at chain start")

    decoder = Lzma2RandomAccessDecoder(folder, packed)
    sequential = StreamingFolderDecoder(folder, packed)
    offset = chunks[dependent].unpacked_offset + 1024
    assert decoder.read_range(offset, 4096) == sequential.read_range(offset, 4096)
