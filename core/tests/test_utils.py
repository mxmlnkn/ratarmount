#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.utils import ceilDiv, LRUCache, Prefetcher  # noqa: E402


def test_ceilDiv():
    assert ceilDiv(0, 1) == 0
    assert ceilDiv(0, 1000) == 0
    assert ceilDiv(0, -1000) == 0

    assert ceilDiv(1, 1000) == 1
    assert ceilDiv(1, 1) == 1

    assert ceilDiv(0, 2) == 0
    assert ceilDiv(1, 2) == 1
    assert ceilDiv(2, 2) == 1
    assert ceilDiv(3, 2) == 2
    assert ceilDiv(4, 2) == 2
    assert ceilDiv(5, 2) == 3


def test_Prefetcher():
    memorySize = 4
    prefetcher = Prefetcher(memorySize)

    # Without any knowledge, it can't give any prefetch suggestion
    assert not prefetcher.prefetch(0)
    assert not prefetcher.prefetch(1)
    assert not prefetcher.prefetch(2)
    assert not prefetcher.prefetch(2)

    assert prefetcher.fetch(32) is None

    # With a single known sample, we can not interpolate anything, so simply return a single example, the next one
    assert not list(prefetcher.prefetch(0))
    assert list(prefetcher.prefetch(1)) == [33]
    assert list(prefetcher.prefetch(2)) == [33]
    assert list(prefetcher.prefetch(3)) == [33]

    assert prefetcher.fetch(33) is None

    assert not list(prefetcher.prefetch(0))
    assert list(prefetcher.prefetch(1)) == [34]
    assert list(prefetcher.prefetch(4)) == [34, 35]

    assert prefetcher.fetch(34) is None
    assert prefetcher.fetch(35) is None

    # When the last fetched cache is fully filled with a monotonic increasing series,
    # then always prefetch the full requested amount
    for n in range(32):
        assert list(prefetcher.prefetch(n)) == list(range(36, 36 + n))

    assert prefetcher.fetch(36) is None
    assert prefetcher.fetch(37) is None

    # Same as above but the cache is limited, so fetching anything more
    # does not change the length of the returned result further
    for n in range(32):
        assert list(prefetcher.prefetch(n)) == list(range(38, 38 + n))

    prefetcher.fetch(0)
    assert list(prefetcher.prefetch(8)) == [1]


def test_LRUCache():
    cache = LRUCache(3)

    assert not cache
    assert len(cache) == 0

    assert cache.get(5) is None

    for n, key, value in zip(range(cache.size), range(5, 5 + cache.size), range(15, 15 + cache.size)):
        # Repeated identical insertions are stable
        for _ in range(10):
            cache[key] = value
            assert len(cache) == n + 1
            assert cache.get(key) == value
            assert cache.get(key) == value

    # Inserting a key when the maximum size has been reached, will evict the least recently used key
    assert len(cache) == cache.size
    cache[32] = 132
    assert len(cache) == cache.size
    # Note that even these get tests will change the least recently used status of the keys
    assert cache.get(32) == 132
    assert cache.get(5) is None
    assert cache.get(6) == 16
    assert cache.get(7) == 17
    assert cache[7] == 17

    cache[33] = 133
    assert 32 not in cache
    assert cache[33] == 133

    assert sorted(list(cache.keys())) == [6, 7, 33]
    del cache[33]
    assert len(cache) == 2
    assert 33 not in cache
    assert sorted(list(cache.keys())) == [6, 7]

    cache.clear()
    assert not cache
    assert len(cache) == 0
    assert 7 not in cache
    assert not cache.keys()
