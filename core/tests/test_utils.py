#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.utils import (  # noqa: E402
    ceilDiv,
    formatNumber,
    isLatinAlpha,
    isLatinDigit,
    isLatinHexAlpha,
    ALPHA,
    DIGITS,
    HEX,
    LRUCache,
    Prefetcher,
)


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


def test_isLatinAlpha():
    assert ALPHA == 'abcdefghijklmnopqrstuvwxyz'
    assert isLatinAlpha(ALPHA)

    assert isLatinAlpha("a")
    assert isLatinAlpha("ab")
    assert isLatinAlpha("z")
    assert not isLatinAlpha("0")
    assert not isLatinAlpha("")

    # Python's isalpha is too Unicode. I only want to match the usual
    # ASCII Latin alphabet not mathematical or Greek symbols!
    # https://util.unicode.org/UnicodeJsps/confusables.jsp
    # https://www.unicode.org/Public/security/revision-03/confusablesSummary.txt
    assert "α".isalpha()
    assert not isLatinAlpha("α")

    assert "𝒻".isalpha()
    assert not isLatinAlpha("𝒻")


def test_isLatinDigit():
    assert DIGITS == '0123456789'
    assert isLatinDigit(DIGITS)

    assert isLatinDigit("0")
    assert isLatinDigit("10")
    assert isLatinDigit("9")
    assert not isLatinDigit("a")
    assert not isLatinDigit("")

    # Python's isdigit is too Unicode. I only want to match the usual
    # ASCII Latin digits not decimal digits from other languages!
    # https://www.compart.com/en/unicode/category/Nd
    assert "௫".isdigit()
    assert not isLatinDigit("௫")

    assert "߂".isdigit()
    assert not isLatinDigit("߂")


def test_isLatinHexAlpha():
    assert HEX == '0123456789abcdef'
    assert isLatinHexAlpha(HEX)

    assert isLatinHexAlpha("0")
    assert isLatinHexAlpha("10")
    assert isLatinHexAlpha("9")
    assert isLatinHexAlpha("a")
    assert isLatinHexAlpha("f")
    assert not isLatinHexAlpha("g")
    assert not isLatinHexAlpha("")

    # Python's isdigit is too Unicode. I only want to match the usual
    # ASCII Latin digits not decimal digits from other languages!
    # https://www.compart.com/en/unicode/category/Nd
    assert "௫".isdigit()
    assert not isLatinHexAlpha("௫")

    assert "߂".isdigit()
    assert not isLatinHexAlpha("߂")


def test_formatNumber():
    assert formatNumber(0, ALPHA) == 'a'
    assert formatNumber(1, ALPHA) == 'b'
    assert formatNumber(25, ALPHA) == 'z'
    assert formatNumber(26, ALPHA) == 'ba'
    assert formatNumber(26 * 26, ALPHA) == 'baa'
    assert formatNumber(26 * 26 + 3, ALPHA) == 'bad'

    assert formatNumber(0, ALPHA, 3) == 'aaa'
    assert formatNumber(1, ALPHA, 3) == 'aab'
    assert formatNumber(25, ALPHA, 3) == 'aaz'
    assert formatNumber(26, ALPHA, 3) == 'aba'
    assert formatNumber(26 * 26, ALPHA, 3) == 'baa'
    assert formatNumber(26 * 26 + 3, ALPHA, 3) == 'bad'

    assert formatNumber(0, HEX) == '0'
    assert formatNumber(1, HEX) == '1'
    assert formatNumber(9, HEX) == '9'
    assert formatNumber(10, HEX) == 'a'
    assert formatNumber(15, HEX) == 'f'
    assert formatNumber(16, HEX) == '10'
    assert formatNumber(16 * 16, HEX) == '100'
    assert formatNumber(16 * 16 + 3, HEX) == '103'

    assert formatNumber(0, HEX, 3) == '000'
    assert formatNumber(1, HEX, 3) == '001'
    assert formatNumber(9, HEX, 3) == '009'
    assert formatNumber(10, HEX, 3) == '00a'
    assert formatNumber(15, HEX, 3) == '00f'
    assert formatNumber(16, HEX, 3) == '010'
    assert formatNumber(16 * 16, HEX, 3) == '100'
    assert formatNumber(16 * 16 + 3, HEX, 3) == '103'

    assert formatNumber(357641610, DIGITS) == '357641610'
