# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.utils import (  # noqa: E402
    ALPHA,
    DIGITS,
    HEX,
    LRUCache,
    Prefetcher,
    ceil_div,
    format_number,
    is_latin_alpha,
    is_latin_digit,
    is_latin_hex_alpha,
    is_random,
)


def test_ceil_div():
    assert ceil_div(0, 1) == 0
    assert ceil_div(0, 1000) == 0
    assert ceil_div(0, -1000) == 0

    assert ceil_div(1, 1000) == 1
    assert ceil_div(1, 1) == 1

    assert ceil_div(0, 2) == 0
    assert ceil_div(1, 2) == 1
    assert ceil_div(2, 2) == 1
    assert ceil_div(3, 2) == 2
    assert ceil_div(4, 2) == 2
    assert ceil_div(5, 2) == 3


def test_prefetcher():
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


def test_lru_cache():
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

    assert sorted(cache.keys()) == [6, 7, 33]
    del cache[33]
    assert len(cache) == 2
    assert 33 not in cache
    assert sorted(cache.keys()) == [6, 7]

    cache.clear()
    assert not cache
    assert len(cache) == 0
    assert 7 not in cache
    assert not cache.keys()


def test_is_latin_alpha():
    assert ALPHA == 'abcdefghijklmnopqrstuvwxyz'
    assert is_latin_alpha(ALPHA)

    assert is_latin_alpha("a")
    assert is_latin_alpha("ab")
    assert is_latin_alpha("z")
    assert not is_latin_alpha("0")
    assert not is_latin_alpha("")

    # Python's isalpha is too Unicode. I only want to match the usual
    # ASCII Latin alphabet not mathematical or Greek symbols!
    # https://util.unicode.org/UnicodeJsps/confusables.jsp
    # https://www.unicode.org/Public/security/revision-03/confusablesSummary.txt
    assert "α".isalpha()
    assert not is_latin_alpha("α")

    assert "𝒻".isalpha()
    assert not is_latin_alpha("𝒻")


def test_is_latin_digit():
    assert DIGITS == '0123456789'
    assert is_latin_digit(DIGITS)

    assert is_latin_digit("0")
    assert is_latin_digit("10")
    assert is_latin_digit("9")
    assert not is_latin_digit("a")
    assert not is_latin_digit("")

    # Python's isdigit is too Unicode. I only want to match the usual
    # ASCII Latin digits not decimal digits from other languages!
    # https://www.compart.com/en/unicode/category/Nd
    assert "௫".isdigit()
    assert not is_latin_digit("௫")

    assert "߂".isdigit()
    assert not is_latin_digit("߂")


def test_is_latin_hex_alpha():
    assert HEX == '0123456789abcdef'
    assert is_latin_hex_alpha(HEX)

    assert is_latin_hex_alpha("0")
    assert is_latin_hex_alpha("10")
    assert is_latin_hex_alpha("9")
    assert is_latin_hex_alpha("a")
    assert is_latin_hex_alpha("f")
    assert not is_latin_hex_alpha("g")
    assert not is_latin_hex_alpha("")

    # Python's isdigit is too Unicode. I only want to match the usual
    # ASCII Latin digits not decimal digits from other languages!
    # https://www.compart.com/en/unicode/category/Nd
    assert "௫".isdigit()
    assert not is_latin_hex_alpha("௫")

    assert "߂".isdigit()
    assert not is_latin_hex_alpha("߂")


def test_format_number():
    assert format_number(0, ALPHA) == 'a'
    assert format_number(1, ALPHA) == 'b'
    assert format_number(25, ALPHA) == 'z'
    assert format_number(26, ALPHA) == 'ba'
    assert format_number(26 * 26, ALPHA) == 'baa'
    assert format_number(26 * 26 + 3, ALPHA) == 'bad'

    assert format_number(0, ALPHA, 3) == 'aaa'
    assert format_number(1, ALPHA, 3) == 'aab'
    assert format_number(25, ALPHA, 3) == 'aaz'
    assert format_number(26, ALPHA, 3) == 'aba'
    assert format_number(26 * 26, ALPHA, 3) == 'baa'
    assert format_number(26 * 26 + 3, ALPHA, 3) == 'bad'

    assert format_number(0, HEX) == '0'
    assert format_number(1, HEX) == '1'
    assert format_number(9, HEX) == '9'
    assert format_number(10, HEX) == 'a'
    assert format_number(15, HEX) == 'f'
    assert format_number(16, HEX) == '10'
    assert format_number(16 * 16, HEX) == '100'
    assert format_number(16 * 16 + 3, HEX) == '103'

    assert format_number(0, HEX, 3) == '000'
    assert format_number(1, HEX, 3) == '001'
    assert format_number(9, HEX, 3) == '009'
    assert format_number(10, HEX, 3) == '00a'
    assert format_number(15, HEX, 3) == '00f'
    assert format_number(16, HEX, 3) == '010'
    assert format_number(16 * 16, HEX, 3) == '100'
    assert format_number(16 * 16 + 3, HEX, 3) == '103'

    assert format_number(357641610, DIGITS) == '357641610'


def test_is_random():
    data = os.urandom(1 << 20)
    for size in [1024, 1280, 2048, 3333, 4096, len(data) // 100, len(data) // 10, len(data) // 2, len(data)]:
        assert is_random(data[:size])

    data = bytes(i for i in range(256))
    assert not is_random(data)
    assert not is_random(data * 4)
    assert not is_random(data * 100)
    # Too few bytes to determine randomness! Also no repeated letter! I guess the random test would have to check
    # specifically for some bit distributions because these are mostly ASCII characters, i.e., the 7-th bit is not
    # set. Or maybe some clustering analysis because the bytes are clustered in a very close range of bytes.
    # But oh well. Simply require >= 256 input bytes.
    # assert is_random(b'SQLite format 3\x00')
    # assert is_random(b'abcde')
    # assert is_random(b'abcdefghijklmnopqrstuvxyz')
    # assert is_random(b'aaaaa')
