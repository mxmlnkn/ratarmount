# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.compressions import (  # noqa: E402
    checkForSequence,
    hasMatchingAlphabets,
    stripSuffixFromArchive,
    stripSuffixFromCompressedFile,
)
from ratarmountcore.utils import ALPHA, DIGITS, HEX, formatNumber  # noqa: E402


def test_strip_suffix_from_compressed_file():
    ssc = stripSuffixFromCompressedFile

    assert ssc('a.tar.bz2') == 'a.tar'
    assert ssc('a.tar.BZ2') == 'a.tar'
    assert ssc('a.tar.BZIP2') == 'a.tar'
    assert ssc('a.tar.gz') == 'a.tar'
    assert ssc('a.tar.gzip') == 'a.tar'
    assert ssc('a.tar.xz') == 'a.tar'
    assert ssc('a.tar.zst') == 'a.tar'
    assert ssc('a.tar') == 'a.tar'
    assert ssc('a.mp3') == 'a.mp3'


def test_strip_suffix_from_archive():
    sst = stripSuffixFromArchive

    assert sst('a.tar.bz2') == 'a'
    assert sst('a.tar.BZ2') == 'a'
    assert sst('a.tar.BZIP2') == 'a'
    assert sst('a.tar.gz') == 'a'
    assert sst('a.tar.gzip') == 'a'
    assert sst('a.tar.xz') == 'a'
    assert sst('a.tar.zst') == 'a'
    assert sst('a.tar') == 'a'
    assert sst('a.mp3') == 'a.mp3'

    assert sst('a.tbz2') == 'a'
    assert sst('a.TBZ2') == 'a'
    assert sst('a.tgz') == 'a'
    assert sst('a.txz') == 'a'
    assert sst('a.tzst') == 'a'


def test_has_matching_alphabets():
    matches = hasMatchingAlphabets

    assert matches('a', 'b')
    assert matches('0', '1')
    assert matches('0', 'a')  # because both might be hexadecimal
    assert matches('1a', 'b0')
    assert matches(HEX, HEX)
    assert matches(DIGITS, DIGITS)
    assert matches(ALPHA, ALPHA)
    assert matches(HEX, DIGITS)
    assert not matches(ALPHA, HEX)
    assert not matches(ALPHA, DIGITS)
    assert not matches('ag', 'b0')


def test_check_for_sequence():
    def to_alpha1(i):
        return formatNumber(i, ALPHA, 1)

    assert checkForSequence(['a'], to_alpha1) == ['a']
    assert checkForSequence(['a', 'b'], to_alpha1) == ['a', 'b']
    assert checkForSequence(['b', 'a'], to_alpha1) == ['a', 'b']
    assert checkForSequence(['b', 'a', 'd'], to_alpha1) == ['a', 'b']
    assert checkForSequence(['a', 'd'], to_alpha1) == ['a']
    assert checkForSequence(['a', 'd', 'e'], to_alpha1) == ['a']
    assert not checkForSequence(['aa'], to_alpha1)
    assert not checkForSequence(['0'], to_alpha1)

    def to_alpha2(i):
        return formatNumber(i, ALPHA, 2)

    assert checkForSequence(['aa'], to_alpha2) == ['aa']
    assert checkForSequence(['aa', 'ab'], to_alpha2) == ['aa', 'ab']
    assert checkForSequence(['ab', 'aa'], to_alpha2) == ['aa', 'ab']
    assert checkForSequence(['ab', 'aa', 'ad'], to_alpha2) == ['aa', 'ab']
    assert checkForSequence(['aa', 'ad'], to_alpha2) == ['aa']
    assert checkForSequence(['aa', 'ad', 'ae'], to_alpha2) == ['aa']
    assert not checkForSequence(['aaa'], to_alpha2)
    assert not checkForSequence(['0'], to_alpha2)
    assert not checkForSequence(['00'], to_alpha2)

    def to_digit3(i):
        return formatNumber(i, DIGITS, 3)

    assert checkForSequence(['000'], to_digit3) == ['000']
    assert checkForSequence(['000', '001'], to_digit3) == ['000', '001']
    assert checkForSequence(['001', '000'], to_digit3) == ['000', '001']
    assert checkForSequence(['001', '000', '003'], to_digit3) == ['000', '001']
    assert checkForSequence(['000', '003'], to_digit3) == ['000']
    assert checkForSequence(['000', '003', '004'], to_digit3) == ['000']
    assert not checkForSequence(['0001'], to_digit3)
