# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import string
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.compressions import (
    check_for_sequence,
    has_matching_alphabets,
    strip_suffix_from_archive,
    strip_suffix_from_compressed_file,
)
from ratarmountcore.utils import HEX, format_number

pytestmark = pytest.mark.order(0)


def test_strip_suffix_from_compressed_file():
    ssc = strip_suffix_from_compressed_file

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
    sst = strip_suffix_from_archive

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
    matches = has_matching_alphabets

    assert matches('a', 'b')
    assert matches('0', '1')
    assert matches('0', 'a')  # because both might be hexadecimal
    assert matches('1a', 'b0')
    assert matches(HEX, HEX)
    assert matches(string.digits, string.digits)
    assert matches(string.ascii_lowercase, string.ascii_lowercase)
    assert matches(HEX, string.digits)
    assert not matches(string.ascii_lowercase, HEX)
    assert not matches(string.ascii_lowercase, string.digits)
    assert not matches('ag', 'b0')


def test_check_for_sequence():
    def to_alpha1(i):
        return format_number(i, string.ascii_lowercase, 1)

    assert check_for_sequence(['a'], to_alpha1) == ['a']
    assert check_for_sequence(['a', 'b'], to_alpha1) == ['a', 'b']
    assert check_for_sequence(['b', 'a'], to_alpha1) == ['a', 'b']
    assert check_for_sequence(['b', 'a', 'd'], to_alpha1) == ['a', 'b']
    assert check_for_sequence(['a', 'd'], to_alpha1) == ['a']
    assert check_for_sequence(['a', 'd', 'e'], to_alpha1) == ['a']
    assert not check_for_sequence(['aa'], to_alpha1)
    assert not check_for_sequence(['0'], to_alpha1)

    def to_alpha2(i):
        return format_number(i, string.ascii_lowercase, 2)

    assert check_for_sequence(['aa'], to_alpha2) == ['aa']
    assert check_for_sequence(['aa', 'ab'], to_alpha2) == ['aa', 'ab']
    assert check_for_sequence(['ab', 'aa'], to_alpha2) == ['aa', 'ab']
    assert check_for_sequence(['ab', 'aa', 'ad'], to_alpha2) == ['aa', 'ab']
    assert check_for_sequence(['aa', 'ad'], to_alpha2) == ['aa']
    assert check_for_sequence(['aa', 'ad', 'ae'], to_alpha2) == ['aa']
    assert not check_for_sequence(['aaa'], to_alpha2)
    assert not check_for_sequence(['0'], to_alpha2)
    assert not check_for_sequence(['00'], to_alpha2)

    def to_digit3(i):
        return format_number(i, string.digits, 3)

    assert check_for_sequence(['000'], to_digit3) == ['000']
    assert check_for_sequence(['000', '001'], to_digit3) == ['000', '001']
    assert check_for_sequence(['001', '000'], to_digit3) == ['000', '001']
    assert check_for_sequence(['001', '000', '003'], to_digit3) == ['000', '001']
    assert check_for_sequence(['000', '003'], to_digit3) == ['000']
    assert check_for_sequence(['000', '003', '004'], to_digit3) == ['000']
    assert not check_for_sequence(['0001'], to_digit3)
