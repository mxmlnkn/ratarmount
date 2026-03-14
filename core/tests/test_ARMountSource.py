#!/usr/bin/env python3

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position

import hashlib
import os
import stat
import sys

import pytest
from helpers import find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.ar import ARMountSource

LLVM_19_AR_FILES = [
    ('/bar', b'foo\n'),
    ('/1bar', b'foo9\n'),
    ('/Datei enthält $Sonderzeichen\nZeichen!?', b'Oh no!\n'),
    ('/nested-file', b'nested\n'),
]

ARCHIVES = [
    (
        "ar-GNU.ar",
        [
            ('/bar', b'foo\n'),
            ('/1bar', b'foo9\n'),
            ('/zeros-32KiB', 'bb7df04e1b0a2570657527a7e108ae23'),
            ('/Datei enthält $Sonderzeichen\nZeichen!?', b'Oh no!\n'),
            ('/nested-file', b'nested\n'),
        ],
    ),
    (
        "ar-bsdtar.ar",
        [
            ('/bar', b'foo\n'),
            ('/1bar', b'foo9\n'),
            ('/zeros-32KiB', 'bb7df04e1b0a2570657527a7e108ae23'),
            ('/Datei enthält $Sonderzeichen\nZeichen!?', b'Oh no!\n'),
            ('/nested-file', b'nested\n'),
        ],
    ),
    ("ar-llvm-19-bsd.ar", LLVM_19_AR_FILES),
    ("ar-llvm-19-coff.ar", LLVM_19_AR_FILES),
    ("ar-llvm-19-gnu.ar", LLVM_19_AR_FILES),
    (
        "ar-llvm-19-darwin.ar",
        [
            # For some reason, this formats pads each file contents with newlines to 8 B!
            # There is NO reliable way to strip them out because the file might end with one or more newlines originally.
            # Maybe it makes sense for binary object files for which this is intended for.
            # Libarchive, as tested with bsdtar -x is also not able to undo this padding.
            ('/bar', b'foo\n\n\n\n\n'),
            ('/1bar', b'foo9\n\n\n\n'),
            ('/Datei enthält $Sonderzeichen\nZeichen!?', b'Oh no!\n\n'),
            ('/nested-file', b'nested\n\n'),
        ],
    ),
    (
        "ar-llvm-19-thin.ar",
        [
            ('/0', 'bar'),
            ('/5', '1bar'),
            ('/11', 'Datei enthält $Sonderzeichen\nZeichen!?'),
            ('/52', 'folder/nested-file'),
        ],
    ),
    (
        "ar-GNU-truncated.ar",
        [
            ('/bar', b'foo\n'),
            ('/1bar', b'foo9\n'),
            # Truncated to 16 B. Note that there must be a trailing / and ä takes up 2 B!
            ('/Datei enthält ', b'Oh no!\n'),
            ('/nested-file', b'nested\n'),
        ],
    ),
    ("ar-GCC-main.a", [('/main.o', '9b1c420c2969e36f511eba7695aeccc4')]),
    (
        "testpkg_0.0.1_all.deb",
        [
            ('/debian-binary', b'2.0\n'),
            ('/control.tar.zst', 'b1b10fd98eb29cba3f57bac7444c93d5'),
            ('/data.tar.zst', '35fa0ecdda4a5d478554cd9f85bc767a'),
        ],
    ),
    (
        "ar-GCC-main.a",
        [
            ('/main.o', '9b1c420c2969e36f511eba7695aeccc4'),
        ],
    ),
]


@pytest.mark.parametrize("archive", ARCHIVES)
def test_ar_archives(archive):
    with ARMountSource(find_test_file(archive[0])) as mountSource:
        for folder in ['/']:
            fileInfo = mountSource.lookup(folder)
            assert fileInfo, folder
            assert stat.S_ISDIR(fileInfo.mode)

            assert mountSource.versions(folder) == 1
            assert mountSource.list(folder)

        for path, content in archive[1]:
            fileInfo = mountSource.lookup(path)
            assert fileInfo, path
            assert not stat.S_ISDIR(fileInfo.mode)

            assert mountSource.versions(path) == 1
            assert not mountSource.list(path)
            if stat.S_ISLNK(fileInfo.mode):
                assert fileInfo.linkname == content
            else:
                with mountSource.open(fileInfo) as file:
                    if isinstance(content, bytes):
                        assert file.read() == content
                    else:
                        assert hashlib.md5(file.read()).hexdigest() == content
