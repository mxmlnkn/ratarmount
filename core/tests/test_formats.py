# pylint: disable=unused-import
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

try:
    import sqlcipher3  # noqa: F401
except ImportError:
    sqlcipher3 = None  # type:ignore

import pytest
from helpers import find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Appends proper format checkers to 7Z, EXT4, FAT, RAR formats.
import ratarmountcore.mountsource.archives  # noqa: F401
from ratarmountcore.formats import (
    COMPRESSION_FORMATS,
    FILE_FORMATS,
    FileFormatID,
    detect_formats,
    might_be_format,
)
from ratarmountcore.mountsource.archives import ARCHIVE_BACKENDS
from ratarmountcore.mountsource.factory import find_backends_by_extension


@pytest.mark.order(0)
def test_format_detection():
    # This test assumes that we use correct extensions for all files in the tests folder.
    folder = os.path.dirname(find_test_file("tests/single-file.tar"))
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if not os.path.isfile(path):
            continue

        with open(path, 'rb') as file:
            # The caching should not change the results!
            formats = detect_formats(file)
            assert formats == {fid for fid, info in FILE_FORMATS.items() if might_be_format(file, info)}, name
            assert formats == {fid for fid in FILE_FORMATS if might_be_format(file, fid)}, name

            # Skip tests for which backends are not installed to test on some broken systems.
            backends = find_backends_by_extension(name)
            assert all(backend in ARCHIVE_BACKENDS for backend in backends)
            hasBackend = True
            for backend in backends:
                modules = ARCHIVE_BACKENDS[backend].requiredModules
                if modules and not all(module in sys.modules for module, _ in modules):
                    hasBackend = False
                    print(f"Ignoring test for: {backend} because required modules are missing: {modules}")
                    break
            if 'encrypted' in name and name.endswith('.sqlar') and sqlcipher3 is None:
                hasBackend = False
            if not hasBackend:
                continue

            splitName = name.rsplit('.', 1)
            if len(splitName) > 1 and name and name[0] != '.':
                extension = splitName[-1]
                message = f"{name} with extension {extension} was detected as possibly {formats}"
                # Except for encrypted SQLAR, the format and extension should match.
                # The only chimera file: chimera-tbz2-zip has explicitly no extension!
                if extension != 'sqlar' and FileFormatID.SQLAR in formats:
                    formats.remove(FileFormatID.SQLAR)

                # Deflate format detection can throw quite a lot of false positives because only 1 bit is checked
                # in the worst case.
                if extension != 'deflate' and FileFormatID.DEFLATE in formats:
                    formats.remove(FileFormatID.DEFLATE)

                # The name should be self-explanatory. Do not test for it being recognized as ZIP
                # because it is not a bug if it is not recognized as such.
                if name == 'rar-misrecognized-as-zip.rar' and FileFormatID.ZIP in formats:
                    formats.remove(FileFormatID.ZIP)

                # The file is still large enough that it should be recognized as a (truncated) TAR.
                if name == 'single-file-split.tar.001':
                    extension = 'tar'

                if name == 'single-file-split.tar.002':
                    # This and empty.tar are both filled only with zero-bytes!
                    pass
                elif len(formats) == 0:
                    assert extension in ['001', '002', 'ini', 'sh', 'snar', 'txt', 'py'], message
                elif len(formats) == 1:
                    formatID = next(iter(formats))
                    extensions = set(FILE_FORMATS[formatID].extensions)
                    if formatID in COMPRESSION_FORMATS:
                        extensions.update({'t' + e for e in extensions})
                    if formatID == FileFormatID.RATARMOUNT_INDEX:
                        extensions.add('sqlite')
                    assert extension in extensions, message
                elif len(formats) > 1:
                    # SQLite files can be Ratarmount indexes or SQLAR
                    # EXT4 images can be interpreted as TAR files (with only zero blocks at the start).
                    assert extension in ['ext4', 'sqlar'], message
