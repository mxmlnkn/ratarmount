# pylint: disable=unused-import
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import os
import sys

from helpers import find_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Appends proper format checkers to 7Z, EXT4, FAT, RAR formats.
import ratarmountcore.mountsource.archives  # noqa: E402, F401
from ratarmountcore.formats import (  # noqa: E402
    COMPRESSION_FORMATS,
    FILE_FORMATS,
    FileFormatID,
    detect_formats,
    might_be_format,
)


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

            splitName = name.rsplit('.', 1)
            if len(splitName) > 1 and name and name[0] != '.':
                extension = splitName[-1]
                message = f"{name} with extension {extension} was detected as possibly {formats}"
                # Except for encrypted SQLAR, the format and extension should match.
                # The only chimera file: chimera-tbz2-zip has explicitly no extension!
                if extension != 'sqlar' and FileFormatID.SQLAR in formats:
                    formats.remove(FileFormatID.SQLAR)

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
                    extensions = FILE_FORMATS[formatID].extensions
                    if formatID in COMPRESSION_FORMATS:
                        extensions += ['t' + e for e in extensions]
                    if formatID == FileFormatID.RATARMOUNT_INDEX:
                        extensions.append('sqlite')
                    assert extension in extensions, message
                elif len(formats) > 1:
                    # SQLite files can be Ratarmount indexes or SQLAR
                    # EXT4 images can be interpreted as TAR files (with only zero blocks at the start).
                    assert extension in ['ext4', 'sqlar'], message
