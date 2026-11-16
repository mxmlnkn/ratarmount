# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import calendar
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from helpers import change_timezone
from ratarmountcore.mountsource.formats.fsspec import FSSpecMountSource


class TestFSSpecMountSource:
    @staticmethod
    def test_ftp_modification_time():
        with change_timezone('UTC-2'):
            entry = {'modify': '20241004165129'}
            assert FSSpecMountSource._get_modification_time(entry) == calendar.timegm((2024, 10, 4, 16, 51, 29))
