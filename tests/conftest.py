import os
import shutil
import subprocess
import sys


def _unmount_leftovers():
    if not shutil.which('fusermount'):
        return

    test_cli = sys.modules.get('test_cli')
    if test_cli is None:
        return

    for mountPoint in getattr(test_cli, 'CREATED_MOUNT_POINTS', []):
        if os.path.ismount(mountPoint):
            subprocess.run(['fusermount', '-u', '--', mountPoint], check=False, capture_output=True)


def pytest_runtest_teardown(item, nextitem):
    _unmount_leftovers()


def pytest_sessionfinish(session, exitstatus):
    _unmount_leftovers()
