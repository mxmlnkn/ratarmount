#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import io
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core')))

from ratarmount import cli as ratarmountcli  # noqa: E402


class RunRatarmount:
    def __init__(self, mountPoint, arguments, debug: int = 3):
        self.debug = debug
        self.timeout = 4
        self.mountPoint = mountPoint
        args = ['-f', '-d', str(debug)] + arguments + [mountPoint]
        self.thread = threading.Thread(target=ratarmountcli, args=(args,))

        self._stdout = None
        self._stderr = None

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()

        self.thread.start()
        self.waitForMountPoint()

        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        try:
            stdout = sys.stdout
            stderr = sys.stderr
            sys.stdout = self._stdout
            sys.stderr = self._stderr
            stdout.seek(0)
            stderr.seek(0)
            output = stdout.read()
            errors = stderr.read()

            if '[Warning]' in output or '[Error]' in output or '[Warning]' in errors or '[Error]' in errors:
                print("===== stdout =====\n", output)
                print("===== stderr =====\n", errors)
                assert False, "There were warnings or errors during execution of ratarmount!"

        finally:
            self.unmount()
            self.thread.join(self.timeout)

    def waitForMountPoint(self):
        t0 = time.time()
        while True:
            if os.path.ismount(self.mountPoint):
                break
            if time.time() - t0 > self.timeout:
                raise RuntimeError("Expected mount point but it isn't one!")
            time.sleep(0.1)

    def unmount(self):
        self.waitForMountPoint()

        ratarmountcli(['-u', self.mountPoint])

        t0 = time.time()
        while True:
            if not os.path.ismount(self.mountPoint):
                break
            if time.time() - t0 > self.timeout:
                raise RuntimeError("Unmounting did not finish in time!")
            time.sleep(0.1)


@pytest.mark.parametrize("compression", ["rar", "zip"])
def test_password(tmpdir, compression):
    # The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
    if compression == "zip" and sys.version_info[0] == 3 and sys.version_info[1] <= 6:
        return

    encryptedFile = "tests/encrypted-nested-tar." + compression
    password = 'foo'
    mountPoint = str(tmpdir)
    with RunRatarmount(mountPoint, ['--password', password, encryptedFile]):
        assert os.path.isdir(os.path.join(mountPoint, "foo"))


@pytest.mark.parametrize("compression", ["rar", "zip"])
@pytest.mark.parametrize("passwords", [["foo"], ["foo", "bar"], ["bar", "foo"]])
def test_password_list(tmpdir, passwords, compression):
    # The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
    if compression == "zip" and sys.version_info[0] == 3 and sys.version_info[1] <= 6:
        return

    encryptedFile = "tests/encrypted-nested-tar." + compression
    passwordFile = os.path.join(tmpdir, "passwords")
    mountPoint = os.path.join(tmpdir, "mountPoint")

    with open(passwordFile, 'wt', encoding='utf-8') as file:
        for password in passwords:
            file.write(password + '\n')

    with RunRatarmount(mountPoint, ['--password-file', passwordFile, encryptedFile]):
        assert os.path.isdir(os.path.join(mountPoint, "foo"))
