#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import contextlib
import hashlib
import io
import os
import sys
import tempfile
import threading
import time

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core')))

from ratarmountcore.compressions import libarchive  # noqa: E402
from ratarmount import cli as ratarmountcli  # noqa: E402


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


@contextlib.contextmanager
def copyTestFile(relativePathOrName):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, os.path.basename(relativePathOrName))
        with open(findTestFile(relativePathOrName), 'rb') as file, open(path, 'wb') as target:
            target.write(file.read())
        yield path


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

            problematicWords = ['[Warning]', '[Error]']
            if any(word in output or word in errors for word in problematicWords):
                print("===== stdout =====\n", output)
                print("===== stderr =====\n", errors)
                assert False, "There were warnings or errors during execution of ratarmount!"

        finally:
            self.unmount()
            self.thread.join(self.timeout)

    def getStdout(self):
        oldPosition = sys.stdout.tell()
        try:
            sys.stdout.seek(0)
            return sys.stdout.read()
        finally:
            sys.stdout.seek(oldPosition)

    def getStderr(self):
        oldPosition = sys.stderr.tell()
        try:
            sys.stderr.seek(0)
            return sys.stderr.read()
        finally:
            sys.stderr.seek(oldPosition)

    def waitForMountPoint(self):
        t0 = time.time()
        while True:
            if os.path.ismount(self.mountPoint):
                break
            if time.time() - t0 > self.timeout:
                raise RuntimeError(
                    "Expected mount point but it isn't one!"
                    + "\n===== stderr =====\n"
                    + self.getStderr()
                    + "\n===== stdout =====\n"
                    + self.getStdout()
                )
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


# 7z encryption is not supported by libarchive and therefore also not by ratarmount.
# https://github.com/libarchive/libarchive/issues/579#issuecomment-118440525
@pytest.mark.parametrize("compression", ["rar", "zip"])
def test_password(tmpdir, compression):
    # The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
    if compression == "zip" and sys.version_info[0] == 3 and sys.version_info[1] <= 6:
        return

    password = 'foo'
    mountPoint = str(tmpdir)
    with copyTestFile("encrypted-nested-tar." + compression) as encryptedFile, RunRatarmount(
        mountPoint, ['--password', password, encryptedFile]
    ):
        assert os.path.isdir(os.path.join(mountPoint, "foo"))
        assert os.path.isdir(os.path.join(mountPoint, "foo", "fighter"))
        filePath = os.path.join(mountPoint, "foo", "fighter", "ufo")
        assert os.path.exists(filePath)
        assert os.path.isfile(filePath)
        assert open(filePath, 'rb').read()


@pytest.mark.parametrize("compression", ["rar", "zip"])
@pytest.mark.parametrize("passwords", [["foo"], ["foo", "bar"], ["bar", "foo"]])
def test_password_list(tmpdir, passwords, compression):
    # The file object returned by ZipFile.open is not seekable in Python 3.6 for some reason.
    if compression == "zip" and sys.version_info[0] == 3 and sys.version_info[1] <= 6:
        return

    passwordFile = os.path.join(tmpdir, "passwords")
    mountPoint = os.path.join(tmpdir, "mountPoint")

    with open(passwordFile, 'wt', encoding='utf-8') as file:
        for password in passwords:
            file.write(password + '\n')

    with copyTestFile("tests/encrypted-nested-tar." + compression) as encryptedFile, RunRatarmount(
        mountPoint, ['--password-file', passwordFile, encryptedFile]
    ):
        assert os.path.isdir(os.path.join(mountPoint, "foo"))


ARCHIVES_TO_TEST = [
    ("2709a3348eb2c52302a7606ecf5860bc", "file-in-non-existing-folder.rar", "foo2/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.rar", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.rar", "foo/jet/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-file-implicitly-with-folder.tar", "foo/fighter"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-file-implicitly-with-folder.tar", "foo.versions/2/fighter"),
    ("c157a79031e1c40f85931829bc5fc552", "updated-file-implicitly-with-folder.tar", "foo.versions/1"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-file-implicitly-with-folder.tar", "bar/par/sora/natsu"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-file-implicitly-with-folder.tar", "bar/par/sora.versions/2/natsu"),
    ("cd85c6a5e5053c04f95e1df301c80755", "updated-file-implicitly-with-folder.tar", "bar/par/sora.versions/1"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.tar", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file-with-leading-dot-slash.tar", "bar"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "folder-with-leading-dot-slash.tar", "foo/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-with-leading-dot-slash.tar", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "denormal-paths.rar", "ufo"),
    ("d3b07384d113edec49eaa6238ad5ff00", "denormal-paths.rar", "root/bar"),
    ("c157a79031e1c40f85931829bc5fc552", "denormal-paths.rar", "foo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "denormal-paths.tar", "ufo"),
    ("d3b07384d113edec49eaa6238ad5ff00", "denormal-paths.tar", "root/bar"),
    ("c157a79031e1c40f85931829bc5fc552", "denormal-paths.tar", "foo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "single-nested-file.tar", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "single-nested-folder.tar", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar.tar", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar.tar", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "directly-nested-tar.tar", "fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "directly-nested-tar.tar", "lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar-with-overlapping-name.tar", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar-with-overlapping-name.tar", "foo/fighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "hardlink.tar", "hardlink/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "hardlink.tar", "hardlink/natsu"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "concatenated.tar", "foo/fighter"),
    ("2709a3348eb2c52302a7606ecf5860bc", "concatenated.tar", "foo/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-symlinks.tar", "foo/foo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-symlinks.tar", "foo/fighter/foo"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file.tar", "foo/fighter/ufo"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file.tar", "foo/fighter/ufo.versions/3"),
    ("9a12be5ebb21d497bd1024d159f2cc5f", "updated-file.tar", "foo/fighter/ufo.versions/2"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-file.tar", "foo/fighter/ufo.versions/1"),
    ("9a12be5ebb21d497bd1024d159f2cc5f", "updated-folder-with-file.tar", "foo"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-folder-with-file.tar", "foo.versions/1/fighter"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-folder-with-file.tar", "foo.versions/1/fighter.versions/2"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-folder-with-file.tar", "foo.versions/1/fighter.versions/1"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-folder-with-file.tar", "foo.versions/2/fighter"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-folder-with-file.tar", "foo.versions/2/fighter.versions/2"),
    ("2709a3348eb2c52302a7606ecf5860bc", "updated-folder-with-file.tar", "foo.versions/2/fighter.versions/1"),
    ("9a12be5ebb21d497bd1024d159f2cc5f", "updated-folder-with-file.tar", "foo.versions/3"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file-with-folder.tar", "foo/fighter"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file-with-folder.tar", "foo/fighter.versions/1"),
    ("9a12be5ebb21d497bd1024d159f2cc5f", "updated-file-with-folder.tar", "foo.versions/1"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file-with-folder.tar", "foo.versions/2/fighter"),
    ("b3de7534cbc8b8a7270c996235d0c2da", "updated-file-with-folder.tar", "foo.versions/2/fighter.versions/1"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.bz2", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.gz", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.xz", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.zlib", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.zst", "simple"),
    ("2709a3348eb2c52302a7606ecf5860bc", "file-existing-as-non-link-and-link.tar", "foo/fighter/ufo"),
    ("d3b07384d113edec49eaa6238ad5ff00", "two-self-links-to-existing-file.tar", "bar"),
    ("c9172d469a8faf82fe598c0ce978fcea", "base64.gz", "base64"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-directly-compressed.tar.bz2", "directly-compressed/ufo.bz2/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-directly-compressed.tar.bz2", "directly-compressed/ufo.gz/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-directly-compressed.tar.bz2", "directly-compressed/ufo.xz/ufo"),
    ("c157a79031e1c40f85931829bc5fc552", "absolute-file-incremental.tar", "14130612002/tmp/foo"),
    ("8ddd8be4b179a529afa5f2ffae4b9858", "incremental-backup.level.0.tar", "root-file.txt"),
    ("5bbf5a52328e7439ae6e719dfe712200", "incremental-backup.level.0.tar", "foo/1"),
    ("c193497a1a06b2c72230e6146ff47080", "incremental-backup.level.0.tar", "foo/2"),
    ("febe6995bad457991331348f7b9c85fa", "incremental-backup.level.0.tar", "foo/3"),
    ("3d45efe945446cd53a944972bf60810c", "incremental-backup.level.1.tar", "foo/3"),
    ("5bbf5a52328e7439ae6e719dfe712200", "incremental-backup.level.1.tar", "foo/moved"),
    ("c157a79031e1c40f85931829bc5fc552", "single-file-incremental-mockup.tar", "14130613451/foo"),
    (
        "c157a79031e1c40f85931829bc5fc552",
        "single-file-incremental-long-name-mockup.tar",
        "14130613451/00000000010000000002000000000300000000040000000005000000000600000000070000000008"
        "0000000009000000000A000000000B000000000C",
    ),
    (
        "c157a79031e1c40f85931829bc5fc552",
        "single-file-incremental-long-name.tar",
        "000000000100000000020000000003000000000400000000050000000006000000000700000000080000000009"
        "000000000A000000000B000000000C",
    ),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "01.sparse1.bin"),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "02.normal1.bin"),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "03.sparse1.bin"),
]


ZIP_ARCHIVES_TO_TEST = [
    ("2709a3348eb2c52302a7606ecf5860bc", "file-in-non-existing-folder.zip", "foo2/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "rar.zip", "natsu.rar/ufo"),
    ("10d6977ec2ab378e60339323c24f9308", "rar.zip", "natsu.rar/foo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-with-symlink.zip", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-with-symlink.zip", "foo/fighter/saucer"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-with-symlink.zip", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.zip", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.zip", "foo/jet/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "mockup-self-extracting.zip", "ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "denormal-paths.zip", "ufo"),
    ("d3b07384d113edec49eaa6238ad5ff00", "denormal-paths.zip", "root/bar"),
    ("c157a79031e1c40f85931829bc5fc552", "denormal-paths.zip", "foo"),
]

# zipfile returns unseekable file object with Python 3.6.
if sys.version_info[:1] > (3, 6):
    ARCHIVES_TO_TEST += ZIP_ARCHIVES_TO_TEST


LIBARCHIVE_ARCHIVES_TO_TEST = [
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-with-symlink.7z", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-with-symlink.7z", "foo/fighter/saucer"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-with-symlink.7z", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "zip.7z", "natsu.zip/ufo"),
    ("10d6977ec2ab378e60339323c24f9308", "zip.7z", "natsu.zip/foo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "file-in-non-existing-folder.7z", "foo2/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.7z", "foo/fighter/ufo"),
    ("2709a3348eb2c52302a7606ecf5860bc", "folder-symlink.7z", "foo/jet/ufo"),
    (
        "2709a3348eb2c52302a7606ecf5860bc",
        "double-compressed-nested-tar.tar.7z.7z",
        "nested-tar.tar.7z/nested-tar.tar/foo/fighter/ufo",
    ),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.lzma", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.lrz", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.lz4", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.lzip", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.lzo", "simple"),
    ("19696f24a91fc4e8950026f9c801a0d0", "simple.Z", "simple"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.ar", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.cab", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.iso.bz2", "single-file.iso/bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.xar", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.bin.cpio", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.crc.cpio", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.hpbin.cpio", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.hpodc.cpio", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.newc.cpio", "bar"),
    ("d3b07384d113edec49eaa6238ad5ff00", "single-file.odc.cpio", "bar"),
    # The contents of files and file hierarchy of WARC is subject to change.
    (
        "4aecced75ff52fdd39bb52dae192258f",
        "hello-world.warc",
        "warc-specifications/primers/web-archive-formats/hello-world.txt",
    ),
]

if libarchive:
    ARCHIVES_TO_TEST += LIBARCHIVE_ARCHIVES_TO_TEST


@pytest.mark.parametrize("parallelization", [1, 2, 0])
@pytest.mark.parametrize("checksum,archivePath,pathInArchive", ARCHIVES_TO_TEST)
def test_file_in_archive(archivePath, pathInArchive, checksum, parallelization):
    with copyTestFile(archivePath) as tmpArchive:
        assert os.path.isfile(tmpArchive)
        mountPoint = os.path.join(os.path.dirname(tmpArchive), "mountPoint")
        args = [
            "--index-minimum-file-count",
            "0",
            "-P",
            str(parallelization),
            "--detect-gnu-incremental",
            "--ignore-zeros",
            "--recursive",
            tmpArchive,
        ]
        print(f"ratarmount -P {parallelization} {tmpArchive} mounted at {mountPoint} -> access: {pathInArchive}")

        # Test with forced index recreation first and then with index loading.
        for forceIndexCreation in [True, False]:
            testArgs = ["-c"] + args if forceIndexCreation else args
            with RunRatarmount(mountPoint, testArgs) as ratarmountInstance:
                path = os.path.join(mountPoint, pathInArchive)
                assert os.path.isfile(path)
                stats = os.stat(path)  # implicitly tests that this does not throw
                assert stats.st_size > 0

                with open(path, 'rb') as file:
                    contents = file.read()  # extra line because it might fail
                    assert hashlib.md5(contents).hexdigest() == checksum

                if '.tar' in tmpArchive and '.7z' not in tmpArchive:
                    output = ratarmountInstance.getStdout() + ratarmountInstance.getStderr()
                    if forceIndexCreation:
                        if "Creating offset dictionary" not in output:
                            print(
                                "Looks like index was not created while executing: ratarmount "
                                + ' '.join(testArgs + [mountPoint])
                            )
                    else:
                        if "Successfully loaded offset dictionary" not in output:
                            print(
                                "Looks like index was not loaded while executing: ratarmount "
                                + ' '.join(testArgs + [mountPoint])
                            )
