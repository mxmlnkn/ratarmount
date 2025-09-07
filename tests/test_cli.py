# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import hashlib
import io
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../core/tests')))

from helpers import copy_test_file
from ratarmountcore.compressions import libarchive
from ratarmountcore.utils import ceil_div

from ratarmount.cli import cli as ratarmountcli

try:
    import ext4
except ImportError:
    ext4 = None  # type:ignore

try:
    import sqlcipher3
except ImportError:
    sqlcipher3 = None  # type:ignore


class RunRatarmount:
    def __init__(self, mountPoint, arguments, debug: int = 3):
        self.debug = debug
        # sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst takes 4.5 s on my system.
        self.timeout = 20
        self.mountPoint = mountPoint
        args = ['-f', '-d', str(debug), *arguments, mountPoint]
        self.thread = threading.Thread(target=ratarmountcli, args=(args,))

        self._stdout = None
        self._stderr = None

    def __enter__(self):
        self._stdout = sys.stdout
        self._stderr = sys.stderr
        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()

        self.thread.start()
        self.wait_for_mount_point()

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
                raise AssertionError("There were warnings or errors during execution of ratarmount!")

        finally:
            self.unmount()
            self.thread.join(self.timeout)

    def get_stdout(self):
        oldPosition = sys.stdout.tell()
        try:
            sys.stdout.seek(0)
            return sys.stdout.read()
        finally:
            sys.stdout.seek(oldPosition)

    def get_stderr(self):
        oldPosition = sys.stderr.tell()
        try:
            sys.stderr.seek(0)
            return sys.stderr.read()
        finally:
            sys.stderr.seek(oldPosition)

    def wait_for_mount_point(self):
        t0 = time.time()
        while True:
            if os.path.ismount(self.mountPoint):
                break
            if time.time() - t0 > self.timeout:
                mount_list = "<Unable to run mount command>"
                try:
                    mount_list = subprocess.run("mount", capture_output=True, check=True).stdout.decode()
                except Exception as exception:
                    mount_list += f"\n{exception}"
                raise RuntimeError(
                    "Expected mount point but it isn't one!"
                    "\n===== stderr =====\n"
                    + self.get_stderr()
                    + "\n===== stdout =====\n"
                    + self.get_stdout()
                    + "\n===== mount =====\n"
                    + mount_list
                )
            time.sleep(0.1)

    def unmount(self):
        self.wait_for_mount_point()

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
@pytest.mark.parametrize("compression", ["rar", "zip"] + ([] if sqlcipher3 is None else ["sqlar"]))
def test_password(tmpdir, compression):
    password = 'foo'
    mountPoint = str(tmpdir)
    with (
        copy_test_file("encrypted-nested-tar." + compression) as encryptedFile,
        RunRatarmount(mountPoint, ['--password', password, encryptedFile]),
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
    passwordFile = os.path.join(tmpdir, "passwords")
    mountPoint = os.path.join(tmpdir, "mountPoint")

    with open(passwordFile, 'w', encoding='utf-8') as file:
        file.writelines(password + '\n' for password in passwords)

    with (
        copy_test_file("tests/encrypted-nested-tar." + compression) as encryptedFile,
        RunRatarmount(mountPoint, ['--password-file', passwordFile, encryptedFile]),
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
    # https://github.com/libarchive/libarchive/issues/2692
    # ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar.skippable-frame.lz4", "foo/fighter/ufo"),
    # ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar.skippable-frame.lz4", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar.tar.pzstd", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar.tar.pzstd", "foo/lighter.tar/fighter/bar"),
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
    ("bf619eac0cdf3f68d496ea9344137e8b", "sparse.gnu.tar", "sparse-512B"),
    ("bf619eac0cdf3f68d496ea9344137e8b", "sparse.pax.sparse-0.0.tar", "sparse-512B"),
    ("bf619eac0cdf3f68d496ea9344137e8b", "sparse.pax.sparse-0.1.tar", "sparse-512B"),
    ("bf619eac0cdf3f68d496ea9344137e8b", "sparse.pax.sparse-1.0.tar", "sparse-512B"),
    ("370a398ecaff897a26df4747c2036ee5", "sparse.gnu.tar", "sparse-513B"),
    ("370a398ecaff897a26df4747c2036ee5", "sparse.pax.sparse-0.0.tar", "sparse-513B"),
    ("370a398ecaff897a26df4747c2036ee5", "sparse.pax.sparse-0.1.tar", "sparse-513B"),
    ("370a398ecaff897a26df4747c2036ee5", "sparse.pax.sparse-1.0.tar", "sparse-513B"),
    ("3d8197de2852aebea45828751bac572e", "sparse.gnu.tar", "sparse-1MiB"),
    ("3d8197de2852aebea45828751bac572e", "sparse.pax.sparse-0.0.tar", "sparse-1MiB"),
    ("3d8197de2852aebea45828751bac572e", "sparse.pax.sparse-0.1.tar", "sparse-1MiB"),
    ("3d8197de2852aebea45828751bac572e", "sparse.pax.sparse-1.0.tar", "sparse-1MiB"),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "01.sparse1.bin"),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "02.normal1.bin"),
    ("832c78afcb9832e1a21c18212fc6c38b", "gnu-sparse-files.tar", "03.sparse1.bin"),
    (
        "cb5d4faf665db396dc34df1689ef1da8",
        "sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst",
        "sparse",
    ),
    (
        "c157a79031e1c40f85931829bc5fc552",
        "sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst",
        "foo",
    ),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar.asar", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar.asar", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar.sqlar", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar.sqlar", "foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar-compressed.sqlar", "foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar-compressed.sqlar", "foo/lighter.tar/fighter/bar"),
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


EXT4_TO_TEST = [
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar-1M.ext4.bz2", "nested-tar-1M.ext4/foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar-1M.ext4.bz2", "nested-tar-1M.ext4/foo/lighter.tar/fighter/bar"),
    ("2709a3348eb2c52302a7606ecf5860bc", "nested-tar-10M.ext4.bz2", "nested-tar-10M.ext4/foo/fighter/ufo"),
    ("2b87e29fca6ee7f1df6c1a76cb58e101", "nested-tar-10M.ext4.bz2", "nested-tar-10M.ext4/foo/lighter.tar/fighter/bar"),
]

if ext4:
    ARCHIVES_TO_TEST += EXT4_TO_TEST


@pytest.mark.parametrize("parallelization", [1, 2, 0])
@pytest.mark.parametrize(("checksum", "archivePath", "pathInArchive"), ARCHIVES_TO_TEST)
def test_file_in_archive(archivePath, pathInArchive, checksum, parallelization):
    with copy_test_file(archivePath) as tmpArchive:
        assert os.path.isfile(tmpArchive)
        mountPoint = os.path.join(os.path.dirname(tmpArchive), "mountPoint")
        args = [
            "--index-minimum-file-count",
            "0",
            "-P",
            str(parallelization),
            "--detect-gnu-incremental",
            "--recursive",
            tmpArchive,
        ]

        # The compressed .ext4.bz2 files need SQLiteIndexedTar to undo the compression (which is a leftover legacy
        # and architecture design failure) and with --ignore-zeros it would mount the nested TAR int the same step!
        if 'ext4' not in archivePath:
            args.insert(0, "--ignore-zeros")

        print(f"ratarmount -P {parallelization} {tmpArchive} mounted at {mountPoint} -> access: {pathInArchive}")

        # Test with forced index recreation first and then with index loading.
        for forceIndexCreation in [True, False]:
            testArgs = ["-c", *args] if forceIndexCreation else args
            with RunRatarmount(mountPoint, testArgs) as ratarmountInstance:
                path = Path(mountPoint) / pathInArchive
                assert path.is_file()
                stats = path.stat()  # implicitly tests that this does not throw
                assert stats.st_size > 0

                if pathInArchive == "02.normal.bin":
                    assert stats.st_size == 10 * 1024 * 1024 + 1
                    # https://linux.die.net/man/2/stat The number of blocks is ALWAYS with 512 B block size.
                    assert stats.st_blocks == ceil_div(10 * 1024 * 1024 + 1, 512)

                hash_md5 = hashlib.md5()
                with path.open('rb') as file:
                    while True:
                        contents = file.read(1024 * 1024)
                        if not contents:
                            break
                        hash_md5.update(contents)
                assert hash_md5.hexdigest() == checksum

                if '.tar' in tmpArchive and '.7z' not in tmpArchive:
                    output = ratarmountInstance.get_stdout() + ratarmountInstance.get_stderr()
                    if forceIndexCreation:
                        if "Creating offset dictionary" not in output:
                            print(
                                "Looks like index was not created while executing: ratarmount "
                                + ' '.join([*testArgs, mountPoint])
                            )
                    else:
                        if "Successfully loaded offset dictionary" not in output:
                            print(
                                "Looks like index was not loaded while executing: ratarmount "
                                + ' '.join([*testArgs, mountPoint])
                            )
