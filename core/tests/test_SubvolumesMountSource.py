# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name

import dataclasses
import io
import os
import stat
import sys
import tarfile
from pathlib import Path
from typing import Optional

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.compositing.singlefile import SingleFileMountSource  # noqa: E402
from ratarmountcore.mountsource.compositing.subvolumes import SubvolumesMountSource  # noqa: E402
from ratarmountcore.mountsource.formats.folder import FolderMountSource  # noqa: E402
from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar  # noqa: E402
from ratarmountcore.mountsource.MountSource import MountSource  # noqa: E402
from ratarmountcore.utils import RatarmountError  # noqa: E402


@dataclasses.dataclass
class SampleArchive:
    path: Path
    folders: list[str]
    files: dict[str, bytes]


def _create_file(tarArchive, name, contents):
    tinfo = tarfile.TarInfo(name)
    tinfo.size = len(contents)
    tarArchive.addfile(tinfo, io.BytesIO(contents if isinstance(contents, bytes) else contents.encode()))


def _make_folder(tarArchive, name):
    tinfo = tarfile.TarInfo(name)
    tinfo.type = tarfile.DIRTYPE
    tarArchive.addfile(tinfo, io.BytesIO())


def _populate_folder(sampleArchive: SampleArchive):
    for folder in sampleArchive.folders:
        os.makedirs(os.path.join(sampleArchive.path, folder.strip('/')), exist_ok=True)
    for path, contents in sampleArchive.files.items():
        (sampleArchive.path / path.strip('/')).write_bytes(contents)


def _populate_tar(sampleArchive: SampleArchive):
    with tarfile.open(name=sampleArchive.path, mode="w:bz2") as tarFile:
        for folder in sampleArchive.folders:
            _make_folder(tarFile, folder)
        for path, contents in sampleArchive.files.items():
            _create_file(tarFile, path, contents)


@pytest.fixture(name="sample_folder_a")
def fixture_sample_folder_a(tmp_path):
    sampleArchive = SampleArchive(
        path=tmp_path / "folderA",
        folders=["subfolder"],
        files={"/subfolder/world": b"hello\n", "/ufo": b"iriya in folder 1\n"},
    )
    _populate_folder(sampleArchive)
    return sampleArchive


@pytest.fixture(name="sample_folder_b")
def fixture_sample_folder_b(tmp_path):
    sampleArchive = SampleArchive(
        path=tmp_path / "folderB",
        folders=["subfolder"],
        files={"/ufo": b"iriya\n"},
    )
    _populate_folder(sampleArchive)
    return sampleArchive


@pytest.fixture(name="sample_tar_a")
def fixture_sample_tar_a(tmp_path):
    sampleArchive = SampleArchive(
        path=tmp_path / "sampleA.tar",
        folders=["subfolder"],
        files={"/ufo": b"inside", "/README.md": b"readme inside", "/subfolder/world": b"HELLO"},
    )
    _populate_tar(sampleArchive)
    return sampleArchive


@pytest.fixture(name="sample_tar_b")
def fixture_sample_tar_b(tmp_path):
    sampleArchive = SampleArchive(
        path=tmp_path / "sampleB.tar",
        folders=["/src", "/dist", "/dist/a", "/dist/a/b"],
        files={"/README.md": b"hello world", "/src/test.sh": b"echo hi", "/dist/a/b/test2.sh": "echo two"},
    )
    _populate_tar(sampleArchive)
    return sampleArchive


class TestSubvolumesMountSource:
    @staticmethod
    def _check_file(mountSource: MountSource, pathToCheck: str, version: int, contents: Optional[bytes] = None):
        for path in [pathToCheck.lstrip('/'), '/' + pathToCheck.lstrip('/')]:
            fileInfo = mountSource.lookup(path, version)
            assert fileInfo, f"Path: {path}"

            if contents is None:
                assert stat.S_ISDIR(fileInfo.mode)
            else:
                assert not stat.S_ISDIR(fileInfo.mode)
                assert stat.S_ISREG(fileInfo.mode)

                # The MountSource interface only allows to open files in binary mode, which returns bytes not string.
                if isinstance(contents, str):
                    contents = contents.encode()
                with mountSource.open(fileInfo) as file:
                    assert file.read() == contents

    @staticmethod
    @pytest.mark.parametrize('paths', [("foo", "/foo"), ("/foo", "//foo"), ("folder/foo", "folder//foo")])
    def test_duplicate_paths(paths):
        with pytest.raises(RatarmountError, match="exists"):
            SubvolumesMountSource({path: SingleFileMountSource("bar", io.BytesIO(b"bar")) for path in paths})

    @staticmethod
    def test_unite_two_folders(sample_folder_a, sample_folder_b):
        union = SubvolumesMountSource(
            {"folderA": FolderMountSource(sample_folder_a.path), "folderB": FolderMountSource(sample_folder_b.path)}
        )

        assert union.lookup("folderC") is None

        # Check folders
        for path in sample_folder_a.folders:
            TestSubvolumesMountSource._check_file(union, "folderA/" + path, 0, None)
        for path in sample_folder_a.folders + sample_folder_b.folders:
            TestSubvolumesMountSource._check_file(union, "folderB/" + path, 0, None)

        # Check files
        for path, contents in sample_folder_a.files.items():
            TestSubvolumesMountSource._check_file(
                union, "folderA/" + path, 0 if path not in sample_folder_b.files else 1, contents
            )
        for path, contents in sample_folder_b.files.items():
            TestSubvolumesMountSource._check_file(union, "folderB/" + path, 0, contents)

    @staticmethod
    def test_unite_two_folders_and_update_one(sample_folder_a, sample_folder_b):
        folderA = FolderMountSource(sample_folder_a.path)
        folderB = FolderMountSource(sample_folder_b.path)
        volumes = {"folderA": folderA, "folderB": folderB}
        union = SubvolumesMountSource(volumes)

        contents = b"atarashii iriya\n"
        (sample_folder_a.path / "ufo2").write_bytes(contents)
        os.mkdir(os.path.join(sample_folder_a.path, "subfolder2"))
        (sample_folder_a.path / "subfolder2" / "world").write_bytes(contents)
        os.mkdir(os.path.join(sample_folder_a.path, "subfolder3"))
        (sample_folder_a.path / "subfolder3" / "world").write_bytes(contents)
        (sample_folder_a.path / "second-world").write_bytes(contents)

        TestSubvolumesMountSource._check_file(union, "folderA/ufo2", 0, contents)
        TestSubvolumesMountSource._check_file(union, "folderA/subfolder2", 0, None)
        TestSubvolumesMountSource._check_file(union, "folderA/subfolder2/world", 0, contents)
        TestSubvolumesMountSource._check_file(union, "folderA/subfolder3/world", 0, contents)
        TestSubvolumesMountSource._check_file(union, "folderA/second-world", 0, contents)

        # Check folders

        for path in sample_folder_a.folders:
            TestSubvolumesMountSource._check_file(union, "folderA/" + path, 0, None)
        for path in sample_folder_a.folders + sample_folder_b.folders:
            TestSubvolumesMountSource._check_file(union, "folderB/" + path, 0, None)

        # Test versions

        assert union.versions("/") == 1
        assert union.versions("") == 1
        assert union.versions("/NON_EXISTING") == 0
        assert union.versions("folderA/ufo") == 1
        assert union.versions("folderA/ufo2") == 1
        assert union.versions("folderB/ufo") == 1

        # Test list

        assert sorted(union.list("/").keys()) == ["folderA", "folderB"]
        assert sorted(union.list("").keys()) == ["folderA", "folderB"]
        assert sorted(union.list("/folderB").keys()) == ["subfolder", "ufo"]
        assert sorted(union.list("/folderB/subfolder").keys()) == []
        assert sorted(union.list("/folderA").keys()) == [
            "second-world",
            "subfolder",
            "subfolder2",
            "subfolder3",
            "ufo",
            "ufo2",
        ]

        # Test get_mount_source

        for folder in ["/", ""]:
            fileInfo = union.lookup(folder)
            assert fileInfo, f"Folder: {folder}"
            assert union.get_mount_source(fileInfo) == ("/", union, fileInfo)

        pathsBySubmount = {
            'folderA': ["folderA", "/folderA", "/folderA/ufo2", "folderA/subfolder2", "folderA/subfolder2/world"],
            'folderB': ["folderB", "/folderB", "/folderB/ufo", "folderB/subfolder"],
        }
        for volume, paths in pathsBySubmount.items():
            mountSource = volumes[volume]
            for path in paths:
                fileInfo = union.lookup(path)
                assert fileInfo, f"Path: {path}"
                result = union.get_mount_source(fileInfo)
                fileInfo.userdata.pop()
                assert result == ('/' + volume, mountSource, fileInfo)

        # Test unmounting

        mountSource = union.unmount("folderB")
        assert isinstance(mountSource, MountSource)
        assert sorted(union.list("/").keys()) == ["folderA"]
        union.mount("folderC", mountSource)
        assert sorted(union.list("/").keys()) == ["folderA", "folderC"]
        assert union

        for path in sample_folder_a.folders + sample_folder_b.folders:
            TestSubvolumesMountSource._check_file(union, "folderC/" + path, 0, None)

    @staticmethod
    def test_unite_two_archives(sample_tar_a, sample_tar_b):
        union = SubvolumesMountSource(
            {"folderA": SQLiteIndexedTar(sample_tar_a.path), "folderB": SQLiteIndexedTar(sample_tar_b.path)}
        )

        # Check folders
        for path in sample_tar_a.folders:
            TestSubvolumesMountSource._check_file(union, "folderA/" + path, 0, None)
        for path in sample_tar_b.folders:
            TestSubvolumesMountSource._check_file(union, "folderB/" + path, 0, None)

        # Check files
        for path, contents in sample_tar_a.files.items():
            TestSubvolumesMountSource._check_file(
                union, "folderA/" + path, 0 if path not in sample_tar_b.files else 1, contents
            )
        for path, contents in sample_tar_b.files.items():
            TestSubvolumesMountSource._check_file(union, "folderB/" + path, 0, contents)

    @staticmethod
    def test_unite_folder_and_archive_and_update_folder(sample_tar_a, sample_folder_a):
        union = SubvolumesMountSource(
            {"tar": SQLiteIndexedTar(sample_tar_a.path), "folder": FolderMountSource(sample_folder_a.path)}
        )

        for path in sample_tar_a.folders:
            TestSubvolumesMountSource._check_file(union, "tar/" + path, 0, None)
        for path in sample_folder_a.folders:
            TestSubvolumesMountSource._check_file(union, "folder/" + path, 0, None)

        for path, contents in sample_tar_a.files.items():
            TestSubvolumesMountSource._check_file(
                union, "tar/" + path, 0 if path not in sample_folder_a.files else 1, contents
            )
        for path, contents in sample_folder_a.files.items():
            TestSubvolumesMountSource._check_file(union, "folder/" + path, 0, contents)

        # Create files inside the original folder
        contents = b"atarashii iriya\n"
        (sample_folder_a.path / "ufo2").write_bytes(contents)
        os.mkdir(os.path.join(sample_folder_a.path, "subfolder2"))
        (sample_folder_a.path / "subfolder2" / "world").write_bytes(contents)
        os.mkdir(os.path.join(sample_folder_a.path, "subfolder3"))
        (sample_folder_a.path / "subfolder3" / "world").write_bytes(contents)

        # Check for created files in the bind mount
        TestSubvolumesMountSource._check_file(union, "/folder/ufo2", 0, contents)
        TestSubvolumesMountSource._check_file(union, "/folder/subfolder2", 0, None)
        TestSubvolumesMountSource._check_file(union, "/folder/subfolder2/world", 0, contents)
        TestSubvolumesMountSource._check_file(union, "/folder/subfolder3/world", 0, contents)

    @staticmethod
    def test_simple_mount(sample_tar_a):
        union = SubvolumesMountSource({})
        union.mount("folderA", SQLiteIndexedTar(sample_tar_a.path))

        # Check folders

        for path in sample_tar_a.folders:
            TestSubvolumesMountSource._check_file(union, "folderA/" + path, 0, None)
        for path, contents in sample_tar_a.files.items():
            TestSubvolumesMountSource._check_file(union, "folderA/" + path, 0, contents)
