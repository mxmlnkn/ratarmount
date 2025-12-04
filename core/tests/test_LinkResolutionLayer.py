import builtins
import dataclasses
import io
import os
import stat
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Optional, Union

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402
from ratarmountcore.mountsource import FileInfo, MountSource  # noqa: E402
from ratarmountcore.mountsource.compositing.link import LinkResolutionUnionMountSource  # noqa: E402
from ratarmountcore.mountsource.formats.folder import FolderMountSource  # noqa: E402


# =============================================================================
# Dataclasses for Test Data Organization
# =============================================================================


@dataclasses.dataclass
class SampleFolder:
    """Describes a folder structure for testing."""

    path: Path
    folders: list[str]
    files: dict[str, bytes]
    symlinks: dict[str, str] = dataclasses.field(default_factory=dict[str, str])


# =============================================================================
# Utility Functions
# =============================================================================


def _make_file_info(
    path: str,
    size: int = 100,
    mtime: int = 1234567890,
    mode: int = 0o644 | stat.S_IFREG,
    linkname: str = "",
) -> FileInfo:
    """Create a FileInfo for a regular file."""
    return FileInfo(
        size=size,
        mtime=mtime,
        mode=mode,
        linkname=linkname,
        uid=0,
        gid=0,
        userdata=[path],
    )


def _make_dir_info(path: str, mtime: int = 0) -> FileInfo:
    """Create a FileInfo for a directory."""
    return FileInfo(
        size=0,
        mtime=mtime,
        mode=0o755 | stat.S_IFDIR,
        linkname="",
        uid=0,
        gid=0,
        userdata=[path],
    )


def _make_symlink_info(path: str, target: str, mtime: int = 1234567890) -> FileInfo:
    """Create a FileInfo for a symbolic link."""
    return FileInfo(
        size=0,
        mtime=mtime,
        mode=0o777 | stat.S_IFLNK,
        linkname=target,
        uid=0,
        gid=0,
        userdata=[path],
    )


def _populate_folder(sample: SampleFolder):
    """Populate a folder with the specified structure."""
    os.makedirs(sample.path, exist_ok=True)
    for folder in sample.folders:
        os.makedirs(sample.path / folder.strip('/'), exist_ok=True)
    for path, contents in sample.files.items():
        file_path = sample.path / path.strip('/')
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(contents)
    for link_path, target in sample.symlinks.items():
        symlink_path = sample.path / link_path.strip('/')
        symlink_path.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(target, symlink_path)


def _should_resolve_symlinks(linkname: str, file_type: int) -> bool:
    """Default symlink resolution policy: resolve all symlinks."""
    return file_type == stat.S_IFLNK


def _should_not_resolve(linkname: str, file_type: int) -> bool:
    """Policy that resolves no links."""
    return False


def _should_resolve_all_links(linkname: str, file_type: int) -> bool:
    """Policy that resolves all links (symlinks and hardlinks)."""
    return linkname != ""


def _get_listing_names(listing) -> set[str]:
    """Extract names from a directory listing."""
    if listing is None:
        return set()
    return set(listing.keys()) if isinstance(listing, dict) else set(listing)


# =============================================================================
# Mock Classes
# =============================================================================


class MockFile(io.BytesIO):
    """Mock file that supports context manager protocol."""

    def __init__(self, content: bytes = b"mock file content"):
        super().__init__(content)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockMountSource(MountSource):
    """Mock mount source for testing LinkResolutionUnionMountSource."""

    def __init__(self, files: dict[str, FileInfo]):
        self.files = files
        self._immutable = True

    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.files.get(path)

    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        children = {}
        if path == "/":
            for file_path, file_info in self.files.items():
                if file_path != "/" and "/" not in file_path[1:]:
                    name = file_path[1:]
                    children[name] = file_info
        else:
            path_prefix = path.rstrip("/") + "/"
            for file_path, file_info in self.files.items():
                if file_path.startswith(path_prefix) and file_path != path:
                    relative_path = file_path[len(path_prefix) :]
                    if "/" not in relative_path:
                        children[relative_path] = file_info
        return children if children else None

    def versions(self, path: str) -> int:
        return 1 if path in self.files else 0

    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        return MockFile()

    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        mock_data = b"mock data" * (size // 9 + 1)
        return mock_data[:size]

    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        return ["user.test"]

    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        if key == "user.test":
            return b"test_value"
        return None

    def is_immutable(self) -> bool:
        return self._immutable

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass


def _create_mock_source(files_spec: dict[str, tuple]) -> MockMountSource:
    """
    Create a MockMountSource from a simplified specification.

    files_spec format:
        {path: ('file', size)} for files
        {path: ('dir',)} for directories
        {path: ('symlink', target)} for symlinks
        {path: ('hardlink', target)} for hardlinks (regular file with linkname)
    """
    files = {}
    for path, spec in files_spec.items():
        file_type = spec[0]
        if file_type == 'file':
            size = spec[1] if len(spec) > 1 else 100
            files[path] = _make_file_info(path, size=size)
        elif file_type == 'dir':
            files[path] = _make_dir_info(path)
        elif file_type == 'symlink':
            target = spec[1]
            files[path] = _make_symlink_info(path, target)
        elif file_type == 'hardlink':
            target = spec[1]
            size = spec[2] if len(spec) > 2 else 100
            files[path] = _make_file_info(path, size=size, linkname=target)
    return MockMountSource(files)


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture(name="basic_mock_files")
def fixture_basic_mock_files():
    """Basic mock files structure for single source tests."""
    return {
        "/": _make_dir_info("/"),
        "/file1.txt": _make_file_info("/file1.txt", size=100),
        "/file2.txt": _make_file_info("/file2.txt", size=200),
        "/symlink1": _make_symlink_info("/symlink1", "/file1.txt"),
        "/symlink2": _make_symlink_info("/symlink2", "file2.txt"),
        "/hardlink1": _make_file_info("/hardlink1", size=100, linkname="/file1.txt"),
        "/dir1": _make_dir_info("/dir1"),
        "/dir1/file3.txt": _make_file_info("/dir1/file3.txt", size=300),
        "/dir1/symlink3": _make_symlink_info("/dir1/symlink3", "../file2.txt"),
        "/dir1/symlink4": _make_symlink_info("/dir1/symlink4", "file3.txt"),
        "/recursive_symlink": _make_symlink_info("/recursive_symlink", "/recursive_symlink"),
    }


@pytest.fixture(name="branch1_folder")
def fixture_branch1_folder(tmp_path):
    """Branch1 folder structure for issue #164 tests."""
    sample = SampleFolder(
        path=tmp_path / "branch1",
        folders=["subdir1/subdir2"],
        files={"/subdir1/subdir2/file1": b"content1"},
        symlinks={"/subdir0": "./subdir1"},
    )
    _populate_folder(sample)
    return sample


@pytest.fixture(name="branch2_folder")
def fixture_branch2_folder(tmp_path):
    """Branch2 folder structure for issue #164 tests."""
    sample = SampleFolder(
        path=tmp_path / "branch2",
        folders=["subdir0/subdir2", "subdir1/subdir2"],
        files={
            "/subdir0/subdir2/file2": b"content2",
            "/subdir1/subdir2/file3": b"content3",
        },
    )
    _populate_folder(sample)
    return sample


@pytest.fixture(name="infinite_depth_mock")
def fixture_infinite_depth_mock():
    """Mock source with symlink creating infinite depth."""
    return _create_mock_source({
        "/": ('dir',),
        "/dir": ('dir',),
        "/dir/file.txt": ('file', 100),
        "/dir/subdir": ('symlink', "/dir"),
    })


@pytest.fixture(name="mutual_symlinks_mock")
def fixture_mutual_symlinks_mock():
    """Mock source with mutual symlinks creating cycles."""
    return _create_mock_source({
        "/": ('dir',),
        "/a": ('dir',),
        "/a/file.txt": ('file', 100),
        "/a/to_b": ('symlink', "/b"),
        "/b": ('dir',),
        "/b/file.txt": ('file', 200),
        "/b/to_a": ('symlink', "/a"),
    })


# =============================================================================
# Test Classes
# =============================================================================


class TestLinkResolutionBasic:
    """Basic test cases for LinkResolutionUnionMountSource."""

    @staticmethod
    def test_no_link_resolution(basic_mock_files):
        """Test LinkResolutionUnionMountSource with no link resolution."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_not_resolve
        )

        file_info = layer.lookup("/file1.txt")
        assert file_info is not None
        assert file_info.size == 100

        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.linkname == "/file1.txt"

        listing = layer.list("/")
        assert listing is not None
        assert "symlink1" in _get_listing_names(listing)

    @staticmethod
    def test_absolute_symlink_resolution(basic_mock_files):
        """Test resolving absolute symbolic links."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.size == 100

        assert layer.versions("/symlink1") == 1

    @staticmethod
    def test_relative_symlink_resolution(basic_mock_files):
        """Test resolving relative symbolic links."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        # Relative symlink in root
        symlink_info = layer.lookup("/symlink2")
        assert symlink_info is not None
        assert symlink_info.size == 200

        # Relative symlink with parent navigation
        symlink_info = layer.lookup("/dir1/symlink3")
        assert symlink_info is not None
        assert symlink_info.size == 200

        # Relative symlink within same directory
        symlink_info = layer.lookup("/dir1/symlink4")
        assert symlink_info is not None
        assert symlink_info.size == 300

    @staticmethod
    def test_hardlink_resolution(basic_mock_files):
        """Test resolving hard links."""
        mock_source = MockMountSource(basic_mock_files)

        def should_resolve_hardlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFREG and linkname != ""

        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=should_resolve_hardlinks
        )

        hardlink_info = layer.lookup("/hardlink1")
        assert hardlink_info is not None
        assert hardlink_info.size == 100

    @staticmethod
    def test_mixed_link_resolution(basic_mock_files):
        """Test resolving both symlinks and hardlinks."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_all_links
        )

        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.size == 100

        hardlink_info = layer.lookup("/hardlink1")
        assert hardlink_info is not None
        assert hardlink_info.size == 100

    @staticmethod
    def test_directory_listing(basic_mock_files):
        """Test directory listing with link resolution."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        listing = layer.list("/")
        assert listing is not None

        dir_listing = layer.list("/dir1")
        assert dir_listing is not None

    @staticmethod
    def test_directory_listing_with_modes(basic_mock_files):
        """Test directory listing with file modes."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        listing = layer.list_mode("/")
        assert listing is not None
        names = _get_listing_names(listing)
        assert "file1.txt" in names
        assert "dir1" in names

    @staticmethod
    def test_file_operations(basic_mock_files):
        """Test file operations on resolved links."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None

        file_handle = layer.open(symlink_info)
        assert file_handle is not None

        data = layer.read(symlink_info, 100, 0)
        assert data is not None

    @staticmethod
    def test_exists_and_is_dir(basic_mock_files):
        """Test exists() and is_dir() methods."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        assert layer.exists("/file1.txt")
        assert layer.exists("/symlink1")
        assert not layer.exists("/nonexistent")

        assert layer.is_dir("/dir1")
        assert not layer.is_dir("/file1.txt")
        assert not layer.is_dir("/symlink1")

    @staticmethod
    def test_extended_attributes(basic_mock_files):
        """Test extended attribute operations."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None

        xattr_list = layer.list_xattr(symlink_info)
        assert xattr_list == ["user.test"]

        xattr_value = layer.get_xattr(symlink_info, "user.test")
        assert xattr_value == b"test_value"

        assert layer.get_xattr(symlink_info, "user.nonexistent") is None

    @staticmethod
    def test_mount_source_delegation(basic_mock_files):
        """Test delegation to underlying mount source."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        assert layer.is_immutable() == mock_source.is_immutable()

        statfs_info = layer.statfs()
        assert isinstance(statfs_info, dict)

        file_info = layer.lookup("/file1.txt")
        assert file_info is not None
        mount_point, mount_source, source_file_info = layer.get_mount_source(file_info)
        assert isinstance(mount_point, str)
        assert isinstance(mount_source, MountSource)
        assert isinstance(source_file_info, FileInfo)

    @staticmethod
    def test_context_manager(basic_mock_files):
        """Test context manager interface."""
        mock_source = MockMountSource(basic_mock_files)

        with LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        ) as layer:
            file_info = layer.lookup("/file1.txt")
            assert file_info is not None

    @staticmethod
    def test_circular_symlink_handling(basic_mock_files):
        """Test handling of circular symbolic links."""
        mock_source = MockMountSource(basic_mock_files)
        layer = LinkResolutionUnionMountSource(
            mountSources=[mock_source], shouldResolveLink=_should_resolve_symlinks
        )

        # Should not crash due to infinite recursion
        try:
            layer.lookup("/recursive_symlink")
        except RecursionError:
            pytest.fail("Should not get RecursionError for circular symlinks")


class TestLinkResolutionMultiMount:
    """Test cases for LinkResolutionUnionMountSource with multiple mount sources."""

    @staticmethod
    def test_union_with_two_mount_sources_basic():
        """Test basic union functionality with two mount sources."""
        mount_source_a = _create_mock_source({
            "/": ('dir',),
            "/file_a.txt": ('file', 100),
            "/dir_a": ('dir',),
        })
        mount_source_b = _create_mock_source({
            "/": ('dir',),
            "/file_b.txt": ('file', 200),
            "/dir_b": ('dir',),
        })

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b], shouldResolveLink=_should_not_resolve
        )

        assert layer.exists("/file_a.txt")
        assert layer.exists("/file_b.txt")

        file_a = layer.lookup("/file_a.txt")
        assert file_a is not None
        assert file_a.size == 100

        file_b = layer.lookup("/file_b.txt")
        assert file_b is not None
        assert file_b.size == 200

        names = _get_listing_names(layer.list("/"))
        assert {"file_a.txt", "file_b.txt", "dir_a", "dir_b"}.issubset(names)

    @staticmethod
    def test_cross_mount_absolute_symlink_resolution():
        """Test resolving absolute symlink across mount sources."""
        mount_source_a = _create_mock_source({
            "/": ('dir',),
            "/target.txt": ('file', 100),
        })
        mount_source_b = _create_mock_source({
            "/": ('dir',),
            "/link": ('symlink', "/target.txt"),
        })

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b], shouldResolveLink=_should_resolve_symlinks
        )

        link_info = layer.lookup("/link")
        assert link_info is not None
        assert link_info.size == 100

        file_handle = layer.open(link_info)
        assert file_handle is not None

        data = layer.read(link_info, 100, 0)
        assert data is not None
        assert len(data) == 100

    @staticmethod
    def test_file_operations_preserve_mount_source():
        """Test that file operations preserve mount source userdata."""
        mount_source_a = _create_mock_source({
            "/": ('dir',),
            "/file_a.txt": ('file', 100),
        })
        mount_source_b = _create_mock_source({
            "/": ('dir',),
            "/link": ('symlink', "/file_a.txt"),
        })

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b], shouldResolveLink=_should_resolve_symlinks
        )

        link_info = layer.lookup("/link")
        assert link_info is not None

        layer.open(link_info)
        assert link_info.userdata is not None

        layer.read(link_info, 100, 0)
        assert link_info.userdata is not None

        layer.get_xattr(link_info, "user.test")
        assert link_info.userdata is not None

        layer.list_xattr(link_info)
        assert link_info.userdata is not None

    @staticmethod
    def test_overlapping_files_precedence():
        """Test file versioning with overlapping files from multiple mount sources."""
        mount_source_a = _create_mock_source({
            "/": ('dir',),
            "/file.txt": ('file', 100),
        })
        mount_source_b = _create_mock_source({
            "/": ('dir',),
            "/file.txt": ('file', 200),
        })

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b], shouldResolveLink=_should_not_resolve
        )

        assert layer.versions("/file.txt") == 2

        # Version 0 from rightmost mount (B)
        file_v0 = layer.lookup("/file.txt", fileVersion=0)
        assert file_v0 is not None
        assert file_v0.size == 200

        # Version 1 from mount A
        file_v1 = layer.lookup("/file.txt", fileVersion=1)
        assert file_v1 is not None
        assert file_v1.size == 100


class TestInfiniteDepthSymlinks:
    """Test cases for infinite depth recursive symbolic link directories."""

    @staticmethod
    def test_symlink_to_parent_creates_infinite_depth(infinite_depth_mock):
        """Test directory symlink pointing to parent creates infinite depth."""
        layer = LinkResolutionUnionMountSource(
            mountSources=[infinite_depth_mock], shouldResolveLink=_should_resolve_symlinks
        )

        dir_info = layer.lookup("/dir")
        assert dir_info is not None
        assert stat.S_ISDIR(dir_info.mode)

        file_info = layer.lookup("/dir/file.txt")
        assert file_info is not None
        assert file_info.size == 100

        subdir_info = layer.lookup("/dir/subdir")
        assert subdir_info is not None
        assert stat.S_ISDIR(subdir_info.mode)

        try:
            # Through one level
            file_via_subdir = layer.lookup("/dir/subdir/file.txt")
            assert file_via_subdir is not None
            assert file_via_subdir.size == 100

            # Through two levels
            file_via_subdir2 = layer.lookup("/dir/subdir/subdir/file.txt")
            assert file_via_subdir2 is not None
            assert file_via_subdir2.size == 100

            # Deep traversal
            deep_path = "/dir" + "/subdir" * 10 + "/file.txt"
            file_deep = layer.lookup(deep_path)
            assert file_deep is not None
            assert file_deep.size == 100
        except RecursionError:
            pytest.fail("Should not get RecursionError for infinite depth symlinks")

    @staticmethod
    def test_mutual_directory_symlinks(mutual_symlinks_mock):
        """Test mutual directory symlinks that create cycles."""
        layer = LinkResolutionUnionMountSource(
            mountSources=[mutual_symlinks_mock], shouldResolveLink=_should_resolve_symlinks
        )

        a_info = layer.lookup("/a")
        assert a_info is not None
        assert stat.S_ISDIR(a_info.mode)

        b_info = layer.lookup("/b")
        assert b_info is not None
        assert stat.S_ISDIR(b_info.mode)

        try:
            b_via_a = layer.lookup("/a/to_b")
            assert b_via_a is not None
            assert stat.S_ISDIR(b_via_a.mode)

            b_file_via_a = layer.lookup("/a/to_b/file.txt")
            assert b_file_via_a is not None
            assert b_file_via_a.size == 200

            a_via_b = layer.lookup("/b/to_a")
            assert a_via_b is not None
            assert stat.S_ISDIR(a_via_b.mode)

            a_file_via_b = layer.lookup("/b/to_a/file.txt")
            assert a_file_via_b is not None
            assert a_file_via_b.size == 100

            # Cycle traversal
            cycle_path = layer.lookup("/a/to_b/to_a")
            assert cycle_path is not None
            assert stat.S_ISDIR(cycle_path.mode)

            file_through_cycle = layer.lookup("/a/to_b/to_a/file.txt")
            assert file_through_cycle is not None
            assert file_through_cycle.size == 100

            deep_cycle = layer.lookup("/a/to_b/to_a/to_b/to_a/to_b/file.txt")
            assert deep_cycle is not None
            assert deep_cycle.size == 200
        except RecursionError:
            pytest.fail("Should not get RecursionError for mutual symlinks")

    @staticmethod
    def test_directory_listing_with_infinite_depth_symlink(infinite_depth_mock):
        """Test listing a directory accessed through infinite depth symlinks."""
        layer = LinkResolutionUnionMountSource(
            mountSources=[infinite_depth_mock], shouldResolveLink=_should_resolve_symlinks
        )

        try:
            root_names = _get_listing_names(layer.list("/"))
            assert "dir" in root_names

            dir_names = _get_listing_names(layer.list("/dir"))
            assert "file.txt" in dir_names
            assert "subdir" in dir_names

            subdir_names = _get_listing_names(layer.list("/dir/subdir"))
            assert "file.txt" in subdir_names
            assert "subdir" in subdir_names

            deep_names = _get_listing_names(layer.list("/dir/subdir/subdir/subdir"))
            assert "file.txt" in deep_names
            assert "subdir" in deep_names
        except RecursionError:
            pytest.fail("Should not get RecursionError when listing infinite depth directories")


class TestLinkResolutionIntegration:
    """
    Integration tests for LinkResolutionUnionMountSource with real folder mount sources.

    These tests verify the "late binding" (mixin-like) behavior where symlinks resolve
    within the merged view rather than their original source context. This is the expected
    behavior described in GitHub issue #164.
    """

    @staticmethod
    def _check_file(mount_source, path, expected_content: Optional[bytes] = None):
        """Verify file exists and optionally check content."""
        file_info = mount_source.lookup(path)
        assert file_info is not None, f"Path not found: {path}"
        if expected_content is not None:
            with mount_source.open(file_info) as f:
                assert f.read() == expected_content, f"Content mismatch at {path}"
        return file_info

    @staticmethod
    def test_issue_164_symlink_directory_merge_order1(branch1_folder, branch2_folder):
        """
        Test issue #164: symlink + directory merge consistency (branch1, branch2 order).

        Structure:
            branch1/subdir0     -> ./subdir1 (symlink)
            branch1/subdir1/subdir2/file1
            branch2/subdir0/subdir2/file2 (real directory)
            branch2/subdir1/subdir2/file3

        Expected with late binding:
            /subdir0 should resolve the symlink to ./subdir1 in the merged view.
        """
        with FolderMountSource(branch1_folder.path) as source1, FolderMountSource(branch2_folder.path) as source2:
            layer = LinkResolutionUnionMountSource(
                mountSources=[source1, source2], shouldResolveLink=_should_resolve_symlinks
            )

            subdir0_info = layer.lookup("/subdir0")
            assert subdir0_info is not None, "subdir0 should exist"
            assert stat.S_ISDIR(subdir0_info.mode), "subdir0 should resolve to directory"

            names = _get_listing_names(layer.list("/subdir0/subdir2"))
            assert "file1" in names, "file1 should be accessible via symlink (from branch1/subdir1)"
            assert "file3" in names, "file3 should be accessible via symlink (from branch2/subdir1)"

    @staticmethod
    def test_issue_164_symlink_directory_merge_order2(branch1_folder, branch2_folder):
        """
        Test issue #164: symlink + directory merge consistency (branch2, branch1 order - reverse).
        """
        with FolderMountSource(branch2_folder.path) as source2, FolderMountSource(branch1_folder.path) as source1:
            layer = LinkResolutionUnionMountSource(
                mountSources=[source2, source1], shouldResolveLink=_should_resolve_symlinks
            )

            subdir0_info = layer.lookup("/subdir0")
            assert subdir0_info is not None, "subdir0 should exist"
            assert stat.S_ISDIR(subdir0_info.mode), "subdir0 should resolve to directory"

            names = _get_listing_names(layer.list("/subdir0/subdir2"))
            assert "file1" in names, "file1 should be accessible (from branch1/subdir1 via symlink)"
            assert "file3" in names, "file3 should be accessible (from branch2/subdir1 via symlink)"
            assert "file2" in names, "file2 should be accessible (from branch2/subdir0 directly)"

    @staticmethod
    def test_late_binding_cross_mount_symlink_resolution(tmp_path):
        """
        Test that symlinks resolve to targets that exist only in other mount sources.
        """
        source_a = SampleFolder(
            path=tmp_path / "source_a",
            folders=[],
            files={},
            symlinks={"/link_to_target": "/target_dir"},
        )
        _populate_folder(source_a)

        source_b = SampleFolder(
            path=tmp_path / "source_b",
            folders=["target_dir"],
            files={"/target_dir/file.txt": b"target content"},
        )
        _populate_folder(source_b)

        with FolderMountSource(source_a.path) as src_a, FolderMountSource(source_b.path) as src_b:
            layer = LinkResolutionUnionMountSource(
                mountSources=[src_a, src_b], shouldResolveLink=_should_resolve_symlinks
            )

            link_info = layer.lookup("/link_to_target")
            assert link_info is not None, "link_to_target should exist"
            assert stat.S_ISDIR(link_info.mode), "link_to_target should resolve to directory"

            file_info = layer.lookup("/link_to_target/file.txt")
            assert file_info is not None, "file.txt should be accessible through symlink"

            with layer.open(file_info) as f:
                assert f.read() == b"target content", "File content should match"

    @staticmethod
    def test_relative_symlink_parent_navigation_in_merged_view(tmp_path):
        """
        Test relative symlinks with parent navigation (..) resolve correctly in merged view.

        Structure:
            source_a/dir1/link  -> ../dir2/file.txt
            source_b/dir2/file.txt
        """
        source_a = SampleFolder(
            path=tmp_path / "source_a",
            folders=["dir1"],
            files={},
            symlinks={"/dir1/link": "../dir2/file.txt"},
        )
        _populate_folder(source_a)

        source_b = SampleFolder(
            path=tmp_path / "source_b",
            folders=["dir2"],
            files={"/dir2/file.txt": b"cross mount content"},
        )
        _populate_folder(source_b)

        with FolderMountSource(source_a.path) as src_a, FolderMountSource(source_b.path) as src_b:
            layer = LinkResolutionUnionMountSource(
                mountSources=[src_a, src_b], shouldResolveLink=_should_resolve_symlinks
            )

            link_info = layer.lookup("/dir1/link")
            assert link_info is not None, "link should exist"
            assert stat.S_ISREG(link_info.mode), "link should resolve to regular file"

            with layer.open(link_info) as f:
                assert f.read() == b"cross mount content", "File content should match"

    @staticmethod
    def test_symlink_to_directory_merges_contents(tmp_path):
        """
        Test that a symlink to a directory sees merged contents from all mount sources.

        Structure:
            source_a/real_dir/file_a.txt
            source_a/link_dir -> real_dir
            source_b/real_dir/file_b.txt
        """
        source_a = SampleFolder(
            path=tmp_path / "source_a",
            folders=["real_dir"],
            files={"/real_dir/file_a.txt": b"content_a"},
            symlinks={"/link_dir": "real_dir"},
        )
        _populate_folder(source_a)

        source_b = SampleFolder(
            path=tmp_path / "source_b",
            folders=["real_dir"],
            files={"/real_dir/file_b.txt": b"content_b"},
        )
        _populate_folder(source_b)

        with FolderMountSource(source_a.path) as src_a, FolderMountSource(source_b.path) as src_b:
            layer = LinkResolutionUnionMountSource(
                mountSources=[src_a, src_b], shouldResolveLink=_should_resolve_symlinks
            )

            link_dir_info = layer.lookup("/link_dir")
            assert link_dir_info is not None, "link_dir should exist"
            assert stat.S_ISDIR(link_dir_info.mode), "link_dir should resolve to directory"

            names = _get_listing_names(layer.list("/link_dir"))
            assert "file_a.txt" in names, "file_a.txt should be visible through symlink"
            assert "file_b.txt" in names, "file_b.txt should be visible (from merged real_dir)"

    @staticmethod
    def test_deep_nested_symlink_resolution(tmp_path):
        """
        Test symlink resolution through multiple levels of directories.

        Structure:
            source_a/a/b/c/link -> ../../../target/file.txt
            source_b/target/file.txt
        """
        source_a = SampleFolder(
            path=tmp_path / "source_a",
            folders=["a/b/c"],
            files={},
            symlinks={"/a/b/c/link": "../../../target/file.txt"},
        )
        _populate_folder(source_a)

        source_b = SampleFolder(
            path=tmp_path / "source_b",
            folders=["target"],
            files={"/target/file.txt": b"deep content"},
        )
        _populate_folder(source_b)

        with FolderMountSource(source_a.path) as src_a, FolderMountSource(source_b.path) as src_b:
            layer = LinkResolutionUnionMountSource(
                mountSources=[src_a, src_b], shouldResolveLink=_should_resolve_symlinks
            )

            link_info = layer.lookup("/a/b/c/link")
            assert link_info is not None, "link should exist"
            assert stat.S_ISREG(link_info.mode), "link should resolve to regular file"

            with layer.open(link_info) as f:
                assert f.read() == b"deep content", "File content should match"

    @staticmethod
    def test_unite_two_folders_with_symlinks(tmp_path):
        """Test union of two folders where one has symlinks resolved in merged view."""
        folder_a = SampleFolder(
            path=tmp_path / "folder_a",
            folders=["data"],
            files={"/data/file_a.txt": b"data from A"},
            symlinks={"/link_to_data": "data"},
        )
        _populate_folder(folder_a)

        folder_b = SampleFolder(
            path=tmp_path / "folder_b",
            folders=["data"],
            files={"/data/file_b.txt": b"data from B"},
        )
        _populate_folder(folder_b)

        with FolderMountSource(folder_a.path) as src_a, FolderMountSource(folder_b.path) as src_b:
            layer = LinkResolutionUnionMountSource(
                mountSources=[src_a, src_b], shouldResolveLink=_should_resolve_symlinks
            )

            # Direct access to data directory
            data_names = _get_listing_names(layer.list("/data"))
            assert "file_a.txt" in data_names
            assert "file_b.txt" in data_names

            # Access through symlink should see same merged content
            link_names = _get_listing_names(layer.list("/link_to_data"))
            assert "file_a.txt" in link_names
            assert "file_b.txt" in link_names

            # Verify file content through symlink
            file_info = layer.lookup("/link_to_data/file_b.txt")
            assert file_info is not None
            with layer.open(file_info) as f:
                assert f.read() == b"data from B"
