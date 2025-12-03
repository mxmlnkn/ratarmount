import io
import stat
import unittest
from typing import IO, Dict, Iterable, List, Optional, Union

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.compositing.link import LinkResolutionUnionMountSource


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

    def __init__(self, files: Dict[str, FileInfo]):
        self.files = files
        self._immutable = True

    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.files.get(path)

    def list(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        # Return children of the given path
        children = {}
        if path == "/":
            # Root directory - find all top-level items
            for file_path, file_info in self.files.items():
                if file_path != "/" and "/" not in file_path[1:]:  # Top-level items
                    name = file_path[1:]  # Remove leading slash
                    children[name] = file_info
        else:
            path_prefix = path.rstrip("/") + "/"
            for file_path, file_info in self.files.items():
                if file_path.startswith(path_prefix) and file_path != path:
                    relative_path = file_path[len(path_prefix) :]
                    if "/" not in relative_path:  # Direct child
                        children[relative_path] = file_info

        return children if children else None

    def versions(self, path: str) -> int:
        return 1 if path in self.files else 0

    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        # Return a mock file object with context manager support
        return MockFile()

    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        # Return mock data
        mock_data = b"mock data" * (size // 9 + 1)
        return mock_data[:size]

    def list_xattr(self, fileInfo: FileInfo) -> List[str]:
        return ["user.test"]

    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        if key == "user.test":
            return b"test_value"
        return None

    def is_immutable(self) -> bool:
        return self._immutable

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass


class TestLinkResolutionUnionMountSource(unittest.TestCase):
    """Test cases for LinkResolutionUnionMountSource."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_files = {
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file1.txt": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file1.txt"],
            ),
            "/file2.txt": FileInfo(
                size=200,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file2.txt"],
            ),
            "/symlink1": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="/file1.txt",
                uid=0,
                gid=0,
                userdata=["/symlink1"],
            ),
            "/symlink2": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="file2.txt",
                uid=0,
                gid=0,
                userdata=["/symlink2"],
            ),
            "/hardlink1": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="/file1.txt",
                uid=0,
                gid=0,
                userdata=["/hardlink1"],
            ),
            "/dir1": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/dir1"],
            ),
            "/dir1/file3.txt": FileInfo(
                size=300,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/dir1/file3.txt"],
            ),
            "/dir1/symlink3": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="../file2.txt",
                uid=0,
                gid=0,
                userdata=["/dir1/symlink3"],
            ),
            "/dir1/symlink4": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="file3.txt",
                uid=0,
                gid=0,
                userdata=["/dir1/symlink4"],
            ),
            "/recursive_symlink": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="/recursive_symlink",
                uid=0,
                gid=0,
                userdata=["/recursive_symlink"],
            ),
        }

    def test_no_link_resolution(self):
        """Test LinkResolutionUnionMountSource with no link resolution."""
        mock_source = MockMountSource(self.test_files)

        def should_not_resolve_link(linkname: str, file_type: int) -> bool:
            return False

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_not_resolve_link)

        # Regular file should work normally
        file_info = layer.lookup("/file1.txt")
        assert file_info is not None
        assert file_info.size == 100

        # Symlink should not be resolved
        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.linkname == "/file1.txt"

        # Directory listing should include the symlink as-is
        listing = layer.list("/")
        assert listing is not None
        assert isinstance(listing, (set, dict))
        if isinstance(listing, dict):
            assert "symlink1" in listing
        else:
            assert "symlink1" in listing

    def test_absolute_symlink_resolution(self):
        """Test resolving absolute symbolic links."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Absolute symlink should be resolved
        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.size == 100  # Should have size of target file

        # Versions should work
        versions = layer.versions("/symlink1")
        assert versions == 1

    def test_relative_symlink_resolution(self):
        """Test resolving relative symbolic links."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Relative symlink should be resolved
        symlink_info = layer.lookup("/symlink2")
        assert symlink_info is not None
        assert symlink_info.size == 200  # Should have size of target file

        # Relative symlink with parent directory navigation
        symlink_info = layer.lookup("/dir1/symlink3")
        assert symlink_info is not None
        assert symlink_info.size == 200  # Should resolve to /file2.txt

        # Relative symlink within same directory
        symlink_info = layer.lookup("/dir1/symlink4")
        assert symlink_info is not None
        assert symlink_info.size == 300  # Should resolve to /dir1/file3.txt

    def test_hardlink_resolution(self):
        """Test resolving hard links."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_hardlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFREG and linkname != ""

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_hardlinks)

        # Hardlink should be resolved
        hardlink_info = layer.lookup("/hardlink1")
        assert hardlink_info is not None
        assert hardlink_info.size == 100  # Should have size of target file

    def test_mixed_link_resolution(self):
        """Test resolving both symlinks and hardlinks."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_all_links(linkname: str, file_type: int) -> bool:
            return linkname != ""

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_all_links)

        # Both symlinks and hardlinks should be resolved
        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None
        assert symlink_info.size == 100

        hardlink_info = layer.lookup("/hardlink1")
        assert hardlink_info is not None
        assert hardlink_info.size == 100

    def test_directory_listing(self):
        """Test directory listing with link resolution."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Root directory listing
        listing = layer.list("/")
        print(f"DEBUG: listing = {listing}")
        print(f"DEBUG: mock source list for '/': {mock_source.list('/')}")
        assert listing is not None

        # Directory with symlinks
        dir_listing = layer.list("/dir1")
        assert dir_listing is not None

    def test_directory_listing_with_modes(self):
        """Test directory listing with file modes."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Root directory listing with modes
        listing = layer.list_mode("/")
        assert listing is not None
        if isinstance(listing, dict):
            assert "file1.txt" in listing
            assert "dir1" in listing

    def test_file_operations(self):
        """Test file operations on resolved links."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Get file info for a resolved symlink
        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None

        # Test opening the file
        file_handle = layer.open(symlink_info)
        assert file_handle is not None

        # Test reading from the file
        data = layer.read(symlink_info, 100, 0)
        assert data is not None

    def test_exists_and_is_dir(self):
        """Test exists() and is_dir() methods."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Test exists
        assert layer.exists("/file1.txt")
        assert layer.exists("/symlink1")
        assert not layer.exists("/nonexistent")

        # Test is_dir
        assert layer.is_dir("/dir1")
        assert not layer.is_dir("/file1.txt")
        assert not layer.is_dir("/symlink1")  # Resolved to file

    def test_extended_attributes(self):
        """Test extended attribute operations."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Get file info for a resolved symlink
        symlink_info = layer.lookup("/symlink1")
        assert symlink_info is not None

        # Test extended attributes
        xattr_list = layer.list_xattr(symlink_info)
        assert xattr_list == ["user.test"]

        xattr_value = layer.get_xattr(symlink_info, "user.test")
        assert xattr_value == b"test_value"

        # Should return None for non-existent attribute
        xattr_value_nonexistent = layer.get_xattr(symlink_info, "user.nonexistent")
        assert xattr_value_nonexistent is None

    def test_mount_source_delegation(self):
        """Test delegation to underlying mount source."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # Test immutability delegation
        assert layer.is_immutable() == mock_source.is_immutable()

        # Test statfs delegation
        statfs_info = layer.statfs()
        assert isinstance(statfs_info, dict)

        # Test get_mount_source
        file_info = layer.lookup("/file1.txt")
        assert file_info is not None
        mount_point, mount_source, source_file_info = layer.get_mount_source(file_info)
        assert isinstance(mount_point, str)
        assert isinstance(mount_source, MountSource)
        assert isinstance(source_file_info, FileInfo)

    def test_context_manager(self):
        """Test context manager interface."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        with LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks) as layer:
            # Should work normally within context
            file_info = layer.lookup("/file1.txt")
            assert file_info is not None

    def test_circular_symlink_handling(self):
        """Test handling of circular symbolic links."""
        mock_source = MockMountSource(self.test_files)

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(mountSources=[mock_source], shouldResolveLink=should_resolve_symlinks)

        # This should not crash due to infinite recursion
        # The implementation should handle circular references gracefully
        try:
            layer.lookup("/recursive_symlink")
            # May be None or may have some info depending on implementation
            # The important thing is that it doesn't crash
        except RecursionError:
            self.fail("Should not get RecursionError for circular symlinks")


class TestLinkResolutionUnionMountSourceMultiMount(unittest.TestCase):
    """Test cases for LinkResolutionUnionMountSource with multiple mount sources."""

    def test_union_with_two_mount_sources_basic(self):
        """Test basic union functionality with two mount sources."""
        # Create two mock mount sources with different files
        mount_source_a = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file_a.txt": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file_a.txt"],
            ),
            "/dir_a": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/dir_a"],
            ),
        })

        mount_source_b = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file_b.txt": FileInfo(
                size=200,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file_b.txt"],
            ),
            "/dir_b": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/dir_b"],
            ),
        })

        def should_not_resolve(linkname: str, file_type: int) -> bool:
            return False

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b],
            shouldResolveLink=should_not_resolve
        )

        # Files from both sources should be accessible
        assert layer.exists("/file_a.txt")
        assert layer.exists("/file_b.txt")

        file_a = layer.lookup("/file_a.txt")
        assert file_a is not None
        assert file_a.size == 100

        file_b = layer.lookup("/file_b.txt")
        assert file_b is not None
        assert file_b.size == 200

        # Directory listing should merge files from both sources
        listing = layer.list("/")
        assert listing is not None
        if isinstance(listing, dict):
            names = set(listing.keys())
        else:
            names = set(listing)

        assert "file_a.txt" in names
        assert "file_b.txt" in names
        assert "dir_a" in names
        assert "dir_b" in names

    def test_cross_mount_absolute_symlink_resolution(self):
        """Test resolving absolute symlink across mount sources (CRITICAL)."""
        # Mount A has the target file
        mount_source_a = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/target.txt": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/target.txt"],
            ),
        })

        # Mount B has a symlink pointing to the target in mount A
        mount_source_b = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/link": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="/target.txt",
                uid=0,
                gid=0,
                userdata=["/link"],
            ),
        })

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b],
            shouldResolveLink=should_resolve_symlinks
        )

        # Symlink should resolve to target file from different mount source
        link_info = layer.lookup("/link")
        assert link_info is not None
        assert link_info.size == 100  # Resolved to target

        # File operations should work through resolved symlink
        file_handle = layer.open(link_info)
        assert file_handle is not None

        data = layer.read(link_info, 100, 0)
        assert data is not None
        assert len(data) == 100

        # Verify userdata tracking - should have mount source info
        assert link_info.userdata is not None
        assert len(link_info.userdata) > 0

    def test_file_operations_preserve_mount_source(self):
        """Test that file operations preserve mount source userdata (CRITICAL)."""
        # Mount A has a file with xattrs
        mount_source_a = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file_a.txt": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file_a.txt"],
            ),
        })

        # Mount B has a symlink to the file in mount A
        mount_source_b = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/link": FileInfo(
                size=0,
                mtime=1234567890,
                mode=0o777 | stat.S_IFLNK,
                linkname="/file_a.txt",
                uid=0,
                gid=0,
                userdata=["/link"],
            ),
        })

        def should_resolve_symlinks(linkname: str, file_type: int) -> bool:
            return file_type == stat.S_IFLNK

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b],
            shouldResolveLink=should_resolve_symlinks
        )

        # Get file info through symlink
        link_info = layer.lookup("/link")
        assert link_info is not None

        # Test open() operation
        file_handle = layer.open(link_info)
        assert file_handle is not None
        # Userdata should still be present after open
        assert link_info.userdata is not None

        # Test read() operation
        data = layer.read(link_info, 100, 0)
        assert data is not None
        # Userdata should still be present after read
        assert link_info.userdata is not None

        # Test get_xattr() operation
        xattr_value = layer.get_xattr(link_info, "user.test")
        assert xattr_value == b"test_value"
        # Userdata should still be present after get_xattr
        assert link_info.userdata is not None

        # Test list_xattr() operation
        xattr_list = layer.list_xattr(link_info)
        assert xattr_list == ["user.test"]
        # Userdata should still be present after list_xattr
        assert link_info.userdata is not None

    def test_overlapping_files_precedence(self):
        """Test file versioning with overlapping files from multiple mount sources."""
        # Mount A has file.txt with content "from A"
        mount_source_a = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file.txt": FileInfo(
                size=100,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file.txt", "from A"],
            ),
        })

        # Mount B has file.txt with content "from B"
        mount_source_b = MockMountSource({
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
            "/file.txt": FileInfo(
                size=200,
                mtime=1234567890,
                mode=0o644 | stat.S_IFREG,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/file.txt", "from B"],
            ),
        })

        def should_not_resolve(linkname: str, file_type: int) -> bool:
            return False

        layer = LinkResolutionUnionMountSource(
            mountSources=[mount_source_a, mount_source_b],
            shouldResolveLink=should_not_resolve
        )

        # Check file versions - should have 2 versions
        versions = layer.versions("/file.txt")
        assert versions == 2

        # LinkResolutionUnionMountSource iterates mount sources in reverse order (right to left),
        # so version 0 is from the rightmost mount (B) and version 1 is from mount A.
        # This is consistent with UnionMountSource which gives rightmost precedence.

        # Version 0 should be from rightmost mount (B)
        file_v0 = layer.lookup("/file.txt", fileVersion=0)
        assert file_v0 is not None
        assert file_v0.size == 200  # From mount B

        # Version 1 should be from mount A
        file_v1 = layer.lookup("/file.txt", fileVersion=1)
        assert file_v1 is not None
        assert file_v1.size == 100  # From mount A


if __name__ == "__main__":
    unittest.main()
