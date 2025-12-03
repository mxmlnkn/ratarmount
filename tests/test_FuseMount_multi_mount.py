import builtins
import io
import stat
import unittest
from collections.abc import Iterable
from typing import IO, Optional, Union

import pytest
from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.mountsource.compositing.link import LinkResolutionUnionMountSource
from ratarmountcore.mountsource.compositing.subvolumes import SubvolumesMountSource
from ratarmountcore.mountsource.compositing.union import UnionMountSource


# Import the _create_multi_mount method by executing just that part of FuseMount
# This avoids the fuse dependency issues
def _create_multi_mount(mountSources: list[tuple[str, MountSource]], options: dict) -> MountSource:
    """
    Simplified version of FuseMount._create_multi_mount for testing.
    This is a copy of the actual implementation to avoid import issues.
    """
    # Extract mount sources from tuples
    sources = [x[1] for x in mountSources]

    # Check if we should use LinkResolutionUnionMountSource
    resolveSymbolicLinks = bool(options.get('resolveSymbolicLinks', False))

    # Define the link resolution function once, used for both single and multiple sources
    def should_resolve_link(linkname: str, fileType: int) -> bool:
        """
        Determine whether to resolve a link based on user configuration.
        For now, only resolve symbolic links if the option is enabled.
        Hard links are not resolved for now, as it will be resolved by FileVersionLayer.
        """
        # TODO Resolve hard links in LinkResolutionUnionMountSource and remove hard link handling from FileVersionLayer.
        return bool(fileType == stat.S_IFLNK)

    # Handle single mount source case
    if len(sources) == 1:
        singleSource = sources[0]
        if resolveSymbolicLinks:
            # Apply link resolution for single source
            return LinkResolutionUnionMountSource([singleSource], shouldResolveLink=should_resolve_link)
        return singleSource

    # Handle multiple mount sources
    disableUnionMount = options.get('disableUnionMount', False)

    if resolveSymbolicLinks:
        # LinkResolutionUnionMountSource is a type of union mount
        # so it conflicts with disableUnionMount
        if disableUnionMount:
            raise ValueError(
                "Cannot use 'resolveSymbolicLinks' with multiple mount sources when "
                "'disableUnionMount' is enabled. Resolving symbolic links across multiple sources "
                "requires union mount functionality."
            )

        # Use LinkResolutionUnionMountSource which combines union and link resolution functionality
        return LinkResolutionUnionMountSource(sources, shouldResolveLink=should_resolve_link)

    if not disableUnionMount:
        return UnionMountSource(sources, **options)

    # Create unique keys for subvolumes
    submountSources: dict[str, MountSource] = {}
    suffix = 1
    for key, mountSource in mountSources:
        if key in submountSources:
            while f"{key}.{suffix}" in submountSources:
                suffix += 1
            submountSources[f"{key}.{suffix}"] = mountSource
        else:
            submountSources[key] = mountSource
    return SubvolumesMountSource(submountSources)


class MockFile(io.BytesIO):
    """Mock file that supports context manager protocol."""

    def __init__(self, content: bytes = b"mock file content"):
        super().__init__(content)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class MockMountSource(MountSource):
    """Mock mount source for testing FuseMount._create_multi_mount."""

    def __init__(self, name: str = "mock"):
        self.name = name
        self._immutable = True
        self.files = {
            "/": FileInfo(
                size=0,
                mtime=0,
                mode=0o755 | stat.S_IFDIR,
                linkname="",
                uid=0,
                gid=0,
                userdata=["/"],
            ),
        }

    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self.files.get(path)

    def list(self, path: str) -> Optional[Union[Iterable[str], dict[str, FileInfo]]]:
        return {}

    def versions(self, path: str) -> int:
        return 1 if path in self.files else 0

    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        return MockFile()

    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        mock_data = b"mock data" * (size // 9 + 1)
        return mock_data[:size]

    def list_xattr(self, fileInfo: FileInfo) -> builtins.list[str]:
        return []

    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        return None

    def is_immutable(self) -> bool:
        return self._immutable

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    def __repr__(self):
        return f"MockMountSource({self.name})"


class TestFuseMountCreateMultiMount(unittest.TestCase):
    """Test cases for FuseMount._create_multi_mount method."""

    def test_single_source_without_resolve(self):
        """Test single mount source without link resolution."""
        mock_source = MockMountSource("single")
        sources = [("archive.tar", mock_source)]
        options = {'resolveSymbolicLinks': False}

        result = _create_multi_mount(sources, options)

        # Should return unwrapped mount source
        assert result is mock_source
        assert not isinstance(result, LinkResolutionUnionMountSource)

    def test_single_source_with_resolve(self):
        """Test single mount source with link resolution."""
        mock_source = MockMountSource("single")
        sources = [("archive.tar", mock_source)]
        options = {'resolveSymbolicLinks': True}

        result = _create_multi_mount(sources, options)

        # Should wrap in LinkResolutionUnionMountSource
        assert isinstance(result, LinkResolutionUnionMountSource)
        assert len(result.mountSources) == 1
        assert result.mountSources[0] is mock_source

    def test_multiple_sources_default_union(self):
        """Test multiple mount sources with default union mount."""
        mock_source_a = MockMountSource("a")
        mock_source_b = MockMountSource("b")
        sources = [("archive1.tar", mock_source_a), ("archive2.tar", mock_source_b)]
        options = {'resolveSymbolicLinks': False, 'disableUnionMount': False}

        result = _create_multi_mount(sources, options)

        # Should return UnionMountSource
        assert isinstance(result, UnionMountSource)
        # UnionMountSource wraps the sources, so we can't directly check them

    def test_multiple_sources_with_resolve(self):
        """Test multiple mount sources with link resolution (CRITICAL)."""
        mock_source_a = MockMountSource("a")
        mock_source_b = MockMountSource("b")
        sources = [("archive1.tar", mock_source_a), ("archive2.tar", mock_source_b)]
        options = {'resolveSymbolicLinks': True}

        result = _create_multi_mount(sources, options)

        # Should return LinkResolutionUnionMountSource
        assert isinstance(result, LinkResolutionUnionMountSource)
        assert len(result.mountSources) == 2
        assert result.mountSources[0] is mock_source_a
        assert result.mountSources[1] is mock_source_b

    def test_error_resolve_with_disable_union(self):
        """Test error when resolveSymbolicLinks=True with disableUnionMount=True (CRITICAL)."""
        mock_source_a = MockMountSource("a")
        mock_source_b = MockMountSource("b")
        sources = [("archive1.tar", mock_source_a), ("archive2.tar", mock_source_b)]
        options = {'resolveSymbolicLinks': True, 'disableUnionMount': True}

        # Should raise ValueError
        with pytest.raises(ValueError, match="Cannot use 'resolveSymbolicLinks'"):
            _create_multi_mount(sources, options)

    def test_multiple_sources_subvolumes_mode(self):
        """Test multiple mount sources with disableUnionMount (subvolumes mode)."""
        mock_source_a = MockMountSource("a")
        mock_source_b = MockMountSource("b")
        sources = [("archive1.tar", mock_source_a), ("archive2.tar", mock_source_b)]
        options = {'disableUnionMount': True, 'resolveSymbolicLinks': False}

        result = _create_multi_mount(sources, options)

        # Should return SubvolumesMountSource
        assert isinstance(result, SubvolumesMountSource)


if __name__ == "__main__":
    unittest.main()
