import itertools
import os
import os.path
import stat
import sys
from abc import abstractmethod
from dataclasses import dataclass, field
from functools import reduce
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from ratarmountcore.mountsource import FileInfo, MountSource
from ratarmountcore.utils import cached_property, overrides
from typing_extensions import Final, Self, final


@final
@dataclass
class _FileVersion:
    """
    An underlying file version bound to a union path.
    """

    path: Final[str]
    """
    The absolute path in the underlying mount source.
    """
    version: Final[int]
    """
    The version number of the file in the underlying mount source.
    """

    parent: Final[Optional["_FileVersion"]]
    """
    The parent folder version, or None if the current path is '/'.

    This establishes a "physical" parent chain that is used for relative parent path resolution (e.g., for '..'). This behavior is analogous to **lexical scoping** in programming languages, where the meaning of a relative path is determined by its static location in the filesystem hierarchy, not by how
    it is accessed.
    """

    unionPath: Final["_UnionPath"]
    """
    The union path to which this file version belongs.
    """

    def list_child_names(self) -> Optional[Iterable[str]]:
        underlying_list = self.unionPath.layer.mountSource.list_mode(self.path)
        if underlying_list is None:
            return None
        if isinstance(underlying_list, Mapping):
            return underlying_list.keys()
        assert isinstance(underlying_list, Iterable)
        return underlying_list

    @cached_property
    def file_info(self) -> FileInfo:
        """The FileInfo for this file version."""
        return self.unionPath.layer.mountSource.lookup(self.path, fileVersion=self.version)

    @cached_property
    def link_target(self) -> Optional["_UnionPath"]:
        """
        Resolves the link if this is a symlink or hardlink that should be resolved, otherwise returns None.
        """
        if self.file_info.linkname:
            normalizedLinkname = os.path.normpath(self.file_info.linkname)
            if self.unionPath.layer.shouldResolveLink(normalizedLinkname, stat.S_IFMT(self.file_info.mode)):
                if os.path.isabs(normalizedLinkname):
                    return self.unionPath.layer._root_union_path._lookup_absolute_path(normalizedLinkname)
                if self.parent is None:
                    raise FileNotFoundError(f"Cannot resolve relative link {normalizedLinkname} from root")
                parts = normalizedLinkname.split(os.path.sep)
                partGroups = itertools.groupby(parts, lambda part: part == os.path.pardir)
                isPart0Pardirs, partGroup0 = next(partGroups)
                if isPart0Pardirs:

                    def resolve_parent(parent: _FileVersion, part: str) -> Optional[_FileVersion]:
                        if parent is None:
                            raise FileNotFoundError(f"Cannot resolve parent for {normalizedLinkname} from {self.path}")
                        assert part == os.path.pardir
                        return parent.parent

                    resolvedParent = reduce(resolve_parent, partGroup0, self.parent)
                    if resolvedParent is None:
                        raise FileNotFoundError(
                            f"Cannot resolve outer parent when resolving {normalizedLinkname} from {self.path}"
                        )
                    isPart1Pardirs, normalParts = next(partGroups, (False, ()))
                    assert not isPart1Pardirs
                else:
                    resolvedParent = self.parent
                    normalParts = partGroup0
                resolvedUnionPath = reduce(
                    lambda parent, part: parent.lookup_child(part),
                    normalParts,
                    resolvedParent.unionPath,
                )
                nextPartGroup = next(partGroups, None)
                assert nextPartGroup is None, "Unexpected part group after normal parts in link resolution"

                return resolvedUnionPath

            return None
        return None


@dataclass
class _UnionPath:
    """
    Represents a path in the union filesystem, which may correspond to multiple
    underlying file versions.
    """

    layer: Final["LinkResolutionLayer"]
    """The LinkResolutionLayer this path belongs to."""

    @cached_property
    @abstractmethod
    def path(self) -> str:
        """The absolute path of this union path."""
        ...

    @cached_property
    @abstractmethod
    def file_versions(self) -> Iterable[_FileVersion]:
        """The file versions that constitute this union path."""
        ...

    def generate_direct_link_targets(self):
        """Generates the direct link targets of the file versions in this union path."""
        for fileVersion in self.file_versions:
            if fileVersion.link_target is not None:
                yield fileVersion.link_target

    @cached_property
    def deduplicated_transitive_link_targets(self) -> Iterable["_UnionPath"]:
        """
        Returns all transitive link targets of this union path, deduplicated.
        """
        visited: Dict[str, _UnionPath] = {}

        def visit(unionPath: _UnionPath):
            if unionPath.path not in visited:
                visited[unionPath.path] = unionPath
                for fileVersion in unionPath.file_versions:
                    if fileVersion.link_target is not None:
                        visit(fileVersion.link_target)

        visit(self)
        return visited.values()

    def generate_own_versions(self) -> Iterator[_FileVersion]:
        """Generates the file versions that are directly part of this union path, not a result of a link."""
        for versionedPath in self.file_versions:
            if versionedPath.link_target is None:
                yield versionedPath

    def generate_resolved_versions(self) -> Iterator[_FileVersion]:
        """Generates all file versions for this path, including resolved links."""
        for linkTarget in self.deduplicated_transitive_link_targets:
            yield from linkTarget.generate_own_versions()

    @cached_property
    def resolved_folder_versions(self) -> Sequence[_FileVersion]:
        """Returns all resolved folder versions for this path."""
        return tuple(
            versionedPath
            for versionedPath in self.generate_resolved_versions()
            if stat.S_ISDIR(versionedPath.file_info.mode)
        )

    @cached_property
    def resolved_nonfolder_versions(self) -> Sequence[_FileVersion]:
        """Returns all resolved non-folder versions for this path."""
        return tuple(
            versionedPath
            for versionedPath in self.generate_resolved_versions()
            if not stat.S_ISDIR(versionedPath.file_info.mode)
        )

    def lookup_child(self, name: str) -> "_ChildUnionPath":
        """Looks up a child of this union path.

        This lookup operates on the logical, merged view of the filesystem. It searches for the child `name` within all concrete folder versions that constitute this `_UnionPath`. This behavior is analogous to **dynamic dispatch** in object-oriented programming, where the operation is dispatched to the concrete implementations at runtime instead of being statically bound.
        """
        return _ChildUnionPath(
            layer=self.layer,
            parent=self,
            name=name,
        )

    def lookup_version(self, fileVersion: int) -> Optional[FileInfo]:
        """Looks up a specific version of this path."""
        if fileVersion < 0:
            return None
        if self.resolved_folder_versions:
            if fileVersion == 0:
                return FileInfo(
                    size=0,
                    mtime=max(folder.file_info.mtime for folder in self.resolved_folder_versions),
                    mode=0o777 | stat.S_IFDIR,
                    linkname="",
                    uid=os.getuid(),
                    gid=os.getgid(),
                    userdata=[],
                )
            if fileVersion - 1 >= len(self.resolved_nonfolder_versions):
                return None
            return self.resolved_nonfolder_versions[fileVersion - 1].file_info
        if self.resolved_nonfolder_versions:
            if fileVersion >= len(self.resolved_nonfolder_versions):
                return None
            return self.resolved_nonfolder_versions[fileVersion].file_info
        return None


@final
@dataclass
class _ChildUnionPath(_UnionPath):
    """Represents a non-root path in the union filesystem."""

    name: Final[str]
    """The name of this path segment."""
    parent: Final[_UnionPath]
    """The parent union path."""

    @cached_property
    def path(self) -> str:
        """The absolute path of this union path."""
        return os.path.join(
            self.parent.path,
            self.name,
        )

    def generate_file_versions(self) -> Iterator[_FileVersion]:
        for parentVersion in self.parent.resolved_folder_versions:
            childPath = os.path.join(parentVersion.path, self.name)
            for version in range(self.layer.mountSource.versions(childPath)):
                yield _FileVersion(
                    path=childPath,
                    version=version,
                    parent=parentVersion,
                    unionPath=self,
                )

    @cached_property
    def file_versions(self):
        """The file versions that constitute this union path."""
        return tuple(self.generate_file_versions())


@final
@dataclass
class _RootUnionPath(_UnionPath):
    """Represents the root path in the union filesystem."""

    @cached_property
    def path(self) -> str:
        """The absolute path of this union path."""
        return "/"

    @cached_property
    def file_versions(self) -> Iterable[_FileVersion]:
        """The file versions that constitute this union path."""
        return tuple(
            _FileVersion(
                path="/",
                version=version,
                parent=None,
                unionPath=self,
            )
            for version in range(self.layer.mountSource.versions("/"))
        )

    def _lookup_absolute_path(self, path: str) -> _UnionPath:
        """
        Looks up a _UnionPath for a given path string.

        Supports both absolute and relative paths. Relative paths are treated as absolute paths by prepending a "/" if needed.
        """
        parts = os.path.normpath(path).split(os.path.sep)
        return reduce(
            lambda parent, part: parent.lookup_child(part),
            itertools.dropwhile(lambda part: part == "", parts),
            self,
        )


@dataclass
class LinkResolutionLayer(MountSource):
    """
    A MountSource layer that resolves symbolic links in an underlying MountSource.

    This class wraps another MountSource and provides a view where symbolic links and hard links
    are resolved. It can be configured with a `shouldResolveLink` function to
    control which links are treated as transparent links and which are kept as
    symbolic link entries or hard link entries.
    """

    mountSource: Final[MountSource]
    """The underlying MountSource to resolve links in."""
    shouldResolveLink: Final[Callable[[str, int], bool]]
    """A function that determines whether a given link should be resolved.

    Args:
        linkname (str): The link target string.
        file_type (int): The file type of the link, as returned by `stat.S_IFMT(mode)`.
    """

    @cached_property
    def _root_union_path(self) -> _RootUnionPath:
        return _RootUnionPath(layer=self)

    @overrides(MountSource)
    def versions(self, path: str) -> int:
        """
        Returns the number of available versions for a given path, after link resolution.
        """
        unionPath = self._root_union_path._lookup_absolute_path(path)
        numberOfFileVersions = len(unionPath.resolved_nonfolder_versions)
        numberOfFolderVersions = 1 if unionPath.resolved_folder_versions else 0
        return numberOfFileVersions + numberOfFolderVersions

    @overrides(MountSource)
    def lookup(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        """
        Looks up file information for a given path and version, after link resolution.
        """
        unionPath = self._root_union_path._lookup_absolute_path(path)
        fileInfo = unionPath.lookup_version(fileVersion)
        if fileInfo is None:
            return None
        fileInfo.userdata.append(unionPath)
        return fileInfo

    def _list(self, path: str) -> Optional[Iterable[str]]:
        unionPath = self._root_union_path._lookup_absolute_path(path)
        if unionPath.resolved_folder_versions:
            return {
                childName
                for versionedPath in unionPath.resolved_folder_versions
                for childName in versionedPath.list_child_names()
            }
        return None

    @overrides(MountSource)
    def list(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        """
        Lists the contents of a directory, after link resolution.
        """
        return self._list(path)

    @overrides(MountSource)
    def list_mode(self, path: str) -> Optional[Union[Iterable[str], Dict[str, int]]]:
        """
        Lists the contents of a directory with file modes, after link resolution.
        """
        return self._list(path)

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo, buffering=-1) -> IO[bytes]:
        """
        Opens a file for reading, after link resolution.
        """
        unionPath = fileInfo.userdata.pop()
        try:
            return self.mountSource.open(fileInfo, buffering)
        finally:
            fileInfo.userdata.append(unionPath)

    @overrides(MountSource)
    def read(self, fileInfo: FileInfo, size: int, offset: int) -> bytes:
        """
        Reads data from a file, after link resolution.
        """
        unionPath = fileInfo.userdata.pop()
        try:
            return self.mountSource.read(fileInfo, size, offset)
        finally:
            fileInfo.userdata.append(unionPath)

    @overrides(MountSource)
    def list_xattr(self, fileInfo: FileInfo) -> List[str]:
        """
        Lists extended attributes of a file, after link resolution.
        """
        unionPath = fileInfo.userdata.pop()
        try:
            return self.mountSource.list_xattr(fileInfo)
        finally:
            fileInfo.userdata.append(unionPath)

    @overrides(MountSource)
    def get_xattr(self, fileInfo: FileInfo, key: str) -> Optional[bytes]:
        """
        Gets an extended attribute of a file, after link resolution.
        """
        unionPath = fileInfo.userdata.pop()
        try:
            return self.mountSource.get_xattr(fileInfo, key)
        finally:
            fileInfo.userdata.append(unionPath)

    @overrides(MountSource)
    def is_immutable(self) -> bool:
        """
        Returns whether the underlying mount source is immutable.
        """
        return self.mountSource.is_immutable()

    @overrides(MountSource)
    def exists(self, path: str) -> bool:
        """
        Checks if a path exists, after link resolution.
        """
        unionPath = self._root_union_path._lookup_absolute_path(path)
        return bool(unionPath.resolved_folder_versions or unionPath.resolved_nonfolder_versions)

    @overrides(MountSource)
    def is_dir(self, path: str) -> bool:
        """
        Checks if a path is a directory, after link resolution.
        """
        unionPath = self._root_union_path._lookup_absolute_path(path)
        return bool(unionPath.resolved_folder_versions)

    @overrides(MountSource)
    def get_mount_source(self, fileInfo: FileInfo) -> Tuple[str, MountSource, FileInfo]:
        """
        Gets the mount source for a file, after link resolution.
        """
        sourceFileInfo = fileInfo.clone()
        unionPath = sourceFileInfo.userdata.pop()
        assert isinstance(unionPath, _UnionPath)
        if sourceFileInfo.userdata:
            return self.mountSource.get_mount_source(sourceFileInfo)
        return "/", self, fileInfo

    @overrides(MountSource)
    def statfs(self) -> Dict[str, Any]:
        """
        Returns filesystem statistics.
        """
        return self.mountSource.statfs()

    @overrides(MountSource)
    def __exit__(self, exception_type, exception_value, exception_traceback):
        """
        Cleanup method for the mount source.
        """
        return super().__exit__(exception_type, exception_value, exception_traceback) or self.mountSource.__exit__(
            exception_type, exception_value, exception_traceback
        )

    @overrides(MountSource)
    def __enter__(self) -> Self:
        """
        Context manager entry point for the mount source.
        """
        self.mountSource.__enter__()
        return super().__enter__()
