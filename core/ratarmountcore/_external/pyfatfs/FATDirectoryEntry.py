# -*- coding: utf-8 -*-

"""Directory entry operations with PyFAT."""
import posixpath
import struct
import warnings

from time import timezone

from pyfatfs.DosDateTime import DosDateTime
from pyfatfs.EightDotThree import EightDotThree
from pyfatfs._exceptions import PyFATException, NotAnLFNEntryException, \
    BrokenLFNEntryException
from pyfatfs import FAT_OEM_ENCODING, FAT_LFN_ENCODING

import errno


class FATDirectoryEntry:
    """Represents directory entries in FAT (files & directories)."""

    #: Marks a directory entry as empty
    FREE_DIR_ENTRY_MARK = 0xE5
    #: Marks all directory entries after this one as empty
    LAST_DIR_ENTRY_MARK = 0x00

    #: Bit set in DIR_Attr if entry is read-only
    ATTR_READ_ONLY = 0x01
    #: Bit set in DIR_Attr if entry is hidden
    ATTR_HIDDEN = 0x02
    #: Bit set in DIR_Attr if entry is a system file
    ATTR_SYSTEM = 0x04
    #: Bit set in DIR_Attr if entry is a volume id descriptor
    ATTR_VOLUME_ID = 0x8
    #: Bit set in DIR_Attr if entry is a directory
    ATTR_DIRECTORY = 0x10
    #: Bit set in DIR_Attr if entry is an archive
    ATTR_ARCHIVE = 0x20
    #: Bits set in DIR_Attr if entry is an LFN entry
    ATTR_LONG_NAME = ATTR_READ_ONLY | ATTR_HIDDEN | \
        ATTR_SYSTEM | ATTR_VOLUME_ID
    #: Bitmask to check if entry is an LFN entry
    ATTR_LONG_NAME_MASK = ATTR_READ_ONLY | ATTR_HIDDEN | ATTR_SYSTEM | \
        ATTR_VOLUME_ID | ATTR_DIRECTORY | ATTR_ARCHIVE

    #: Directory entry header layout in struct formatted string
    FAT_DIRECTORY_LAYOUT = "<11sBBBHHHHHHHL"
    #: Size of a directory entry header in bytes
    FAT_DIRECTORY_HEADER_SIZE = struct.calcsize(FAT_DIRECTORY_LAYOUT)
    #: Maximum allowed file size, dictated by size of DIR_FileSize
    MAX_FILE_SIZE = 0xFFFFFFFF
    #: Directory entry headers
    FAT_DIRECTORY_VARS = ["DIR_Name", "DIR_Attr", "DIR_NTRes",
                          "DIR_CrtTimeTenth", "DIR_CrtTime",
                          "DIR_CrtDate", "DIR_LstAccessDate",
                          "DIR_FstClusHI", "DIR_WrtTime",
                          "DIR_WrtDate", "DIR_FstClusLO",
                          "DIR_FileSize"]

    def __init__(self,
                 DIR_Name: EightDotThree, DIR_Attr: int,
                 DIR_NTRes: int, DIR_CrtTimeTenth: int,
                 DIR_CrtTime: int, DIR_CrtDate: int, DIR_LstAccessDate: int,
                 DIR_FstClusHI: int, DIR_WrtTime: int, DIR_WrtDate: int,
                 DIR_FstClusLO: int, DIR_FileSize: int,
                 encoding: str = FAT_OEM_ENCODING,
                 fs: "pyfatfs.PyFat.PyFat" = None,  # noqa: F821
                 lazy_load: bool = False, lfn_entry=None):
        """FAT directory entry constructor.

        :param DIR_Name: `EightDotThree` class instance
        :param DIR_Attr: Attributes of directory
        :param DIR_NTRes: Reserved attributes of directory entry
        :param DIR_CrtTimeTenth: Milliseconds at file creation
        :param DIR_CrtTime: Creation timestamp of entry
        :param DIR_CrtDate: Creation date of entry
        :param DIR_LstAccessDate: Last access date of entry
        :param DIR_FstClusHI: High cluster value of entry data
        :param DIR_WrtTime: Modification timestamp of entry
        :param DIR_WrtDate: Modification date of entry
        :param DIR_FstClusLO: Low cluster value of entry data
        :param DIR_FileSize: File size in bytes
        :param encoding: Encoding of filename
        :param lfn_entry: FATLongDirectoryEntry instance or None
        """
        self.__filesize = 0

        self.name: EightDotThree = DIR_Name
        self.attr = int(DIR_Attr)
        self.ntres = int(DIR_NTRes)
        self.crttimetenth = int(DIR_CrtTimeTenth)
        self.crttime = int(DIR_CrtTime)
        self.crtdate = int(DIR_CrtDate)
        self.lstaccessdate = int(DIR_LstAccessDate)
        self.fstclushi = int(DIR_FstClusHI)
        self.wrttime = int(DIR_WrtTime)
        self.wrtdate = int(DIR_WrtDate)
        self.fstcluslo = int(DIR_FstClusLO)
        self.filesize = int(DIR_FileSize)

        self.__lazy_load = lazy_load
        self.__fs = fs

        self._parent = None

        # Handle LFN entries
        self.lfn_entry = None
        try:
            self.set_lfn_entry(lfn_entry)
        except BrokenLFNEntryException:
            warnings.warn("Broken LFN entry detected, omitting "
                          "long file name.")

        self.__dirs = []
        self.__encoding = encoding

    @property
    def _encoding(self):
        return self.__encoding

    @property
    def filesize(self):
        """Size of the file in bytes.

        :getter: Get the currently set filesize in bytes
        :setter: Set new filesize. FAT chain must be extended
                 separately. Raises `PyFATException` with
                 `errno=E2BIG` if filesize is larger than
                 `FATDirectoryEntry.MAX_FILE_SIZE`.
        :type: int
        """
        return self.__filesize

    @filesize.setter
    def filesize(self, size: int):
        if size > self.MAX_FILE_SIZE:
            raise PyFATException(f"Specified file size {size} too large "
                                 f"for FAT-based filesystems.",
                                 errno=errno.E2BIG)

        self.__filesize = size

    @staticmethod
    def new(name: EightDotThree, tz: timezone, encoding: str,
            attr: int = 0, ntres: int = 0, cluster: int = 0,
            filesize: int = 0) -> "FATDirectoryEntry":
        """Create a new directory entry with sane defaults.

        :param name: ``EightDotThree``: SFN of new dentry
        :param tz: ``timezone``: Timezone value to use for new timestamp
        :param encoding: ``str``: Encoding for SFN
        :param attr: ``int``: Directory attributes
        :param ntres: ``int``: Reserved NT directory attributes
        :param cluster: ``int``: Cluster number of dentry
        :param filesize: ``int``: Size of file referenced by dentry
        :returns: ``FATDirectoryEntry`` instance
        """
        dt = DosDateTime.now(tz=tz)
        dentry = FATDirectoryEntry(
            DIR_Name=name,
            DIR_Attr=attr,
            DIR_NTRes=ntres,
            DIR_CrtTimeTenth=0,
            DIR_CrtTime=dt.serialize_time(),
            DIR_CrtDate=dt.serialize_date(),
            DIR_LstAccessDate=dt.serialize_date(),
            DIR_FstClusHI=0x00,
            DIR_WrtTime=dt.serialize_time(),
            DIR_WrtDate=dt.serialize_date(),
            DIR_FstClusLO=0x00,
            DIR_FileSize=filesize,
            encoding=encoding
        )
        dentry.set_cluster(cluster)
        return dentry

    def get_ctime(self) -> DosDateTime:
        """Get dentry creation time."""
        return self.__combine_dosdatetime(self.crtdate, self.crttime)

    def get_mtime(self) -> DosDateTime:
        """Get dentry modification time."""
        return self.__combine_dosdatetime(self.wrtdate, self.wrttime)

    def get_atime(self) -> DosDateTime:
        """Get dentry access time."""
        return DosDateTime.deserialize_date(self.lstaccessdate)

    @staticmethod
    def __combine_dosdatetime(dt, tm) -> DosDateTime:
        dt = DosDateTime.deserialize_date(dt)
        return dt.combine(dt, DosDateTime.deserialize_time(tm))

    def get_checksum(self) -> int:
        """Get calculated checksum of this directory entry.

        :returns: Checksum as int
        """
        return self.name.checksum()

    def set_lfn_entry(self, lfn_entry):
        """Set LFN entry for current directory entry.

        :param: lfn_entry: Can be either of type `FATLongDirectoryEntry`
                or `None`.
        """
        if not isinstance(lfn_entry, FATLongDirectoryEntry):
            return

        # Verify LFN entries checksums
        chksum = self.get_checksum()
        for entry in lfn_entry.lfn_entries:
            entry_chksum = lfn_entry.lfn_entries[entry]["LDIR_Chksum"]
            if entry_chksum != chksum:
                raise BrokenLFNEntryException(f'Checksum verification for '
                                              f'LFN entry of directory '
                                              f'"{self.get_short_name()}" '
                                              f'failed')
        self.lfn_entry = lfn_entry

    def get_entry_size(self):
        """Get size of directory entry.

        :returns: Entry size in bytes as int
        """
        if self.is_directory():
            self.__populate_dirs()

        sz = self.FAT_DIRECTORY_HEADER_SIZE
        if isinstance(self.lfn_entry, FATLongDirectoryEntry):
            sz *= len(self.lfn_entry.lfn_entries)
        sz += self.FAT_DIRECTORY_HEADER_SIZE * len(self.__dirs)+1

        return sz

    def get_size(self):
        """Get filesize or directory entry size.

        :returns: Filesize or directory entry size in bytes as int
        """
        import warnings
        warnings.warn(f"{self.__class__}.get_size is deprecated, this "
                      f"method will be removed in PyFatFS 2.0; please "
                      f"use the filesize property instead!",
                      DeprecationWarning)
        return self.filesize

    def set_size(self, size: int):
        """Set filesize.

        :param size: `int`: File size in bytes
        """
        import warnings
        warnings.warn(f"{self.__class__}.set_size is deprecated, this "
                      f"method will be removed in PyFatFS 2.0; please "
                      f"use the filesize property instead!",
                      DeprecationWarning)
        self.filesize = size

    def get_cluster(self):
        """Get cluster address of directory entry.

        :returns: Cluster address of entry
        """
        return self.fstcluslo + (self.fstclushi << 16)

    def set_cluster(self, first_cluster):
        """Set low and high cluster address in directory headers."""
        self.fstcluslo = (first_cluster >> (16 * 0) & 0xFFFF)
        self.fstclushi = (first_cluster >> (16 * 1) & 0xFFFF)

    def __bytes__(self):
        """Represent directory entry as bytes.

        Note: Also represents accompanying LFN entries

        :returns: Entry & LFN entry as bytes-object
        """
        entry = b''
        if isinstance(self.lfn_entry, FATLongDirectoryEntry):
            entry += bytes(self.lfn_entry)

        entry += struct.pack(self.FAT_DIRECTORY_LAYOUT,
                             self.name.name,
                             self.attr, self.ntres, self.crttimetenth,
                             self.crttime, self.crtdate, self.lstaccessdate,
                             self.fstclushi, self.wrttime, self.wrtdate,
                             self.fstcluslo, self.filesize)

        return entry

    def _add_parent(self, cls):
        """Add parent directory link to current directory entry.

        raises: PyFATException
        """
        if self._parent is not None:
            raise PyFATException("Trying to add multiple parents to current "
                                 "directory!", errno=errno.ETOOMANYREFS)

        if not isinstance(cls, FATDirectoryEntry):
            raise PyFATException("Trying to add a non-FAT directory entry "
                                 "as parent directory!", errno=errno.EBADE)

        self._parent = cls

    def _get_parent_dir(self, sd):
        """Build path name for recursive directory entries."""
        name = self.__repr__()
        if self.__repr__() == "/":
            name = ""
        sd += [name]

        if self._parent is None:
            return sd

        return self._parent._get_parent_dir(sd)

    def get_full_path(self):
        """Iterate all parents up and join them by "/"."""
        parent_dirs = [self.__repr__()]

        if self._parent is None:
            return "/"

        return posixpath.join(*list(reversed(
            self._parent._get_parent_dir(parent_dirs))))

    def get_parent_dir(self):
        """Get the parent directory entry."""
        if self._parent is None:
            raise PyFATException("Cannot query parent directory of "
                                 "root directory", errno=errno.ENOENT)

        return self._parent

    def is_special(self):
        """Determine if dir entry is a dot or dotdot entry.

        :returns: Boolean value whether or not entry is
                  a dot or dotdot entry
        """
        return self.get_short_name() in [".", ".."]

    def is_read_only(self):
        """Determine if dir entry has read-only attribute set.

        :returns: Boolean value indicating read-only attribute is set
        """
        return (self.ATTR_READ_ONLY & self.attr) > 0

    def is_hidden(self):
        """Determine if dir entry has the hidden attribute set.

        :returns: Boolean value indicating hidden attribute is set
        """
        return (self.ATTR_HIDDEN & self.attr) > 0

    def is_system(self):
        """Determine if dir entry has the system file attribute set.

        :returns: Boolean value indicating system attribute is set
        """
        return (self.ATTR_SYSTEM & self.attr) > 0

    def is_volume_id(self):
        """Determine if dir entry has the volume ID attribute set.

        :returns: Boolean value indicating volume ID attribute is set
        """
        return (self.ATTR_VOLUME_ID & self.attr) > 0

    def _verify_is_directory(self):
        """Verify that current entry is a directory.

        raises: PyFATException: If current entry is not a directory.
        """
        if not self.is_directory():
            raise PyFATException("Cannot get entries of this entry, as "
                                 "it is not a directory.",
                                 errno=errno.ENOTDIR)

    def is_directory(self):
        """Determine if dir entry has directory attribute set.

        :returns: Boolean value indicating directory attribute is set
        """
        return (self.ATTR_DIRECTORY & self.attr) > 0

    def is_archive(self):
        """Determine if dir entry has archive attribute set.

        :returns: Boolean value indicating archive attribute is set
        """
        return (self.ATTR_ARCHIVE & self.attr) > 0

    def is_empty(self):
        """Determine if directory does not contain any directories."""
        self._verify_is_directory()
        self.__populate_dirs()

        for d in self.__dirs:
            if d.is_special():
                continue
            return False

        return True

    def __populate_dirs(self):
        if self.__lazy_load is False:
            return

        clus = self.get_cluster()
        self.__dirs = self.__fs.parse_dir_entries_in_cluster_chain(clus)
        for dir_entry in self.__dirs:
            dir_entry._add_parent(self)
        self.__lazy_load = False

    def _get_entries_raw(self):
        """Get a full list of entries in current directory."""
        self._verify_is_directory()
        self.__populate_dirs()

        return self.__dirs

    def get_entries(self):
        """Get entries of directory.

        :raises: PyFatException: If entry is not a directory
        :returns: tuple: root (current path, full),
                 dirs (all dirs), files (all files)
        """
        dirs = []
        files = []
        specials = []

        for d in self._get_entries_raw():
            if d.is_special() or d.is_volume_id():
                # Volume IDs and dot/dotdot entries
                specials += [d]
            elif d.is_directory():
                # Directories
                dirs += [d]
            else:
                # Everything else must be a file
                files += [d]

        return dirs, files, specials

    def _search_entry(self, name: str):
        """Find given dir entry by walking current dir.

        :param name: Name of entry to search for
        :raises: PyFATException: If entry cannot be found
        :returns: FATDirectoryEntry: Found entry
        """
        dirs, files, _ = self.get_entries()
        for entry in dirs+files:
            try:
                if entry.get_long_name() == name:
                    return entry
            except NotAnLFNEntryException:
                pass
            if entry.get_short_name() == name:
                return entry

        raise PyFATException(f'Cannot find entry {name}',
                             errno=errno.ENOENT)

    def get_entry(self, path: str):
        """Get sub-entry if current entry is a directory.

        :param path: Relative path of entry to get
        :raises: PyFATException: If entry cannot be found
        :returns: FATDirectoryEntry: Found entry
        """
        entry = self
        for segment in filter(None, path.split("/")):
            entry._verify_is_directory()
            entry = entry._search_entry(segment)
        return entry

    def walk(self):
        """Walk all directory entries recursively.

        :returns: tuple: root (current path, full),
                         dirs (all dirs), files (all files)
        """
        self._verify_is_directory()
        self.__populate_dirs()

        root = self.get_full_path()
        dirs, files, _ = self.get_entries()

        yield root, dirs, files
        for d in self.__dirs:
            if d.is_special():
                # Ignore dot and dotdot
                continue

            if not d.is_directory():
                continue

            yield from d.walk()

    def add_subdirectory(self, dir_entry, recursive: bool = True):
        """Register a subdirectory in current directory entry.

        :param dir_entry: FATDirectoryEntry
        :raises: PyFATException: If current entry is not a directory or
                                 given directory entry already has a parent
                                 directory set
        """
        # Check if current dir entry is even a directory!
        self._verify_is_directory()
        self.__populate_dirs()

        dir_entry._add_parent(self)
        self.__dirs += [dir_entry]

    def mark_empty(self):
        """Mark this directory entry as empty."""
        # Also mark LFN entries as empty
        try:
            self.lfn_entry.mark_empty()
        except AttributeError:
            pass

        self.name.name[0] = self.FREE_DIR_ENTRY_MARK

    def remove_dir_entry(self, name):
        """Remove given dir_entry from dir list.

        **NOTE:** This will also remove special entries such
        as ».«, »..« and volume labels!
        """
        # Iterate all entries
        for dir_entry in self._get_entries_raw():
            sn = dir_entry.get_short_name()
            try:
                ln = dir_entry.get_long_name()
            except NotAnLFNEntryException:
                ln = None
            if name in [sn, ln]:
                self.__dirs.remove(dir_entry)
                return

        raise PyFATException(f"Cannot remove '{name}', no such "
                             f"file or directory!", errno=errno.ENOENT)

    def __repr__(self):
        """String-represent directory entry by (preferably) LFN.

        :returns: str: Long file name if existing, 8DOT3 otherwise
        """
        try:
            return self.get_long_name()
        except NotAnLFNEntryException:
            return self.get_short_name()

    def get_short_name(self):
        """Get short name of directory entry.

        :returns: str: Name of directory entry
        """
        return self.name.get_unpadded_filename()

    def get_long_name(self):
        """Get long name of directory entry.

        :raises: NotAnLFNEntryException: If entry has no long file name
        :returns: str: Long file name of directory entry
        """
        if self.lfn_entry is None:
            raise NotAnLFNEntryException("No LFN entry found for this "
                                         "dir entry.")

        return str(self.lfn_entry)


class FATLongDirectoryEntry(object):
    """Represents long file name (LFN) entries."""

    #: LFN entry header layout in struct formatted string
    FAT_LONG_DIRECTORY_LAYOUT = "<B10sBBB12sH4s"
    #: LFN header fields when extracted with `FAT_LONG_DIRECTORY_LAYOUT`
    FAT_LONG_DIRECTORY_VARS = ["LDIR_Ord", "LDIR_Name1", "LDIR_Attr",
                               "LDIR_Type", "LDIR_Chksum", "LDIR_Name2",
                               "LDIR_FstClusLO", "LDIR_Name3"]
    #: Ordinance of last LFN entry in a chain
    LAST_LONG_ENTRY = 0x40
    #: Length for long file name in bytes per entry
    LFN_ENTRY_LENGTH = 26

    def __init__(self):
        """Initialize empty LFN directory entry object."""
        self.lfn_entries = {}

    def get_entries(self, reverse: bool = False):
        """Get LFS entries in correct order (based on `LDIR_Ord`).

        :param reverse: `bool`: Returns LFN entries in reversed order.
                        This is required for byte representation.
        """
        for _, e in sorted(self.lfn_entries.items(),
                           key=lambda x: x[1]["LDIR_Ord"],
                           reverse=reverse):
            yield e

    def mark_empty(self):
        """Mark LFN entry as empty."""
        free_dir_entry_mark = FATDirectoryEntry.FREE_DIR_ENTRY_MARK
        for k in self.lfn_entries.keys():
            self.lfn_entries[k]["LDIR_Ord"] = free_dir_entry_mark

    def __bytes__(self):
        """Represent LFN entries as bytes."""
        entries_bytes = b""
        for e in self.get_entries(reverse=True):
            entries_bytes += struct.pack(self.FAT_LONG_DIRECTORY_LAYOUT,
                                         e["LDIR_Ord"], e["LDIR_Name1"],
                                         e["LDIR_Attr"], e["LDIR_Type"],
                                         e["LDIR_Chksum"], e["LDIR_Name2"],
                                         e["LDIR_FstClusLO"], e["LDIR_Name3"])
        return entries_bytes

    def __str__(self):
        """Remove padding from LFN entry and decode it.

        :returns: `str` decoded string of filename
        """
        name = b''

        for e in self.get_entries():
            for h in ["LDIR_Name1", "LDIR_Name2", "LDIR_Name3"]:
                name += e[h]

        while name.endswith(b'\xFF\xFF'):
            name = name[:-2]

        name = name.decode(FAT_LFN_ENCODING)

        if name.endswith('\0'):
            name = name[:-1]

        return name

    @staticmethod
    def is_lfn_entry(LDIR_Ord, LDIR_Attr):
        """Verify that entry is an LFN entry.

        :param LDIR_Ord: First byte of the directory header, ordinance
        :param LDIR_Attr: Attributes segment of directory header
        :returns: `True` if entry is a valid LFN entry
        """
        lfn_attr = FATDirectoryEntry.ATTR_LONG_NAME
        lfn_attr_mask = FATDirectoryEntry.ATTR_LONG_NAME_MASK
        is_attr_set = (LDIR_Attr & lfn_attr_mask) == lfn_attr

        return is_attr_set and \
            LDIR_Ord != FATDirectoryEntry.FREE_DIR_ENTRY_MARK

    def add_lfn_entry(self, LDIR_Ord, LDIR_Name1, LDIR_Attr, LDIR_Type,
                      LDIR_Chksum, LDIR_Name2, LDIR_FstClusLO, LDIR_Name3):
        """Add LFN entry to this instances chain.

        :param LDIR_Ord: Ordinance of LFN entry
        :param LDIR_Name1: First name field of LFN entry
        :param LDIR_Attr: Attributes of LFN entry
        :param LDIR_Type: Type of LFN entry
        :param LDIR_Chksum: Checksum value of following 8dot3 entry
        :param LDIR_Name2: Second name field of LFN entry
        :param LDIR_FstClusLO: Cluster address of LFN entry. Always zero.
        :param LDIR_Name3: Third name field of LFN entry
        """
        # Check if attribute matches
        if not self.is_lfn_entry(LDIR_Ord, LDIR_Attr):
            raise NotAnLFNEntryException("Given LFN entry is not a long "
                                         "file name entry or attribute "
                                         "not set correctly!")

        # Check if FstClusLO is 0, as required by the spec
        if LDIR_FstClusLO != 0:
            raise PyFATException("Given LFN entry has an invalid first "
                                 "cluster ID, don't know what to do.",
                                 errno=errno.EFAULT)

        # Check if item with same index has already been added
        if LDIR_Ord in self.lfn_entries.keys():
            raise PyFATException("Given LFN entry part with index \'{}\'"
                                 "has already been added to LFN "
                                 "entry list.".format(LDIR_Ord))

        mapped_entries = dict(zip(self.FAT_LONG_DIRECTORY_VARS,
                                  (LDIR_Ord, LDIR_Name1, LDIR_Attr, LDIR_Type,
                                   LDIR_Chksum, LDIR_Name2, LDIR_FstClusLO,
                                   LDIR_Name3)))

        self.lfn_entries[LDIR_Ord] = mapped_entries

    def is_lfn_entry_complete(self):
        """Verify that LFN object forms a complete chain.

        :returns: `True` if `LAST_LONG_ENTRY` is found
        """
        for k in self.lfn_entries.keys():
            if (int(k) & self.LAST_LONG_ENTRY) == self.LAST_LONG_ENTRY:
                return True

        return False
