# -*- coding: utf-8 -*-

"""8DOT3 file name helper class & functions."""

import errno
import os
import struct

from pyfatfs import FAT_OEM_ENCODING, _init_check
from pyfatfs._exceptions import PyFATException, NotAFatEntryException


class EightDotThree:
    """8DOT3 filename representation."""

    #: Length of the byte representation in a directory entry header
    SFN_LENGTH = 11

    #: Invalid characters for 8.3 file names
    INVALID_CHARACTERS = [range(0x0, 0x20)] + [0x22, 0x2A, 0x2B, 0x2C, 0x2E,
                                               0x2F, 0x3A, 0x3B, 0x3C, 0x3D,
                                               0x3E, 0x3F, 0x5B, 0x5C, 0x5D,
                                               0x7C]

    def __init__(self, encoding: str = FAT_OEM_ENCODING):
        """Offer 8DOT3 filename operation.

        :param encoding: Codepage for the 8.3 filename.
                         Defaults to `FAT_OEM_ENCODING` as per FAT spec.
        """
        self.name: bytearray = None
        self.encoding = encoding
        self.initialized = False

    def __str__(self):
        """Decode and un-pad SFN string."""
        name = self.name
        if name[0] == 0x05:
            # Translate 0x05 to 0xE5
            name[0] = 0xE5

        base = name[:8].decode(self.encoding).rstrip()
        ext = name[8:11].decode(self.encoding).rstrip()
        sep = "." if len(ext) > 0 else ""

        return sep.join([base, ext])

    def __bytes__(self):
        """Byte representation of the 8DOT3 name dir entry headers."""
        return bytes(self.name)

    @_init_check
    def get_unpadded_filename(self) -> str:
        """Retrieve the human readable filename."""
        return str(self)

    @staticmethod
    def __raise_8dot3_nonconformant(name: str):
        raise PyFATException(f"Given directory name "
                             f"{name} is not conform "
                             f"to 8.3 file naming convention.",
                             errno=errno.EINVAL)

    def __set_name(self, name: bytes):
        """Set self.name and verify for correctness."""
        if len(name) != 11:
            self.__raise_8dot3_nonconformant(name.decode(self.encoding))

        self.name = name
        self.initialized = True

    def set_byte_name(self, name: bytes):
        """Set the name as byte input from a directory entry header.

        :param name: `bytes`: Padded (must be 11 bytes) 8dot3 name
        """
        if not isinstance(name, bytes):
            raise TypeError(f"Given parameter must be of type bytes, "
                            f"but got {type(name)} instead.")

        name = bytearray(name)

        if len(name) != 11:
            raise ValueError("Invalid byte name supplied, must be exactly "
                             "11 bytes long (8+3).")

        if name[0] == 0x0 or name[0] == 0xE5:
            # Empty directory entry
            raise NotAFatEntryException("Given dir entry is invalid and has "
                                        "no valid name.", free_type=name[0])

        self.__set_name(name)

    def set_str_name(self, name: str):
        """Set the name as string from user input (i.e. folder creation)."""
        if not isinstance(name, str):
            raise TypeError(f"Given parameter must be of type str, "
                            f"but got {type(name)} instead.")

        if not self.is_8dot3_conform(name, self.encoding):
            self.__raise_8dot3_nonconformant(name)

        name = bytearray(self._pad_8dot3_name(name).encode(self.encoding))
        if name[0] == 0xE5:
            name[0] = 0x05
        self.name = name
        self.initialized = True

    @_init_check
    def checksum(self) -> int:
        """Calculate checksum of byte string.

        :returns: Checksum as int
        """
        chksum = 0
        for c in self.name:
            chksum = ((chksum >> 1) | (chksum & 1) << 7) + c
            chksum &= 0xFF
        return chksum

    @staticmethod
    def __check_characters(name: str, encoding: str) -> bool:
        """Test if given string contains invalid chars for 8.3 names.

        :param name: `str`: Filename to parse
        :raises: `ValueError` if the given string contains invalid
                 8.3 filename characters.
        """
        name = name.encode(encoding)
        name = list(struct.unpack(f"{len(name)}c", name))
        for c in name:
            if ord(c) in EightDotThree.INVALID_CHARACTERS:
                raise ValueError(f"Invalid characters in string '{name}', "
                                 f"cannot be used as part of an 8.3 "
                                 f"conform file name.")

    @staticmethod
    def is_8dot3_conform(entry_name: str, encoding: str = FAT_OEM_ENCODING):
        """Indicate conformance of given entries name to 8.3 standard.

        :param entry_name: Name of entry to check
        :param encoding: ``str``: Encoding for SFN
        :returns: bool indicating conformance of name to 8.3 standard
        """
        if entry_name != entry_name.upper():
            # Case sensitivity check
            return False

        root, ext = os.path.splitext(entry_name)
        ext = ext[1:]
        if len(root) + len(ext) > 11:
            return False
        elif len(root) > 8 or len(ext) > 3:
            return False

        # Check for valid characters in both filename segments
        for i in [root, ext]:
            try:
                EightDotThree.__check_characters(i, encoding=encoding)
            except ValueError:
                return False

        return True

    @staticmethod
    def _pad_8dot3_name(name: str):
        """Pad 8DOT3 name to 11 bytes for header operations.

        This is required to pass the correct value to the `FATDirectoryEntry`
        constructor as a DIR_Name.
        """
        root, ext = os.path.splitext(name)
        ext = ext[1:]
        name = root.strip().ljust(8) + ext.strip().ljust(3)
        return name

    @staticmethod
    def make_8dot3_name(dir_name: str,
                        parent_dir_entry) -> str:
        """Generate filename based on 8.3 rules out of a long file name.

        In 8.3 notation we try to use the first 6 characters and
        fill the rest with a tilde, followed by a number (starting
        at 1). If that entry is already given, we increment this
        number and try again until all possibilities are exhausted
        (i.e. A~999999.TXT).

        :param dir_name: Long name of directory entry.
        :param parent_dir_entry: `FATDirectoryEntry`: Dir entry of parent dir.
        :returns: `str`: 8DOT3 compliant filename.
        :raises: PyFATException: If parent dir is not a directory
                                 or all name generation possibilities
                                 are exhausted
        """
        dirs, files, _ = parent_dir_entry.get_entries()
        dir_entries = [e.get_short_name() for e in dirs + files]

        extsep = "."

        def map_chars(name: bytes) -> bytes:
            """Map 8DOT3 valid characters.

            :param name: `str`: input name
            :returns: `str`: mapped output character
            """
            _name: bytes = b''
            for b in struct.unpack(f"{len(name)}c", name):
                if b == b' ':
                    _name += b''
                elif ord(b) in EightDotThree.INVALID_CHARACTERS:
                    _name += b'_'
                else:
                    _name += b
            return _name

        dir_name = dir_name.upper()
        # Shorten to 8 chars; strip invalid characters
        basename = os.path.splitext(dir_name)[0][0:8].strip()
        basename = basename.encode(parent_dir_entry._encoding,
                                   errors="replace")
        basename = map_chars(basename).decode(parent_dir_entry._encoding)

        # Shorten to 3 chars; strip invalid characters
        extname = os.path.splitext(dir_name)[1][1:4].strip()
        extname = extname.encode(parent_dir_entry._encoding,
                                 errors="replace")
        extname = map_chars(extname).decode(parent_dir_entry._encoding)

        if len(extname) == 0:
            extsep = ""

        # Loop until suiting name is found
        i = 0
        while len(str(i)) + 1 <= 7:
            if i > 0:
                maxlen = 8 - (1 + len(str(i)))
                basename = f"{basename[0:maxlen]}~{i}"

            short_name = f"{basename}{extsep}{extname}"

            if short_name not in dir_entries:
                return short_name
            i += 1

        raise PyFATException("Cannot generate 8dot3 filename, "
                             "unable to find suiting short file name.",
                             errno=errno.EEXIST)
