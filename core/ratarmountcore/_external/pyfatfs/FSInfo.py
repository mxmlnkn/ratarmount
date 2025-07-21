# -*- coding: utf-8 -*-

"""FSInfo structure handling for FAT32 filesystems."""

import struct
from collections import OrderedDict


class FSInfo(OrderedDict):
    """Class for FAT32 FSInfo sector implementation."""

    #: Magic value at the start of an FSInfo struct
    LEAD_SIG_MAGIC = 0x41615252
    #: Magic value in the middle of an FSInfo struct
    STRUCT_SIG_MAGIC = 0x61417272
    #: Magic value at the end of an FSInfo struct
    TRAIL_SIG_MAGIC = 0xAA550000

    #: FSI header layout in struct formatted string
    HEADER_LAYOUT = "<L480xLLL12xL"
    #: FSI header fields
    HEADER_VARS = ["FSI_LeasSig",
                   "FSI_StrucSig",
                   "FSI_Free_Count",
                   "FSI_Nxt_Free",
                   "FSI_TrailSig"]

    def __init__(self, free_count=None, next_free=None):
        """Initialize an empty Boot sector header."""
        super().__init__()
        self.update({"FSI_LeasSig": FSInfo.LEAD_SIG_MAGIC,
                     "FSI_StrucSig": FSInfo.STRUCT_SIG_MAGIC,
                     "FSI_Free_Count": free_count,
                     "FSI_Nxt_Free": next_free,
                     "FSI_TrailSig": FSInfo.TRAIL_SIG_MAGIC})

    def __bytes__(self):
        """Serialize header data back to bytes."""
        return struct.pack(self.HEADER_LAYOUT, *self.values())

    def __len__(self):
        """Return struct size."""
        return struct.calcsize(self.HEADER_LAYOUT)

    def parse_header(self, data: bytes) -> "FSInfo":
        """Deserialize FSInfo binary data into FSInfo class instance.

        :param data: `bytes`: 512 bytes of binary data to be deserialized
        """
        if len(data) < len(self):
            raise ValueError(f"Invalid FSInfo sector data supplied "
                             f"for {type(self)}. len(data)={len(data)}, "
                             f"len(header)={len(self)}")

        fsinfo = struct.unpack(self.HEADER_LAYOUT, data[:len(self)])
        self.update(dict(zip(self.HEADER_VARS, fsinfo)))
