# -*- coding: utf-8 -*-

"""Boot sector header specific implementation for FAT12/16 and FAT32."""

import struct
from collections import OrderedDict


class BootSectorHeader(OrderedDict):
    """Base/Interface class for Boot sector header implementation."""

    #: BPB header layout in struct formatted string
    HEADER_LAYOUT = "<3s8sHBHBHHBHHHLL"
    #: BPB header fields when extracted with bpb_header_layout
    HEADER_VARS = ["BS_jmpBoot", "BS_OEMName", "BPB_BytsPerSec",
                   "BPB_SecPerClus", "BPB_RsvdSecCnt", "BPB_NumFATs",
                   "BPB_RootEntCnt", "BPB_TotSec16", "BPB_Media",
                   "BPB_FATSz16", "BPB_SecPerTrk", "BPB_NumHeads",
                   "BPB_HiddSec", "BPB_TotSec32"]

    def __init__(self):
        """Initialize an empty Boot sector header."""
        super().__init__()
        self.update(dict.fromkeys(self.HEADER_VARS))

    def __bytes__(self):
        """Serialize header data back to bytes."""
        return struct.pack(self.HEADER_LAYOUT, *self.values())

    def __len__(self):
        """Return struct size."""
        return struct.calcsize(self.HEADER_LAYOUT)

    def parse_header(self, data: bytes):
        """Parse header data from bytes.

        :param data: `bytes`: Raw header data from disk
        """
        if len(data) < len(self):
            raise ValueError(f"Invalid Boot sector header data supplied "
                             f"for {type(self)}. len(data)={len(data)}, "
                             f"len(header)={len(self)}")

        header = struct.unpack(self.HEADER_LAYOUT, data[:len(self)])
        self.update(dict(zip(self.HEADER_VARS, header)))


class FAT12BootSectorHeader(BootSectorHeader):
    """FAT12/16-specific header implementation."""

    #: FAT12/16 header layout in struct formatted string
    HEADER_LAYOUT = BootSectorHeader.HEADER_LAYOUT + "BBBL11s8s"
    #: FAT12/16 header fields when extracted with fat12_header_layout
    HEADER_VARS = BootSectorHeader.HEADER_VARS + \
        ["BS_DrvNum", "BS_Reserved1", "BS_BootSig",
         "BS_VolID", "BS_VolLab", "BS_FilSysType"]


class FAT32BootSectorHeader(BootSectorHeader):
    """FAT32-specific header implementation."""

    #: FAT32 header layout in struct formatted string
    HEADER_LAYOUT = BootSectorHeader.HEADER_LAYOUT + "LHHLHH12sBBBL11s8s"
    #: FAT32 header fields when extracted with fat32_header_layout
    HEADER_VARS = BootSectorHeader.HEADER_VARS + \
        ["BPB_FATSz32", "BPB_ExtFlags", "BPB_FSVer", "BPB_RootClus",
         "BPB_FSInfo", "BPB_BkBootSec", "BPB_Reserved", "BS_DrvNum",
         "BS_Reserved1", "BS_BootSig", "BS_VolID", "BS_VolLab",
         "BS_FilSysType"]
