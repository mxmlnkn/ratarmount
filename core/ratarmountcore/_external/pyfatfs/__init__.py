# -*- coding: utf-8 -*-

"""
Python FAT filesystem module with :doc:`PyFilesystem2 <pyfilesystem2:index>` \
compatibility.

pyfatfs allows interaction with FAT12/16/32 filesystems, either via
:doc:`PyFilesystem2 <pyfilesystem2:index>` for file-level abstraction
or direct interaction with the filesystem for low-level access.
"""

#: Specifies default ("OEM") encoding
from pyfatfs._exceptions import PyFATException

FAT_OEM_ENCODING = 'ibm437'
#: Specifies the long file name encoding, which is always UTF-16 (LE)
FAT_LFN_ENCODING = 'utf-16-le'


def _init_check(func):
    def _wrapper(*args, **kwargs):
        initialized = args[0].initialized

        if initialized is True:
            return func(*args, **kwargs)
        else:
            raise PyFATException("Class has not yet been fully initialized, "
                                 "please instantiate first.")

    return _wrapper
