#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Ratarmount

This is the frontend for ratarmount.
It is normally not intended to be used as a library.

The installed ratarmount script will load this module and call its 'cli' function,
which could also be done programmatically.

Example:

    from ratarmountcore.factory as openMountSource

    archive = openMountSource("foo.tar", recursive=True)
    archive.listDir("/")
    info = archive.getFileInfo("/bar")

    print "Contents of /bar:"
    with archive.open(info) as file:
        print(file.read())
"""

from .version import __version__
