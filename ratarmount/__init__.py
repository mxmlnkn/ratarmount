"""Ratarmount

This is the frontend for ratarmount.
It is normally not intended to be used as a library.

The installed ratarmount script will load this module and call its 'cli' function,
which could also be done programmatically.

Example:

    from ratarmountcore.factory as open_mount_source

    archive = open_mount_source("foo.tar", recursive=True)
    archive.list("/")
    info = archive.lookup("/bar")

    print "Contents of /bar:"
    with archive.open(info) as file:
        print(file.read())
"""

from .version import __version__
