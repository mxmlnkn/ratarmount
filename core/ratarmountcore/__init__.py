"""Ratarmount Core

This is the backend of ratarmount. It is intended to be used as a library.

This library offers an interface which is sufficient to work with FUSE for
read-only access.

The most common usecase should be covered by the open_mount_source factory
function. For more information, see the ratarmountcore.mountsource submodule.

Example:

    from ratarmountcore.mountsources.factory import open_mount_source

    archive = open_mount_source("foo.tar", recursive=True)
    archive.list("/")
    info = archive.lookup("/bar")

    print("Contents of /bar:")
    with archive.open(info) as file:
        print(file.read())
"""

from .version import __version__
