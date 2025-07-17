"""
This module offers a MountSource interface, which has methods for listing paths
and getting file metadata and contents. File lookup returns a FileInfo object,
which uniquely identifies the file, similar to a filesystem inode, and can be
used to open the file.

There are multiple implementations of the MountSource interface, largely split
into two submodules: "formats" and "compositing".

"formats" are MountSource implementations that work on an input file object or
URL and exposes its file structure and contained files:

 - SQLiteIndexedTar: This is the oldest and most powerful implementation.
                     It supports fast access to files inside (compressed) TARs.
 - RarMountSource: An implementation for RARs using rarfile.
 - ZipMountSource: An implementation for ZIPs using zipfile.
 - FolderMountSource: An implementation taking an existing folder as input.

The "compositing" submodule contains MountSource implementations that offer
higher-level abstractions or functionalities on top of one or more input
MountSource implementation.

 - UnionMountSource: Takes multiple MountSource implementations and merges them.
 - SubvolumesMountSource: Takes multiple MountSource implementations and mounts
                          each in separate subfolders with specified names.
 - FileVersionLayer: Takes a MountSource as input, decodes the requested
                     paths, also accepting "<file>.version/<number>" paths,
                     and calls the methods of the MountSource with the given
                     file version.
 - AutoMountLayer: Takes one MountSource, goes over all its files and mounts
                   archives recursively in a similar manner to UnionMountSource.

The factory function 'open_mount_source' opens one of the format MountSource
implementations according to the file type.
For performance and maintenance reasons, there are almost no module-level
reimports, ergo you should specify the full module hierarchy for imports.

Example:

    from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar
    from ratarmountcore.mountsource.factory import open_mount_source

    archive = SQLiteIndexedTar("foo.tar")
    # or alternatively:
    archive = open_mount_source("foo.tar", recursive=True)

    archive.list("/")
    info = archive.lookup("/bar")

    print "Contents of /bar:"
    with archive.open(info) as file:
        print(file.read())
"""

from .MountSource import FileInfo, MountSource, create_root_file_info, merge_statfs
