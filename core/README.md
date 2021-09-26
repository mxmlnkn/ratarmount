# Random Access Read-Only Tar Mount (Ratarmount) Library

This is the library used as backend by ratarmount (CLI).
For a full description including motivation and performance comparisons, see [ratarmount](https://github.com/mxmlnkn/ratarmount).


# Table of Contents

1. [Installation](#installation)
2. [Usage](#usage)


# Installation


## PIP Package Installation

In many cases a simple pip install should work:

```bash
pip install ratarmountcore[full]
```

If there is trouble with one of the compression dependencies, first try installing it without dependencies:

```bash
pip install ratarmountcore
```

And if that works, only install those dependencies you need, e.g.:

```bash
pip install ratarmountcore[bzip2,gzip]
```

You can install the latest development version with:

```bash
python3 -m pip install --user --force-reinstall 'git+https://github.com/mxmlnkn/ratarmount.git@develop#egginfo=ratarmountcore&subdirectory=core'
```


## Dependencies

Python 3.6+ and preferably pip 19.0+ are required.
These should be preinstalled on most systems.

Ratarmountcore has as few required dependencies as necessary in order to cause the least troubles on all possible systems.
This means that only uncompressed TAR and ZIP support will work by default.
All optional dependencies are offered as extras.


## Extras

Ratarmountcore offers these extras (optional dependencies):

 - full, bzip2, gzip, rar, xz, zip, zstd

Full includes all dependencies of the other extras.
The `zip` extra is currently only a placeholder because the built-in `zipfile` module is being used.

In order to install one of these extract, append them in brackets:

```bash
python3 -m pip install --user ratarmount[bzip2]
```

If you are installing on a system for which there exists no manylinux wheel, then you'll have to install dependencies required to build from source:

```bash
sudo apt install python3 python3-pip fuse build-essential software-properties-common zlib1g-dev libzstd-dev liblzma-dev
```


# Usage

This library offers an interface which is sufficient to work with FUSE.
This `MountSource` interface has methods for listing paths and getting file metadata and contents.

The ratarmountcore library offers multiple implementations of `MountSource` for different archive formats:

 - `SQLiteIndexedTar`: 
    This is the oldest and most powerful implementation.
    It supports fast access to files inside (compressed) TARs.
 - `RarMountSource`: An implementation for RARs using rarfile.
 - `ZipMountSource`: An implementation for ZIPs using zipfile.
 - `FolderMountSource`: An implementation taking an existing folder as input.

There also are these functional implementations of `MountSource`:

 - `UnionMountSource`: Takes multiple MountSource implementations and shows a merged view of their file hierarchy.
 - `FileVersionLayer`:
    Takes a MountSource as input, decodes the requested paths, also accepting `<file>.version/<number>` paths,
    and calls the methods of the `MountSource` with the given file version.
 - `AutoMountLayer`: 
    Takes one `MountSource`, goes over all its files and mounts archives recursively in a similar manner to `UnionMountSource`.

The factory function `open` opens one of the archive `MountSource` implementations according to the file type.

[![Mount Source Class Diagram](doc/MountSource.png)](doc/MountSource.svg)


## Example

```Python3
import ratarmountcore as rmc

archive = rmc.open("foo.tar", recursive=True)
archive.listDir("/")
info = archive.getFileInfo("/bar")

print "Contents of /bar:"
with archive.open(info) as file:
    print(file.read())
```
