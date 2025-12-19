<div align="center">

![Ratarmount Logo](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/ratarmount.svg "Purple 'ratar' and dark green 'mount'. The `raarmoun` letters pleasingly have the exact same height, and the two bars of the 't's are spread over them. The t-bars are animated to connect in a full horizontal line from time to time.")

# Random Access To Archived Resources (Ratarmount)

[![Python Version](https://img.shields.io/pypi/pyversions/ratarmount)](https://pypi.org/project/ratarmount/)
[![PyPI version](https://badge.fury.io/py/ratarmount.svg)](https://badge.fury.io/py/ratarmount)
[![Downloads](https://static.pepy.tech/badge/ratarmount/month)](https://pepy.tech/project/ratarmount)
[![Conda](https://img.shields.io/conda/v/conda-forge/ratarmount?color=dark-green)](https://anaconda.org/conda-forge/ratarmount)
</br>
[![Changelog](https://img.shields.io/badge/Changelog-Markdown-blue)](https://github.com/mxmlnkn/ratarmount/blob/master/CHANGELOG.md)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](http://opensource.org/licenses/MIT)
[![DOI](https://zenodo.org/badge/171697459.svg)](https://doi.org/10.5281/zenodo.10683766)
![GitHub repo size](https://img.shields.io/github/repo-size/mxmlnkn/ratarmount)
[![Build Status](https://github.com/mxmlnkn/ratarmount/actions/workflows/tests.yml/badge.svg)](https://github.com/mxmlnkn/ratarmount/actions/workflows/tests.yml)
[![Discord](https://img.shields.io/discord/783411320354766878?label=discord)](https://discord.gg/Wra6t6akh2)
[![Telegram](https://img.shields.io/badge/Chat-Telegram-%2330A3E6)](https://t.me/joinchat/FUdXxkXIv6c4Ib8bgaSxNg)

</div>

Ratarmount collects all file positions inside a TAR so that it can easily jump to and read from any file without extracting it.
It, then, **mounts** the **TAR** using [mfusepy](https://github.com/mxmlnkn/mfusepy), a fork of [fusepy](https://github.com/fusepy/fusepy), for read access just like [archivemount](https://github.com/cybernoid/archivemount/).
In [contrast](https://github.com/libarchive/libarchive#notes-about-the-library-design) to [libarchive](https://github.com/libarchive/libarchive), on which archivemount is based, random access and true seeking is supported.
And in contrast to [tarindexer](https://github.com/devsnd/tarindexer), which also collects file positions for random access, ratarmount offers easy access via FUSE and support for compressed TARs.

*Capabilities:*

 - **Random Access:** Care was taken to achieve fast random access inside compressed streams for bzip2, gzip, xz, and zstd and inside TAR files by building indices containing seek points.
 - **Highly Parallelized:** By default, all cores are used for parallelized algorithms like for the gzip, bzip2, and xz decoders.
   This can yield huge speedups on most modern processors but requires more main memory.
   It can be controlled or completely turned off using the `-P <cores>` option.
 - **Recursive Mounting:** Ratarmount will also mount TARs inside TARs inside TARs, ... recursively into folders of the same name, which is useful for the 1.31TB ImageNet data set.
 - **Mount Compressed Files:** You may also mount files with one of the supported compression schemes.
   Even if these files do not contain a TAR, you can leverage ratarmount's true seeking capabilities when opening the mounted uncompressed view of such a file.
 - **Read-Only Bind Mounting:** Folders may be mounted read-only to other folders for usecases like merging a backup TAR with newer versions of those files residing in a normal folder.
 - **Union Mounting:** Multiple TARs, compressed files, and bind mounted folders can be mounted under the same mountpoint.
 - **Write Overlay:** A folder can be specified as write overlay.
   All changes below the mountpoint will be redirected to this folder and deletions are tracked so that all changes can be applied back to the archive.
 - **Remote Files and Folders:** A remote archive or whole folder structure can be mounted similar to tools like [sshfs](https://github.com/libfuse/sshfs) thanks to the [filesystem_spec](https://github.com/fsspec/filesystem_spec) project.
   These can be specified with URIs as explained in the section ["Remote Files"](#remote-files).
   Supported remote protocols include: FTP, HTTP, HTTPS, SFTP, [SSH](https://github.com/fsspec/sshfs), Git, Github, [S3](https://github.com/fsspec/s3fs), Samba [v2 and v3](https://github.com/jborean93/smbprotocol), Dropbox, ... Many of these are very experimental and may be slow. Please open a feature request if further backends are desired.

A complete list of supported formats can be found [here](supported-formats).

# Examples

 - `ratarmount archive.tar.gz` to mount a compressed archive at a folder called `archive` and make its contents browsable.
 - `ratarmount --recursive archive.tar mountpoint` to mount the archive and recursively all its contained archives under a folder called `mountpoint`.
 - `ratarmount folder mountpoint` to bind-mount a folder.
 - `ratarmount folder1 folder2 mountpoint` to bind-mount a merged view of two (or more) folders under `mountpoint`.
 - `ratarmount folder archive.zip folder` to mount a merged view of a folder on top of archive contents.
 - `ratarmount -o modules=subdir,subdir=squashfs-root archive.squashfs mountpoint` to mount an archive subfolder `squashfs-root` under `mountpoint`.
 - `ratarmount http://server.org:80/archive.rar folder folder` Mount an archive that is accessible via HTTP range requests.
 - `ratarmount ssh://hostname:22/relativefolder/ mountpoint` Mount a folder hierarchy via SSH.
 - `ratarmount ssh://hostname:22//tmp/tmp-abcdef/ mountpoint`
 - `ratarmount github://mxmlnkn:ratarmount@v0.15.2/tests/ mountpoint` Mount a github repo as if it was checked out at the given tag or SHA or branch.
 - `AWS_ACCESS_KEY_ID=01234567890123456789 AWS_SECRET_ACCESS_KEY=0123456789012345678901234567890123456789 ratarmount s3://127.0.0.1/bucket/single-file.tar mounted` Mount an archive inside an S3 bucket reachable via a custom endpoint with the given credentials. Bogus credentials may be necessary for unsecured endpoints.


# Table of Contents

1. [Installation](#installation)
   1. [Installation via AppImage](#installation-via-appimage)
   2. [Installation via Package Manager](#installation-via-package-manager)
      1. [Arch Linux](#arch-linux)
   3. [System Dependencies for PIP Installation (Rarely Necessary)](#system-dependencies-for-pip-installation-rarely-necessary)
   4. [PIP Package Installation](#pip-package-installation)
   5. [Argument Completion](#argument-completion)
   6. [Graphical User Interface (GUI)](#graphical-user-interface-gui)
2. [Supported Formats](#supported-formats)
   1. [TAR compressions supported for random access](tar-compressions-supported-for-random-access)
   2. [Other supported archive formats](other-supported-archive-formats)
2. [Benchmarks](#benchmarks)
3. [The Problem](#the-problem)
4. [The Solution](#the-solution)
5. [Usage](#usage)
   1. [Metadata Index Cache](#metadata-index-cache)
   2. [Bind Mounting](#bind-mounting)
   3. [Union Mounting](#union-mounting)
   4. [File versions](#file-versions)
   5. [Compressed non-TAR files](#compressed-non-tar-files)
   6. [Xz and Zst Files](#xz-and-zst-files)
   7. [Remote Files](#remote-files)
   8. [Writable Mounting](#writable-mounting)
   9. [As a Library](#as-a-library)
   10. [Fsspec Integration](#fsspec-integration)
   11. [File Joining](#file-joining)


# Installation

You can install ratarmount either by simply downloading the [AppImage](https://github.com/mxmlnkn/ratarmount/releases) or via pip.
The latter might require [installing additional dependencies](#system-dependencies-for-pip-installation-rarely-necessary).
The latest nightly build AppImage can be found [here](https://github.com/mxmlnkn/ratarmount/releases/tag/nightly).

```bash
pip install ratarmount
```

If you want all [features](https://github.com/mxmlnkn/ratarmount/blob/master/pyproject.toml#L60-L76), some of which may possibly result in installation errors on some systems, install with:

```bash
pip install ratarmount[full]
```


## Installation via AppImage

The [AppImage](https://appimage.org/) files are attached under "Assets" on the [releases page](https://github.com/mxmlnkn/ratarmount/releases).
They require no installation and can be simply executed like a portable executable.
If you want to install it, you can simply copy it into any of the folders listed in your `PATH`.

```bash
appImageName=ratarmount-0.15.0-x86_64.AppImage
wget 'https://github.com/mxmlnkn/ratarmount/releases/download/v0.15.0/$appImageName'
chmod u+x -- "$appImageName"
./"$appImageName" --help  # Simple test run
sudo cp -- "$appImageName" /usr/local/bin/ratarmount  # Example installation
```

## Installation via Package Manager

[![Packaging status](https://repology.org/badge/vertical-allrepos/ratarmount.svg)](https://repology.org/project/ratarmount/versions)

### Arch Linux

Arch Linux's AUR offers ratarmount as [stable](https://aur.archlinux.org/packages/ratarmount) and [development](https://aur.archlinux.org/packages/ratarmount-git) package.
Use an [AUR helper](https://wiki.archlinux.org/title/AUR_helpers), like [yay](https://github.com/Jguer/yay) or [paru](https://github.com/Morganamilo/paru), to install one of them:

```console
# stable version
paru -Syu ratarmount
# development version
paru -Syu ratarmount-git
```

### Conda

```bash
conda install -c conda-forge ratarmount
```


## System Dependencies for PIP Installation (Rarely Necessary)

Python 3.6+, preferably pip 19.0+, FUSE, and sqlite3 are required.
These should be preinstalled on most systems.

On Debian-like systems like Ubuntu, you can install/update all dependencies using:

```bash
sudo apt install python3 python3-pip fuse sqlite3 unar libarchive13 lzop gcc liblzo2-dev
```

On macOS, you have to install [macFUSE](https://osxfuse.github.io/) and other optional dependencies with:

```bash
brew install macfuse unar libarchive lrzip lzop lzo
```

If you are installing on a system for which there exists no manylinux wheel, then you'll have to install further dependencies that are required to build some of the Python packages that ratarmount depends on from source:

```bash
sudo apt install \
    python3 python3-pip fuse \
    build-essential software-properties-common \
    zlib1g-dev libzstd-dev liblzma-dev cffi libarchive-dev liblzo2-dev gcc
```

## PIP Package Installation

Then, you can simply install ratarmount from PyPI:
```bash
pip install ratarmount
```

Or, if you want to test the latest version:
```bash
python3 -m pip install --user --force-reinstall \
    'git+https://github.com/mxmlnkn/ratarmount.git@develop#egginfo=ratarmountcore&subdirectory=core' \
    'git+https://github.com/mxmlnkn/ratarmount.git@develop#egginfo=ratarmount'
```

If there are troubles with the compression backend dependencies, you can try the pip `--no-deps` argument.
Ratarmount will work without the compression backends.
The hard requirements are `fusepy` and for Python versions older than 3.7.0 `dataclasses`.

## Argument Completion

Ratarmount has support for argument completion in bash and zsh via [argcomplete](https://github.com/kislyuk/argcomplete) if it is installed.

On Debian-like systems, this sets everything up in `/etc/bash_completion.d/global-python-argcomplete` to work out-of-the-box with any Python tool that supports argcomplete:

```bash
sudo apt install python3-argcomplete
# Restart your shell.
ratarmount --<tab><tab>
```

Manual installation also works:

```bash
pip install argcomplete

# Either add this to your .bashrc
eval "$( register-python-argcomplete ratarmount )"
# Or run this script to install argcomplete globally (into `~/.bash_completion` and `~/.zshenv`):
activate-global-python-argcomplete  # Requires a restart of your shell to.

ratarmount --<tab><tab>
```


## Graphical User Interface (GUI)

If a graphical user interface is wanted, give one of these a try:

 - [Ratarmount UI](https://github.com/jendap/ratarmount_ui): Created by Jan Prach based on GTK4, and with Gnome Nautilus integration
 - A work-in-progress Qt-based Ratarmount GUI by me is available on the [gui](https://github.com/mxmlnkn/ratarmount/tree/gui) branch. It can be installed with `pip install --user --force-reinstall \
    'git+https://github.com/mxmlnkn/ratarmount.git@gui#egginfo=ratarmount'{'core&subdirectory=core',}` and run with `ratarmount --gui <archive>`. It is still very experimental, but basic functionality should work. Feedback would be welcome.


# Supported Formats

## TAR compressions supported for random access

 - **BZip2** as provided by [indexed_bzip2](https://github.com/mxmlnkn/indexed_bzip2) as a backend, which is a refactored and extended version of [bzcat](https://github.com/landley/toybox/blob/c77b66455762f42bb824c1aa8cc60e7f4d44bdab/toys/other/bzcat.c) from [toybox](https://landley.net/code/toybox/). See also the [reverse engineered specification](https://github.com/dsnet/compress/blob/master/doc/bzip2-format.pdf).
 - **Gzip** and **Zlib** as provided by [rapidgzip](https://github.com/mxmlnkn/rapidgzip) or [indexed_gzip](https://github.com/pauldmccarthy/indexed_gzip) by Paul McCarthy. See also [RFC1952](https://tools.ietf.org/html/rfc1952) and [RFC1950](https://tools.ietf.org/html/rfc1950).
 - **Xz** as provided by [python-xz](https://github.com/Rogdham/python-xz) by Rogdham or [lzmaffi](https://github.com/r3m0t/backports.lzma) by Tomer Chachamu. See also [The .xz File Format](https://tukaani.org/xz/xz-file-format.txt).
 - **Zstd** as provided by [indexed_zstd](https://github.com/martinellimarco/indexed_zstd) by Marco Martinelli. See also [Zstandard Compression Format](https://github.com/facebook/zstd/blob/master/doc/zstd_compression_format.md).

## Other supported archive formats

 - **TAR, Docker/OCI Images** as provided by [CPython's tarfile module](https://docs.python.org/3/library/tarfile.html). This includes support for [Docker Images](https://github.com/moby/docker-image-spec) and [OCI images](https://github.com/opencontainers/image-spec), which are TAR files with specified layouts and metadata files.
 - **Rar** as provided by [rarfile](https://github.com/markokr/rarfile) by Marko Kreen. See also the [RAR 5.0 archive format](https://www.rarlab.com/technote.htm).
 - **SquashFS, AppImage, Snap, SIF** as provided by [PySquashfsImage](https://github.com/matteomattei/PySquashfsImage) by Matteo Mattei. There seems to be no authoritative, open format specification, only [this nicely-done reverse-engineered description](https://dr-emann.github.io/squashfs/squashfs.html), I assume based on the [source code](https://github.com/plougher/squashfs-tools). Note that [Snaps](https://snapcraft.io/docs/the-snap-format), [Appimages](https://github.com/AppImage/AppImageSpec/blob/master/draft.md#type-2-image-format), and [Singularity Image Format](https://github.com/apptainer/sif) are, or contain, SquashFS images, with an executable prepended for AppImages, and other data prepended for SIF files.
 - **Zip** as provided by [zipfile](https://docs.python.org/3/library/zipfile.html), which is distributed with Python itself. See also the [ZIP File Format Specification](https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT).
 - **7z** via [libarchive](https://github.com/libarchive/libarchive) or [py7zr](https://github.com/miurahr/py7zr) for encrypted 7z archives by Hiroshi Miura.
 - **FAT12/FAT16/FAT32/VFAT** as provided by [PyFatFS](https://github.com/nathanhi/pyfatfs) by Nathan-J. Hirschauer. See also [Microsoft's FAT32 File System Specification](https://download.microsoft.com/download/1/6/1/161ba512-40e2-4cc9-843a-923143f3456c/fatgen103.doc).
 - **EXT4** as provided by [python-ext4](https://github.com/Eeems/python-ext4) by Nathaniel van Diepen. See also the [Linux kernel docs for EXT4](https://docs.kernel.org/filesystems/ext4/).
 - **SQLAR** via [CPython's](https://docs.python.org/3/library/sqlite3.html) [sqlite3](https://sqlite.org/) module or via the [Python3 bindings](https://github.com/coleifer/sqlcipher3) to [sqlcipher](https://www.zetetic.net/sqlcipher/) for encrypted archives.
 - **HTML** files with embedded data URLs, such as those created by [Firefox's Save Page WE extension](https://addons.mozilla.org/en-US/firefox/addon/save-page-we/) or similar ones. The base64-encoded embedded files are exposed via the virtual file system in subfolders based on `data-src` URLs in a similar manner to the [`Page Info -> Media`](https://support.mozilla.org/en-US/kb/firefox-page-info-window#w_media) functionality.
 - **Ratarmount Indexes** can also be mounted directly without the associated archive. This can be useful for viewing the file tree hierarchy in cases where the contents are not required, e.g., to search in indexes to archives stored in cold storage. A longer term goal of mine would be some kind metadata database with computed hashes, thumbnails, and others that can be mounted and searched, similar to the [locate family of commands](https://en.wikipedia.org/wiki/Locate_(Unix)) on Linux, but for archival usage, i.e., for disconnected media, and with recursion into archives, something like [Tracker](https://wiki.ubuntu.com/Tracker) but less [intrusive](https://unix.stackexchange.com/questions/694065/how-to-really-completely-disable-gnome-tracker) and for browsing, not just searching in the metadata, something like [iRods](https://irods.org/) but less complicated. (If anyone knows of such a tool or needs help with it, please contact me, e.g., via an issue.)
 - **Many Others** as provided by [libarchive](https://github.com/libarchive/libarchive) via [python-libarchive-c](https://github.com/Changaco/python-libarchive-c).
   - Formats with tests:
     [7z](https://github.com/ip7z/7zip/blob/main/DOC/7zFormat.txt),
     ar,
     [cab](https://download.microsoft.com/download/4/d/a/4da14f27-b4ef-4170-a6e6-5b1ef85b1baa/[ms-cab].pdf),
     compress, cpio,
     [iso](http://www.brankin.com/main/technotes/Notes_ISO9660.htm),
     [lrzip](https://github.com/ckolivas/lrzip),
     [lzma](https://www.7-zip.org/a/lzma-specification.7z),
     [lz4](https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md),
     [lzip](https://www.ietf.org/archive/id/draft-diaz-lzip-09.txt),
     lzo,
     [warc](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/),
     xar.
   - Untested formats that might work or not: deb, grzip,
     [rpm](https://refspecs.linuxbase.org/LSB_4.1.0/LSB-Core-generic/LSB-Core-generic/pkgformat.html),
     [uuencoding](https://en.wikipedia.org/wiki/Uuencoding).
   - Beware that libarchive has no performant random access to files and to file contents.
     In order to seek or open a file, in general, it needs to be assumed that the archive has to be parsed from the beginning.
     If you have a performance-critical use case for a format only supported via libarchive,
     then please open a feature request for a faster customized archive format implementation.
     The hope would be to add suitable stream compressors such as "short"-distance LZ-based compressions to [rapidgzip](https://github.com/mxmlnkn/rapidgzip).


# Benchmarks

<div align="center">

[![Benchmark comparison between ratarmount, archivemount, and fuse-archive](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/benchmarks/plots/archivemount-comparison-2022-02-19.png)](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/benchmarks/plots/archivemount-comparison-2022-02-19.png)

</div>

 - Not shown in the benchmarks, but ratarmount can mount files with **preexisting index sidecar files** in under a second making it **vastly more efficient** compared to archivemount for every **subsequent mount**.
   Also, archivemount has no progress indicator making it very unlikely the user will wait hours for the mounting to finish.
   Fuse-archive, an iteration on archivemount, has the `--asyncprogress` option to give a progress indicator using the timestamp of a dummy file.
   Note that fuse-archive daemonizes instantly but the mount point will not be usable for a long time and everything trying to use it will hang until then when not using `--asyncprogress`!
 - **Getting file contents** of a mounted archive is generally **vastly faster** than archivemount and fuse-archive and does not increase with the archive size or file count resulting in the largest observed speedups to be around 5 orders of magnitude!
 - **Memory consumption** of ratarmount is mostly **less** than archivemount and mostly does not grow with the archive size.
   Not shown in the plots, but the memory usage will be much smaller when not specifying `-P 0`, i.e., when not parallelizing.
   The gzip backend grows linearly with the archive size because the data for seeking is thousands of times larger than the simple two 64-bit offsets required for bzip2.
   The memory usage of the zstd backend only seems humongous because it uses `mmap` to open.
   The memory used by `mmap` is not even counted as used memory when showing the memory usage with `free` or `htop`.
 - For empty files, mounting with ratarmount and archivemount does not seem be bounded by decompression nor I/O bandwidths but instead by the algorithm for creating the internal file index.
   This algorithm scales **linearly** for ratarmount and fuse-archive but seems to scale worse than even quadratically for archives containing more than 1M files when using archivemount.
   Ratarmount 0.10.0 improves upon earlier versions by batching SQLite insertions.
 - Mounting **bzip2** and **xz** archives has actually become **faster** than archivemount and fuse-archive with `ratarmount -P 0` on most modern processors because it actually uses more than one core for decoding those compressions. `indexed_bzip2` supports block **parallel decoding** since version 1.2.0.
 - **Gzip** compressed TAR files are two times slower than archivemount during first time mounting.
   It is not totally clear to me why that is because streaming the file contents after the archive being mounted is comparably fast, see the next benchmarks below.
   In order to have superior speeds for both of these, I am [experimenting](https://github.com/mxmlnkn/indexed_bzip2/tree/parallelgz) with a parallelized gzip decompressor like the prototype [pugz](https://github.com/Piezoid/pugz) offers for non-binary files only.
 - For the other cases, mounting times become roughly the same compared to archivemount for archives with 2M files in an approximately 100GB archive.
 - **Getting a lot of metadata** for archive contents as demonstrated by calling `find` on the mount point is an order of magnitude **slower** compared to archivemount. Because the C-based fuse-archive is even slower than ratarmount, the difference is very likely that archivemount uses the low-level FUSE interface while ratarmount and fuse-archive use the high-level FUSE interface.

<div align="center">

[![Reading bandwidth benchmark comparison between ratarmount, archivemount, and fuse-archive](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/benchmarks/plots/bandwidth-comparison.png)](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/benchmarks/plots/bandwidth-comparison.png)

</div>

 - Reading files from the archive with archivemount are scaling quadratically instead of linearly.
   This is because archivemount starts reading from the beginning of the archive for each requested I/O block.
   The block size depends on the program or operating system and should be in the order of 4 kiB.
   Meaning, the scaling is `O( (sizeOfFileToBeCopiedFromArchive / readChunkSize)^2 )`.
   Both, ratarmount and fuse-archive avoid this behavior.
   Because of this quadratic scaling, the average bandwidth with archivemount seems like it decreases with the file size.
 - Reading bz2 and xz are both an order of magnitude faster, as tested on my 12/24-core Ryzen 3900X, thanks to parallelization.
 - Memory is bounded in these tests for all programs but ratarmount is a lot more lax with memory because it uses a Python stack and because it needs to hold caches for a constant amount of blocks for parallel decoding of bzip2 and xz files.
   The zstd backend in ratarmount looks unbounded because it uses mmap, whose memory usage will automatically stop and be freed if the memory limit has been reached.
 - The peak for the xz decoder reading speeds happens because some blocks will be cached when loading the index, which is not included in the benchmark for technical reasons. The value for the 1 GiB file size is more realistic.


Further benchmarks can be viewed [here](benchmarks/BENCHMARKS.md).


# The Problem

You downloaded a large TAR file from the internet, for example the [1.31TB](http://academictorrents.com/details/564a77c1e1119da199ff32622a1609431b9f1c47) large [ImageNet](http://image-net.org/), and you now want to use it but lack the space, time, or a file system fast enough to extract all the 14.2 million image files.


<details>
<summary>Existing Partial Solutions</summary>

## Partial Solutions

### Archivemount

[Archivemount](https://github.com/cybernoid/archivemount/) seems to have large performance issues for too many files and large archive for both mounting and file access in version 0.8.7. A more in-depth comparison benchmark can be found [here](benchmarks/BENCHMARKS.md).

  - Mounting the 6.5GB ImageNet Large-Scale Visual Recognition Challenge 2012 validation data set, and then testing the speed with: `time cat mounted/ILSVRC2012_val_00049975.JPEG | wc -c` takes 250ms for archivemount and 2ms for ratarmount.
  - Trying to mount the 150GB [ILSVRC object localization data set](https://www.kaggle.com/c/imagenet-object-localization-challenge) containing 2 million images was given up upon after 2 hours. Ratarmount takes ~15min to create a ~150MB index and <1ms for opening an already created index (SQLite database) and mounting the TAR. In contrast, archivemount will take the same amount of time even for subsequent mounts.
  - Does not support recursive mounting. Although, you could write a script to stack archivemount on top of archivemount for all contained TAR files.

### Tarindexer

[Tarindex](https://github.com/devsnd/tarindexer) is a command line to tool written in Python which can create index files and then use the index file to extract single files from the tar fast. However, it also has some caveats which ratarmount tries to solve:

  - It only works with single files, meaning it would be necessary to loop over the extract-call. But this would require loading the possibly quite large tar index file into memory each time. For example for ImageNet, the resulting index file is hundreds of MB large. Also, extracting directories will be a hassle.
  - It's difficult to integrate tarindexer into other production environments. Ratarmount instead uses FUSE to mount the TAR as a folder readable by any other programs requiring access to the contained data.
  - Can't handle TARs recursively. In order to extract files inside a TAR which itself is inside a TAR, the packed TAR first needs to be extracted.


### TAR Browser

I didn't find out about [TAR Browser](https://github.com/tomorrow-nf/tar-as-filesystem/) before I finished the ratarmount script. That's also one of it's cons:

  - Hard to find. I don't seem to be the only one who has trouble finding it as it has one star on Github after 7 years compared to 45 stars for tarindexer after roughly the same amount of time.
  - Hassle to set up. Needs compilation and I gave up when I was instructed to set up a MySQL database for it to use. Confusingly, the setup instructions are not on its Github but [here](https://web.wpi.edu/Pubs/E-project/Available/E-project-030615-133259/unrestricted/TARBrowserFinal.pdf).
  - Doesn't seem to support recursive TAR mounting. I didn't test it because of the MysQL dependency but the code does not seem to have logic for recursive mounting.
  - Xz compression also is only block or frame based, i.e., only works faster with files created by [pixz](https://github.com/vasi/pixz) or [pxz](https://github.com/jnovy/pxz).

Pros:
  - supports bz2- and xz-compressed TAR archives

</details>

## The Solution

Ratarmount creates an index file with file names, ownership, permission flags, and offset information.
This sidecar is stored at the TAR file's location or in `~/.ratarmount/`.
Ratarmount can load that index file in under a second if it exists and then offers FUSE mount integration for easy access to the files inside the archive.

Here is a more recent test for version 0.2.0 with the new default SQLite backend:

  - TAR size: 124GB
  - Contains TARs: yes
  - Files in TAR: 1000
  - Files in TAR (including recursively in contained TARs): 1.26 million
  - Index creation (first mounting): 15m 39s
  - Index size: 146MB
  - Index loading (subsequent mounting): 0.000s
  - Reading a 64kB file: ~4ms
  - Running 'find mountPoint -type f | wc -l' (1.26M stat calls): 1m 50s

The reading time for a small file simply verifies the random access by using file seek to be working. The difference between the first read and subsequent reads is not because of ratarmount but because of operating system and file system caches.

<details>
<summary>Older test with 1.31 TB Imagenet (Fall 2011 release)</summary>

The test with the first version of ratarmount ([50e8dbb](https://github.com/mxmlnkn/ratarmount/commit/50e8dbb10696d51de2e613dee560662be580cbd4)), which used the, as of now removed, pickle backend for serializing the metadata index, for the [ImageNet data set](https://academictorrents.com/details/564a77c1e1119da199ff32622a1609431b9f1c47):

  - TAR size: 1.31TB
  - Contains TARs: yes
  - Files in TAR: ~26 000
  - Files in TAR (including recursively in contained TARs): 14.2 million
  - Index creation (first mounting): 4 hours
  - Index size: 1GB
  - Index loading (subsequent mounting): 80s
  - Reading a 40kB file: 100ms (first time) and 4ms (subsequent times)

Index loading is relatively slow with 80s because of the pickle backend, which now has been replaced with SQLite and should take less than a second now.

</details>


# Usage

## Command Line Options

See `ratarmount --help` or [here](https://raw.githubusercontent.com/mxmlnkn/ratarmount/master/tests/ratarmount-help.txt).

## Metadata Index Cache

In order to reduce the mounting time, the created index for random access
to files inside the tar will be saved to one of these locations. These
locations are checked in order and the first, which works sufficiently, will
be used. This is the default location order:

  1. <path to tar>.index.sqlite
  2. ~/.ratarmount/<path to tar: '/' -> '_'>.index.sqlite
     E.g., ~/.ratarmount/_media_cdrom_programm.tar.index.sqlite

This list of fallback folders can be overwritten using the `--index-folders`
option. Furthermore, an explicitly named index file may be specified using
the `--index-file` option. If `--index-file` is used, then the fallback
folders, including the default ones, will be ignored!

## Bind Mounting

The mount sources can be TARs and/or folders.  Because of that, ratarmount
can also be used to bind mount folders read-only to another path similar to
`bindfs` and `mount --bind`. So, for:

    ratarmount folder mountpoint

all files in `folder` will now be visible in mountpoint.

## Union Mounting

If multiple mount sources are specified, the sources on the right side will be
added to or update existing files from a mount source left of it. For example:

    ratarmount folder1 folder2 mountpoint

will make both, the files from folder1 and folder2, visible in mountpoint.
If a file exists in both multiple source, then the file from the rightmost
mount source will be used, which in the above example would be `folder2`.

If you want to update / overwrite a folder with the contents of a given TAR,
you can specify the folder both as a mount source and as the mount point:

    ratarmount folder file.tar folder

The FUSE option -o nonempty will be automatically added if such a usage is
detected. If you instead want to update a TAR with a folder, you only have to
swap the two mount sources:

    ratarmount file.tar folder folder

## File versions

If a file exists multiple times in a TAR or in multiple mount sources, then
the hidden versions can be accessed through special <file>.versions folders.
For example, consider:

    ratarmount folder updated.tar mountpoint

and the file `foo` exists both in the folder and as two different versions
in `updated.tar`. Then, you can list all three versions using:

    ls -la mountpoint/foo.versions/
        dr-xr-xr-x 2 user group     0 Apr 25 21:41 .
        dr-x------ 2 user group 10240 Apr 26 15:59 ..
        -r-x------ 2 user group   123 Apr 25 21:41 1
        -r-x------ 2 user group   256 Apr 25 21:53 2
        -r-x------ 2 user group  1024 Apr 25 22:13 3

In this example, the oldest version has only 123 bytes while the newest and
by default shown version has 1024 bytes. So, in order to look at the oldest
version, you can simply do:

    cat mountpoint/foo.versions/1

Note that these version numbers are the same as when used with tar's
`--occurrence=N` option.

## Prefix Removal

Use `ratarmount -o modules=subdir,subdir=<prefix>` to remove path prefixes
using the FUSE `subdir` module. Because it is a standard FUSE feature, the
`-o ...` argument should also work for other FUSE applications.

When mounting an archive created with absolute paths, e.g.,
`tar -P cf /var/log/apt/history.log`, you would see the whole `var/log/apt`
hierarchy under the mount point. To avoid that, specified prefixes can be
stripped from paths so that the mount target directory **directly** contains
`history.log`. Use `ratarmount -o modules=subdir,subdir=/var/log/apt/` to do
so. The specified path to the folder inside the TAR will be mounted to root,
i.e., the mount point.

## Compressed non-TAR files

If you want a compressed file not containing a TAR, e.g., `foo.bz2`, then
you can also use ratarmount for that. The uncompressed view will then be
mounted to `<mountpoint>/foo` and you will be able to leverage ratarmount's
seeking capabilities when opening that file.

## Xz and Zst Files

In contrast to bzip2 and gzip compressed files, true seeking on XZ and ZStandard files is only possible at block or frame boundaries.
This wouldn't be noteworthy, if both standard compressors for [xz](https://tukaani.org/xz/) and [zstd](https://github.com/facebook/zstd) were not by default creating unsuited files.
Even though both file formats do support multiple frames and XZ even contains a frame table at the end for easy seeking, both compressors write only a single frame and/or block out, making this feature unusable.
The standard zstd tool does not support setting smaller block sizes yet although an [issue](https://github.com/facebook/zstd/issues/2121) does exist.

In order to generate truly seekable compressed files, you'll have to use [pixz](https://github.com/vasi/pixz) for XZ files.
You can check with `xz -l <file>` for the number of streams and blocks in the generated XZ file.

For ZStandard, you can use `zstd -l <file>` to check that a file contains more than one frame and therefore is seekable.
These are some possibilities to create seekable ZStandard files:

 - [pzstd](https://github.com/facebook/zstd): It comes installed with the `zstd` Ubuntu/Debian package.
   Unfortunately, it it is in ["maintenance-only mode"](https://github.com/facebook/zstd/issues/3650#issuecomment-1997938922) even though there is no replacement for the multi-stream functionality.
   `zstd -T0` does use parallelism but creates only a single frame and hence unseekable file.
 - [zeekstd](https://github.com/rorosen/zeekstd): Rust implementation of the ZStandard Seekable Format.
   It includes a [CLI Tool](https://github.com/rorosen/zeekstd/tree/main/cli#zeekstd-cli), which can be used standalone or as a tar compressor `tar --use-compress-program 'zeekstd -' ...`.
 - [t2sz](https://github.com/martinellimarco/t2sz): There is a `deb` package on the releases page.
   If that cannot be used, it has to be compiled from the C sources using CMake.
 - [zstd-seekable-format-go](https://github.com/SaveTheRbtz/zstd-seekable-format-go): Some of the releases, e.g., [v0.7.1](https://github.com/SaveTheRbtz/zstd-seekable-format-go/releases/tag/v0.7.1) contain static binaries that can be downloaded and used without installation.
   Unfortunately, not all releases seem to have static binaries. So, if some other version is required it might be necessary to [install from source](https://github.com/SaveTheRbtz/zstd-seekable-format-go/issues/199) with a Go compiler.
 - You can manually split the original file into parts, compress those parts, and then concatenate those parts together to get a suitable multiframe ZStandard file.
   Here is a bash function, which can be used for that:

   <details>
   <summary>Bash script: createMultiFrameZstd</summary>

   ```bash
   createMultiFrameZstd()
   (
       # Detect being piped into
       if [ -t 0 ]; then
           file=$1
           frameSize=$2
           if [[ ! -f "$file" ]]; then echo "Could not find file '$file'." 1>&2; return 1; fi
           fileSize=$( stat -c %s -- "$file" )
       else
           if [ -t 1 ]; then echo 'You should pipe the output to somewhere!' 1>&2; return 1; fi
           echo 'Will compress from stdin...' 1>&2
           frameSize=$1
       fi
       if [[ ! $frameSize =~ ^[0-9]+$ ]]; then
           echo "Frame size '$frameSize' is not a valid number." 1>&2
           return 1
       fi

       # Create a temporary file. I avoid simply piping to zstd
       # because it wouldn't store the uncompressed size.
       if [[ -d /dev/shm ]]; then frameFile=$( mktemp --tmpdir=/dev/shm ); fi
       if [[ -z $frameFile ]]; then frameFile=$( mktemp ); fi
       if [[ -z $frameFile ]]; then
           echo "Could not create a temporary file for the frames." 1>&2
           return 1
       fi

       if [ -t 0 ]; then
           true > "$file.zst"
           for (( offset = 0; offset < fileSize; offset += frameSize )); do
               dd if="$file" of="$frameFile" bs=$(( 1024*1024 )) \
                  iflag=skip_bytes,count_bytes skip="$offset" count="$frameSize" 2>/dev/null
               zstd -c -q -- "$frameFile" >> "$file.zst"
           done
       else
           while true; do
               dd of="$frameFile" bs=$(( 1024*1024 )) \
                  iflag=count_bytes count="$frameSize" 2>/dev/null
               # pipe is finished when reading it yields no further data
               if [[ ! -s "$frameFile" ]]; then break; fi
               zstd -c -q -- "$frameFile"
           done
       fi

       'rm' -f -- "$frameFile"
   )
   ```

   In order to compress a file named `foo` into a multiframe zst file called `foo.zst`, which contains frames sized 4MiB of uncompressed ata, you would call it like this:

   ```bash
   createMultiFrameZstd foo  $(( 4*1024*1024 ))
   ```

   It also works when being piped to. This can be useful for recompressing files to avoid having to decompress them first to disk.

   ```bash
   lbzip2 -cd well-compressed-file.bz2 | createMultiFrameZstd $(( 4*1024*1024 )) > recompressed.zst
   ```

   </details>


## Remote Files

The [fsspec](https://github.com/fsspec/filesystem_spec) API backend adds support for mounting many remote archive or folders.
Please refer to the linked respective backend documentation to see the full configuration options, especially for specifying credentials.
Some often-used configuration environment variables are copied here for easier viewing.

| Symbol        | Description               |
| ------------- | ------------------------- |
| `[something]` | Optional "something"      |
| `(one\|two)`  | Either "one" or "two"     |

 - `git://[path-to-repo:][ref@]path/to/file`</br>
   Uses the current path if no repository path is specified.
   Backend: [ratarmountcore](https://github.com/mxmlnkn/ratarmount/blob/master/core/ratarmountcore/GitMountSource.py)
   via [pygit2](https://github.com/libgit2/pygit2)
 - `github://org:repo@[sha]/path-to/file-or-folder`</br>
   Example: `github://mxmlnkn:ratarmount@v0.15.2/tests/single-file.tar`</br>
   Backend: [fsspec](https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/github.py)
 - `http[s]://hostname[:port]/path-to/archive.rar`</br>
   Backend: [fsspec](https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/http.py)
   via [aiohttp](https://github.com/aio-libs/aiohttp)
 - `(ipfs|ipns)://content-identifier`</br>
   Example: `ipfs daemon & sleep 2 && ratarmount -f ipfs://QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG mounted`</br>
   Backend: [fsspec/ipfsspec](https://github.com/fsspec/ipfsspec)</br>
   Tries to connect to running local [`ipfs daemon`](https://github.com/ipfs/kubo) instance by default, which needs to be started beforehand.
   ~~Alternatively, a (public) gateway can be specified with the environment variable `IPFS_GATEWAY`, e.g., `https://127.0.0.1:8080`.~~
   Specifying a public gateway does not (yet) work because of [this](https://github.com/fsspec/ipfsspec/issues/39) issue.
 - `s3://[endpoint-hostname[:port]]/bucket[/single-file.tar[?versionId=some_version_id]]`</br>
   Backend: [fsspec/s3fs](https://github.com/fsspec/s3fs) via [boto3](https://github.com/boto/boto3)</br>
   The URL will default to AWS according to the Boto3 library defaults when no endpoint is specified.
   Boto3 will check, among others, [these environment variables](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html), for credentials:
    - `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, `AWS_DEFAULT_REGION`

   [fsspec/s3fs](https://github.com/fsspec/s3fs) furthermore supports this environment variable:
    - [`FSSPEC_S3_ENDPOINT_URL`](https://github.com/fsspec/s3fs/pull/704), e.g., `http://127.0.0.1:8053`
 - `ftp://[user[:password]@]hostname[:port]/path-to/archive.rar`</br>
   Backend: [fsspec](https://github.com/fsspec/filesystem_spec/blob/master/fsspec/implementations/ftp.py)
   via [ftplib](https://docs.python.org/3/library/ftplib.html)
 - `(ssh|sftp)://[user[:password]@]hostname[:port]/path-to/archive.rar`</br>
   Backend: [fsspec/sshfs](https://github.com/fsspec/sshfs)
   via [asyncssh](https://github.com/ronf/asyncssh)</br>
   The usual configuration via [`~/.ssh/config`](https://linux.die.net/man/5/ssh_config) is supported.
 - `smb://[workgroup;][user:password@]server[:port]/share/folder/file.tar`
 - `webdav://[user:password@]host[:port][/path]`</br>
   Backend: [webdav4](https://github.com/skshetry/webdav4) via [httpx](https://github.com/encode/httpx)</br>
   Environment variables: `WEBDAV_USER`, `WEBDAV_PASSWORD`
 - `dropbox://path`</br>
   Backend: [fsspec/dropboxdrivefs](https://github.com/fsspec/dropboxdrivefs) via [dropbox-sdk-python](https://github.com/dropbox/dropbox-sdk-python)</br>
   Follow [these instructions](https://dropbox.tech/developers/generate-an-access-token-for-your-own-account) to create an [app](https://www.dropbox.com/developers/apps). Check the `files.metadata.read` and `files.content.read` permissions and press "submit" and **after** that create the (long) OAuth 2 token and store it in the environment variable `DROPBOX_TOKEN`. Ignore the (short) app key and secret. This creates a corresponding app folder that can be filled with data.

[Many other](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations) fsspec-based projects may also work when installed.

This functionality of ratarmount offers a hopefully more-tested and out-of-the-box experience over the experimental [fsspec.fuse](https://filesystem-spec.readthedocs.io/en/latest/features.html#mount-anything-with-fuse) implementation.
And, it also works in conjunction with the other features of ratarmount such as union mounting and recursive mounting.

Index files specified with `--index-file` can also be compressed and/or be an fsspec ([chained](https://filesystem-spec.readthedocs.io/en/latest/features.html#url-chaining)) URL, e.g., `https://host.org/file.tar.index.sqlite.gz`.
In such a case, the index file will be downloaded and/or extracted into the default temporary folder.
If the default temporary folder has insufficient disk space, it can be changed by setting the `RATARMOUNT_INDEX_TMPDIR` environment variable.


## Writable Mounting

The  `--write-overlay <folder>` option can be used to create a writable mount point.
The original archive will not be modified.

 - File creations will create these files in the specified overlay folder.
 - File deletions and renames will be registered in a database that also resides in the overlay folder.
 - File modifications will copy the file from the archive into the overlay folder before applying the modification.

This overlay folder can be stored alongside the archive or it can be deleted after unmounting the archive.
This is useful when building the executable from a source tarball without extracting.
After installation, the intermediary build files residing in the overlay folder can be safely removed.

If it is desired to apply the modifications to the original archive, then the `--commit-overlay` can be prepended to the original ratarmount call.

Here is an example for applying modifications to a writable mount and then committing those modifications back to the archive:

 1. Mount it with a write overlay and add new files. The original archive is not modified.
    ```bash
    ratarmount --write-overlay example-overlay example.tar example-mount-point
    echo "Hello World" > example-mount-point/new-file.txt
    ```

 2. Unmount. Changes persist solely in the overlay folder.
    ```bash
    fusermount -u example-mount-point
    ```

 3. Commit changes to the original archive.
    ```bash
    ratarmount --commit-overlay --write-overlay example-overlay example.tar example-mount-point
    ```
    Output:
    ```bash
    To commit the overlay folder to the archive, these commands have to be executed:
    
        tar --delete --null --verbatim-files-from --files-from='/tmp/tmp_ajfo8wf/deletions.lst' \
            --file 'example.tar' 2>&1 |
           sed '/^tar: Exiting with failure/d; /^tar.*Not found in archive/d'
        tar --append -C 'zlib-wiki-overlay' --null --verbatim-files-from --files-from='/tmp/tmp_ajfo8wf/append.lst' --file 'example.tar'

    Committing is an experimental feature!
    Please confirm by entering "commit". Any other input will cancel.
    > 
    Committed successfully. You can now remove the overlay folder at example-overlay.
    ```

 4. Verify the modifications to the original archive.
    ```bash
    tar -tvlf example.tar
    ```
    Output:
    ```
    -rw-rw-r-- user/user 652817 2022-08-08 10:44 example.txt
    -rw-rw-r-- user/user     12 2023-02-16 09:49 new-file.txt
    ```

 5. Remove the obsole write overlay folder.
    ```bash
    rm -r example-overlay
    ```

## As a Library

Ratarmount can also be used as a library.
Using [ratarmountcore](core/), files inside archives can be accessed directly from Python code without requiring FUSE.
For a more detailed description, see the [ratarmountcore readme here](core/).


## Fsspec Integration

To use all fsspec features, either install via `pip install ratarmount[fsspec]` or `pip install ratarmount[fsspec]`.
It should also suffice to simply `pip install fsspec` if ratarmountcore is already installed.
The optional [fsspec](https://github.com/fsspec/filesystem_spec) integration is threefold:

 1. Files can be specified on the command line via URLs pointing to remotes as explained in [this section](#remote-files).
 2. A `ratarmountcore.MountSource` wrapping fsspec `AbstractFileSystem` [implementation](https://github.com/mxmlnkn/ratarmount/blob/master/core/ratarmountcore/SQLiteIndexedTarFsspec.py) has been added.
    A specialized `SQLiteIndexedTarFileSystem` as a more performant and direct replacement for `fsspec.implementations.TarFileSystem` has also been added.
    ```python3
    from ratarmountcore.SQLiteIndexedTarFsspec import SQLiteIndexedTarFileSystem as ratarfs
    fs = ratarfs("tests/single-file.tar")
    print("Files in root:", fs.ls("/", detail=False))
    print("Contents of /bar:", fs.cat("/bar"))
    ```
 3. During installation ratarmountcore registers the `ratar://` protocol [with fsspec](https://filesystem-spec.readthedocs.io/en/latest/developer.html#implementing-a-backend) via an [entrypoint](https://setuptools.pypa.io/en/latest/userguide/quickstart.html#entry-points-and-automatic-script-creation) group.
    This enables usages with `fsspec.open`.
    The fsspec [URL chaining](https://filesystem-spec.readthedocs.io/en/latest/features.html#url-chaining) feature must be used in order for this to be useful.
    Example for opening the file `bar`, which is contained inside the file `tests/single-file.tar.gz` with ratarmountcore:
    ```python3
    import fsspec
    with fsspec.open("ratar://bar::file://tests/single-file.tar.gz") as file:
        print("Contents of file bar:", file.read())
    ```
    This also [works with pandas](https://pandas.pydata.org/docs/user_guide/io.html#reading-writing-remote-files):
    ```python3
    import fsspec
    import pandas as pd
    with fsspec.open("ratar://bar::file://tests/single-file.tar.gz", compression=None) as file:
        print("Contents of file bar:", file.read())
    ```
    The `compression=None` argument is currently necessary because of [this](https://github.com/pandas-dev/pandas/issues/60028) Pandas bug.


## File Joining

Files with sequentially numbered extensions can be mounted as a joined file.
If it is an archive, then the joined archive file will be mounted.
Only one of the files, preferably the first one, should be specified.
For example:

```bash
base64 /dev/urandom | head -c $(( 1024 * 1024 )) > 1MiB.dat
tar -cjf- 1MiB.dat | split -d --bytes=320K - file.tar.gz.
ls -la
# 320K  file.tar.gz.00
# 320K  file.tar.gz.01
# 138K  file.tar.gz.02
ratarmount file.tar.gz.00 mounted
ls -la mounted
# 1.0M  1MiB.dat
```


## Mount Point Control Interface

The FUSE mount contains a special hidden `.ratarmount-control` folder with special files inside it.

 - `.ratarmount-control/output`: Contains the errors and log output of the ratarmount process.
   Especially useful when it runs in the background. Alternatively, `-f` can be used to keep it in the foreground.
 - `.ratarmount-control/command`: Command line invocations can be written into this file to invoke another ratarmount subprocess. Command lines must start with `ratarmount<delimiter>`, where `<delimiter>` can be ` ` (space), `\n` (newline), or `\0` (null byte).

For example, try:

```bash
ratarmount --control-interface mounted
echo "ratarmount -d 3 $PWD/tests/single-file.tar $HOME/mounted" > mounted/.ratarmount-control/command
sleep 1
cat mounted/.ratarmount-control/command
```

## HTML support

HTML files with embedded data files, e.g., as saved with [Firefox's Save Page WE extension](https://addons.mozilla.org/en-US/firefox/addon/save-page-we/) can be mounted to expose these embedded files as actual files in a file hierarchy.
This can be used to inspect resources similar to [Firefox's Page Info Window](https://support.mozilla.org/en-US/kb/firefox-page-info-window).

This HTML file:

```html
<img data-savepage-src="https://example.com/logo.png"
     src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==">
```

will be mounted as:
```
/mnt/html/
└── https:/example.com/logo.png
```
