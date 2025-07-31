
# Version 0.9.2 built on 2025-08-01

 - Querying compositing mount sources with empty string instead of `/` did not work correctly.
 - `SQLARMountSource`: Files with denormal paths did show empty mount points.
 - `SQLARMountSource`: Also mention cryptography module if missing.
 - `SQLiteIndexMountSource`: Do not delete SQLite files that are not ratarmount indexes.
 - Add join_threads to `SingleFileMountSource` and `FileVersionLayer` to avoid hangs when this call is not forwarded.
 - `SubvolumesMountSource`: `__exit__` did not correctly delegate to held mount sources.
 - `SubvolumesMountSource`: Normalize path returned by `get_mount_source`.
 - `SubvolumesMountSource`: Return subvolume root for direct folder access.


# Version 0.9.1 built on 2025-07-23

## Fixes

 - Python file object implementations did not return `True` for `closed` after closing them.
 - Patch broken TAR parsing for >8 GiB file with sparsity.
 - Patch wrong checksum for >8 GiB file with sparsity.
 - Support files larger than 8 GiB for GNU sparse detection.
 - Fix very long parsing time during GNU sparse detection with `--ignore-zeros`.
 - Make `@overrides` a test-time error not a runtime-error.
 - Make fsspec `tar://` protocol work. For most cases this should be avoided though because it does not
   use the performance benefits of ratarmount.
 - Use exceptions over asserts for functional SQLAR magic bytes checks so that it works with `python3 -O`
   optimization mode.
 - Detect Zstandard files created by pzstd, which start with a skippable frame.
 - Show the correct permissions for the archives opened via libarchive.


# Version 0.9.0 built on 2025-06-21

## Feature

 - Add support for SQLAR SQLite archives.
 - Add support for mounting .index.sqlite ratarmount indexes directly without the associated archive-
 - Add support for encrypted 7z files via py7zr.
 - Add support for EXT4 images.
 - Add support for SAR archives.
 - Add new method to specify fine-granular parallelization for each backend.
 - Make `--recursion-depth 0` only undo the compression on TAR files.
 - Show better error message when trying to open supported file formats with missing Python modules.
 - `Libarchive`: Add warning when file contents are encrypted and no password specified.

## Performance

 - Do not import everything in main module to aid some delayed import workflows for smaller latencies.

## API

 - Rename `AutoMountLayer.recursionDepth` to `maxRecursionDepth`.
 - Restructure ratarmountcore file hierarchy. Move `MountSource` implementations into subfolders.
 - Use `snake_case` for functions and class methods, one step further towards PEP 8.
 - Accept `os.PathLike` in `open_mount_source`.

## Fixes

 - The progress bar was wrong for parallelized XZ decompression.
 - Collect correct file permissions for SquashFS, RAR, ZIP, and Libarchive.
 - `FileVersionLayer`: Do not dispatch requests on .versions folder to underlying mount source.
 - `AutoMountLayer`: Account for recursions depths introduced by `SQLiteIndexedTar`.
 - Mounting `github://` without a prefix did not work correctly.
 - Avoid misdetection of images as TAR by libarchive.
 - `SubvolumeMountSource`: Return clone of root file info to avoid duplicate `userdata` elements.


# Version 0.8.2 built on 2025-05-30

## Fixes

 - Fix issues with Python 3.6, 3.7, and 3.8. This will be the last version release for Python 3.6
   because I have no CI to test for it.
 - Fix broken `SubvolumesMountSource.listDirModeOnly`, used by `ratarmount --disable-union-mount`.
 - Execute `VACUUM` on index file after deleting the temporary table.
 - Also accept SQL views as valid tables in case the format will change in a future version.


# Version 0.8.1 built on 2025-05-07

## Fixes

 - Add support for libfuse 3.17+.


# Version 0.8.0 built on 2024-11-01

## Features

 - Add fsspec implementation and register it as ratar://.
 - Add support for new formats: SquashFS, Git, FAT12, FAT16, FAT32.
 - Add support for fsspec backends. Archives and even index files can now be specified via URIs:
   dropbox://, ftp://, git://, github://, http://, https://, ipfs://, ipns://, s3://, ssh://, sftp://, smb://, webdav://.
 - Add support for remote and compressed index files. Ratarmount will automatically look for
   index files with .gz and other common extensions and extracts these into `/tmp/` or `RATARMOUNT_INDEX_TMPDIR`
   before using them.
 - `MountSource.open`: Add `buffering` argument to enable/disable buffering or set the buffer size.

## Fixes

 - Argument to `--gzip-seek-point-spacing` was ignored when using the rapidgzip backend.
 - Index creation did not work with default arguments with an archive in a read-only location.
 - Close sqlite3 dummy connection after querying the SQLite version.
 - Avoid resource leaks in case a `MountSource` constructor throws.
 - `SQLiteIndex`: Do note store checkMetadata callback as a member to avoid dependency cycles.
 - `SQLiteIndex`: Ignore errors when the connection is already closed.
 - `SQLiteIndexedTar`: Avoid resource leak when constructor fails.

## Performance

 - Import [compiled zip decrypter](https://github.com/mxmlnkn/fast-zip-decryption/) for 100x speedup for Standard ZIP 2.0 encrypted ZIP files
 - Speed up `readdir` and therefore simple use cases such as `find` to iterate all files and folders by 3x.
 - Avoid reading the whole appended TAR parts into memory for the check has-been-appended-to check.
 - Fix block size being ignored when reading everything via `io.BufferedReader`.
 - Do not use parallelization with possibly huge prefetches for simple file type checks.

## API

 - Add `getXdgCacheHome` into `ratarmountcore.utils`.
 - `SQLiteIndexedTar`: Fill indexFolders argument with sane defaults if not specified.


# Version 0.7.2 built on 2024-09-01

 - Userdata for root file info was wrong in `AutoMountLayer`.

# Version 0.7.1 built on 2024-06-02

 - Fix the missing indentation for the index version check.
 - Make `--help` and `--version` work even if `libarchive.so` is not installed.
 - Fix `LibarchiveMountSource` compatibility issues with older libarchive versions.
 - Fix the exception in `StenciledFile` when seeking before the file start.

# Version 0.7.0 built on 2024-04-07

 - Add libarchive backend and detection support for:
   grzip, lrzip, lz4, lzip, lzma, lzop, rpm, uuencode, compress, 7zip, ar, cab, deb, xar, cpio, iso, war, cxar.
 - Add `--transform` option to map each archive entry path via a regex to some user-specified one.
 - Upgrade rapidgzip from 0.10 to 0.13 to add zlib support. Other notable features are:
   - Window compression for reduced memory usage
   - The rapidgzip Python library now also bundles `IndexedBzip2File` from `indexed_bzip2`.
   - Enable checksum verification by default.
   - Support for decompression from non-seekable inputs such as stdin.
   - Avoid doubling memory usage during index import and export by streaming the data directly to the output file.
 - Remove `indexed_bzip2` dependency in favor of `rapidgzip`, which in the future should support even more formats.
 - Store backend name into the index and check that the index fits to the current backend / `MountSource`.
 - Store `isGnuIncremental` flag in the index.
 - Determine incremental archives from index rows to avoid seeks.
 - `utils.findModuleVersion`: Return version not name if `__version__` does not exist.
 - Apply specified priorities for opening all archives not just gzip.

# Version 0.6.5 built on 2024-04-06

 - The index should not be created for very small archives.
 - Root file info userdata was not initialized correctly.
 - Index validation did fail for TAR entries with more than 2 metadata blocks.
 - Do not check for folder consistency because parent folders get automatically added to the index.
 - Move `_createFileInfo` out of `MountSource` class to fix "protected-access" warning.
 - Joined files (`a.001`, `a.002`) did not work because of an accidentally shared list.
 - Do not check file header for zip, only for the footer, to detect self-extracting archives.

# Version 0.6.4 built on 2024-03-23

 - Return a valid file info and file version count for `/`.
 - Make the original archive viewable as an older file version when using `AutoMountLayer`.
 - Resolve symbolic links pointing outside the source folder given to `FolderMountSource` to not break them.
 - Do not return a valid `FileInfo` for invalid paths such as `../..`.
 - Make `--index-minimum-file-count` work for the TAR backend.

# Version 0.6.3 built on 2024-02-23

 - Add `__enter__` and `__exit__` to `MountSource` interface.
 - Properly close opened file objects in mount sources.
 - Fix `open` and `read` in `SubvolumesMountSource`.
 - Fix that `SQLiteIndexedTar` only heeded the order in `prioritizedBackends` when `rapidgzip` and `indexed_gzip`
   are specified instead of specifying only one, which should be prioritized.
 - Fix detection of self-extracting RAR files.
 - Improve the error message when a file cannot be read because of a missing dependency.
 - Improve debug message when the index does not yet contain a gzip index.

# Version 0.6.2 built on 2023-12-26

 - Fix faulty seek forward for file opened via `RarMountSource` when whence is not `io.SEEK_SET`.

# Version 0.6.1 built on 2023-10-29

 - Skip data offset collection for zip files because it takes too long and is unused.

# Version 0.6.0 built on 2023-09-03

 - Use `rapidgzip` instead of `indexed_gzip` by default.
 - Do not parallelize gzip on slow drives because `pread` is slower than
   sequential `read` on those.
 - Enable profiling output for rapidgzip with -d 2 or greater.
 - Do not check for GNU incremental TAR archive after an index has been loaded because
   it is only necessary for index creation. Note that this means that`SQLiteIndexedTar.isGnuIncremental`
   member will remain `False` when an index has been loaded.
 - Test for incremental archive after loading the compression index to avoid having
   to recreate it.
 - Fix missing warning for multi-frame requirement of Zstandard archives.
 - Fix unnecessary warning about mismatching `gzip-seek-point-spacing`
   when loading an index for
   archives without gzip compression.
 - Change the default value of the `SQLiteIndexedTar` constructor argument
   `gzipSeekPointSpacing` from 4 MiB tp 16 MiB to make it consistent with the ratarmount CLI option.

# Version 0.5.0 built on 2023-02-19

 - Split off SQLite backend from `SQLiteIndexedTar` into `SQLiteIndex`.
 - Remove obsolete `SQLiteIndexedTar.isDir`. Use `getFileInfo` instead.
 - Split up `getFileInfo` method into `getFileInfo`, `listDir`, and `fileVersions`.
 - Use XDG_CACHE_HOME according to FreeDesktop as default fallback
   when the archive folder is not writable.
 - Create an SQLite index file for zip archives to speed up metadata lookup.
 - Fix issue with folders for mounted zip files not being traversed with find.

# Version 0.4.0 built on 2022-11-13

 - `AutoMountLayer` now takes a `recursionDepth` argument instead of `recursive`.
 - Fix handling of paths in zip and rar files that contain `../`.
 - Add backend prioritization option to `SQLiteIndexedTar`.

# Version 0.3.2 built on 2022-06-25

 - Fix exception when trying to mount a RAR archive containing files without timestamps.

# Version 0.3.1 built on 2022-04-10

 - Fix duplicate mounting of uncompressed TARs inside TARs when using `--recursive`.

# Version 0.3.0 built on 2022-04-06

 - Relax the check for GNU incremental TAR detection because the prefix field
   might contain binary data.
 - Improve performance by factor ~5 and avoid storage requirements for index
   reading for gzip compressed archives.
 - Improve performance by ~40% and avoid storage requirements for index
   writing for gzip compressed archives.
 - Improve performance for indexing uncompressed TARs that have been
   appended to by only analyzing the new files.
 - Fix uncompressed archives having been appended very small files (<10 KiB)
   were not detected as changed by default.
 - Fix problem triggered by combining `--recursive` and `-P 0` with a recursive bzip2 archive.

# Version 0.2.4 built on 2022-04-04

 - Recursive mounting of nested uncompressed TARs did skip some files for depth > 2.

# Version 0.2.3 built on 2022-04-03

 - Fix uncaught exception when a folder contains an invalid character.

# Version 0.2.2 built on 2022-02-20

 - Improve performance for gzip files significantly by using a larger buffer.
 - Do not use `ParallelXZReader` for single-block xz files to avoid memory issues.

# Version 0.2.1 built on 2022-02-07

 - Fix (the last 1000) files not showing for truncated / incomplete TAR files.

# Version 0.2.0 built on 2022-01-15

 - Add support for GNU incremental TAR file format.
 - Extend listDir interface to also return FileInfo objects for each file.
 - Improve performance for large Union Mounts by keeping a path cache for read-only mount sources. This way,
   each FUSE getattr only has to query mount sources known to have the file instead of iterating over all of them.
 - Fix file objects returned by `SQLiteIndexedTar` to be thread-safe when reading and seeking.
 - Improve performance of index creation for uncompressed TARs by batching SQLite index insertions among others.
 - Add elapsed time output to progress indicator.

# Version 0.1.5 built on 2022-02-20

 - Improve performance for gzip files significantly by using a larger buffer.

# Version 0.1.4 built on 2021-12-21

 - Fix mounting of TAR fails if there are special characters like `#` in the path.
 - Fix recursive mounting of simple compressed files (file.bz2) inside TAR.
 - Fix file objects being returned by `SQLiteIndexedTar` not being independent from each other
   because of the shared underlying file object.

# Version 0.1.3 built on 2021-12-08

 - Workaround did test for the wrong libSqlite major version.

# Version 0.1.2 built on 2021-12-05

 - Avoid SQLite error for libsqlite 3.11.0 on Ubuntu 16.04.
 - Fix `<file object>.index.sqlite` files being created when opening file objects
   without specifying a tarFileName and when using writeIndex=True.

# Version 0.1.1 built on 2021-10-11

 - Importing did fail for all Python versions x.y.z where z <= 6.
 - Updating files in TAR with implicitly defined folders did not work.

# Version 0.1.0 built on 2021-10-04

 - First experimental version uploaded to PyPI.
