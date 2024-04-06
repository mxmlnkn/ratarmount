
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
