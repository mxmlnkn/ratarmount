
# Version 1.1.1 built on 2025-07-23

## Fixes

 - Fix possible issue when deleting from write overlay.
 - Do not filter chained fsspec protocols as unknown protocol.
 - Return the correct number of blocks for files. This fixes results from `du`.
 - Improve automatic mount point inference.

# Version 1.1.0 built on 2025-06-21

## Features

 - Add support for argument completion in your shell with `argcomplete` if it is installed.
 - Make `--unmount` accept multiple mount points to unmount.
 - Add support for extended file attributes.
 - Extract libfuse3 support into `mfusepy`, a fork of `fusepy`, and depend on it.
 - Add `--log-file` option to write output to file when daemonized.

## Performance

 - Try backends in smarter order based on file suffix.


# Version 1.0.0 built on 2024-11-01

Semantic versioning of GUIs and CLIs is not easy to define.
The simplest GUI usage has not changed since the first version and even the index file format has been mostly compatible since version 0.2.0 and there is a lot of code for version checking.
Based on the [semver](https://semver.org/) FAQ, the 1.0.0. release should probably have been a long time ago.
Here it is now!

## Features

 - Bundle modified fusepy in order to add libfuse3 support in case only that one is installed.
   Contributions to the [mfusepy](https://github.com/mxmlnkn/mfusepy) fork would be welcome!
 - Add message for first time users to show the result mount point.
 - (ratarmountcore 0.8.0) Add fsspec implementation and register it as ratar://.
 - (ratarmountcore 0.8.0) Add support for new formats: SquashFS, Git, FAT12, FAT16, FAT32.
 - (ratarmountcore 0.8.0) Add support for fsspec backends. Archives and even index files can now be specified via URIs:
   dropbox://, ftp://, git://, github://, http://, https://, ipfs://, ipns://, s3://, ssh://, sftp://, smb://, webdav://.
 - (ratarmountcore 0.8.0) Add support for remote and compressed index files. Ratarmount will automatically look for
   index files with .gz and other common extensions and extracts these into `/tmp/` or `RATARMOUNT_INDEX_TMPDIR`
   before using them.

## Performance

 - Disable Python-side buffering when opening files via FUSE.
 - Forward underlying archive block sizes to statfs and stat implementations.
 - (ratarmountcore 0.8.0) Import [compiled zip decrypter](https://github.com/mxmlnkn/fast-zip-decryption/) for 100x speedup for Standard ZIP 2.0 encrypted ZIP files
 - (ratarmountcore 0.8.0) Speed up `readdir` and therefore simple use cases such as `find` to iterate all files and folders by 3x.
 - (ratarmountcore 0.8.0) Avoid reading the whole appended TAR parts into memory for the check has-been-appended-to check.
 - (ratarmountcore 0.8.0) Fix block size being ignored when reading everything via `io.BufferedReader`.
 - (ratarmountcore 0.8.0) Do not use parallelization with possibly huge prefetches for simple file type checks.

## Fixes

 - `statfs` did not work when using a write overlay and calling it on a file not in the overlay folder.
 - Union mounting of inputs with the same name, even if in different folders, ignored all but the first.
 - Suppress teardown warning in case the mount source was not even created yet.
 - Make mounting work with only a write overlay.
 - Avoid hangs and errors caused by non-joined threads before forking into the background by checking for running threads.
 - Set locale to C when calling GNU tar to get more reproducible output on `--commit-overlay`.
 - (ratarmountcore 0.8.0) Argument to `--gzip-seek-point-spacing` was ignored when using the rapidgzip backend.
 - (ratarmountcore 0.8.0) Index creation did not work with default arguments with an archive in a read-only location.
 - (ratarmountcore 0.8.0) Close sqlite3 dummy connection after querying the SQLite version.
 - (ratarmountcore 0.8.0) Avoid resource leaks in case a `MountSource` constructor throws.


# Version 0.15.2 built on 2024-09-01

 - Setting either the owner or group for a file with `--write-overlay` would reset the group or user respectively.
 - Setting owner or group to 0 (root) with `--write-overlay` was not working.
 - (ratarmountcore 0.7.2) Fix error when combining `--recursive` or `--recursion-depth` with `--write-overlay`.

# Version 0.15.1 built on 2024-06-02

 - Show install suggestions when FUSE is missing.
 - File deletion with `--commit-overlay` did not work.
 - (ratarmountcore 0.7.1) Fix the missing indentation for the index version check.
 - (ratarmountcore 0.7.1) Make `--help` and `--version` work even if `libarchive.so` is not installed.
 - (ratarmountcore 0.7.1) Fix `LibarchiveMountSource` compatibility issues with older libarchive versions.
 - (ratarmountcore 0.7.1) Fix the exception in `StenciledFile` when seeking before the file start.

# Version 0.15.0 built on 2024-04-07

 - Print indicators for versions of loaded shared libraries.
 - (ratarmountcore 0.7.0) Add libarchive backend and detection support for:
   grzip, lrzip, lz4, lzip, lzma, lzop, rpm, uuencode, compress, 7zip, ar, cab, deb, xar, cpio, iso, war, cxar.
 - (ratarmountcore 0.7.0) Add `--transform` option to map each archive entry path via a regex to some user-specified one.
 - (ratarmountcore 0.7.0) Upgrade rapidgzip from 0.10 to 0.13 to add zlib support. Other notable features are:
 - (ratarmountcore 0.7.0) Remove `indexed_bzip2` dependency in favor of `rapidgzip`, which in the future should support even more formats.
 - (ratarmountcore 0.7.0) Store backend name into the index and check that the index fits to the current backend / `MountSource`.
 - (ratarmountcore 0.7.0) Store `isGnuIncremental` flag in the index.
 - (ratarmountcore 0.7.0) Determine incremental archives from index rows to avoid seeks.
 - (ratarmountcore 0.7.0) `utils.findModuleVersion`: Return version not name if `__version__` does not exist.
 - (ratarmountcore 0.7.0) Apply specified priorities for opening all archives not just gzip.

# Version 0.14.2 built on 2024-04-06

 - Do not check mount point because of faulty `os.path.ismount`, simply try fusermount.
 - Avoid total I/O hang when lazy-mounting a folder with archives onto itself
 - (ratarmountcore 0.6.4) Return a valid file info and file version count for `/`.
 - (ratarmountcore 0.6.4) Make the original archive viewable as an older file version when using `AutoMountLayer`.
 - (ratarmountcore 0.6.4) Resolve symbolic links pointing outside the source folder given to `FolderMountSource` to not break them.
 - (ratarmountcore 0.6.4) Do not return a valid `FileInfo` for invalid paths such as `../..`.
 - (ratarmountcore 0.6.4) Make `--index-minimum-file-count` work for the TAR backend.
 - (ratarmountcore 0.6.5) The index should not be created for very small archives.
 - (ratarmountcore 0.6.5) Root file info userdata was not initialized correctly.
 - (ratarmountcore 0.6.5) Index validation did fail for TAR entries with more than 2 metadata blocks.
 - (ratarmountcore 0.6.5) Do not check for folder consistency because parent folders get automatically added to the index.
 - (ratarmountcore 0.6.5) Move `_createFileInfo` out of `MountSource` class to fix "protected-access" warning.
 - (ratarmountcore 0.6.5) Joined files (`a.001`, `a.002`) did not work because of an accidentally shared list.
 - (ratarmountcore 0.6.5) Do not check file header for zip, only for the footer, to detect self-extracting archives.

# Version 0.14.1 built on 2024-02-23

 - Fix `AttributeError: module 'fuse' has no attribute 'errno'`.
 - Fix `--commit-overlay`, which did not add newly created empty folders to TARs.
 - Do not ask for confirmation if there is nothing to commit when using `--commit-overlay`.
 - Improve unmounting with `ratarmount -u`, especially with the AppImage.
 - (ratarmountcore 0.6.3) Properly close opened file objects in mount sources.
 - (ratarmountcore 0.6.3) Fix `--disable-union-mount`, which returned an I/O error.
 - (ratarmountcore 0.6.3) Fix that `--use-backend=indexed_gzip` still did use `rapidgzip`.
   It only worked when both were specified.
 - (ratarmountcore 0.6.3) Fix detection of self-extracting RAR files.
 - (ratarmountcore 0.6.3) Improve the error message when a file cannot be read because of a missing dependency.
 - (ratarmountcore 0.6.3) Improve debug message when the index does not yet contain a gzip index.
 - (ratarmountcore 0.6.2) Fix faulty seek forward for files opened via `RarMountSource` when whence is not `io.SEEK_SET`.
 - (ratarmountcore 0.6.1) Skip data offset collection for zip files because it takes too long and is unused.

# Version 0.14.0 built on 2023-09-03

 - Add `--disable-union-mount` option to present multiple archives in subfolders under the mount point.
 - Add fine-grained control over the union mount cache:
   - `--union-mount-cache-max-depth`
   - `--union-mount-cache-max-entries`
   - `--union-mount-cache-timeout`
 - Only use the number of cores the ratarmount process has affinity for by default.
 - Disable auto-detection of GNU incremental TAR archives by default.
   Use `--detect-gnu-incremental` or better `--gnu-incremental`/`--no-gnu-incremental`
   instead.
 - Add `--index-minimum-file-count` with sane default (1000) to avoid creating index files for
   rather small ZIP archives.
 - Apply name change: pragzip -> rapigzip.
 - Fix ambiguous FUSE behavior that resulted in `find` not descending into recursively
   mounted subarchives.
 - (ratarmountcore 0.6.0) Use `rapidgzip` instead of `indexed_gzip` by default.
 - (ratarmountcore 0.6.0) Do not parallelize gzip on slow drives because `pread` is slower than
   sequential `read` on those.
 - (ratarmountcore 0.6.0) Enable profiling output for rapidgzip with -d 2 or greater.
 - (ratarmountcore 0.6.0) Do not check for GNU incremental TAR archive after an index has been loaded because
   it is only necessary for index creation. Note that this means that`SQLiteIndexedTar.isGnuIncremental`
   member will remain `False` when an index has been loaded.
 - (ratarmountcore 0.6.0) Test for incremental archive after loading the compression index to avoid having
   to recreate it.
 - (ratarmountcore 0.6.0) Fix missing warning for multi-frame requirement of Zstandard archives.
 - (ratarmountcore 0.6.0) Fix unnecessary warning about mismatching `gzip-seek-point-spacing`
   when loading an index for
   (ratarmountcore 0.6.0) archives without gzip compression.
 - (ratarmountcore 0.6.0) Change the default value of the `SQLiteIndexedTar` constructor argument
   `gzipSeekPointSpacing` from 4 MiB tp 16 MiB to make it consistent with the ratarmount CLI option.

# Version 0.13.0 built on 2023-02-19

 - (ratarmountcore 0.5.0) Use XDG_CACHE_HOME according to FreeDesktop as default fallback
   when the archive folder is not writable.
 - (ratarmountcore 0.5.0) Create an SQLite index file for zip archives to speed up metadata lookup.
 - (ratarmountcore 0.5.0) Fix issue with folders for mounted zip files not being traversed with find.

# Version 0.12.0 built on 2022-11-13

 - Add --recursion-depth argument for more fine-grained recursion control.
 - Add support to show a joined view of split files, e.g., <file>.001, and also support split archives.
 - Add --use-backend option to choose between multiple available backends, e.g., 'lzmaffi' and 'xz' for xz files.
 - Add support for pragzip when it has been installed and specified with '--use-backend pragzip'.
   Pragzip offers unique parallelized decompression of arbitrary gzip files. Using a 12-core processor,
   it can show speedups of 12 over gzip and speedups of 8 over pigz for sequentially decoding base64 files.
   Furthermore, similar to indexed_bzip2 it is based on a block cache and therefore can substantially speed up
   concurrent access streams as might happen when accessing multiple files through ratarmount concurrently.
   Currently, indexed_gzip will still be used to create the index in the first pass and pragzip is used
   for subsequent accesses with '--use-backend pragzip'.
 - (ratarmountcore 0.4.0) AutoMountLayer now takes a 'recursionDepth' argument instead of 'recursive'.
 - (ratarmountcore 0.4.0) Fix handling of paths in zip and rar files that contain '../'.
 - (ratarmountcore 0.4.0) Add backend prioritization option to SQLiteIndexedTar.

# Version 0.11.3 built on 2022-06-25

 - Fix files with leading "./" not being deleted when using --commit-overlay
 - Fix files directly under the mount point not being deleted correctly with --write-overlay.
 - (ratarmountcore 0.3.2) Fix exception when trying to mount a RAR archive containing files without timestamps.

# Version 0.11.2 built on 2022-05-27

 - Fix --password option not working.

# Version 0.11.1 built on 2022-04-10

 - Fix compatibility of --commit-overlay with older GNU tar versions by removing the redundant --verbatim-file option.
 - (ratarmountcore 0.3.1) Fix duplicate mounting of uncompressed TARs inside TARs when using --recursive.

# Version 0.11.0 built on 2022-04-06

 - Add --write-overlay option to enable write support at the mount point.
 - Add -u option for more consistent mount/unmount call signatures.
 - Add output for versions of all dependencies to --version.
 - Add support for AppImage builds which will be offered on the GitHub Releases page.
 - Add --transform-recursive-mount-point option to control the mount path of recursively mounted archives.
 - Change default for --parallelization from 1 to 0, i.e., maximum number of cores.
 - (ratarmountcore 0.2.1) Fix (the last 1000) files not showing for truncated / incomplete TAR files.
 - (ratarmountcore 0.2.2) Improve performance for gzip files significantly by using a larger buffer.
 - (ratarmountcore 0.2.2) Do not use ParallelXZReader for single-block xz files to avoid memory issues.
 - (ratarmountcore 0.2.3) Fix uncaught exception when a folder contains an invalid character.
 - (ratarmountcore 0.2.4) Recursive mounting of nested uncompressed TARs did skip some files for depth > 2.
 - (ratarmountcore 0.3.0) Relax the check for GNU incremental TAR detection because the prefix field
   might contain binary data.
 - (ratarmountcore 0.3.0) Improve performance by factor ~5 and avoid storage requirements for index
   reading for gzip compressed archives.
 - (ratarmountcore 0.3.0) Improve performance by ~40% and avoid storage requirements for index
   writing for gzip compressed archives.
 - (ratarmountcore 0.3.0) Improve performance for indexing uncompressed TARs that have been
   appended to by only analyzing the new files.
 - (ratarmountcore 0.3.0) Fix uncompressed archives having been appended very small files (<10 KiB)
   were not detected as changed by default.
 - (ratarmountcore 0.3.0) Fix problem triggered by combining --recursive and -P 0 with a recursive bzip2 archive.

# Version 0.10.0 built on 2022-01-15

 - Split ratarmount into ratarmount, which contains the CLI, and ratarmountcore.
 - Migrate to setup.cfg and pyproject.toml for distribution to pip.
 - Add support for GNU incremental TAR file format.
 - Improve performance by returning file attributes in the readdir FUSE call to avoid
   hundreds of subsequent calls. This improves performance of `find` and others by factor 8 in some tests.
 - Improve performance for large Union Mounts by keeping a path cache for read-only mount sources. This way,
   each FUSE getattr only has to query mount sources known to have the file instead of iterating over all of them.
   Note that the old union cache backported to 0.9.2 was accidentally not actually used and would be buggy anyway.
   This will not be backported to 0.9.x.
 - Improve performance of index creation for uncompressed TARs by batching SQLite index insertions among others.
 - Fixed progress indicator not working for XZ files when using the python-xz backend.
 - Add elapsed time output to progress indicator.
 - Depend on python-xz 0.4.0 to fix runaway memory usage for large files.
 - Parallelize XZ decoder backend for huge speedups for mounting and reading large files inside the mount point.

# Version 0.9.3 built on 2021-12-21

 - Fix mounting of TAR fails if there are special characters like '#' in the path.
 - Fix recursive mounting of simple compressed files (file.bz2) inside TAR.
 - Fix file objects being returned by SQLiteIndexedTar not being independent from each other
   because of the shared underlying file object.

# Version 0.9.2 built on 2021-11-28

 - Fix StenciledFile accidentally throwing an exception when accessing empty files.
 - Improve performance when using union mounting by using a folder-to-archive lookup cache.
 - Implicitly added parent folders will now show a size of 0B instead of arbitrary 1B.
 - Fix version ordering for implicitly added folders.
 - Fix permissions to not remove write permissions. FUSE will still return a
   "Read-only file system" error when trying to modify the file system.
 - Fix RAR files were not found when ratarmount was daemonized and changed the current working directory.

# Version 0.9.1 built on 2021-09-26

 - Fix recursive mounting failing for archives in top-level directory.
 - Replace the optional lzmaffi dependency with a python-xz dependency.
 - Fix missing files in ZIP and RAR if parent folders are not in the archive.
 - Fix archives getting misrecognized as ZIP because of lenient zipfile.is_zipfile function.
 - Make indexed_zstd an optional dependency on macOS because wheels are missing.
 - Fix CRC errors thrown by rarfile when reading after seeking back file objects for files inside RARs.
 - Fix ratarmount not working if rarfile or zipfile were not installed.
 - Disable ZIP support with Python 3.6 and older because the returned file object is not seekable.

# Version 0.9.0 built on 2021-09-16

 - Refactor the code and introduce a MountSource interface.
 - Add support for zip and rar archives including password-encrypted ones.
 - Improve recursive mounting. If supported by the decompression backend,
   compressed archives inside compressed archives can be mounted recursively.
 - Fixes for macOS and Windows.

# Version 0.8.1 built on 2021-07-11

 - Fix "BLOB longer than INT_MAX bytes" error for .gz archives larger than ~400GB.
 - Add missing "dataclasses" dependency for Python 3.6.
 - Open SQLite index read-only to allow multiple ratarmount instances to use it.
 - Raise exception if SQLiteIndexedTar cannot read given file object.
 - Warn on newer index versions.
 - Add support for "--index-name :memory:" to create in-memory indexes.

# Version 0.8.0 built on 2021-06-27

 - Fix SQLiteIndexedTar class when being called with a file object.
 - Add -P argument option to activate the parallel BZ2 decoder of indexed_bzip2 1.2.0.
 - Add --lazy option, which works in tandem with --recursive, and bind mounted folders.
   TARs inside the source folder will be mounted only after the first access to it through the mount point.
 - TARs containing hard links to files with exactly the same name will be interpreted as referring
   to a prior version of that file.

# Version 0.7.0 built on 2020-12-20

 - Add CLI options --index-file and --index-folder to specify an index location or folder.
 - Add support for zstd compressed TARs.
 - Add support for xz compressed TARs when compressed with pixz or pxz or similar tools
   limiting the block size.
 - Fix index location check not accounting for SQLite special requirements.
 - Add warnings for when the existing index was created with different arguments,
   e.g., without --recursive, even though --recursive was requested.
 - Add specialized exceptions used by ratarmount.
 - Add recursive mounting for folders in order to mount all contained TARs.

# Version 0.6.1 built on 2020-10-02

 - Fix broken CLI and add test for it.

# Version 0.6.0 built on 2020-10-02

 - Fix bug with filename encoding on some special characters like accented vowels or umlauts.
 - Add support to mount simple .gz or .bz2 files, which are not compressed TARs.
 - Fix number of hardlinks being always shown as 2. Changed to show 1.
 - Fix wrong size reporting when index size has changed.
 - Fix blocksize and therefore `du` results always showing 0.
 - Add command line option to turn on modification timestamp checking.
 - Add command to mount recursive tars at folders where the .tar extension is stripped.
 - Fix recursion limit hit when mounting a TAR with more than or equal 1000 contained TARs.

# Version 0.5.0 built on 2020-05-09

 - Add support for sparse files inside the TAR.
 - Automatically detect if the TAR file has grown since the last index creation.
 - Add support for truncated TAR files, e.g., during downloading.
 - Add support for concatenated TAR files.
 - Add support for hardlinks in the TAR file.
 - Add support for union mounting.
 - Add support for accessing older versions of a file if the TAR contains multiple versions.
 - Detect changes in TAR files and recreate the index if so.
 - Remove support for old non-SQLite index backends.

# Version 0.4.1 built on 2020-04-10

 - Add option for gzip index seek point spacing to CLI.
 - Fix absolute symbolic links being stripped of their leading '/'.
 - Fix returned error codes.
 - Fix detection and recreation of found incomplete indexes.
 - Add -o alternative short version for --fuse.

# Version 0.4.0 built on 2019-12-15

 - Make bzip2 seek support standalone module.
 - Fix memory leak because BZ2Reader destructor was never called.
 - Fix SQLite backend loading not working when index is read-only.
 - Improve faulty index detection.
 - Fix typo causing bug with gzip support detection.
 - Deprecate legacy serializers.

# Version 0.3.4 built on 2019-12-06

 - Fix performance bug for index creation of uncompressed TARs by not
   using streaming mode in that case.

# Version 0.3.3 built on 2019-12-05

 - Fix bz2 decompression bug with repeated sequences at block boundaries.
 - Add version information to SQLite index.
 - Fix progress bar estimates for compressed files to account for compression ratio.
 - Fix argument parsing error when calling script directly instead of installing it.

# Version 0.3.2 built on 2019-12-01

 - Fix PyPI source tarball build failing on older compilers because
   `static_assert` was used without a message string.

# Version 0.3.1 built on 2019-11-23

 - Fix missing bzip2.h header in tarball on PyPI.

# Version 0.3.0 built on 2019-11-23

 - Add --version option to CLI.
 - Add support for gzip compressed TAR archives.
 - Add support for bzip2 compressed TAR archives.
 - Improve SQLite index creation memory footprint and performance using
   a temporary unsorted table and then sort it once by file name.

# Version 0.2.0 built on 2019-11-17

 - Add SQLite backend to improve memory usage.
 - Reduce memory usage by clearing tarfile's internal index periodically.
 - Add argument option to forward options to FUSE.

# Version 0.1.0 built on 2019-11-14

 - First version uploaded to PyPI.
