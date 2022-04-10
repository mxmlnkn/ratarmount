
# Version 0.12.0 built on 2022-06-xx

 - Add --recursion-depth argument for more fine-grained recursion control.
 - Add support to show a joined view of split files, e.g., <file>.001, and also support split archives.
 - (ratarmountcore 0.4.0) AutoMountLayer now takes a 'recursionDepth' argument instead of 'recursive'.

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
 - Add support for accessing older versions of a file if the TAR containts multiple versions.
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
