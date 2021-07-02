
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
