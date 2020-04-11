
# Version 0.5.0 built on 2019-04-??

 - Add support for sparse files inside the TAR.
 - Automatically detect if the TAR file has grown since the last index creation.

# Version 0.4.1 built on 2019-04-10

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
