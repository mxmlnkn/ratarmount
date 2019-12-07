
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
