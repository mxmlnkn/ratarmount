
# Version 0.4.0 built on 2022-06-xx

 - AutoMountLayer now takes a 'recursionDepth' argument instead of 'recursive'.

# Version 0.3.2 built on 2022-06-25

 - Fix exception when trying to mount a RAR archive containing files without timestamps.

# Version 0.3.1 built on 2022-04-10

 - Fix duplicate mounting of uncompressed TARs inside TARs when using --recursive.

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
 - Fix problem triggered by combining --recursive and -P 0 with a recursive bzip2 archive.

# Version 0.2.4 built on 2022-04-04

 - Recursive mounting of nested uncompressed TARs did skip some files for depth > 2.

# Version 0.2.3 built on 2022-04-03

 - Fix uncaught exception when a folder contains an invalid character.

# Version 0.2.2 built on 2022-02-20

 - Improve performance for gzip files significantly by using a larger buffer.
 - Do not use ParallelXZReader for single-block xz files to avoid memory issues.

# Version 0.2.1 built on 2022-02-07

 - Fix (the last 1000) files not showing for truncated / incomplete TAR files.

# Version 0.2.0 built on 2022-01-15

 - Add support for GNU incremental TAR file format.
 - Extend listDir interface to also return FileInfo objects for each file.
 - Improve performance for large Union Mounts by keeping a path cache for read-only mount sources. This way,
   each FUSE getattr only has to query mount sources known to have the file instead of iterating over all of them.
 - Fix file objects returned by SQLiteIndexedTar to be thread-safe when reading and seeking.
 - Improve performance of index creation for uncompressed TARs by batching SQLite index insertions among others.
 - Add elapsed time output to progress indicator.

# Version 0.1.5 built on 2022-02-20

 - Improve performance for gzip files significantly by using a larger buffer.

# Version 0.1.4 built on 2021-12-21

 - Fix mounting of TAR fails if there are special characters like '#' in the path.
 - Fix recursive mounting of simple compressed files (file.bz2) inside TAR.
 - Fix file objects being returned by SQLiteIndexedTar not being independent from each other
   because of the shared underlying file object.

# Version 0.1.3 built on 2021-12-08

 - Workaround did test for the wrong libSqlite major version.

# Version 0.1.2 built on 2021-12-05

 - Avoid SQLite error for libsqlite 3.11.0 on Ubuntu 16.04.
 - Fix '<file object>.index.sqlite' files being created when opening file objects
   without specifying a tarFileName and when using writeIndex=True.

# Version 0.1.1 built on 2021-10-11

 - Importing did fail for all Python versions x.y.z where z <= 6.
 - Updating files in TAR with implicitly defined folders did not work.

# Version 0.1.0 built on 2021-10-04

 - First experimental version uploaded to PyPI.
