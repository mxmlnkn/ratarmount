
# Version 0.2.0 built on 2022-01-15

 - Add support for GNU incremental TAR file format.
 - Improve performance for large Union Mounts by keeping a path cache for read-only mount sources. This way,
   each FUSE getattr only has to query mount sources known to have the file instead of iterating over all of them.
   Note that the old union cache backported to 0.9.2 was accidentally not actually used and would be buggy anyway.
   This will not be backported to 0.9.x.
 - Improve performance of index creation for uncompressed TARs by batching SQLite index insertions among others.
 - Fixed progress indicator not working for XZ files when using the python-xz backend.
 - Add elapsed time output to progress indicator.
 - Depend on python-xz 0.4.0 to fix runaway memory usage for large files.

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
