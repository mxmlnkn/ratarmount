
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
