/* This whole file should be idempotent, i.e., executing it twice in a row should work (use IF NOT EXISTS). */

CREATE TABLE IF NOT EXISTS "files" (
    "path"           VARCHAR(65535) NOT NULL,  /* path with leading and without trailing slash */
    "name"           VARCHAR(65535) NOT NULL,
    "offsetheader"   INTEGER,  /* seek offset from TAR file where the TAR metadata for this file resides */
    "offset"         INTEGER,  /* seek offset from TAR file where these file's contents resides */
    "size"           INTEGER,
    "mtime"          REAL,
    "mode"           INTEGER,
    "type"           INTEGER,
    "linkname"       VARCHAR(65535),
    "uid"            INTEGER,
    "gid"            INTEGER,
    /* True for valid TAR files. Internally used to determine where to mount recursive TAR files. */
    "istar"          BOOL   ,
    "issparse"       BOOL   ,  /* For sparse files the file size refers to the expanded size! */
    "isgenerated"    BOOL   ,  /* True for entries generated for parent folders by ratarmount. */
    "recursiondepth" INTEGER,  /* Normally 0. 1+ if the file is recursively in an archive. */
    /* See SQL benchmarks for decision on the primary key.
     * See also https://www.sqlite.org/optoverview.html
     * (path,name) tuples might appear multiple times in a TAR if it got updated.
     * In order to also be able to show older versions, we need to add
     * the offsetheader column to the primary key. */
    PRIMARY KEY ("path","name","offsetheader")
);

/* Creates xattrs view built upon string-deduplicated xattrkeys and xattrsdata tables. */

CREATE TABLE IF NOT EXISTS "xattrkeys" (
    "id" INTEGER PRIMARY KEY,
    "name" VARCHAR(65535) UNIQUE
);

CREATE TABLE IF NOT EXISTS "xattrsdata" (
    "offsetheader" INTEGER,
    "keyid" INTEGER,
    "value" VARCHAR(65535),  /* Binary Data (Python Bytes) */
    PRIMARY KEY ("offsetheader","keyid"),
    FOREIGN KEY ("keyid") REFERENCES xattrkeys("id")
);

CREATE VIEW IF NOT EXISTS "xattrs" ( "offsetheader", "key", "value" ) AS
    SELECT offsetheader, xattrkeys.name, value FROM "xattrsdata"
    INNER JOIN xattrkeys ON xattrkeys.id = xattrsdata.keyid;

CREATE TRIGGER IF NOT EXISTS "xattrs_insert" INSTEAD OF INSERT ON "xattrs"
BEGIN
    INSERT OR IGNORE INTO xattrkeys(name) VALUES (NEW.key);
    INSERT OR REPLACE INTO xattrsdata(offsetheader, keyid, value) VALUES (
        NEW.offsetheader,
        (SELECT xattrkeys.id FROM xattrkeys WHERE name = NEW.key),
        NEW.value
    );
END;

/* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
 * Therefore, it will not be sorted and inserting will be faster!
 *
 * This was never being inserted into :(!
 * - The introducing commit 45fb991a7ca23aa5f53e7285b88fab16dd6c0fac 2019-11-21
 *   "use intermediary SQL table to avoid continuous resorts during creation"
 *   was flawed from its inception. It introduced "filestmp" but never inserted into it in _set_file_infos.
 * - It did not work in ratarmount.py _setFileInfo in aa8b2a7 2020-12-16.
 * - It did not work in core/ratarmountcore/SQLiteIndexedTar.py _setFileInfo(s) in v0.12.0 2022-11-12.
 * - It did not work in core/ratarmountcore/SQLiteIndex.py setFileInfo(s) in v0.15.2 2024-09-01.
 * - It does not work in core/ratarmountcore/SQLiteIndex.py set_file_infos on 2026-04-25.
 **/
CREATE TABLE IF NOT EXISTS "filestmp" AS SELECT * FROM "files" WHERE 0;

CREATE TABLE IF NOT EXISTS "parentfolders" (
    "path"          VARCHAR(65535) NOT NULL,
    "name"          VARCHAR(65535) NOT NULL,
    "offsetheader"  INTEGER,
    "offset"        INTEGER,
    PRIMARY KEY ("path","name")
);

/* Table for storing information regarding the options used for creating the index.
 * Common keys: tarstats, arguments, isGnuIncremental, backendName */
CREATE TABLE IF NOT EXISTS "metadata" (
    "key"      VARCHAR(65535) NOT NULL, /* e.g. "tarsize" */
    "value"    VARCHAR(65535) NOT NULL, /* e.g. size in bytes as integer */
    PRIMARY KEY ("key")
);

/* Similar to metadata but explicitly for versions. Common keys: ratarmount, index */
CREATE TABLE IF NOT EXISTS "versions" (
    "name"     VARCHAR(65535) NOT NULL, /* which component the version belongs to */
    "version"  VARCHAR(65535) NOT NULL, /* free form version string */
    /* Semantic Versioning 2.0.0 (semver.org) parts if they can be specified:
     *   MAJOR version when you make incompatible API changes,
     *   MINOR version when you add functionality in a backwards compatible manner, and
     *   PATCH version when you make backwards compatible bug fixes. */
    "major"    INTEGER,
    "minor"    INTEGER,
    "patch"    INTEGER,
    PRIMARY KEY ("name")
);
