#!/usr/bin/env python3

import os
import re
import sqlite3
import sys
import tempfile
import time
from typing import Any, Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.lines import Line2D

rowsPerInsert = 1000
benchmarkWithDuplicates = True

if False:
    fileNameLength = 256
    databaseFile = '/media/e/benchmarks/benchmarkSchemas.sqlite'  # on disk
else:
    fileNameLength = 128
    databaseFile = tempfile.mkstemp(
        prefix='benchmarkSchemas', suffix='.sqlite', dir='/dev/shm/' if os.path.isdir('/dev/shm') else None
    )[1]

benchmarkPrimary = True
if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
    folder = os.path.abspath(sys.argv[1])
    if 'primary-key-benchmark' in folder:
        benchmarkPrimary = True
    if 'view-benchmark' in folder:
        benchmarkPrimary = False

if benchmarkPrimary:
    comparePrimaryKeys = True
    comparePostProcessing = True
    compareSchemaWithViews = False

    # 16_000_000 and fileNameLength=256 does not fit into ~70 GB of free RAM when using /dev/shm.
    #   The problem is the 'view, sort and deduplicate after, unique rows' benchmark.
    #   The 'sort after, no vacuum, unique rows' also already has a database size of 14 426 091 520 B.
    nFilesToBenchmark = [
        3_000,
        10_000,
        32_000,
        100_000,
        320_000,
        1_000_000,
        2_000_000,
        4_000_000,
        8_000_000,
        16_000_000,
    ]
else:
    comparePrimaryKeys = False
    comparePostProcessing = False
    compareSchemaWithViews = True
    nFilesToBenchmark = [3_000, 10_000, 32_000, 100_000, 320_000, 1_000_000, 2_000_000, 4_000_000]
    nFilesToBenchmark = [
        3_000,
        10_000,
        32_000,
        100_000,
        320_000,
        1_000_000,
        2_000_000,
        4_000_000,
        8_000_000,
        16_000_000,
    ]


schemas = {}

if comparePrimaryKeys:
    schemas.update(
        {
            'varchar primary key': 'CREATE TABLE "files" ( "path" VARCHAR(65535) PRIMARY KEY, "hash" INTEGER );',
            'integer primary key': 'CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" INTEGER PRIMARY KEY );',
            'varchar,integer primary key': '''
            CREATE TABLE "files" (
                "path" VARCHAR(65535),
                "hash" INTEGER,
                PRIMARY KEY (path,hash)
            );''',
        }
    )

schemaFilesTemporary = '''
    CREATE TABLE "files" ( "path" VARCHAR(65535), "hash" VARCHAR(65535), PRIMARY KEY (path,hash) );
    /* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
     * Therefore, it will not be sorted and insertion will be faster! */
    CREATE TABLE "filestmp" AS SELECT * FROM "files" WHERE 0;
    '''
schemas.update(
    {
        'varchar,varchar primary key': '''
        CREATE TABLE "files" (
            "path" VARCHAR(65535),
            "hash" VARCHAR(65535),
            PRIMARY KEY (path,hash)
        );''',
        # The database schema as used in ratarmount 1.0.0.
        'varchar,varchar primary key sort after, no vacuum': (
            schemaFilesTemporary,
            # Post-processing
            '''
            INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","hash",rowid;
            DROP TABLE "filestmp";
            ''',
        ),
    }
)

if comparePostProcessing:
    schemas.update(
        {
            # Without the ORDER BY path,hash, the post-processing INSERT scales O(n log(n)) instead of O(n)!
            # But, this is weird! This basically implies that sorting takes O(n) time Oo?!
            # https://avi.im/blag/2021/fast-sqlite-inserts/
            #  -> Has a much simpler database with only integer primary keys, so not comparable!
            #  -> Prepared statements are already the default in Python: https://bugs.python.org/issue12993
            #  -> https://stackoverflow.com/questions/11474151/insert-with-order-by-is-faster
            #     Unfortunately, no helpful answer because the question was too imprecise.
            #     Yes, indexes can slow insertion down, but this does not explain the difference to ORDER BY.
            # https://voidstar.tech/sqlite_insert_speed/
            #  -> The "create index after insertion" tip is basically what we already do with the temporary table.
            'varchar,varchar primary key sort after, no vacuum, no ORDER BY path-hash': (
                schemaFilesTemporary,
                # Post-processing
                '''
                INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY rowid;
                DROP TABLE "filestmp";
                ''',
            ),
            # Use vacuum to reduce size!
            'varchar,varchar primary key sort after': (
                schemaFilesTemporary,
                # Post-processing
                '''
                INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY "path","hash",rowid;
                DROP TABLE "filestmp";
                VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
                ''',
            ),
        }
    )

# From the above 4, the last one is now used because it is the most performance,
# especially because the LIKE statement is not necessary with it!
# The below variants compare different methods to deduplicate path and hash/name string columns.
# This definitely should reduce the database size, and ideally it would also speed up some queries.
# Unfortunately it will have some overhead for database insertion, which might be reducable when
# avoiding the insert trigger on the view and instead doing batch inserts.
# Furthermore, the question is whether filestmp should already do the deduplication or should it be done in post.
# Looking at CPU utilization during post-processing indicates that libsqlite does not use any parallelization
# for those steps! This is unfortunate.

if compareSchemaWithViews:
    # I have basically tried three versions to define the string-id mapping tables as below.
    # According to the benchmarks, there is no performance difference between these, therefore use the shortest.
    wordToIdPrimaryKey = '''
        CREATE TABLE "paths" ( "value" VARCHAR(65535) PRIMARY KEY );
        CREATE TABLE "hashes" ( "value" VARCHAR(65535) PRIMARY KEY );
    '''
    wordToIdUniqueConstraint = '''
        CREATE TABLE "paths" ( "rowid" INTEGER PRIMARY KEY, "value" VARCHAR(65535) UNIQUE );
        CREATE TABLE "hashes" ( "rowid" INTEGER PRIMARY KEY, "value" VARCHAR(65535) UNIQUE );
    '''
    wordToIdUniqueIndex = '''
        CREATE TABLE "paths" ( "rowid" INTEGER PRIMARY KEY, "value" VARCHAR(65535) );
        CREATE UNIQUE INDEX unique_paths ON paths(value);
        CREATE TABLE "hashes" ( "rowid" INTEGER PRIMARY KEY, "value" VARCHAR(65535) );
        CREATE UNIQUE INDEX unique_hashes ON hashes(value);
    '''

    schemaFilesView = '''
        CREATE TABLE "filesdata" ( "pathid" INTEGER, "hashid" INTEGER, PRIMARY KEY (pathid,hashid) );
        CREATE VIEW "files" ( "path", "hash" ) AS
            SELECT p.value, h.value FROM "filesdata" AS f
            INNER JOIN paths AS p ON p.rowid = f.pathid
            INNER JOIN hashes AS h ON h.rowid = f.hashid;

        CREATE TRIGGER "files_insert" INSTEAD OF INSERT ON "files"
        BEGIN
            INSERT OR IGNORE INTO paths(value) VALUES (NEW.path);
            INSERT OR IGNORE INTO hashes(value) VALUES (NEW.hash);
            INSERT OR IGNORE INTO filesdata(pathid, hashid) VALUES(
                (SELECT paths.rowid FROM paths WHERE value = NEW.path),
                (SELECT hashes.rowid FROM hashes WHERE value = NEW.hash)
            );
        END;
        '''
    schemaFilesViewWithTemporary = (
        wordToIdPrimaryKey
        + schemaFilesView
        + '''
        /* "A table created using CREATE TABLE AS has no PRIMARY KEY and no constraints of any kind"
         * Therefore, it will not be sorted and insertion will be faster! */
        CREATE TABLE "filestmp" AS SELECT * FROM "files" WHERE 0;
        '''
    )
    schemas.update(
        {
            # Use a view and deduplication on insert trigger.
            'varchar,varchar view (primary key)': wordToIdPrimaryKey + schemaFilesView,
            'varchar,varchar view (unique constraint)': wordToIdUniqueConstraint + schemaFilesView,
            'varchar,varchar view (unique index)': wordToIdUniqueIndex + schemaFilesView,
            # Use a view and temporary and deduplication on insert trigger.
            'varchar,varchar view, sort and deduplicate after': (
                schemaFilesViewWithTemporary,
                # Post-processing
                # "ORDER by rowid" is always required for correctness with TAR files and overwritten files,
                # but the ORDER BY path,name is neither necessary, or probably helpful when inserting into the view,
                # because, for the actual data, the PRIMARY KEY tuple uses the IDs for each path/name and therefore
                # may have a different order.
                '''
                INSERT OR REPLACE INTO "files" SELECT * FROM "filestmp" ORDER BY rowid;
                DROP TABLE "filestmp";
                VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
                ''',
            ),
            # Use a view and deduplicate manually without insert trigger.
            'varchar,varchar view, sort and deduplicate (batch inner join) after': (
                schemaFilesViewWithTemporary,
                # Post-processing
                '''
                /* The ORDER BY is important for performance! See also the comments for comparing the ORDER BY
                 * on the filestmp -> files INSERT statement comparison. It changes algorithmic complexity!
                 * These statements still have the same complexity as the non-view version, i.e., O(n). */
                INSERT OR IGNORE INTO paths(value) SELECT DISTINCT "path" FROM "filestmp" ORDER BY "path";
                INSERT OR IGNORE INTO hashes(value) SELECT DISTINCT "hash" FROM "filestmp" ORDER BY "hash";

                /* This worsens algorithmic complexity, probably because of the string-to-ID lookups! */
                INSERT OR REPLACE INTO filesdata(pathid, hashid)
                    SELECT p.rowid, h.rowid FROM "filestmp" AS f
                    INNER JOIN paths AS p ON p.value = f.path
                    INNER JOIN hashes AS h ON h.value = f.hash
                    ORDER BY f.rowid;

                DROP TABLE "filestmp";
                VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
                ''',
            ),
            #'varchar,varchar view, sort and deduplicate (batch inner join ORDER BY) after': (
            #    schemaFilesViewWithTemporary,
            #    # Post-processing
            #    f'''
            #    /* The ORDER BY is important for performance! See also the comments for comparing the ORDER BY
            #     * on the filestmp -> files INSERT statement comparison. It changes algorithmic complexity!
            #     * These statements still have the same complexity as the non-view version, i.e., O(n). */
            #    INSERT OR IGNORE INTO paths(value) SELECT DISTINCT "path" FROM "filestmp" ORDER BY "path";
            #    INSERT OR IGNORE INTO hashes(value) SELECT DISTINCT "hash" FROM "filestmp" ORDER BY "hash";
            #
            #    /* This worsens algorithmic complexity, probably because of the string-to-ID lookups! */
            #    CREATE TABLE "filesdata2" ( "pathid" INTEGER, "hashid" INTEGER );
            #    INSERT OR REPLACE INTO filesdata2(pathid, hashid)
            #        SELECT p.rowid, h.rowid FROM "filestmp" AS f
            #        INNER JOIN paths AS p ON p.value = f.path
            #        INNER JOIN hashes AS h ON h.value = f.hash
            #        /* THIS is the difference to the previous method! */
            #        ORDER BY f.rowid;
            #    /* This is not correct because it does not insert into "filesdata", but it already is
            #     * of slower complexity without this missing step, so we can discard this. */
            #
            #    DROP TABLE "filestmp";
            #    VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
            #    ''',
            # ),
            # Use a view and deduplicate manually without insert trigger.
            'varchar,varchar view, sort and deduplicate (batch insert trigger) after': (
                schemaFilesViewWithTemporary,
                # Post-processing
                '''
                INSERT OR IGNORE INTO paths(value) SELECT DISTINCT "path" FROM "filestmp" ORDER BY "path";
                INSERT OR IGNORE INTO hashes(value) SELECT DISTINCT "hash" FROM "filestmp" ORDER BY "hash";

                /* Even this version is slow, probably because the SELECT statement to look up path and hash
                 * has O(log(n)) complexity. Note that this reason actually favors the on-demand insert because
                 * the amount of unique keys to look up in is smaller, i.e., the total complexity becomes
                 * sum(log(n) for n in range(N)) ~ N(log(N)-1) instead of N log(N). */
                DROP TRIGGER IF EXISTS "files_insert";
                CREATE TRIGGER "files_insert" INSTEAD OF INSERT ON "files"
                BEGIN
                    INSERT OR IGNORE INTO filesdata(pathid, hashid) VALUES(
                        (SELECT paths.rowid FROM paths WHERE value = NEW.path),
                        (SELECT hashes.rowid FROM hashes WHERE value = NEW.hash)
                    );
                END;
                INSERT OR REPLACE INTO files(path, hash) SELECT * FROM "filestmp" ORDER BY rowid;

                DROP TABLE "filestmp";
                VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
                ''',
            ),
            'varchar,varchar view, sort and deduplicate (full batch insert trigger) after': (
                schemaFilesViewWithTemporary,
                # Post-processing
                '''
                DROP TRIGGER IF EXISTS "files_insert";
                CREATE TRIGGER "files_insert" INSTEAD OF INSERT ON "files"
                BEGIN
                    INSERT OR IGNORE INTO paths(value) VALUES (NEW.path);
                    INSERT OR IGNORE INTO hashes(value) VALUES (NEW.hash);
                    INSERT OR IGNORE INTO filesdata(pathid, hashid) VALUES(
                        (SELECT paths.rowid FROM paths WHERE value = NEW.path),
                        (SELECT hashes.rowid FROM hashes WHERE value = NEW.hash)
                    );
                END;
                INSERT OR REPLACE INTO files(path, hash) SELECT * FROM "filestmp" ORDER BY rowid;

                DROP TABLE "filestmp";
                VACUUM;  /* Without VACUUM, the database size is larger than without filestmp! */
                ''',
            ),
        }
    )

if not comparePrimaryKeys:
    schemas = {
        re.sub(r'^varchar,varchar (primary key ?)?', 'tuple key, ', label): schema for label, schema in schemas.items()
    }

# fmt: off
timeRegexes = {
    'name'              : r'(?:Label: (.*)|(CREATE TABLE .*))',
    'tinsert'           : r'Inserting ([0-9]+) file names with [0-9]+ characters took ([0-9.]+) s '
                           'when excluding PRNG time',
    'tselectpath'       : r'Selecting ([0-9]+) paths took ([0-9.]+) s',
    'tselectpathstart'  : r'Selecting ([0-9]+) paths starting with took ([0-9.]+) s',
    'tselecthash'       : r'Selecting ([0-9]+) hashes took ([0-9.]+) s',
    'tselecttuple'      : r'Selecting ([0-9]+) path,hash took ([0-9.]+) s',
    'dbsize'            : r'SQL database size in bytes: ([0-9]+)',
}
# fmt: on

keyToLabel = {
    'tinsert': "INSERT",
    'tselectpath': "SELECT PATH == x",
    'tselectpathstart': "SELECT PATH LIKE x.%",
    'tselecthash': "SELECT HASH == y",
    'tselecttuple': "SELECT (PATH,HASH) == (x,y)",
    'dbsize': "Database Size",
}


def extractValuesFromBlock(block: str) -> Dict[str, Any]:
    namemap = {schema: label for label, schema in schemas.items()}

    values = {}
    for key, timeRegex in timeRegexes.items():
        matches = list(filter(None, [re.match(timeRegex, line.strip()) for line in block.split('\n')]))
        if not matches:
            continue
        match = matches[0]

        if key.startswith('t'):
            values[key] = float(match.group(2)) / float(match.group(1))
        elif key == 'dbsize':
            values[key] = int(match.group(1))
        elif key == 'name':
            values[key] = namemap[match.group(1)] if match.group(1) in namemap else match.group(1)
        else:
            values[key] = match.group(1)
        if 'tselect' in key:
            values['nrowsselect'] = int(match.group(1))
        if 'tinsert' in key:
            values['nrowsinsert'] = int(match.group(1))
    return values


def extractValuesFromLog(path: str):
    with open(path, encoding='utf-8') as file:
        return [extractValuesFromBlock(block) for block in file.read().split('\n\n') if block]


def plotSummary(logFiles: List[str]):
    values = [values for logFile in logFiles for values in extractValuesFromLog(logFile)]
    names = sorted({x['name'] for x in values if re.sub(', (unique|duplicate) rows$', '', x['name']) in schemas})
    if not names:
        return

    if comparePrimaryKeys:
        fig, axs = plt.subplots(2, 2, figsize=(10, 9), layout='constrained')
        axes = [axs[0, 0], axs[0, 1], axs[1, 0], axs[1, 1]]
        keys = ['tinsert', 'tselectpath', 'tselecthash' if comparePrimaryKeys else 'tselecttuple', 'tselectpathstart']
    else:
        fig, ax = plt.subplots(1, 1, figsize=(6, 7), layout='constrained')
        axes = [ax]
        keys = ['tinsert']

    labelToColor: Dict[str, Any] = {}

    def plot(ax, x, y, label, **kwargs):
        match = re.match('(.*), (unique|duplicate) rows$', label)
        if match:
            hasDuplicates = match.group(2) == 'duplicate'

            if 'marker' in kwargs:
                kwargs['marker'] = '+' if hasDuplicates else 'o'
            if 'linestyle' in kwargs:
                kwargs['linestyle'] = '--' if hasDuplicates else '-'
            label = match.group(1)

        if label in labelToColor:
            ax.plot(x, y, **kwargs, color=labelToColor[label])
        elif 'color' not in kwargs:
            (line,) = ax.plot(x, y, **kwargs)
            labelToColor[label] = line.get_color()

    for ikey, key in enumerate(keys):
        slowOperation = key == 'tselectpathstart' or ('select' in key and any('varchar' not in name for name in names))
        ax = axes[ikey]
        ax.set_xlabel('Number of Rows')
        ax.set_ylabel(keyToLabel.get(key, key) + ' per row / s')
        ax.set_xscale('log')
        ax.set_yscale('log' if slowOperation else 'linear')

        for name in names:
            # always use nrowsinsert as that is the database size and times are already per row
            xvalues = np.array([x['nrowsinsert'] for x in values if x['name'] == name and key in x])
            yvalues = np.array([x[key] for x in values if x['name'] == name and key in x])
            iSorted = np.argsort(xvalues)
            plot(
                ax,
                xvalues[iSorted],
                yvalues[iSorted],
                label=name,
                marker='o',
                linestyle='--' if 'duplicate rows' in name else '-',
            )

        minx = min(min(line.get_xdata()) for line in ax.lines)
        maxx = max(max(line.get_xdata()) for line in ax.lines)
        miny = min(min(line.get_ydata()) for line in ax.lines)
        maxy = max(max(line.get_ydata()) for line in ax.lines)
        maxy0 = max(line.get_ydata()[0] for line in ax.lines)
        x = 10 ** np.linspace(np.log10(minx), np.log10(maxx))
        if key == 'tselectpathstart':
            plot(ax, x, x / x[0] * miny, linestyle='--', color='0.5', label='linear scaling')
            # y = x**2 / x[-1] ** 2 * maxy
            # plot(ax, x[y > miny], y[y > miny], linestyle=':', color='0.5', label='quadratic scaling')
            # y = x**3 / x[-1] ** 3 * maxy
            # plot(ax, x[y > miny], y[y > miny], linestyle='-', color='0.5', label='cubic scaling')
        if key in ['tselectpath', 'tselecthash', 'tselecttuple'] and comparePrimaryKeys:
            plot(ax, x, x / x[0] * maxy0, linestyle='--', color='0.5', label='linear scaling')

        # if key in ['tselectpath', 'tselecthash', 'tselecttuple']:
        #    plot(ax, x, np.log(x) / np.log(x[0]) * miny, linestyle='-.', color='0.5', label='logarithmic scaling')

        if key == 'tinsert':
            if slowOperation:
                y = x / x[-1] * maxy
                plot(ax, x[y > miny], y[y > miny], linestyle='--', color='0.5', label='linear scaling')
            plot(
                ax,
                x,
                np.log(x) / np.log(x[0]) * maxy0,
                linestyle='-.',
                color='0.5',
                label='logarithmic scaling',
            )

        if ax.get_yscale() == 'linear':
            ax.set_ylim((0, ax.get_ylim()[1]))

    lines = [Line2D([0], [0], color=color) for _, color in labelToColor.items()]
    labels = list(labelToColor.keys())
    labelMatches = [re.match('(.*), (unique|duplicate) rows$', name) for name in names]
    hasDifferentData = len({match.group(1) for match in labelMatches if match}) > 1
    if hasDifferentData:
        lines += [
            Line2D([0], [0], color='0.5', linestyle='-', marker='o'),
            Line2D([0], [0], color='0.5', linestyle='--', marker='+'),
        ]
        labels += ['unique data', 'duplicate PATH per batch']

    # outside is a matplotlib 3.7+ feature! https://stackoverflow.com/a/75453792/2191065
    fig.legend(lines, labels, loc='outside upper center')
    fig.savefig("sqlite primary key benchmark times over row count.pdf")
    fig.savefig("sqlite primary key benchmark times over row count.png")

    ############### size plot ###############

    fig, ax = plt.subplots(1, 1, figsize=(6, 7), layout='constrained')
    ax.set_xlabel('Number of Rows')
    ax.set_ylabel('Database Size / B')
    ax.set_xscale('log')
    ax.set_yscale('log')

    for name in names:
        # always use nrowsinsert as that is the database size and times are already per row
        key = 'dbsize'
        xvalues = np.array([x['nrowsinsert'] for x in values if x['name'] == name and key in x])
        yvalues = np.array([x[key] for x in values if x['name'] == name and key in x])
        iSorted = np.argsort(xvalues)
        plot(ax, xvalues[iSorted], yvalues[iSorted], marker='o', label=name)

    miny = min(min(x.get_ydata()) for x in ax.lines)
    ax.plot(x, x / x[0] * miny * 1.2, linestyle='--', color='0.5', label='linear scaling')
    fig.legend(lines, labels, loc='outside upper center')
    fig.savefig("sqlite primary key benchmark sizes over row count.pdf")
    fig.savefig("sqlite primary key benchmark sizes over row count.png")


def getSchemasToBenchmark() -> List[Tuple[str, str, bool]]:
    schemasToBenchmark = []
    for label, schema in schemas.items():
        schemasToBenchmark.append((label + ', unique rows', schema, False))
        if (benchmarkWithDuplicates and 'deduplicat' in label) or 'view' in label:
            schemasToBenchmark.append((label + ', duplicate rows', schema, True))
    return schemasToBenchmark


def plotOperationMeasurements(labelAndPath: List[Tuple[str, str]], targetFileName: str):
    if not labelAndPath:
        return

    labelToColor: Dict[str, Any] = {}

    def plot(ax, x, y, label, **kwargs):
        match = re.match('(.*), (unique|duplicate) rows$', label)
        if match:
            hasDuplicates = match.group(2) == 'duplicate'

            if 'marker' in kwargs:
                kwargs['marker'] = '+' if hasDuplicates else '.'
            label = match.group(1)

        if label in labelToColor:
            ax.plot(x, y, **kwargs, color=labelToColor[label])
        elif 'color' not in kwargs:
            (line,) = ax.plot(x, y, **kwargs)
            labelToColor[label] = line.get_color()

    fig, ax = plt.subplots(1, 1, figsize=(6, 7), layout='constrained')
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_ylabel(f"Execution Time for Single Batch of {rowsPerInsert} Rows / s")
    ax.set_title('INSERT')

    labelMatches = [re.match('(.*), (unique|duplicate) rows$', label) for label, _ in labelAndPath]
    hasDifferentData = len({match.group(1) for match in labelMatches if match}) > 1
    for label, path in labelAndPath:
        insertTimes = np.genfromtxt(path)
        tTotalInsert = np.sum(insertTimes)
        if not hasDifferentData:
            label += f", total time: {tTotalInsert:.1f} s"
        plot(ax, 1 + np.arange(len(insertTimes)), insertTimes, linestyle='', marker='.', label=label)

    # outside is a matplotlib 3.7+ feature! https://stackoverflow.com/a/75453792/2191065
    lines = [Line2D([0], [0], color=color) for _, color in labelToColor.items()]
    labels = list(labelToColor.keys())
    if hasDifferentData:
        lines += [Line2D([0], [0], color='0.5', marker='.'), Line2D([0], [0], color='0.5', marker='+')]
        labels += ['unique data', 'duplicate PATH per batch']

    minx = min(min(line.get_xdata()) for line in ax.lines)
    maxx = max(max(line.get_xdata()) for line in ax.lines)
    maxy10 = max(line.get_ydata()[10] for line in ax.lines)
    x = 10 ** np.linspace(np.log10(minx), np.log10(maxx))
    (line,) = ax.plot(x, np.log(x) / np.log(x[10]) * maxy10, linestyle='-.', color='0.5')
    lines.append(line)
    labels.append('logarithmic scaling')

    fig.legend(lines, labels, loc='outside upper center')
    if targetFileName:
        fig.savefig(targetFileName + ".pdf")
        fig.savefig(targetFileName + ".png")


# Plot existing log files in the specified folder.
if len(sys.argv) > 1 and os.path.isdir(sys.argv[1]):
    singleOperationMeasurements = {}
    logFiles = []
    folder = sys.argv[1]
    for fname in sorted(os.listdir(folder)):
        path = os.path.abspath(os.path.join(folder, fname))
        if fname.endswith('.log') and os.path.isfile(path):
            logFiles += [path]

        match = re.fullmatch('sqlite primary key benchmark ([0-9]+)k files (.*) insert[.]dat', fname)
        if match:
            nFiles = match.group(1)
            if nFiles not in singleOperationMeasurements:
                singleOperationMeasurements[nFiles] = {}
            label = match.group(2)
            singleOperationMeasurements[nFiles][label] = path

    for nFiles, measurements in singleOperationMeasurements.items():
        plotOperationMeasurements(
            [(label, measurements[label]) for label, schema, _ in getSchemasToBenchmark() if label in measurements],
            os.path.join(folder, f"sqlite primary key benchmark {nFiles}k files insert"),
        )

    plotSummary(logFiles)
    plt.show()
    sys.exit(0)


def benchmarkSchemas(nFiles: int, log, plotAllMeasurements: bool) -> None:
    assert nFiles % rowsPerInsert == 0

    fname = f"sqlite primary key benchmark {nFiles // 1000}k files"

    if plotAllMeasurements:
        fig = plt.figure(figsize=(8, 6))
        axi = plt.subplot(111, xscale='log', yscale='log', title='INSERT')
    else:
        fig = None
        axi = None

    log(f"file name length: {fileNameLength}")
    log(f"rows per insert: {rowsPerInsert}")
    log(f"number of rows to insert: {nFiles}")

    for label, schema, dataWithDuplicates in getSchemasToBenchmark():
        log(f"Label: {label}")

        cleanUpDatabase = None
        if isinstance(schema, tuple):
            schema, cleanUpDatabase = schema

        if os.path.isfile(databaseFile):
            os.remove(databaseFile)

        db = sqlite3.connect(databaseFile)
        db.executescript(schema)
        # benchmarks were done on 1M rows
        db.execute('PRAGMA LOCKING_MODE = EXCLUSIVE;')  # only ~10% speedup but does not hurt
        db.execute('PRAGMA TEMP_STORE = MEMORY;')  # no real speedup but does hurt integrity!
        db.execute('PRAGMA JOURNAL_MODE = OFF;')  # no real speedup but does hurt integrity!
        db.execute('PRAGMA SYNCHRONOUS = OFF;')  # ~10% speedup but does hurt integrity!
        # default (-2000): ~20s, 100: ~18s, 1000: ~18.6s, ~10000: 15s, ~100 000: ~8s
        # -100 000: ~12s, -400000 (~400MB): ~8s, -768000: ~8s
        # Benchmark with 4M rows: default(-2000): 114s, -512 000: ~70s
        # default page size: 4096 -> positive cache sizes are in page sizes, negative in kiB
        # I guess this normally is not that relevant because an increasing integer ID is used but
        # for this use case where it seems like on each transaction, the database gets resorted by
        # a string key, it might help to do the sorting less frequently and in memory on larger data.
        # Also, 512MB is alright for a trade-off. If you have a TAR with that much metadata in it,
        # you should have a system which can afford 512MB in the first place.
        # As shown in later benchmarks, the solution with the temporary table does not profit from larger caches.
        # db.execute('PRAGMA CACHE_SIZE = -512000;')
        db.execute('PRAGMA optimize;')

        ########### INSERT benchmark ###########

        t0InsertAll = time.time()
        insertTimes = []
        table = 'filestmp' if 'filestmp' in schema else 'files'
        for i in range(nFiles // rowsPerInsert):
            # Generates random names for path and increasing sequence for the hash integers,
            # which in the real database corresponds to the file name.
            # For some of the schemas, the 'path' and 'hash' columns must be unique, but for the
            # database size benchmarks, they should not be unique to test unique string compression.
            # However, for the same unique string compression it would be interesting to test the case
            # with incompressibility. Therefore this flag.
            if dataWithDuplicates:
                path = os.urandom(fileNameLength // 2).hex()
                rows = [(path, j) for j in range(rowsPerInsert)]
            else:
                rows = [
                    # Have a common prefix, which is common for path strings.
                    # This is likely to worsen string comparison / SELECT performance because it always
                    # needs to check half of the string instead of only the first 1-3 bytes!
                    ('/' * (fileNameLength // 2) + os.urandom(fileNameLength // 4).hex(), j)
                    for j in range(i * rowsPerInsert, i * rowsPerInsert + rowsPerInsert)
                ]

            t0 = time.time()
            db.executemany(f'INSERT INTO {table} VALUES (?,?)', rows)
            # db.commit() # data is written to disk automatically when it becomes too much
            t1 = time.time()
            # print( f"Inserting {rowsPerInsert} rows with {fileNameLength} character file names took {t1 - t0:.3f} s" )
            insertTimes += [t1 - t0]

        t1InsertAll = time.time()
        log(f"Inserting {nFiles} file names with {fileNameLength} characters took {t1InsertAll - t0InsertAll:.3f} s")

        t0Sort = time.time()
        if cleanUpDatabase:
            db.executescript(cleanUpDatabase)
        db.execute('PRAGMA optimize;')
        t1Sort = time.time()
        log(f"Sorting took {t1Sort - t0Sort:.3f} s")

        t0Commit = time.time()
        db.commit()
        t1Commit = time.time()
        log(f"Commit took {t1Commit - t0Commit:.3f} s")

        tTotalInsert = sum(insertTimes) + (t1Commit - t0Commit) + (t1Sort - t0Sort)
        log(
            f"Inserting {nFiles} file names with {fileNameLength} characters took {tTotalInsert:.3f} s "
            "when excluding PRNG time"
        )

        if plotAllMeasurements:
            with open(f"{fname} {label} insert.dat", 'w', encoding='utf-8') as dataFile:
                dataFile.writelines(str(t) + '\n' for t in insertTimes)

        if axi:
            axi.plot(insertTimes, linestyle='', marker='.', label=f'{label}, total time: {tTotalInsert:.3f}s')

        ########### SELECT benchmarks ###########

        def benchmarkSelect(connection, entity, sqlCommand, makeRow, label=label):
            nFilesSelect = (
                10
                if 'LIKE' in sqlCommand or entity == 'hashes' or (entity == 'paths' and 'integer primary' in label)
                else 1000
            )
            t0Select = time.time()
            selectTimes = []
            for i in range(nFilesSelect):
                t0 = time.time()
                connection.execute(sqlCommand, makeRow(i)).fetchall()
                t1 = time.time()
                selectTimes += [t1 - t0]
            t1Select = time.time()
            log(f"Selecting {nFilesSelect} {entity} took {t1Select - t0Select:.3f} s")

            tTotalSelectTime = sum(selectTimes)
            log(f"Selecting {nFilesSelect} {entity} took {tTotalSelectTime:.3f} s excluding PRNG time")

        benchmarkSelect(
            db,
            'paths',
            'SELECT hash FROM files WHERE path == (?)',
            lambda j: (os.urandom(fileNameLength // 2).hex(),),
        )

        # It is now known that this is always expensive (linear search over database is necessary).
        # No need to benchmark this after the (path,name) schema, which avoids LIKE match clauses.
        if comparePrimaryKeys:
            benchmarkSelect(
                db,
                'paths starting with',
                'SELECT hash FROM files WHERE path LIKE (?)',
                lambda j: (os.urandom(fileNameLength // 2).hex() + '%',),
            )

            # Normally, selecting only by name is not done! Only selecting by (path,name) tuples.
            benchmarkSelect(
                db,
                'hashes',
                'SELECT path FROM files WHERE "hash" == (?);',
                lambda j: (np.random.randint(0, nFiles),),
            )
        else:
            benchmarkSelect(
                db,
                'path,hash',
                'SELECT path FROM files WHERE ("path", "hash") == (?,?);',
                lambda j: (np.random.randint(0, nFiles), np.random.randint(0, nFiles)),
            )

        ########### Cleanup ###########

        # db.execute('VACUUM') # does not help because we don't delete anything but still adds significant time overhead
        db.close()
        stats = os.stat(databaseFile)
        log(f"SQL database size in bytes: {stats.st_size}")
        os.remove(databaseFile)

        log("")

    if plotAllMeasurements:
        axi.legend(loc='best')
        fig.tight_layout()
        fig.savefig(fname + ".pdf")
        fig.savefig(fname + ".png")


logFilePath = "sqlite primary key benchmark.log"
with open(logFilePath, 'w', encoding='utf-8') as logFile:

    def log(line):
        print(line)
        logFile.write(line + '\n')
        logFile.flush()

    for nFiles in nFilesToBenchmark:
        benchmarkSchemas(nFiles, log, plotAllMeasurements=nFiles == max(nFilesToBenchmark))

plotSummary([logFilePath])
plt.show()


"""
Results:
  - SELECT query times are fairly constant
  - SELECT == on first primary key is ~100x faster
  - SELECT PATH LIKE x.% is always 100x slower because it can't bisect and has to check every row.
    Also the scaling changes from presumably logarithmic to linear.
  - SELECT on stringified ID is ~65% slower than comparing integers
  - INSERT time over row count seems to grow slower than power law, O(log(n))? as a bisect would suggest?
  - Primary key over two varchars is fast on lookup for the first but slow over the second
    (basically only the first can be used for bisecting, at least than all of the first a distinct!)
  - starting from roughly 1 million rows (~600MB for varchar primary key),
    a new timing regime for executemany appears, which is roughly 10x slower, this is presumably
    an automatically triggered disk-write when the cache becomes full.
  - The database with integer as primary key is less than half as large than varchar as primary key!
    Calling VACUUM does not help. It looks like SQL keeps a second list for the sorted lookup to an internal index.
    The overhead might not be so large if the primary key is not too large in comparison to other columns.
  - Increasing cache size from 2MB to 512MB, gives roughly 40% performance boost
  - The 10M rows test case is unproportionally slower than all others. Some limit seems to have been hit.
  - database size grows linearly
  - Insertion time per row seems to grow O(n) after 1M rows, i.e., the total database creation time grows with O(n^2)!

To Plot:
  - (file sizes/row count, total insertion time / row count, ...) over row count -> four lines one for each schema

Conclusions:
  - Looks like it is best to split the file path in 'path' and 'name' and use these two as primary key.
    the 8ms for one single lookup, would still result in 1s time for a directory listing of 100 files, so it'd be
    better to cache the last SELECT path == "folder" lookup, then everything should work fine.
    In the first place, the benchmarks above have relatively long file names unlike in practice and are 100% flat.
  - I don't know whether the O(n^2) creation time could be a trade-off, but for 16M files and
    PRIMARY KEY(VARCHAR,VARCHAR), it takes 918s ~15min.
    ImageNet has 14M files with quite a lot shorter file names (which probably is also a factor in speed!)
    and still takes 4h anyways, so this SQLite overhead might not be all that bad. But for archives
    with 100M files it might "better" to use the nested dictionary and the custom serialization
    as long as there is enough memory.
    ILSVRC has 2M files and takes 45min with the custom serialization whereas SQLite insertion of 2M takes 20s!

"""
