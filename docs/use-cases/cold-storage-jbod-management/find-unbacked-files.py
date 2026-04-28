import argparse
import sqlite3
from contextlib import closing
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        description='Find files in main database that exist in fewer than N other databases.',
    )
    parser.add_argument('--main-db', required=True, help='Path to main SQLite database (required)')
    parser.add_argument(
        '--other-dbs',
        nargs='+',
        required=True,
        help='Paths to other SQLite databases to compare against (1 or more required)',
    )
    parser.add_argument(
        '-t', '--threshold', type=int, default=1, help='Files existing in fewer than N other databases (default: 1)'
    )
    parser.add_argument(
        '--exclude-crc-mismatch',
        action='store_true',
        help='Exclude files with mismatched CRC32 checksums from the results',
    )
    args = parser.parse_args()

    # Generate other_sha CTE
    other_selects = []
    for i, _other_db in enumerate(args.other_dbs, start=2):
        alias = f'b{i}'
        other_selects.append(
            f"SELECT '{alias}' AS src, value AS sha256 FROM {alias}.xattrs WHERE key = 'user.hash.sha256'"
        )

    # Replace placeholders in SQL template
    sql_template = (Path(__file__).parent / 'find-unbacked-files.sql').read_text()
    final_sql = sql_template.replace('{{OTHER_SHA_CTE}}', ' UNION ALL '.join(other_selects)).replace(
        '{{THRESHOLD}}', '?'
    )

    # Connect to main database and attach others
    with closing(
        sqlite3.connect(Path(args.main_db).expanduser().resolve().as_uri() + '?mode=ro', uri=True)
    ) as connection:
        for i, other_db in enumerate(args.other_dbs, start=2):
            connection.execute(f"ATTACH DATABASE '{Path(other_db).expanduser().resolve().as_uri()}?mode=ro' AS b{i}")

        # Load CRC mismatch paths if requested
        crc_mismatch_paths = set()
        if args.exclude_crc_mismatch:
            crc_cursor = connection.execute((Path(__file__).parent / 'find-mismatching-crc32.sql').read_text())
            crc_mismatch_paths = {row[0] for row in crc_cursor}

        # Print results
        for row in connection.execute(final_sql, (args.threshold,)):
            if row[0] in crc_mismatch_paths:
                continue
            print(f"{row[0]} ({row[1]} backups)")


if __name__ == '__main__':
    main()
