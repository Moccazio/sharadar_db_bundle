#!/usr/bin/env python3
import argparse
import os
import shutil
import sqlite3
from contextlib import closing

def default_db_path() -> str:
    zipline_root = os.environ.get("ZIPLINE_ROOT", os.path.join(os.path.expanduser("~"), ".zipline"))
    return os.path.join(zipline_root, "data", "sharadar", "latest", "prices.sqlite")


def count_duplicate_rows(conn: sqlite3.Connection) -> int:
    sql = """
    SELECT COALESCE(SUM(cnt - 1), 0)
    FROM (
        SELECT COUNT(*) AS cnt
        FROM prices
        GROUP BY date, sid
        HAVING COUNT(*) > 1
    )
    """
    cur = conn.execute(sql)
    return int(cur.fetchone()[0])


def count_duplicate_groups(conn: sqlite3.Connection) -> int:
    sql = """
    SELECT COUNT(*)
    FROM (
        SELECT 1
        FROM prices
        GROUP BY date, sid
        HAVING COUNT(*) > 1
    )
    """
    cur = conn.execute(sql)
    return int(cur.fetchone()[0])


def print_duplicate_samples(conn: sqlite3.Connection, limit: int = 20) -> None:
    sql = """
    SELECT date, sid, COUNT(*) AS cnt, MIN(rowid) AS min_rowid, MAX(rowid) AS max_rowid
    FROM prices
    GROUP BY date, sid
    HAVING COUNT(*) > 1
    ORDER BY date, sid
    LIMIT ?
    """
    rows = conn.execute(sql, (limit,)).fetchall()
    if not rows:
        print("No duplicate (date, sid) groups found.")
        return

    print("Sample duplicate groups (date, sid, count, min_rowid, max_rowid):")
    for row in rows:
        print(row)


def dedupe_prices(conn: sqlite3.Connection) -> int:
    # Keep newest row (max rowid) for each (date, sid) and remove the rest.
    delete_sql = """
    DELETE FROM prices
    WHERE rowid NOT IN (
        SELECT MAX(rowid)
        FROM prices
        GROUP BY date, sid
    )
    """
    cur = conn.execute(delete_sql)
    return cur.rowcount if cur.rowcount is not None else 0


def ensure_prices_table_exists(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='prices'"
    ).fetchone()
    if row is None:
        raise RuntimeError("Table 'prices' not found in database.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Detect and remove duplicate rows in prices.sqlite based on (date, sid)."
    )
    parser.add_argument(
        "--db",
        default=default_db_path(),
        help="Path to prices sqlite DB (default: ~/.zipline/data/sharadar/latest/prices.sqlite)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report duplicates; do not modify the database.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create a .bak copy before deleting duplicates.",
    )
    args = parser.parse_args()

    db_path = os.path.expanduser(args.db)
    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        return 2

    if args.backup:
        backup_path = db_path + ".bak"
        shutil.copy2(db_path, backup_path)
        print(f"Backup created: {backup_path}")

    with closing(sqlite3.connect(db_path)) as conn:
        ensure_prices_table_exists(conn)

        dup_groups_before = count_duplicate_groups(conn)
        dup_rows_before = count_duplicate_rows(conn)
        print(f"Duplicate groups before: {dup_groups_before}")
        print(f"Duplicate extra rows before: {dup_rows_before}")
        print_duplicate_samples(conn)

        if dup_rows_before == 0:
            print("Nothing to clean.")
            return 0

        if args.dry_run:
            print("Dry run mode: no rows deleted.")
            return 0

        conn.execute("BEGIN")
        deleted = dedupe_prices(conn)
        conn.commit()

        dup_groups_after = count_duplicate_groups(conn)
        dup_rows_after = count_duplicate_rows(conn)

        print(f"Deleted rows: {deleted}")
        print(f"Duplicate groups after: {dup_groups_after}")
        print(f"Duplicate extra rows after: {dup_rows_after}")

        if dup_rows_after > 0:
            print("Warning: duplicates still remain. Inspect DB manually.")
            return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
