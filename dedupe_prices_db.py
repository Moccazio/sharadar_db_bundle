#!/usr/bin/env python3
"""Detect and remove duplicate rows in sharadar SQLite databases.
- python dedupe_prices_db.py --dry-run

Checks:
  prices.sqlite      — prices            keyed on (date, sid)
  adjustments.sqlite — splits            keyed on (effective_date, sid)
                     — mergers           keyed on (effective_date, sid)
                     — dividends         keyed on (effective_date, sid)
                     — dividend_payouts  keyed on (date, sid)
                     — stock_dividend_payouts keyed on (sid, ex_date)
  assets-*.sqlite    — equity_supplementary_mappings keyed on (sid, field, start_date)
                       (covers both fundamentals and daily metrics)
"""
import argparse
import glob
import os
import shutil
import sqlite3
from contextlib import closing
from typing import List, Tuple


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def default_prices_path() -> str:
    zipline_root = os.environ.get("ZIPLINE_ROOT", os.path.join(os.path.expanduser("~"), ".zipline"))
    return os.path.join(zipline_root, "data", "sharadar", "latest", "prices.sqlite")


def sibling_path(prices_path: str, filename: str) -> str:
    return os.path.join(os.path.dirname(prices_path), filename)


def find_assets_db(prices_path: str) -> str:
    """Return the first assets-*.sqlite found next to prices.sqlite, or empty string."""
    pattern = sibling_path(prices_path, "assets-*.sqlite")
    matches = sorted(glob.glob(pattern))
    return matches[-1] if matches else ""


# ---------------------------------------------------------------------------
# Generic duplicate helpers
# ---------------------------------------------------------------------------

def _group_by(key_cols: Tuple[str, ...]) -> str:
    return ", ".join(f'"{c}"' for c in key_cols)


def count_duplicate_rows(conn: sqlite3.Connection, table: str, key_cols: Tuple[str, ...]) -> int:
    gb = _group_by(key_cols)
    sql = f"""
    SELECT COALESCE(SUM(cnt - 1), 0)
    FROM (
        SELECT COUNT(*) AS cnt
        FROM "{table}"
        GROUP BY {gb}
        HAVING COUNT(*) > 1
    )
    """
    return int(conn.execute(sql).fetchone()[0])


def count_duplicate_groups(conn: sqlite3.Connection, table: str, key_cols: Tuple[str, ...]) -> int:
    gb = _group_by(key_cols)
    sql = f"""
    SELECT COUNT(*)
    FROM (
        SELECT 1
        FROM "{table}"
        GROUP BY {gb}
        HAVING COUNT(*) > 1
    )
    """
    return int(conn.execute(sql).fetchone()[0])


def print_duplicate_samples(
    conn: sqlite3.Connection,
    table: str,
    key_cols: Tuple[str, ...],
    limit: int = 10,
) -> None:
    gb = _group_by(key_cols)
    col_list = ", ".join(f'"{c}"' for c in key_cols)
    sql = f"""
    SELECT {col_list}, COUNT(*) AS cnt, MIN(rowid) AS min_rowid, MAX(rowid) AS max_rowid
    FROM "{table}"
    GROUP BY {gb}
    HAVING COUNT(*) > 1
    ORDER BY {gb}
    LIMIT ?
    """
    rows = conn.execute(sql, (limit,)).fetchall()
    if rows:
        headers = key_cols + ("cnt", "min_rowid", "max_rowid")
        print(f"  Sample duplicates ({', '.join(headers)}):")
        for row in rows:
            print(f"    {row}")


def dedupe_table(conn: sqlite3.Connection, table: str, key_cols: Tuple[str, ...]) -> int:
    """Delete all but the highest-rowid row for each key group."""
    gb = _group_by(key_cols)
    delete_sql = f"""
    DELETE FROM "{table}"
    WHERE rowid NOT IN (
        SELECT MAX(rowid)
        FROM "{table}"
        GROUP BY {gb}
    )
    """
    cur = conn.execute(delete_sql)
    return cur.rowcount if cur.rowcount is not None else 0


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


# ---------------------------------------------------------------------------
# Per-database check/clean routines
# ---------------------------------------------------------------------------

TableSpec = Tuple[str, Tuple[str, ...]]  # (table_name, key_cols)

PRICES_TABLES: List[TableSpec] = [
    ("prices", ("date", "sid")),
]

ADJUSTMENT_TABLES: List[TableSpec] = [
    ("splits",                ("effective_date", "sid")),
    ("mergers",               ("effective_date", "sid")),
    ("dividends",             ("effective_date", "sid")),
    ("dividend_payouts",      ("date", "sid")),
    ("stock_dividend_payouts",("sid", "ex_date")),
]

ASSETS_TABLES: List[TableSpec] = [
    ("equity_supplementary_mappings", ("sid", "field", "start_date")),
]


def check_and_clean_db(
    db_path: str,
    table_specs: List[TableSpec],
    dry_run: bool,
    backup: bool,
) -> Tuple[int, int]:
    """
    Returns (total_duplicate_extra_rows_before, total_deleted).
    Skips tables that don't exist in this DB.
    """
    if not os.path.exists(db_path):
        print(f"  [skip] not found: {db_path}")
        return 0, 0

    if backup:
        bak = db_path + ".bak"
        shutil.copy2(db_path, bak)
        print(f"  Backup created: {bak}")

    total_before = 0
    total_deleted = 0

    with closing(sqlite3.connect(db_path)) as conn:
        for table, key_cols in table_specs:
            if not table_exists(conn, table):
                print(f"  [{table}] table not found — skipping")
                continue

            groups = count_duplicate_groups(conn, table, key_cols)
            extra = count_duplicate_rows(conn, table, key_cols)
            key_str = ", ".join(key_cols)
            print(f"  [{table}] duplicate groups: {groups}  extra rows: {extra}  (key: {key_str})")
            total_before += extra

            if extra > 0:
                print_duplicate_samples(conn, table, key_cols)

                if not dry_run:
                    conn.execute("BEGIN")
                    deleted = dedupe_table(conn, table, key_cols)
                    conn.commit()
                    remaining = count_duplicate_rows(conn, table, key_cols)
                    print(f"  [{table}] deleted {deleted} rows — remaining extra rows: {remaining}")
                    if remaining > 0:
                        print(f"  [{table}] WARNING: duplicates still remain — inspect manually")
                    total_deleted += deleted

    return total_before, total_deleted


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Detect and remove duplicate rows in sharadar SQLite databases "
            "(prices, adjustments/splits/dividends, fundamentals/metrics)."
        )
    )
    parser.add_argument(
        "--db",
        default=default_prices_path(),
        help="Path to prices.sqlite (adjustments.sqlite and assets-*.sqlite are auto-located "
             "in the same directory). Default: ~/.zipline/data/sharadar/latest/prices.sqlite",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report duplicates only; do not modify any database.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create .bak copies before modifying any database.",
    )
    args = parser.parse_args()

    prices_path = os.path.expanduser(args.db)
    adj_path = sibling_path(prices_path, "adjustments.sqlite")
    assets_path = find_assets_db(prices_path)

    if args.dry_run:
        print("=== DRY RUN — no changes will be made ===")

    grand_total_before = 0
    grand_total_deleted = 0

    # --- prices.sqlite ---
    print(f"\n[prices.sqlite] {prices_path}")
    before, deleted = check_and_clean_db(prices_path, PRICES_TABLES, args.dry_run, args.backup)
    grand_total_before += before
    grand_total_deleted += deleted

    # --- adjustments.sqlite (splits, mergers, dividends) ---
    print(f"\n[adjustments.sqlite] {adj_path}")
    before, deleted = check_and_clean_db(adj_path, ADJUSTMENT_TABLES, args.dry_run, args.backup)
    grand_total_before += before
    grand_total_deleted += deleted

    # --- assets-*.sqlite (fundamentals + daily metrics) ---
    if assets_path:
        print(f"\n[{os.path.basename(assets_path)}] {assets_path}")
        before, deleted = check_and_clean_db(assets_path, ASSETS_TABLES, args.dry_run, args.backup)
        grand_total_before += before
        grand_total_deleted += deleted
    else:
        print("\n[assets-*.sqlite] not found — skipping fundamentals/metrics check")

    # --- Summary ---
    print(f"\n=== Summary ===")
    print(f"Total duplicate extra rows found: {grand_total_before}")
    if args.dry_run:
        print("Dry run — nothing deleted.")
    else:
        print(f"Total rows deleted: {grand_total_deleted}")

    return 1 if (not args.dry_run and grand_total_deleted != grand_total_before) else 0


if __name__ == "__main__":
    raise SystemExit(main())
