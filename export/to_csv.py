"""marts -> data/mart/*.csv. These CSVs are the only thing Tableau reads.

Internal processing uses DuckDB (equivalent to Parquet); what is handed to Tableau is CSV.
CSV is the reliable file connector for Tableau Public (design note 3.6 / 5).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"
OUT_DIR = ROOT / "data" / "mart"

# The marts handed to Tableau. The regime marts are added as they land
TABLES = [
    "dim_series",
    "fct_observations",
    "fct_regime",
    "fct_yield_curve",
    "fct_indicator_monitor",
    "fct_monitor_caps",
    "fct_stance_episodes",
]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Write the marts out as CSVs for Tableau")
    ap.add_argument("--tables", nargs="*", default=TABLES, help="tables to write out")
    args = ap.parse_args(argv)

    if not WAREHOUSE.exists():
        raise SystemExit(f"{WAREHOUSE.relative_to(ROOT)} does not exist. Run `dbt build` first.")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        for table in args.tables:
            out = OUT_DIR / f"{table}.csv"
            con.execute(f"copy (select * from main.{table}) to '{out}' (header, delimiter ',')")
            n, = con.execute(f"select count(*) from main.{table}").fetchone()
            size_kb = out.stat().st_size / 1024
            print(f"  {out.relative_to(ROOT)}  {n:>8,} rows  {size_kb:>8,.0f} KB")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
