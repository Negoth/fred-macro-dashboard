"""Flatten config/series.yaml into a CSV for the dbt seed.

DuckDB cannot read YAML directly, so the config is passed to the SQL layer via a seed.
series.yaml is the source of truth; this CSV is a build artifact (overwritten every time).
"""

from __future__ import annotations

import csv
from pathlib import Path

from ingest.config import load

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "seeds" / "series_config.csv"

FIELDS = [
    "series_id",
    "title_display",
    "pillar",        # plain grouping category; the pillar scores themselves were retired
    "freq",
    "ingest",
    "role",          # series | derived | component | context
    "is_derived",    # whether it is a derived series
    "derived_from",  # components of a derived series (| separated)
    "origin",        # initial = inherited from the initial 5-series proposal
]


def build_rows(cfg: dict) -> list[dict]:
    rows: list[dict] = []

    def base(item: dict, *, derived: bool, role: str) -> dict:
        return {
            "series_id": item["id"],
            "title_display": item.get("title_display", ""),
            "pillar": item.get("pillar", ""),
            "freq": item.get("freq", ""),
            "ingest": item.get("ingest", ""),
            "role": role,
            "is_derived": int(derived),
            "derived_from": "|".join(item.get("inputs", [])),
            "origin": item.get("origin", ""),
        }

    for item in cfg.get("series", []):
        rows.append(base(item, derived=False, role="series"))
    for item in cfg.get("derived", []):
        rows.append(base(item, derived=True, role="derived"))
    for item in cfg.get("context", []):
        # Used for display, shading and sanity checks rather than as a dashboard signal
        rows.append(base(item, derived=False, role="context"))

    # Components of the derived series. They are fetched from FRED and land in
    # fct_observations, so dim_series needs rows for them too (without them the
    # relationship test fails)
    known = {r["series_id"] for r in rows}
    for item in cfg.get("derived", []):
        for raw_id in item.get("inputs", []):
            if raw_id in known:
                continue
            known.add(raw_id)
            rows.append({
                "series_id": raw_id,
                "title_display": f"Component of {item['title_display']}",
                "pillar": "",
                "freq": "",
                "ingest": item.get("ingest", ""),
                "role": "component",
                "is_derived": 0,
                "derived_from": "",
                "origin": "",
            })

    return sorted(rows, key=lambda r: r["series_id"])


def main() -> int:
    cfg = load()
    rows = build_rows(cfg)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)
    by_role: dict[str, int] = {}
    for r in rows:
        by_role[r["role"]] = by_role.get(r["role"], 0) + 1
    detail = ", ".join(f"{k} {v}" for k, v in sorted(by_role.items()))
    print(f"{OUT.relative_to(ROOT)}: {len(rows)} rows ({detail})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
