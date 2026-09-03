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
    "pillar",
    "pillar_kind",   # coordinate | score
    "freq",
    "ingest",
    "transform",
    "sign",
    "season_axis",   # rate | credit | (empty)
    "season_role",   # leading_signal | (empty)
    "role",          # series | derived | component | context
    "is_scored",     # whether it counts toward the pillar score
    "is_derived",    # whether it is a derived series
    "derived_from",  # components of a derived series (| separated)
    "origin",        # initial = inherited from the initial 5-series proposal
]


def build_rows(cfg: dict) -> list[dict]:
    pillars = cfg.get("pillars", {})
    rows: list[dict] = []

    def base(item: dict, *, scored: bool, derived: bool, role: str) -> dict:
        pillar = item.get("pillar", "")
        return {
            "series_id": item["id"],
            "title_display": item.get("title_display", ""),
            "pillar": pillar,
            "pillar_kind": pillars.get(pillar, {}).get("kind", ""),
            "freq": item.get("freq", ""),
            "ingest": item.get("ingest", ""),
            "transform": item.get("transform", ""),
            "sign": item.get("sign", ""),
            "season_axis": item.get("season_axis", ""),
            "season_role": item.get("season_role", ""),
            "role": role,
            "is_scored": int(scored),
            "is_derived": int(derived),
            "derived_from": "|".join(item.get("inputs", [])),
            "origin": item.get("origin", ""),
        }

    for item in cfg.get("series", []):
        rows.append(base(item, scored=True, derived=False, role="series"))
    for item in cfg.get("derived", []):
        rows.append(base(item, scored=True, derived=True, role="derived"))
    for item in cfg.get("context", []):
        # Not counted in the scores, but used for display, shading and sanity checks
        rows.append(base(item, scored=False, derived=False, role="context"))

    # Components of the derived series. They are fetched from FRED and land in
    # fct_observations, so dim_series needs rows for them too (without them the
    # relationship test fails). They do not count toward the pillar scores themselves
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
                "pillar_kind": "",
                "freq": "",
                "ingest": item.get("ingest", ""),
                "transform": "",
                "sign": "",
                "season_axis": "",
                "season_role": "",
                "role": "component",
                "is_scored": 0,
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
    scored = sum(r["is_scored"] for r in rows)
    print(f"{OUT.relative_to(ROOT)}: {len(rows)} rows (scored {scored} / context {len(rows)-scored})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
