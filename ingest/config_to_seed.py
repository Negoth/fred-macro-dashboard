"""Flatten config/series.yaml into CSVs for the dbt seeds.

DuckDB cannot read YAML directly, so the config is passed to the SQL layer via seeds.
series.yaml is the source of truth; these CSVs are build artifacts (overwritten every time).

Writes:
    seeds/series_config.csv      the series dimension
    seeds/regime_indicators.csv  indicator monitor: thresholds, +/-2 rules, flags
    seeds/regime_seasons.csv     season labels, descriptions and what does well in each
    seeds/regime_phases.csv      credit cycle phase labels and descriptions
    seeds/regime_flags.csv       the 11 flags, their rules and why each threshold sits there
    seeds/regime_stances.csv     the four stances and what each means for the portfolio
"""

from __future__ import annotations

import csv
from pathlib import Path

from ingest.config import load

ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "seeds"
OUT = SEEDS / "series_config.csv"

# Regime seeds: (filename stem, config key under `regime`, columns).
# Columns are listed explicitly rather than inferred from the first row, so a missing
# optional key in the YAML becomes an empty cell instead of shifting the whole CSV.
REGIME_SEEDS = [
    ("regime_seasons", "seasons",
     ["key", "label", "curve_move", "rates", "curve", "description",
      "asset_class", "bonds", "sectors"]),
    ("regime_phases", "phases",
     ["key", "phase_no", "label", "equities", "spreads", "banks", "companies"]),
    ("regime_stances", "stances", ["key", "label", "description"]),
    ("regime_flags", "flags",
     ["key", "stage", "label", "rule", "rationale", "series"]),
    ("regime_indicators", "indicators",
     ["key", "monitor_group", "title", "unit", "score_rule", "score_t1", "score_t2",
      "flag_key", "flag_rule", "flag_threshold", "ref_lines", "first_month", "clip"]),
]

# Columns whose absence means something other than "empty". Without this, `clip` would be
# 1 on two rows and blank on ten, which DuckDB reads as NULL rather than false.
SEED_DEFAULTS = {"regime_indicators": {"clip": 0}}

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


def _cell(v: object) -> object:
    """Booleans reach DuckDB as the strings 'True'/'False' otherwise, which read as text."""
    if isinstance(v, bool):
        return int(v)
    return "" if v is None else v


def write_csv(path: Path, fields: list[str], rows: list[dict],
              defaults: dict | None = None) -> None:
    d = defaults or {}
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: _cell(r.get(k, d.get(k, ""))) for k in fields})


def main() -> int:
    cfg = load()
    rows = build_rows(cfg)
    SEEDS.mkdir(parents=True, exist_ok=True)
    write_csv(OUT, FIELDS, rows)
    by_role: dict[str, int] = {}
    for r in rows:
        by_role[r["role"]] = by_role.get(r["role"], 0) + 1
    detail = ", ".join(f"{k} {v}" for k, v in sorted(by_role.items()))
    print(f"{OUT.relative_to(ROOT)}: {len(rows)} rows ({detail})")

    regime = cfg.get("regime", {})
    for stem, key, fields in REGIME_SEEDS:
        items = regime.get(key, [])
        path = SEEDS / f"{stem}.csv"
        write_csv(path, fields, items, SEED_DEFAULTS.get(stem))
        print(f"{path.relative_to(ROOT)}: {len(items)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
