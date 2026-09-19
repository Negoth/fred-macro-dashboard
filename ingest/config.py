"""Reading config/series.yaml and resolving which series IDs to fetch.

series.yaml is the single input. ingest / transform / export all go through here.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "series.yaml"

# Valid ingestion cadences. These correspond to the three CI tracks (design note 3.7)
CADENCES = ("daily", "weekly", "monthly")


def load(path: Path | None = None) -> dict[str, Any]:
    """Read series.yaml."""
    with open(path or CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def fetch_targets(cfg: dict[str, Any], cadence: str | None = None) -> list[dict[str, str]]:
    """Resolve the series that are actually fetched from FRED.

    The derived series (NET_LIQUIDITY / WORLD_DOLLAR) do not exist on FRED, so they are
    expanded into their `inputs`. The context series are included in the targets as well.

    The same ID can appear in several roles, so targets are deduplicated by ID.
    When that happens the highest-frequency cadence wins (so nothing gets missed).
    """
    order = {"daily": 0, "weekly": 1, "monthly": 2}
    found: dict[str, dict[str, str]] = {}

    def add(series_id: str, ingest: str, origin: str) -> None:
        prev = found.get(series_id)
        if prev is None or order[ingest] < order[prev["ingest"]]:
            found[series_id] = {"series_id": series_id, "ingest": ingest, "origin": origin}

    for item in cfg.get("series", []):
        add(item["id"], item["ingest"], "series")

    for item in cfg.get("derived", []):
        # The derived series themselves are not on FRED. Fetch their components
        for raw_id in item.get("inputs", []):
            add(raw_id, item["ingest"], f"derived:{item['id']}")

    for item in cfg.get("context", []):
        add(item["id"], item["ingest"], "context")

    targets = sorted(found.values(), key=lambda d: d["series_id"])
    if cadence:
        if cadence not in CADENCES:
            raise ValueError(f"unknown cadence: {cadence} (expected one of {CADENCES})")
        targets = [t for t in targets if t["ingest"] == cadence]
    return targets


def equity_targets(cfg: dict[str, Any], cadence: str | None = None) -> list[dict[str, str]]:
    """Resolve the equity series, which are not on FRED and have their own ingester.

    Kept separate from fetch_targets because the two are fetched by different modules
    against different APIs. Same cadence filter, so the CI tracks route both the same way.
    """
    targets = [
        {
            "series_id": item["id"],
            "symbol": item["symbol"],
            "source": item.get("source", "yfinance"),
            "ingest": item["ingest"],
        }
        for item in cfg.get("equities", [])
    ]
    targets.sort(key=lambda d: d["series_id"])
    if cadence:
        if cadence not in CADENCES:
            raise ValueError(f"unknown cadence: {cadence} (expected one of {CADENCES})")
        targets = [t for t in targets if t["ingest"] == cadence]
    return targets
