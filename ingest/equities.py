"""Fetching equity index prices and landing the responses in the raw layer.

Deliberately the same shape as ingest/fred.py: raw is immutable and append-only
(principle 1), the landing path is determined by the ingestion date (principle 2), and
every fetch is accompanied by a manifest (principle 3).

The S&P 500 is not on FRED at a useful length -- FRED's own SP500 series covers only the
last 10 years, and the regime panel starts in 1985 -- so it comes from yfinance.

yfinance is an unofficial API, so a failure here returns non-zero and stops the CI run
before dbt updates the signals from partial data. Keeping a fallback source is an open
question recorded in config/series.yaml.

Usage:
    uv run python -m ingest.equities --dry-run          # only show what would be fetched
    uv run python -m ingest.equities --full             # first run: the full history
    uv run python -m ingest.equities --cadence daily    # CI: the daily symbols only
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from dateutil.relativedelta import relativedelta

from ingest.config import CADENCES, equity_targets, load

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "yfinance"

# The regime panel starts in 1985. Fetch from earlier so its first months are not a
# partial window once the six-month changes are computed.
FULL_START = "1984-01-01"


def fetch(symbol: str, start: str) -> tuple[dict, dict]:
    """Fetch daily closes. Returns (request parameters for the manifest, payload).

    The payload mirrors FRED's observations shape -- {"observations": [{"date", "value"}]}
    -- so the staging model can stay a near-copy of stg_fred_observations rather than
    inventing a second unpacking idiom.

    auto_adjust=False keeps the raw close. The index is a price index, not a total-return
    series, so there is nothing to adjust for, and the reference implementation used the
    same setting.
    """
    import yfinance as yf

    params = {"symbol": symbol, "start": start, "interval": "1d", "auto_adjust": False}
    df = yf.download(symbol, start=start, interval="1d", auto_adjust=False, progress=False)
    if df is None or len(df) == 0:
        raise RuntimeError(f"no data returned for {symbol} from {start}")

    close = df["Close"]
    # yfinance returns a single-column frame when given one ticker
    if hasattr(close, "columns"):
        close = close.iloc[:, 0]
    close = close.dropna()
    if close.index.tz is not None:
        close.index = close.index.tz_localize(None)
    if len(close) == 0:
        raise RuntimeError(f"{symbol}: every close was empty from {start}")

    observations = [{"date": d.strftime("%Y-%m-%d"), "value": f"{v:.6f}"}
                    for d, v in close.items()]
    payload = {
        "symbol": symbol,
        "count": len(observations),
        "observation_start": observations[0]["date"],
        "observation_end": observations[-1]["date"],
        "observations": observations,
    }
    return params, payload


def land(series_id: str, symbol: str, request_params: dict, payload: dict) -> Path:
    """Land the raw response immutably and idempotently.

    A same-day rerun overwrites the same path = no duplicates are created (principle 2).
    The directory is keyed on series_id (GSPC), not on the symbol (^GSPC): a caret is
    awkward in a path and in the glob the staging model passes to DuckDB. The symbol as
    requested is recorded in the payload and the manifest.
    """
    out = RAW / f"symbol={series_id}" / f"ingested_at={date.today()}"
    out.mkdir(parents=True, exist_ok=True)

    body = json.dumps(payload, sort_keys=True).encode()
    (out / "response.json.gz").write_bytes(gzip.compress(body))

    (out / "manifest.json").write_text(
        json.dumps(
            {
                "source": "yfinance",
                "series_id": series_id,
                "symbol": symbol,
                "endpoint": "https://query2.finance.yahoo.com (via yfinance)",
                "request_params": request_params,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "sha256": hashlib.sha256(body).hexdigest(),
                "n_observations": payload.get("count"),
                "observation_start": payload.get("observation_start"),
                "observation_end": payload.get("observation_end"),
                "title": "S&P 500 index (daily close)",
                "units": "Index",
                "frequency_short": "D",
                "license": "Yahoo Finance terms apply. Unofficial API, personal use only",
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Fetch equity index prices and land the responses in the raw layer")
    ap.add_argument("--cadence", choices=CADENCES,
                    help="fetch only the symbols with this cadence (for CI)")
    ap.add_argument("--full", action="store_true",
                    help="fetch the full history. For the first run, so the regime panel can start in 1985")
    ap.add_argument("--overwrite", action="store_true",
                    help="overwrite even if today's raw data already exists")
    ap.add_argument("--dry-run", action="store_true", help="only show the targets, do not fetch")
    args = ap.parse_args(argv)

    cfg = load()
    targets = equity_targets(cfg, args.cadence)

    lookback = None if args.full else cfg["meta"].get("lookback_months", 24)
    start = FULL_START if lookback is None else (
        date.today() - relativedelta(months=lookback)).isoformat()
    scope = "full history" if lookback is None else f"trailing {lookback} months"
    print(f"{len(targets)} symbols / range: {scope} (from {start})"
          + (f" / cadence={args.cadence}" if args.cadence else ""))

    if args.dry_run:
        for t in targets:
            print(f"  {t['series_id']:<14} {t['symbol']:<10} {t['ingest']:<8} {t['source']}")
        return 0

    if not targets:
        print("Nothing to fetch for this cadence")
        return 0

    # Same guard as ingest.fred: the landing path is keyed on the ingestion date, so an
    # incremental run on the same day as a --full run would overwrite the full history with
    # a 24-month window. Skipping makes a same-day rerun a no-op instead.
    if lookback is not None and not args.overwrite:
        already = [
            t for t in targets
            if (RAW / f"symbol={t['series_id']}" / f"ingested_at={date.today()}").exists()
        ]
        if already:
            targets = [t for t in targets if t not in already]
            print(
                f"  Skipping {len(already)} symbols that already have today's data "
                f"({', '.join(x['series_id'] for x in already)}).\n"
                f"  Pass --overwrite to overwrite them, or --full to refetch the full history."
            )
        if not targets:
            print("Nothing to fetch (everything has already been fetched today)")
            return 0

    failures: list[tuple[str, str]] = []
    for i, t in enumerate(targets, 1):
        sid, symbol = t["series_id"], t["symbol"]
        try:
            params, payload = fetch(symbol, start)
            out = land(sid, symbol, params, payload)
            print(f"  [{i:>2}/{len(targets)}] {sid:<14} {payload['count']:>6} obs "
                  f"-> {out.relative_to(ROOT)}")
        except Exception as e:  # noqa: BLE001 - one failing symbol must not stop the whole run
            failures.append((sid, str(e)))
            print(f"  [{i:>2}/{len(targets)}] {sid:<14} failed: {e}", file=sys.stderr)

    if failures:
        # Non-zero so CI stops before dbt updates the signals from partial data
        print(f"\n{len(failures)} symbols failed:", file=sys.stderr)
        for sid, err in failures:
            print(f"  {sid}: {err}", file=sys.stderr)
        return 1

    print(f"\nDone: {len(targets)} symbols")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
