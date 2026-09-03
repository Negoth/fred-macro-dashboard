"""Fetching from FRED and landing the responses in the raw layer.

Uses strategy A (trailing window) from design note 3.2.
raw is immutable and append-only (principle 1), the landing path is determined by the
ingestion date (principle 2), and every fetch is accompanied by a manifest (principle 3).

Usage:
    uv run python -m ingest.fred --dry-run          # only show what would be fetched
    uv run python -m ingest.fred --full             # first run: the full history
    uv run python -m ingest.fred --cadence daily    # CI: the daily series only
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from ingest.config import CADENCES, fetch_targets, load

API = "https://api.stlouisfed.org/fred/series/observations"
META_API = "https://api.stlouisfed.org/fred/series"
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "fred"

# The FRED rate limit is 120 req/min (design note 4). We make two requests per series,
# so leave some headroom
SLEEP_SEC = 0.6


def _redact(text: str, api_key: str) -> str:
    """Mask the API key in exception messages.

    httpx exceptions include the whole URL, so api_key would appear verbatim as a query
    parameter. CI logs are public, so everything must go through here before being printed.
    """
    return text.replace(api_key, "***") if api_key else text


def _get(client: httpx.Client, url: str, params: dict, api_key: str,
         attempts: int = 3) -> httpx.Response:
    """Retry transient 5xx responses. FRED sporadically returns 502."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r
        except httpx.HTTPStatusError as e:
            # Retrying will not fix a 4xx (nonexistent series, wrong key, and so on)
            if e.response.status_code < 500:
                raise RuntimeError(_redact(str(e), api_key)) from None
            last = e
        except httpx.RequestError as e:
            last = e
        if i < attempts - 1:
            time.sleep(2 ** i)
    raise RuntimeError(_redact(str(last), api_key)) from None


def _params(series_id: str, api_key: str, lookback_months: int | None) -> dict[str, str]:
    p = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
    if lookback_months is not None:
        p["observation_start"] = (date.today() - relativedelta(months=lookback_months)).isoformat()
    return p


def fetch(client: httpx.Client, series_id: str, api_key: str,
          lookback_months: int | None) -> tuple[dict, dict, dict]:
    """Fetch the observations and the metadata.

    Returns: (request parameters to record in the manifest, observations response, series metadata)
    """
    params = _params(series_id, api_key, lookback_months)
    r = _get(client, API, params, api_key)
    m = _get(client, META_API,
             {"series_id": series_id, "api_key": api_key, "file_type": "json"}, api_key)

    # api_key is not recorded in the manifest
    safe = {k: v for k, v in params.items() if k != "api_key"}
    return safe, r.json(), m.json()


def land(series_id: str, request_params: dict, payload: dict, meta: dict) -> Path:
    """Land the raw response immutably and idempotently.

    A same-day rerun overwrites the same path = no duplicates are created (principle 2).
    """
    out = RAW / f"series_id={series_id}" / f"ingested_at={date.today()}"
    out.mkdir(parents=True, exist_ok=True)

    body = json.dumps(payload, sort_keys=True).encode()
    (out / "response.json.gz").write_bytes(gzip.compress(body))

    meta_body = json.dumps(meta, sort_keys=True).encode()
    (out / "series_meta.json.gz").write_bytes(gzip.compress(meta_body))

    seriess = (meta.get("seriess") or [{}])[0]
    (out / "manifest.json").write_text(
        json.dumps(
            {
                "source": "FRED",
                "series_id": series_id,
                "endpoint": API,
                "request_params": request_params,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
                "sha256": hashlib.sha256(body).hexdigest(),
                "n_observations": payload.get("count"),
                "observation_start": seriess.get("observation_start"),
                "observation_end": seriess.get("observation_end"),
                "last_updated": seriess.get("last_updated"),
                "title": seriess.get("title"),
                "units": seriess.get("units"),
                "frequency_short": seriess.get("frequency_short"),
                "seasonal_adjustment_short": seriess.get("seasonal_adjustment_short"),
                "license": "https://fred.stlouisfed.org/legal/",
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Fetch from FRED and land the responses in the raw layer")
    ap.add_argument("--cadence", choices=CADENCES, help="fetch only the series with this cadence (for CI)")
    ap.add_argument("--full", action="store_true",
                    help="fetch the full history. For the first run, and so that the z-score population covers the full history")
    ap.add_argument("--overwrite", action="store_true",
                    help="overwrite even if today's raw data already exists")
    ap.add_argument("--dry-run", action="store_true", help="only show the targets, do not fetch")
    args = ap.parse_args(argv)

    cfg = load()
    targets = fetch_targets(cfg, args.cadence)

    lookback = None if args.full else cfg["meta"].get("lookback_months", 24)
    scope = "full history" if lookback is None else f"trailing {lookback} months"
    print(f"{len(targets)} series / range: {scope}"
          + (f" / cadence={args.cadence}" if args.cadence else ""))

    if args.dry_run:
        for t in targets:
            print(f"  {t['series_id']:<14} {t['ingest']:<8} {t['origin']}")
        return 0

    load_dotenv(ROOT / ".env")
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print(
            "FRED_API_KEY is not set.\n"
            "  Get one at https://fredaccount.stlouisfed.org/apikey and add the line\n"
            "  FRED_API_KEY=<key>\n"
            "  to .env (see .env.example).",
            file=sys.stderr,
        )
        return 1

    # The landing path is determined by the ingestion date (principle 2: idempotency). Running an
    # incremental fetch on the same day as a --full run would let the 24-month window overwrite the
    # full-history data and wipe out the history. If that happened in CI, truncated raw data would be
    # committed and the full history would be lost from git.
    # So skip anything that already has today's data (idempotency is preserved because a same-day
    # rerun becomes a no-op).
    if lookback is not None and not args.overwrite:
        already = [
            t for t in targets
            if (RAW / f"series_id={t['series_id']}" / f"ingested_at={date.today()}").exists()
        ]
        if already:
            targets = [t for t in targets if t not in already]
            print(
                f"  Skipping {len(already)} series that already have today's data "
                f"({', '.join(x['series_id'] for x in already[:5])}"
                f"{'...' if len(already) > 5 else ''}).\n"
                f"  Pass --overwrite to overwrite them, or --full to refetch the full history."
            )
        if not targets:
            print("Nothing to fetch (everything has already been fetched today)")
            return 0

    failures: list[tuple[str, str]] = []
    with httpx.Client(timeout=30) as client:
        for i, t in enumerate(targets, 1):
            sid = t["series_id"]
            try:
                params, payload, meta = fetch(client, sid, api_key, lookback)
                out = land(sid, params, payload, meta)
                print(f"  [{i:>2}/{len(targets)}] {sid:<14} {payload.get('count'):>6} obs -> {out.relative_to(ROOT)}")
            except Exception as e:  # noqa: BLE001 - one failing series must not stop the whole run
                msg = _redact(str(e), api_key)
                failures.append((sid, msg))
                print(f"  [{i:>2}/{len(targets)}] {sid:<14} failed: {msg}", file=sys.stderr)
            time.sleep(SLEEP_SEC)

    if failures:
        print(f"\n{len(failures)} series failed:", file=sys.stderr)
        for sid, err in failures:
            print(f"  {sid}: {err}", file=sys.stderr)
        return 1

    print(f"\nDone: {len(targets)} series")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
