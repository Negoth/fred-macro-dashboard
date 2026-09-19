# Macro Economy Dashboard

A pipeline that pulls FRED macroeconomic statistics and the S&P 500, transforms them in DuckDB,
and hands them to Tableau.

The dashboard answers one question — **"which way should I tilt the centre of gravity of my
assets right now, and how far?"** — with a single stance, and shows every input behind it.

- Design and decision record: [`docs/fred-tableau-architecture.md`](docs/fred-tableau-architecture.md)
- Definition of the target series and every threshold: [`config/series.yaml`](config/series.yaml) ← this is the single input

## Setup

You need a FRED API key (free). Get one at <https://fredaccount.stlouisfed.org/apikey> and
put it in `.env` at the repository root:

```
FRED_API_KEY=<your key>
```

Sync dependencies:

```bash
uv sync
```

## Running

```bash
# Check the target series (does not fetch)
uv run python -m ingest.fred --dry-run

# First run: fetch the full history
uv run python -m ingest.fred --full
uv run python -m ingest.equities --full

# Afterwards: the trailing 24 months (strategy A / design note 3.2)
uv run python -m ingest.fred
uv run python -m ingest.equities

# For CI: fetch by cadence (design note 3.7)
uv run python -m ingest.fred --cadence daily

# config/series.yaml -> dbt seeds
uv run python -m ingest.config_to_seed

# Transform and test
uv run dbt build --profiles-dir .

# Write out the CSVs for Tableau
uv run python -m export.to_csv
```

> **Incremental ingestion skips any series that already has today's raw data.** The landing path is
> determined by the ingestion date (principle 2: idempotency), so running it as-is would let the
> 24-month window overwrite full-history data. Idempotency is preserved because a same-day rerun
> becomes a no-op. Use `--overwrite` to overwrite deliberately, or `--full` to refetch the history.
>
> A consequence worth knowing: **a series added after the initial backfill is stuck at 24 months**,
> because the scheduled runs can never widen their own window. That is what left `DRTSCILM` with 9
> observations for weeks. Run the `full` dispatch below after adding a series.

## Automatic updates

GitHub Actions runs on three tracks. The `ingest` field in `config/series.yaml` is the routing key
(design note 3.7 / principle 5: match ingestion frequency to the publication schedule).

| workflow | cron | target |
| --- | --- | --- |
| `ingest-daily.yml` | weekdays 23:00 UTC | 17 market-data series + the S&P 500 |
| `ingest-weekly.yml` | Thu 13:00 UTC | 7 series |
| `ingest-monthly.yml` | 2nd of each month 06:00 UTC | 14 economic-statistics series |

The steps live in exactly one place, the reusable workflow `ingest.yml`.
`FRED_API_KEY` must be registered in the repository secrets.

**One-off backfills** run from the same workflows: Actions → the track → *Run workflow* → tick
**Fetch the full history**. The key stays in GitHub rather than on a laptop.

Only `data/raw` and `seeds/` are committed. The mart layer is treated as a build artifact.
If the `dbt build` tests fail, the run stops without committing.

## Output

Tableau reads only the CSVs in `data/mart/`.

| file | contents |
| --- | --- |
| `fct_regime.csv` | Month, season, credit phase, the eleven flags, the counts and the stance |
| `fct_yield_curve.csv` | Yield curve now, six months ago and twelve months ago |
| `fct_indicator_monitor.csv` | Each indicator against its threshold, with its score or flag |
| `fct_monitor_caps.csv` | Display-only y-axis caps for the spiking indicators |
| `fct_stance_episodes.csv` | Every period spent in Cut equities, for the falsification check |
| `fct_observations.csv` | Observations at native grain (long format) + NBER recession flag |
| `dim_series.csv` | Series metadata (display name, units, frequency) |

## Layer structure

```
data/raw/fred/       FRED responses landed as-is. Immutable, append-only
data/raw/yfinance/   S&P 500 responses, same layout
models/staging/      Type casting and cleansing
models/intermediate/ int_regime_monthly: the monthly panel behind both cycles
models/marts/        Final BI-facing shape. This is the only layer Tableau reads
data/mart/*.csv      Handoff artifacts for Tableau
```

See design note 2.2 for each layer's responsibilities and prohibitions. No logic goes in the BI
layer (principle 4) — the marts carry the distance-to-threshold values and the display caps so
Tableau does no arithmetic of its own.

## What the signals are, briefly

Two cycles, read independently, because a cycle of about 5 years and one of about 10 years do not
travel one circle together — on the retired map, 22 of 45 season changes ran backwards.

- **Monetary policy cycle.** Six-month changes in fed funds and the 10-year yield give a level and
  a slope, and the four quadrants are the four ways a yield curve moves. Below one policy step
  (25bp) at both ends, the previous season carries over.
- **Credit cycle.** Six-month directions of the S&P 500 and the BAA spread. The phase switches only
  after two consecutive months.

Eleven flags across three stages feed a four-state stance. **Nothing is summed** — the flags are
counted, and two are required before the portfolio moves. Every threshold is a natural boundary,
a policy unit or an official estimate, never a value fitted to history. Design note 6.2 records
the two composite scores that were tested and rejected, and 6.7 lists every period the rules fired
on, including the misses.
