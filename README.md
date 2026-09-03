# Macro Economy Dashboard

A pipeline that pulls FRED macroeconomic statistics, transforms them in DuckDB, and hands them to Tableau.

- Design and decision record: [`docs/fred-tableau-architecture.md`](docs/fred-tableau-architecture.md)
- Definition of the target series: [`config/series.yaml`](config/series.yaml) ← this is the single input

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

# Afterwards: the trailing 24 months (strategy A / design note 3.2)
uv run python -m ingest.fred

# For CI: fetch by cadence (design note 3.7)
uv run python -m ingest.fred --cadence daily

# config/series.yaml -> dbt seed
uv run python -m ingest.config_to_seed

# Transform and test
uv run dbt build --profiles-dir .

# Write out the CSVs for Tableau
uv run python -m export.to_csv
```

`--full` is used on the first run **so that the z-score population covers the full history**.
Starting from `history_start` (2003-01) burns 12 months on `yoy` and 60 months on `min_periods`,
so z-scores would only begin around 2009 and the GFC would fall outside the z-score window.
`history_start` is treated as a display filter on the Tableau side.

> **Incremental ingestion skips any series that already has today's raw data.** The landing path is
> determined by the ingestion date (principle 2: idempotency), so running it as-is would let the
> 24-month window overwrite the full-history data and wipe out the history. Idempotency is preserved
> because a same-day rerun becomes a no-op.
> Use `--overwrite` to overwrite deliberately, or `--full` to refetch the full history.

## Automatic updates

GitHub Actions runs on three tracks. The `ingest` field in `config/series.yaml` is the routing key
(design note 3.7 / principle 5: match ingestion frequency to the publication schedule).

| workflow | cron | target |
| --- | --- | --- |
| `ingest-daily.yml` | weekdays 23:00 UTC | 12 market-data series |
| `ingest-weekly.yml` | Thu 13:00 UTC | 7 series |
| `ingest-monthly.yml` | 2nd of each month 06:00 UTC | 13 economic-statistics series |

The steps live in exactly one place, the reusable workflow `ingest.yml`.
`FRED_API_KEY` must be registered in the repository secrets.

Only `data/raw` is committed. The mart layer is treated as a build artifact
(`fct_observations.csv` alone is 12 MB, and running daily would grow the repository by 5 GB a year).
If the `dbt build` tests fail, the run stops without committing.

## Output

Tableau reads only the CSVs in `data/mart/`.

| file | contents |
| --- | --- |
| `fct_observations.csv` | Observations at native grain (long format) + NBER recession flag |
| `dim_series.csv` | Series metadata (display name, units, frequency, pillar) |

## Layer structure

```
data/raw/fred/     API responses landed as-is. Immutable, append-only
models/staging/    Type casting and cleansing
models/marts/      Final BI-facing shape. This is the only layer Tableau reads
data/mart/*.csv    Handoff artifacts for Tableau
```

See design note 2.2 for each layer's responsibilities and prohibitions. No logic goes in the BI layer (principle 4).
