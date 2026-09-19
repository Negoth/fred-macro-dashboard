# Data ingestion -> Tableau pipeline design

A design note for building a personal-scale analytics pipeline along the lines of professional best
practice. The first half is the general structure; the second half makes it concrete using the FRED
API as the subject.

Created: 2026-08-15 / Updated: 2026-08-18

Status: question and series settled (chapter 6) / existence of the series verified (2026-08-18) /
pipeline implemented (ingestion, transformation, scoring, quadrant maps) / the four-seasons
thresholds are provisional (pending confirmation against the original book)

---

## 1. Purpose

Answer one macro question end to end: **where are we in the interest-rate and credit cycle, and in
the growth/inflation regime?** Everything below serves that question.

The work is split deliberately across two places:

| Part          | Where               | What it carries                                             |
| ------------- | ------------------- | ----------------------------------------------------------- |
| Visualisation | Tableau Public      | The quadrant maps, the trajectory, the interaction           |
| Engineering   | This repository     | Ingestion, transformation, scoring, and the provenance trail |

A `.twbx` cannot show the code inside it, so the pipeline is kept in the open, where each decision
and its reasoning can be read alongside the SQL that implements it.

### Non-goals

- Demonstrating performance on large-scale data (out of proportion to the subject matter)
- Bringing in Iceberg / catalogues / distributed processing that the scale does not justify

---

## 2. General structure

### 2.1 Layers

```
External source (API / file distribution)
      │
      ▼
[ raw ]         Store the fetched responses as-is. Immutable, append-only
      │
      ▼
[ staging ]     Type casting, renaming, cleansing. One model per source
      │
      ▼
[ intermediate ] Joins and intermediate logic
      │
      ▼
[ marts ]       Final business-level shape. The only layer BI reads
      │
      ▼
[ BI ]          Tableau. Visualisation and interaction only
```

The layer names follow the dbt convention (synonymous with the medallion bronze/silver/gold naming,
but consistent with the tool actually used here).

### 2.2 Responsibilities and prohibitions per layer

| Layer        | Does                                                    | Must not do                       |
| ------------ | ------------------------------------------------------- | --------------------------------- |
| raw          | Land API responses as-is                                | Parse, reshape, overwrite, delete |
| staging      | Type casting, column-name normalisation, missing values | Joins, aggregation, business logic |
| intermediate | Joins, grain changes                                    | Presentation-driven processing    |
| marts        | Denormalisation, metric computation, BI-facing shaping  | Refetching the raw data           |
| BI           | Visualisation, filters, dashboard interactions          | SQL, complex computational logic  |

### 2.3 Core principles

**Principle 1: raw is immutable**

- Land each run as a separate file. Never append to a single file
- If a parsing bug turns up later, the full history can be rebuilt without hitting the API again
- If the API changes its specification, the responses already fetched remain on hand

**Principle 2: idempotency**

- Rerunning with the same input must produce the same result
- Make the landing path deterministic (partition by ingestion date, not by run time)
- A same-day rerun overwrites the same path = no duplicates are created

**Principle 3: record the provenance**

Attach a manifest to every fetch. It underwrites reproducibility, and in the field of impact
evaluation it is an academic requirement in its own right.

```yaml
source_url: ...
retrieved_at: 2026-08-15T09:12:00Z
sha256: 3f7a...
license: ...
request_params: { ... }
```

**Principle 4: no logic in the BI layer**

A `.twbx` is a zipped XML whose structure shifts with every GUI action, so diffs are unreadable and
review and version control are effectively impossible. Logic always goes upstream, in the SQL.

**Principle 5: match ingestion frequency to the publication schedule**

Do not poll data daily if it is not updated daily. It wastes storage and run time, and reads as a
lack of judgement.

### 2.4 Technology choices at personal scale

| Role        | Choice                 | Reason                                                                                |
| ----------- | ---------------------- | ------------------------------------------------------------------------------------- |
| Ingestion   | Python + httpx         | Straightforward. Light dependencies                                                   |
| Storage     | Local files + git      | Object storage is unnecessary at this scale. Git history is the equivalent of time travel |
| Transform   | DuckDB (+ dbt-duckdb)  | No server needed, SQL directly against CSV/JSON/Parquet, free                          |
| Execution   | GitHub Actions         | Independent of whether the laptop is powered on. Run history remains as a public log   |
| BI          | Tableau Public         | Free, and yields a public URL                                                          |

**The reason for choosing GitHub Actions** is not only execution reliability but that "this pipeline
is being operated" becomes visible to a third party. Committing the fetched data back turns the git
history itself into version control for the data.

---

## 3. The FRED-specific structure

### 3.1 FRED-specific premises

**Economic statistics get revised.**
GDP and employment figures change from advance to revised to final. The naive incremental logic of
"take only rows newer than the last observation date" **will reliably produce wrong data**.

FRED builds this into the API as ALFRED (ArchivaL FRED).

The actual structure of an observation record:

| series_id | observation_date | value | realtime_start | realtime_end |
| --------- | ---------------- | ----- | -------------- | ------------ |
| GDPC1     | 2026-Q1          | 100.0 | 2026-04-30     | 2026-05-28   |
| GDPC1     | 2026-Q1          | 101.2 | 2026-05-29     | 2026-06-25   |
| GDPC1     | 2026-Q1          | 101.5 | 2026-06-26     | 9999-12-31   |

- Several rows (vintages) can exist for the same `observation_date`
- `realtime_end = 9999-12-31` is the value in effect right now
- `realtime_start <= X <= realtime_end` retrieves "the value known as of X"

### 3.2 Ingestion strategies: A and B

|                    | A: trailing window                 | B: real-time incremental                          |
| ------------------ | ---------------------------------- | ------------------------------------------------- |
| What is fetched    | Refetch the trailing N months every time | Only what is new or revised since the last run |
| API parameters     | `observation_start`, output_type=1 | `realtime_start`, output_type=3                   |
| Deduplication key  | The newer `_ingested_at`           | The newer `realtime_start`                        |
| Keeps vintages     | No (current state only)            | Yes (accumulates history)                         |
| State management   | Not needed                         | Must persist the last **successful** run date     |
| On failure         | Self-healing                       | Writing state as a relative date loses data permanently |

**Decision: start with A.**

- The raw layer has the same structure under A and B, so switching to B later is possible
- Only the fetch parameters and the staging SQL change
- Revisions are mostly concentrated in the recent past, so a 24-month window is enough in practice

**Conditions for switching to B:**
When the vintages themselves are the subject. For example: "was a decision made on the advance
figure justified in light of the final figure?" That puts the impact-evaluation perspective and the
engineering on the same artifact, so if it can be chosen as a subject, the differentiating effect is large.

### 3.3 Repository structure

```
fred-dashboard/
├── README.md                  Design decisions, screenshots, the route to Tableau Public
├── pyproject.toml
├── .env.example               FRED_API_KEY=
├── .gitignore                 .env
│
├── config/
│   └── series.yaml            Definition of the target series (6.4 / settled)
│
├── ingest/
│   ├── fred.py                Fetch and land
│   └── state.py               For B. Unused under A
│
├── state/
│   └── fred_last_run.json     For B. Committed to git
│
├── data/
│   ├── raw/fred/              Immutable. Committed to git
│   │   └── series_id=<ID>/
│   │       └── ingested_at=<DATE>/
│   │           ├── response.json.gz
│   │           └── manifest.json
│   └── mart/
│       ├── fct_observations.csv    <- read by Tableau
│       └── dim_series.csv          <- read by Tableau
│
├── models/
│   ├── staging/
│   │   ├── stg_fred_observations.sql
│   │   └── stg_fred_series_meta.sql
│   ├── intermediate/
│   │   └── int_observations_enriched.sql
│   └── marts/
│       ├── fct_observations.sql
│       └── dim_series.sql
│
├── tableau/
│   └── fred_dashboard.twbx    For reference. The source of truth is the one on Tableau Public
│
└── .github/workflows/
    └── ingest.yml
```

### 3.4 The ingestion implementation

```python
# ingest/fred.py
import os, json, gzip, hashlib
from datetime import date, datetime, timezone
from pathlib import Path
import httpx
from dateutil.relativedelta import relativedelta

API = "https://api.stlouisfed.org/fred/series/observations"
RAW = Path("data/raw/fred")

def fetch(series_id: str, lookback_months: int | None = 24) -> tuple[dict, dict]:
    """Strategy A: refetch the trailing N months. lookback_months=None for the full history (first run)"""
    params = {
        "series_id": series_id,
        "api_key": os.environ["FRED_API_KEY"],
        "file_type": "json",
    }
    if lookback_months is not None:
        params["observation_start"] = (
            date.today() - relativedelta(months=lookback_months)
        ).isoformat()

    r = httpx.get(API, params=params, timeout=30)
    r.raise_for_status()
    # api_key is not recorded in the manifest
    return {k: v for k, v in params.items() if k != "api_key"}, r.json()

def land(series_id: str, request_params: dict, payload: dict) -> Path:
    """Land the raw response immutably and idempotently"""
    out = RAW / f"series_id={series_id}" / f"ingested_at={date.today()}"
    out.mkdir(parents=True, exist_ok=True)

    body = json.dumps(payload, sort_keys=True).encode()
    (out / "response.json.gz").write_bytes(gzip.compress(body))
    (out / "manifest.json").write_text(json.dumps({
        "source": "FRED",
        "series_id": series_id,
        "endpoint": API,
        "request_params": request_params,
        "retrieved_at": datetime.now(timezone.utc).isoformat(),
        "sha256": hashlib.sha256(body).hexdigest(),
        "n_observations": payload.get("count"),
        "license": "https://fred.stlouisfed.org/legal/",
    }, indent=2, ensure_ascii=False))
    return out
```

### 3.5 Transformation

```sql
-- models/staging/stg_fred_observations.sql
with raw as (
    select
        *,
        regexp_extract(filename, 'series_id=([^/]+)', 1)          as series_id,
        regexp_extract(filename, 'ingested_at=([^/]+)', 1)::date  as _ingested_at
    from read_json_auto('data/raw/fred/*/*/response.json.gz', filename := true)
),
flat as (
    select series_id, _ingested_at, unnest(observations) as obs
    from raw
)
select
    series_id,
    obs.date::date                  as observation_date,
    try_cast(obs.value as double)   as value,        -- FRED sends missing values as "."
    obs.realtime_start::date        as realtime_start,
    obs.realtime_end::date          as realtime_end,
    _ingested_at
from flat
qualify row_number() over (
    partition by series_id, observation_date
    order by _ingested_at desc                        -- deduplication for strategy A
) = 1
```

To switch to strategy B, drop this `qualify`, keep every vintage, and add a separate "latest view"
model filtered on `realtime_end = '9999-12-31'`.

### 3.6 Mart design (for Tableau)

What is handed to Tableau should be either a **wide denormalised table** or a **small star schema**.
Joining several still-normalised tables on the Tableau side causes double counting through fan-out.
Do the joins in the mart layer.

**fct_observations**

| Column           | Description                                                              |
| ---------------- | ------------------------------------------------------------------------ |
| series_id        | Key                                                                      |
| observation_date | Observation date                                                         |
| value            | Value                                                                    |
| value_yoy_pct    | Year-over-year change. Computed in SQL (not left to a Tableau table calc) |
| value_mom_pct    | Period-over-period change                                                |
| value_indexed    | Index with the base period = 100. For comparing across series            |
| is_recession     | NBER recession flag (joined from FRED `USREC`)                           |

**dim_series**

| Column              | Description                                          |
| ------------------- | ---------------------------------------------------- |
| series_id           | Key                                                  |
| title               | Display name                                         |
| units               | Units                                                |
| frequency           | Publication frequency                                |
| seasonal_adjustment | Whether it is seasonally adjusted                    |
| category            | Our own classification (employment / prices / production, etc.) |
| last_updated        | Last update on the FRED side                         |

**Output format:** internal processing in Parquet, **CSV for the handoff to Tableau**. The reliable
file connectors on Tableau Public Edition are CSV / Excel / JSON / Google Sheets.

### 3.7 CI

**Following principle 5 (match ingestion frequency to the publication schedule), this is split into
three tracks.** The series in 6.4 mix daily, weekly and monthly frequencies, and a single monthly
workflow would throw away the information in the daily series.

| workflow              | cron                | target                                                          |
| --------------------- | ------------------- | --------------------------------------------------------------- |
| `ingest-daily.yml`    | `0 23 * * 1-5`      | Market data (`DGS*` `T10Y*` `BAA10Y` `BAMLH0A0HYM2` `VIXCLS` `DTWEXBGS` `DEXJPUS` `DCOILWTICO` `DFII10`)|
| `ingest-weekly.yml`   | `0 13 * * 4`        | `ICSA` `NFCI` `WALCL` `RRPONTSYD` `WTREGEN`                      |
| `ingest-monthly.yml`  | `0 6 2 * *`         | Economic statistics (`PAYEMS` `INDPRO` `CPILFESL` and others)     |

The `ingest` field in `config/series.yaml` is the routing key as-is. Below is the skeleton of the
monthly version; the other two differ only in the cron and the filter on target series.

```yaml
# .github/workflows/ingest-monthly.yml
name: ingest-monthly
on:
  schedule:
    - cron: "0 6 2 * *" # 2nd of each month, 06:00 UTC (after the monthly statistics are published)
  workflow_dispatch:

jobs:
  ingest:
    runs-on: ubuntu-latest
    permissions:
      contents: write
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v5
      - run: uv run python -m ingest.fred
        env:
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
      - run: uv run dbt build --project-dir .
      - name: commit data
        run: |
          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/ state/
          git diff --staged --quiet || git commit -m "chore(data): ingest $(date -u +%Y-%m-%d)"
          git push
```

Reflecting the data into Tableau Public is manual (Public supports automatic refresh only from
Google Sheets). It could be automated by syncing the CSVs on GitHub to Google Sheets and having
Tableau read from there.

**Phase 1 is manual refresh, with the dashboard's change columns limited to 1M/3M/1Y** (even though
ingestion is daily, the freshness of the display cannot keep up, so showing a "day-over-day" figure
would be a lie). **Phase 2 adds the Sheets sync, and only then unlocks 1D/1W** (6.8).

---

## 4. Implementation traps specific to FRED

- **The rate limit is 120 req/min.** Insert a sleep if the number of series grows
- **Missing values arrive as the string `"."`.** `float()` will fail. Use `try_cast`
- **Do not mix seasonally adjusted and unadjusted series** (`GDP` vs `GDPC1`; `UNRATE` is SA)
- **The response's column layout changes with `output_type`.**
  Hit the endpoint by hand once before implementing and confirm the shape
- **Hitting `fred/series/updates` first and fetching only the series that changed** is really the
  cleanest approach

## 5. Tableau Public constraints (settled)

- No live connection to a database. File connectors only
- Cannot save locally = **saving publishes** (the student licence has been discontinued)
  - Either mark practice pieces hidden individually, or use separate accounts
- Extracted data is published in full. Do not load anything that is not public data
- Row limit of 15 million (not a constraint for this subject)
- No custom SQL (on macOS, not even the paid Desktop allows it for file connections)
  - -> Show the SQL in the repository instead. This is not a constraint but the correct structure

---

## 6. The dashboard's question (settled)

> **Rewritten 2026-09-19.** The dashboard was refurbished around the monetary policy cycle and
> the credit cycle. The pillar z-scores, the four-seasons quadrant map and the growth x
> inflation map described in earlier versions of this chapter are **retired**, and the models
> that produced them (`fct_maps`, `fct_pillars`, `fct_monthly`, `int_monthly_panel`,
> `int_zscores`, `int_observations_transformed`, `int_observations_ma4`) are deleted. The
> reasoning that led there is preserved below where it still holds, because the failure modes
> it records are what the replacement is designed around.

### 6.1 The central question

> **"Which way should I tilt the centre of gravity of my assets right now, and how far?"**

The answer is a single **stance**, and every input to it is shown alongside:

| Stance | What it means |
| --- | --- |
| Stay the course | Keep the target allocation |
| Don't chase | Hold off on adding to positions or opening new ones |
| Cut equities | Take the equity weight below target |
| Rebuild equities | Bring the equity weight back to target |

**The goal is narrow, and stated in advance.** Not every decline is worth trading:

| Type of decline | Examples | Drawdown | Time to recover | Visible in macro data |
| --- | --- | --- | --- | --- |
| Credit-cycle bust with recession | 2000-02, 2007-09 | −49%, −57% | About 7 and 5.5 years | Yes, with warning signs |
| Recession without a credit bust | 1990, 2020 | −20%, −34% | Under a year | Partly (2020 was exogenous) |
| No recession | 1987, 1998, 2011, 2015-16, 2018, 2022, 2025 | −14% to −34% | About 2 years or less | Barely |

Only the credit-cycle busts do lasting damage, so the dashboard aims to avoid buying near a top
while the credit cycle is levering, to cut once a breakdown is confirmed, and to rebuild once
recovery is confirmed — accepting a lag of one to three months after the trough. It
**deliberately does not try to trade shock-driven corrections with macro data**.

**Thresholds follow four rules.** Every one is a natural boundary, a policy unit or an official
estimate, never a value fitted to history. Measurement windows were fixed in advance. History
is used to falsify (6.6), not to tune. Robustness checks are issue #9.

### 6.2 Why there is no composite score

Two separate scoring proposals were tested and rejected. Both failures shaped the replacement.

**The initial proposal** binarised the year-over-year change of five series into `{+1, -1}` and
summed them. It breaks in three ways:

| # | Problem | Detail |
| --- | --- | --- |
| **A** | Information lost to binarisation | A YoY of `+0.02%` and one of `+3.0%` are both `+1` |
| **B** | The level is missing | YoY alone cannot separate "rates are high" from "rates are rising" |
| **C** | The sign is context-dependent | A rise in `DGS10` is a tailwind in a recovery and a headwind when inflation is the worry |

**C is the essential one.** Of the initial five, only `BAA10Y` has a determinate sign on its own.

**The source framework's own score** (Horii, fig. 9-2: five items scored ±2 and summed)
reproduces the book's March 2022 worked example exactly, then fails out of sample. Checked
against the S&P 500's 11 peaks and 11 troughs from 1987 to 2025:

- It was clearly negative six months ahead of a peak only in 1998, 2007 and 2020. It read **+6
  at the January 2022 peak** and +4 at the February 2025 peak.
- It was negative at all seven troughs since 2007 and still negative six months later at five
  of them. Six months after the December 2018 trough it read **−10**, in a year the S&P 500
  rose about 29%.

The item thresholds are mostly natural boundaries and are not the problem. The structure is:
indicators with different lead times are added together, so an early warning is cancelled by an
indicator that still looks fine; signs are fixed for indicators whose meaning changes over the
cycle; year-over-year transforms lag turning points by construction; and four recessions is too
few to fit thresholds against.

By contrast **the BAA spread peaked within ±3 months of each of the 10 troughs since 1990**. The
*direction* of spreads carries timing information that a year-over-year transform throws away.

→ So nothing is summed. Two cycles are read independently, and the flags behind the stance are
counted, never weighted.

### 6.3 Two cycles, not one circle

The retired map put the direction of rates on one axis and the credit cycle on the other, and
assumed the seasons travel that map in order. Two findings killed it.

**The rate direction was reversed for spring and autumn.** The source defines the seasons by how
the yield curve changes *shape*, not by whether rates rise or fall:

| Season | Curve move | Rates | Curve |
| --- | --- | --- | --- |
| Spring | Bear steepening — long rates rise, hikes feared | Rising | Steepening |
| Summer | Bear flattening — short rates rise, the Fed hikes | Rising | Flattening |
| Autumn | Bull flattening — long rates fall, cuts expected; inverts in late autumn | Falling | Flattening |
| Winter | Bull steepening — short rates fall, the Fed cuts | Falling | Steepening |

**The single-circle assumption fails in the data.** Of 45 season changes on the retired map, 22
ran clockwise, 22 ran backwards and 1 jumped diagonally. A cycle of about 5 years and one of
about 10 years do not travel one circle together.

**The credit axis was `BAA10Y` alone.** `DRTSCILM` held 9 observations, so it never cleared the
z-score minimum and never contributed. The cause was not a data problem: its manifests show it
was **never fetched with `--full`**, because it was added to the config after the initial
backfill and the scheduled runs only ever request a trailing 24-month window. CI had no path to
a full fetch at all; `ingest.yml` now takes a `full` input so a series added later can be
backfilled without putting the API key on a laptop.

### 6.4 The monetary policy cycle (season)

```
dFF   = FF(t)  − FF(t−6)          level = (dFF + d10) / 2
d10   = 10Y(t) − 10Y(t−6)         slope = d10 − dFF

Spring  level >= 0 and slope >= 0     Autumn  level < 0 and slope < 0
Summer  level >= 0 and slope <  0     Winter  level < 0 and slope >= 0

If max(|dFF|, |d10|) < 0.25, the previous season carries over.
```

Equivalently: which end of the curve moved more, and which way. Spring is the long end up,
summer the short end up, autumn the long end down, winter the short end down.

- **25bp is one policy step**, not a fitted value.
- **Six months** is long enough to catch a season of about 15 months (a cycle of about 5 years)
  in its first half. Fixed in advance.
- On the map x = −slope (flattening to the right) and y = level, so the seasons run clockwise
  from the top left.

**`DFF` replaces `FEDFUNDS`** because the monthly series is published only after month-end and
so cannot carry a provisional current month.

**Rates are the monthly mean, not the month-end value.** `DFF` is the *effective* fed funds rate
— the volume-weighted median of actual overnight transactions — not the FOMC target, and before
the floor system it blew out at year end. On 1986-12-30 it printed **16.17% against a policy
stance near 6%**, so that month's last value misstates policy by 7.4pp. The mean suppresses that
and reproduces `FEDFUNDS`. Post-2009 the artifact is essentially gone (median difference 1.5bp,
5 months out of 213 differing by 25bp or more), so the convention matters mainly for the older
history. Issue #8 revisits it.

**`T10YFF` is deliberately not ingested.** It is exactly `DGS10 − DFF` on 99.99% of 16,161 days,
so it carries no information, and the panel needs `DFF` and `DGS10` separately for the two axes
anyway. Reading it as well would put two slightly different definitions of one quantity in the
same model — they disagree on the inversion flag in 3 of 501 months.

### 6.5 The credit cycle (phase)

```
eq6  = S&P500(t) / S&P500(t−6) − 1      dsp6 = BAA(t) − BAA(t−6)

1 Risk-on       eq6 >  0 and dsp6 <  0      3 Risk-off       eq6 <= 0 and dsp6 >= 0
2 Leverage      eq6 >  0 and dsp6 >= 0      4 Deleveraging   eq6 <= 0 and dsp6 <  0

The phase switches only when the new raw phase appears in two consecutive months.
```

On the map x = eq6 and y = −dsp6, so the phases run clockwise from the top right. A separate
cycle of about 10 years, read independently of the season.

**The S&P 500 is the month-end close**, not the monthly mean, because the trend flags compare it
against a 10-month moving average of month-end closes. It comes from yfinance (`^GSPC`): FRED's
own `SP500` covers only 10 years, and the panel starts in 1985. yfinance is an unofficial API,
so a failed fetch stops the CI run rather than updating the signals from partial data; keeping a
fallback source is still open.

### 6.6 Flags and stance

| Stage | Flag | On when | Why this threshold |
| --- | --- | --- | --- |
| Vulnerability | S1 Inverted curve | 10Y − FF < 0 | Natural boundary |
| | S2 Policy rate above neutral | FF > FOMC longer-run projection | The FOMC's own estimate (from 2012) |
| | S3 Banks tightening lending | `DRTSCILM` > 0 | More banks tightening than easing |
| | S4 Leverage phase | Credit phase 2 | Spreads widening while stocks rise |
| Breakdown | T1 Risk-off phase | Credit phase 3 | Stocks falling and spreads widening together |
| | T2 Markets pricing cuts | 2Y − FF < 0 | Natural boundary; expectations lead the policy rate |
| | T3 Labor market weakening | Sahm indicator >= 0.5 | Rule of thumb. Fires 1-3 months after a recession starts, so it confirms rather than warns (false positive 2024-07) |
| | T4 Stocks break trend | S&P 500 < its 10-month average | Trends tend to persist; noisy alone, so it only counts alongside vulnerability |
| Recovery | H1 Deleveraging phase | Credit phase 4 | Spread peaks sit within ±3 months of market troughs |
| | H2 Jobless claims turning down | Claims(t) − Claims(t−3) < 0 | A change of direction |
| | H3 Stocks regain trend | S&P 500 > its 10-month average | The mirror image of T4 |

Missing inputs count as off. In SQL this is an asymmetry that must not be tidied away: S2 and S3
stay `NULL` when their input is missing so the count skips them, while every other flag is
`coalesce(..., false)` — pandas yields `False` where SQL yields `NULL`, and a `NULL` would
silently drop the row from its count.

```
S = vulnerability flags on    T = breakdown flags on    H = recovery flags on
S24 = max(S) over the last 24 months, including this month

Stay the course, Don't chase      Cut equities
  T >= 2 and S24 >= 2 -> Cut        H >= 2 -> Rebuild
  else Don't chase if S >= 2      Rebuild equities
       else Stay the course         T >= 2 and S24 >= 2 -> Cut
                                    T = 0 -> Don't chase if S >= 2, else Stay the course
```

- **Requiring two flags** keeps any single indicator from moving the portfolio.
- **The 24-month memory** exists because vulnerability flags such as inversion fade once a
  breakdown starts. 24 months is the commonly cited upper bound on the lead from inversion to
  recession.

Only `Cut equities` and `Rebuild equities` carry state; the other two are recomputed each month.

### 6.7 Falsification check

Every period the stance spent in Cut equities, with the S&P 500's change from entry to exit. The
table exists to find holes in the rules, not to optimise returns. Publication lags ignored.

| Period | Months | S&P 500 while defensive | Worst drawdown | What was happening | Assessment |
| --- | --- | --- | --- | --- | --- |
| 1990-01 → 1990-03 | 2 | +3.3% | 0.0% | — | Roughly neutral |
| 1990-08 → 1991-05 | 9 | +20.9% | −5.8% | Gulf War recession | Missed a rally |
| 1991-11 → 1992-02 | 3 | +10.0% | 0.0% | — | Missed a rally |
| 1998-08 → 1998-10 | 2 | +14.8% | 0.0% | LTCM and Russia | Missed a rally |
| 1998-11 → 1999-02 | 3 | +6.4% | 0.0% | Same | Missed a rally |
| 2000-09 → 2002-03 | 18 | −20.1% | −27.5% | Dot-com bust | **Avoided a decline** |
| 2002-04 → 2003-06 | 14 | −9.5% | −24.3% | Dot-com, second leg | **Avoided a decline** |
| 2007-11 → 2009-06 | 19 | −37.9% | −50.4% | Global financial crisis | **Avoided a decline** |
| 2022-05 → 2022-11 | 6 | −1.3% | −13.2% | Inflation and rapid hikes | Roughly neutral |
| 2022-12 → 2023-09 | 9 | +11.7% | 0.0% | Prolonged inversion | Missed a rally |
| 2023-10 → 2023-11 | 1 | +8.9% | 0.0% | Same | Missed a rally |
| 2024-07 → 2024-09 | 2 | +4.3% | 0.0% | Sahm rule triggered | Roughly neutral |
| 2025-03 → 2025-07 | 4 | +13.0% | −0.8% | Tariff shock | Missed a rally |

- The rules stepped aside for most of both credit-cycle busts — **the claim being defended**.
- They ignored 2015-16, 2018 and 2020, as intended.
- They whipsawed in 2022-25, and in 1990, 1998 and 2025 re-entered after stocks had already
  risen 13-21%.

**Requiring T1 for Cut equities was proposed to damp the whipsaw, and measurement rejected it.**
Both large 2022 episodes survive unchanged, four small ones disappear, and the 2002 second leg
turns from a −9.5% avoided decline into a +6.9% missed rally. The switch ships as
`regime.stance.require_t1: false`; the whipsaw stays open and wants a different idea (#9).

### 6.8 Screen layout

Five tabs, one question each.

| Tab | Question | Reads |
| --- | --- | --- |
| Where we are | Where are we in both cycles, and how should the portfolio move? | `fct_regime` |
| Monetary policy cycle | Which season, and what would move it to the next? | `fct_regime`, `fct_yield_curve` |
| Credit cycle | Which phase, and is it heading toward a bust? | `fct_regime` |
| Indicator monitor | What sits inside the signals? | `fct_indicator_monitor`, `fct_monitor_caps` |
| Signal logic | How are the signals defined, and where do they fail? | `fct_stance_episodes` + this chapter |

Everything the tabs need is computed in the marts (principle 4). `fct_regime` carries the
distance-to-threshold columns the "what would change the reading" gauges use, and the rates from
three months ago that the season scenario needs, so Tableau does no arithmetic of its own.

**Display-only rule.** Jobless claims and the Sahm indicator spike so far in 2008-09 and 2020
that normal moves become invisible. `fct_monitor_caps` supplies a cap of median + 6 x MAD of the
displayed range, floored at the indicator's own threshold line so capping can never hide the
comparison the chart exists to make. This changes the chart only, never a signal.

### 6.9 Verifying that the series exist (carried out 2026-08-18, extended 2026-09-19)

FRED discontinues and replaces series. Every ID was measured against the public `fredgraph.csv`
endpoint, which needs **no API key**.

```bash
grep -oE '^\s+- id: [A-Z0-9_]+|inputs: \[[A-Z0-9_, ]+\]' config/series.yaml \
  | sed 's/.*- id: //; s/inputs: \[//; s/\]//; s/,/ /g' | tr ' ' '\n' \
  | grep -E '^[A-Z]' | sort -u \
  | while IFS= read -r s; do          # <- zsh does not word-split $VAR. Do not use `for`
      body=$(curl -s --max-time 20 "https://fred.stlouisfed.org/graph/fredgraph.csv?id=$s")
      if printf '%s' "$body" | head -1 | grep -q '^observation_date'; then
        printf '%s\tOK\t%s\t%s\n' "$s" \
          "$(printf '%s\n' "$body" | sed -n '2p' | cut -d, -f1)" \
          "$(printf '%s\n' "$body" | grep -vE '^[[:space:]]*$' | tail -1 | cut -d, -f1)"
      else printf '%s\tNG\t-\t-\n' "$s"; fi
      sleep 0.4
    done
```

**Three series forced a design change**, and they are the reason raw is kept append-only even
for series nothing currently reads: a provider can withdraw history you can no longer refetch.

| Series | Measured | Decision |
| --- | --- | --- |
| `USSLIND` | 1982-01 .. **2020-02** | Effectively discontinued. Replaced by `AWHMAN` and `NEWORDER` |
| `BAMLH0A0HYM2` | **2023-08-21** .. present | ICE BofA licensing restriction. Display only |
| `ISM_PMI` | — | **Removed from FRED entirely** (ISM licensing) |

All ranges and the reasons for non-adoption are recorded in the `verification:` block of
`config/series.yaml`.

**The regime panel starts in 1985** and the display window in 1987. This is not limited by
`history_start` (2003-01), which applied to the retired z-score panel and now only filters
`fct_observations`. The binding constraints on the regime signals are `BAA10Y` (1986-01) and
`DRTSCILM` (1990-04), so S3 counts as off before 1990-05 and S2 before 2012.

### 6.10 Status of the open questions

| Item | Status |
| --- | --- |
| The dashboard's question | Settled (6.1) |
| Composite scores | Rejected twice, with evidence (6.2) |
| Season definition | Settled (6.4). Reproduces the reference implementation across all 483 months |
| Credit phase definition | Settled (6.5) |
| Flags and stance | Settled (6.6). Falsification table in 6.7 |
| Growth x inflation map | **Retired.** `fct_maps` deleted |
| Pillar z-scores | **Retired.** `fct_pillars`, `fct_monthly`, `int_zscores` deleted |

**Still open**

| Item | Tracked |
| --- | --- |
| Monthly aggregation: mean vs month-end for the 10-year leg; the spread definition | #8 |
| Robustness: perturbing the windows and thresholds | #9 |
| The 2022-25 whipsaw — `require_t1` does not fix it | #9 |
| Zero lower bound: the season rests on long rates alone in 2009-15 and 2020-21 | #9 |
| Confirmation lag: March 2020 is confirmed in April | #9 |
| What Cut / Rebuild mean in percentages | A portfolio rule, set outside the dashboard |
| A fallback source for the S&P 500 | yfinance is an unofficial API |
| The dollar index's new home | Moves to a "what to hold" view |

## 7. Design decisions to write up in the README

The value of the piece is decided not by how large the structure is but by **whether the judgements
are appropriate to the scale**. State the following explicitly.

- Why the layered structure was adopted (future sources and testability. At the current scale a
  single table would do)
- Why git rather than object storage (at this scale the git history serves as the equivalent of time travel)
- Why no SQL in the BI layer (version control, review and tuning all become impossible)
- Why ingestion is monthly (it matches the publication schedule of macro indicators)
- How revisions to economic statistics were handled (which of A/B was adopted, and why)
- Why nothing is summed into a score (A/B/C in 6.2, especially the context-dependence of the
  sign, and the out-of-sample failure of the source framework's own score)
- Why two cycles rather than one quadrant map (6.3: of 45 season changes on the retired map, 22
  ran clockwise and 22 ran backwards)
- How far the source framework was reproduced, and where the implementation becomes our own
  (the seasons are the book's; the thresholds, the flags and the stance are ours)
- Why every threshold is a natural boundary, a policy unit or an official estimate, and why the
  falsification table (6.7) lists the misses rather than hiding them
- Why the monthly mean is used for rates but the month-end close for equities (6.4: DFF is the
  effective rate, not the target, and printed 16.17% on 1986-12-30)
- Why ingestion was split into three tracks (keeping up with the publication schedule; the waste of
  both fetching a daily series monthly and polling a monthly series daily)
- Why raw is append-only even for series nothing currently reads (6.9: `ISM_PMI` was removed from
  FRED entirely, and `BAMLH0A0HYM2` lost its history to a licensing change)

The reasoning matters more than the implementation in all of these.
