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
would be a lie). **Phase 2 adds the Sheets sync, and only then unlocks 1D/1W** (6.7).

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

### 6.1 The central question

> **"Which way should I tilt the centre of gravity of my assets right now, and how far?"**

This is answered by **two independent outputs**. They are not collapsed into a single score.

| Output                         | What it decides                | Axes                                | The financial-planning decision                      |
| ------------------------------ | ------------------------------ | ----------------------------------- | ---------------------------------------------------- |
| **(1) The financial four seasons** (primary) | Where we are in the financial cycle | **Direction of rates x credit cycle** | Cash weighting, contribution pace, choosing products suited to the season |
| **(2) Growth x inflation** (secondary)       | Which asset class is favoured  | The four quadrants of **growth x inflation** | Equity/bond split, duration, real assets |

**(1) is a reproduction of an existing framework.** The initial scoring proposal followed Horii's
*Kinri o Mireba Toshi wa Umaku Iku* (revised edition, Cross Media Publishing), whose framework is as
follows. The main visual on the front page reproduces it from data, and the structure then **adds
the axes of (2), which the book does not cover**.

| Chapter                                       | Content                                                            | Corresponding series             |
| --------------------------------------------- | ------------------------------------------------------------------ | -------------------------------- |
| Ch. 2 The economy can be forecast from three interest rates | (1) the policy rate (short rates) (2) the 10-year Treasury yield (long rates) (3) corporate bond yields | `FEDFUNDS` / `DGS10` / `BAA10Y` |
| Ch. 3 The business cycle and interest rates   | "The yield spread is a **leading indicator** of the economy"        | `T10YFF` `T10Y3M`                |
| Ch. 4 The credit cycle                        | "Corporate spreads tell the story of the credit cycle"              | `BAA10Y` `DRTSCILM`              |
| Ch. 5 Money goes around the world             | US dollar liquidity = **the World Dollar** (the US monetary base plus the Treasuries held as FX reserves by non-US central banks) | `WORLD_DOLLAR` (derived) |
| Ch. 9 To succeed at investing                 | "**Measuring the investment climate by interest rates**", "choosing products suited to the season" | <- the source of the initial score |

**Scope:** the US only, plus `DEXJPUS` (USD/JPY). This gives the minimum FX axis needed for the
financial-planning decisions of an investor whose living expenses are in yen. International
comparison (OECD CLI and so on) is not included — it would multiply the number of series and the
normalisation cost by two or three without sharpening the answer to the question.

### 6.2 Why split it in two (the failure mode of the initial proposal)

The initial proposal binarised the year-over-year change of five series (`FEDFUNDS` `DGS10`
`T10YFF` `BAA10Y` `TWEXBGSMTH`) into `{+1, -1}` and summed them. It breaks down in three ways.

| #     | Problem                          | Detail                                                                                                             |
| ----- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| **A** | Information lost to binarisation | A YoY of `+0.02%` and one of `+3.0%` are both `+1`. The score oscillates near the threshold                          |
| **B** | The level is missing             | YoY alone cannot distinguish "rates are high" from "rates are rising". FF `0.25->0.50%` and `5.00->5.25%` are treated identically |
| **C** | The sign is context-dependent    | A rise in `DGS10` is a tailwind during a recovery and a headwind when inflation is the worry. On its own its sign is undetermined |

**C is the essential one.** Of the initial five, only `BAA10Y` (widening = headwind) has a
determinate sign on its own; the other four acquire meaning only in combination with others. That is
why summing them yields no story. "One in five moves the value a lot" is a symptom, not the cause.

-> Before collapsing into a single score, **stand up two axes**. All five of the initial series are
material for (2) (magnitude); what was missing was material for (1) (direction).

Where the initial five series ended up:

| Initial series | Destination                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `FEDFUNDS`     | Carried into pillar 3, but with the transform changed from YoY to `diff12` (12-month difference) (**B**) |
| `T10YFF`       | Carried into pillar 3 unchanged                                        |
| `BAA10Y`       | Carried into pillar 4 unchanged                                        |
| `TWEXBGSMTH`   | **Replaced by `WORLD_DOLLAR`.** What ch. 5 of the book deals with is not the dollar index but the World Dollar: the dollar index is "the **price** of the dollar" and WD is "the **quantity** of dollars" — a different dimension. The initial proposal was using the former as a proxy. `DTWEXBGS` (the price side) also remains, in pillar 5 |
| `DGS10`        | Carried into pillar 3, but with the transform `chg6m` (6-month change). **Not taken as a level** — it already enters three times as a component of `T10YFF`, `T10Y3M` and `DFII10`, which would be triple counting. On the other hand, **the "direction of rates" axis of (1) can only be built from the change in long rates** (both `T10YFF` and `T10Y3M` are levels of a spread, so the axis does not respond when short and long rates move together). The level and the change are different information |

### 6.3 The main visual: the financial four seasons (direction of rates x credit cycle)

The book's four seasons can be expressed as a **two-axis quadrant model**. Spring -> summer ->
autumn -> winter traverses the quadrants in order.

```
                      Credit: easing
        ┌────────────────────────┬────────────────────────┐
        │        Spring          │        Summer          │
        │  Rates falling         │  Rates bottom and turn up │
        │  Banks lending freely  │  Lending starts to tighten │
        │  Equities rising       │  Equities peak         │
Rates ↓ ┼────────────────────────┼────────────────────────┼ Rates ↑
        │        Winter          │        Autumn          │
        │  Rates stay high       │  Rates rising          │
        │  Lending slowly grows  │  Banks reluctant to lend │
        │  Equities creep up     │  Equities falling  ● <-now │
        └────────────────────────┴────────────────────────┘
                    Credit: tightening      ↘ trajectory of the last 24 months
```

| Element                    | Series                | Transform                                |
| -------------------------- | --------------------- | ---------------------------------------- |
| X axis: direction of rates | `DGS10` `FEDFUNDS`    | `chg6m` / `diff12` (the change, not the level) |
| Y axis: credit cycle       | `BAA10Y` `DRTSCILM`   | z-score of the level                     |
| Overlaid marker            | `T10YFF` `T10Y3M`     | Mark on the trajectory where the curve inverted |

**Not using the yield spread as an axis is deliberate.** In the book it is the "**leading
indicator**" of chapter 3 and a different thing from the credit cycle of chapter 4, so it is
overlaid as "advance notice of a change of season" rather than used as an axis.

> WARNING: **The rules for the quadrant boundaries are provisional.** The decision rules of the
> book's chapter 9, "measuring the investment climate by interest rates", are not included in any of
> the published summaries, and confirmation against the original is pending (6.9). In particular,
> **winter (rates "staying high" = a direction near zero) has the most ambiguous boundary of the
> four quadrants**. For now they are placeholders based on the sign of the z-score, and the
> thresholds have been factored out into `seasons.thresholds` in `config/series.yaml` (once the
> original is known, only that needs replacing; the SQL stays untouched).

**Decisions made during implementation (`models/marts/fct_maps.sql`)**

| Item | Decision | Reason |
| --- | --- | --- |
| Dead zone | Distance `0.25` from the origin | Judging per axis (OR) made a point a boundary merely because one axis was near zero, leaving **51.6%** of the whole history unclassified. Using distance brought that to **6.8%** |
| Smoothing | A 3-month **trailing** moving average on the axes | Month-to-month reversal noise scattered the history into 47 intervals. Smoothing brings it to 31 intervals, in which the major regimes are legible. It is not centred, in order to avoid look-ahead bias (same reason as 6.5) |

**Verification results against the real data**

| Period | Assignment | Plausibility |
| --- | --- | --- |
| 2005-01..2006-11 | Summer (23 months) | Fed hiking cycle with credit still easy = equities peaking. Matches the book's description of summer |
| 2007-11..2009-06 | Winter (20 months) | The global financial crisis. Cuts x credit contraction |
| 2016-12..2019-01 | Summer (26 months) | The 2017-2018 hiking cycle |
| 2020-03..2021-01 | Winter (11 months) | The COVID shock |
| 2022-07..2023-12 | Autumn (18 months) | Hikes x reluctance to lend. Matches the book's description of autumn |

### 6.3b The second visual: the growth x inflation quadrant map

An axis pair the book does not cover. Where (1) decides "when, and how far to lean in", this one
decides **what to hold**.

```
                    Inflation ↑
      ┌────────────────────────┬────────────────────────┐
      │      Stagflation       │      Overheating       │
      │  Cash, commodities     │  Commodities, short bonds │
Growth┼────────────────────────┼────────────────────────┼─> Growth
  ↓   │      Reflation         │       Recovery         │
      │  Long bonds (duration) │  Equities (growth      │
      │  favoured              │  especially)   ● <-now │
      └────────────────────────┴────────────────────────┘
                    Inflation ↓
```

- The axes were chosen as the two factors that best explain return differences between asset classes
  (in the lineage of the Investment Clock)
- **The value of placing the two side by side lies in the divergence between them.** The moments
  when the financial cycle (the fast layer, what the market is pricing) and the real economy (the
  slow layer) disagree carry the most information
- Both use the same representation — a point for where we are now and a line for the trajectory of
  the last 24 months — so **the historical-trend chart originally planned as a separate piece is
  absorbed into the trajectory**

### 6.4 Pillars and series

**The distinction between coordinates and scores is the crux of the design.**

- **Pillars 1 and 2 (growth, inflation) are "coordinates"** — they carry no good/bad sign. The value
  itself is a position on an axis
- **Pillars 3, 4 and 5 (financial conditions, credit, dollar) are "scores"** — `sign` defines
  "tailwind/headwind for risk assets"

Confusing this distinction re-creates the 6.2-C failure.

`config/series.yaml` is the source of truth for the exact definitions. What follows is a summary.

#### Pillar 1: Growth (coordinate axis X)

| series_id      | Content                          | Freq  | Transform          |
| -------------- | -------------------------------- | ----- | ------------------ |
| `PAYEMS`       | Nonfarm payroll employment       | M     | 3-month annualised |
| `ICSA`         | Initial jobless claims (4-week average) | **W** | YoY (sign inverted) |
| `INDPRO`       | Industrial production            | M     | YoY                |
| `PERMIT`       | Building permits / strongly leading | M  | YoY                |
| `UMCSENT`      | University of Michigan consumer sentiment | M | z-score of level |
| `AWHMAN`       | Average weekly hours, manufacturing / leads PAYEMS | M | z-score of level |
| `NEWORDER`     | Core capital goods orders / leads capital expenditure | M | YoY        |

> **`USSLIND` (the Philly Fed leading index) was not adopted because it stopped updating in 2020-02**
> (6.8). The leading component was replaced by `AWHMAN` and `NEWORDER`. The ISM PMI has been removed
> from FRED (ISM licensing policy), and the US version of the OECD CLI, `USALOLITONOSTSAM`, also
> stopped in 2024-01.

#### Pillar 2: Inflation (coordinate axis Y)

| series_id      | Content                          | Freq  | Transform          |
| -------------- | -------------------------------- | ----- | ------------------ |
| `CPILFESL`     | Core CPI                         | M     | **3-month annualised** (faster than YoY) |
| `PCEPILFE`     | Core PCE (the Fed's target)      | M     | YoY                |
| `T10YIE`       | Expected inflation (10-year breakeven) | **D** | z-score of level |
| `T5YIFR`       | 5y5y forward expected inflation  | **D** | z-score of level   |
| `DCOILWTICO`   | WTI crude                        | **D** | YoY                |
| `AHETPI`       | Average hourly earnings          | M     | YoY                |

#### Pillar 3: Financial conditions and liquidity (score)

| series_id      | Content                          | Freq  | Transform          |
| -------------- | -------------------------------- | ----- | ------------------ |
| `NFCI`         | Chicago Fed financial conditions index | W | Level (sign inverted) |
| `DGS10`        | 10-year Treasury yield / **the rate axis of the four seasons** | D | **`chg6m` (the change)** |
| `T10Y3M`       | Yield curve (better recession-predictive power than the 2-year version) | D | Level / leading signal |
| `T10YFF`       | **Inherited from the initial proposal** | D | Level            |
| `DFII10`       | 10-year **real** rate (TIPS)     | D     | Level (sign inverted) |
| `FEDFUNDS`     | **Inherited from the initial proposal** | M | 12-month difference (sign inverted) |
| `NET_LIQUIDITY`| `WALCL − RRPONTSYD − WTREGEN` (derived) | W | 13-week change |

#### Pillar 4: Credit and risk appetite (score)

| series_id      | Content                          | Freq  | Transform          |
| -------------- | -------------------------------- | ----- | ------------------ |
| `BAA10Y`       | **Inherited from the initial proposal** | D | Level (sign inverted) |
| `VIXCLS`       | VIX (available on FRED)          | D     | Level (sign inverted) |
| `DRTSCILM`     | Bank lending standards (SLOOS)   | Q     | Level (sign inverted) |

> **`BAMLH0A0HYM2` (US high-yield OAS) is not counted in the scores.** It is single-handedly the most
> informative measure of current credit stress, but licensing restrictions mean the ICE BofA series on
> FRED **only go back about three years, from 2023-08-21**, covering neither 2008 nor 2020. Taking a
> z-score over that history would be meaningless, so it was moved to display-only in the change table
> and the tooltips, in favour of consistency in the time-series scores (`BAMLC0A0CM` is under the same
> restriction). The long-history credit signal is carried by `BAA10Y` (from 1986).

#### Pillar 5: Dollar and external (score)

| series_id      | Content                          | Freq  | Transform          |
| -------------- | -------------------------------- | ----- | ------------------ |
| `DTWEXBGS`     | Trade-weighted dollar (broad, daily) | D | YoY (sign inverted = a stronger dollar is global tightening) |
| `DEXJPUS`      | USD/JPY / the yen-based investor's perspective | D | YoY |
| `WORLD_DOLLAR` | `BOGMBASE + WMTSECL1` (derived) / the World Dollar of the book's ch. 5 | W | YoY |

> **`TWEXBGSMTH` has not been discontinued** (it is still updating as of 2026-07). It is the monthly
> version of the same index as `DTWEXBGS`, and both start in 2006-01. The daily version is taken only
> because it matches the grain of the other market series, not because the initial proposal's series
> was outdated.

### 6.5 How the score is constructed

```
pillar score(t) = mean_i( sign_i x z_i(t) )

z_i(t) = ( x_i(t) − μ_i(t) ) / σ_i(t)     * μ and σ come from an expanding window up to t
```

Four changes corresponding to A/B/C in 6.2:

1. **Binary -> z-score** (continuous, clipped at `±3`). The oscillation near the threshold disappears -> **A**
2. **Hold `z_level` and `z_momentum` (the z-score of the 3M/6M difference) in separate columns.**
   The position on the quadrant is set by the level; the direction of the trajectory by the momentum -> **B**
3. **State the sign, transform and pillar assignment explicitly in `config/series.yaml`** -> **C**
4. **Three to seven series per pillar x five pillars = 26 in total.** If one goes wrong, the pillar
   score moves by at most 1/3 and the overall score by 1/26 -> resolves "one in five moves it a lot"

```yaml
- id: BAMLH0A0HYM2
  pillar: credit
  transform: level
  sign: -1
  z_window: expanding      # <- avoids look-ahead bias
  rationale: "A widening OAS means rising credit risk, a headwind for risk assets"
```

**`z_window: expanding` is the counterpart to strategies A/B in 3.2.** Standardising with the μ/σ of
the whole history amounts to "standardising with knowledge of the future" and makes past scores
hindsight.

|                     | Question                                | Response                    |
| ------------------- | --------------------------------------- | --------------------------- |
| Strategy B (ALFRED) | Was it **obtainable** at that point?     | Keep vintages (Phase 2)     |
| Expanding-window z  | Was it **computable** at that point?     | expanding μ/σ (Phase 1)     |

With both in place you can say outright: "this score is a value that could have been computed in
real time at the time". This is where the README differentiates most (see chapter 7).

### 6.6 Make the historical chart a stack of pillar contributions, not a line

Drawing the overall score as a single line only tells you "it went down". **An area chart stacking
the contribution of each pillar** tells you why.

```
The 2020 deterioration -> the growth pillar falls on its own (COVID)
The 2022 deterioration -> the financial-conditions pillar is the main cause (rate hikes), growth is still holding up
```

The NBER recession shading (`USREC`) is already handled by the `is_recession` of 3.6.

### 6.7 Screen layout

```
┌─ Today's macro environment ────────────────────── as of YYYY-MM-DD ─┐
│ ┌────────────┐ ┌────────────────────────────────────────────────┐ │
│ │ Season      │ │  The financial four seasons                     │ │
│ │ Autumn      │ │  (direction of rates x credit cycle)            │ │
│ │            │ │  (24-month trajectory; click a point to rewind) │ │
│ │ Curve       │ │                                                │ │
│ │ inverted ⚠  │ │  + markers where the yield spread inverted      │ │
│ └────────────┘ └────────────────────────────────────────────────┘ │
├────────────────────────────────────────────────────────────────────┤
│ Growth x inflation quadrant map (second visual, same 24-month trajectory) │
├────────────────────────────────────────────────────────────────────┤
│ The five pillars  Growth +0.8 │ Inflation −0.3 │ Financial −1.2 │ Credit +0.4 │ Dollar −0.1 │
│           A bar per pillar + a 12-month sparkline + the change on the month │
├────────────────────────────────────────────────────────────────────┤
│ What moved                                                          │
│  Indicator │ Latest │ 1M │ 3M │ 1Y │ z │ percentile in history   <- 1D/1W in Phase 2 │
├────────────────────────────────────────────────────────────────────┤
│ Historical trend: stacked pillar contributions + NBER recession shading │
└────────────────────────────────────────────────────────────────────┘
```

**Tableau features to show off (within what is compatible with "no logic in the BI layer", 4.1)**

| Feature                   | Where it is used                                                |
| ------------------------- | --------------------------------------------------------------- |
| **Parameter actions**     | **Click a point on the trajectory -> the whole dashboard rewinds to that moment, a "time machine"**. The centrepiece |
| LOD (FIXED)               | Historical percentile per series                                 |
| Viz in tooltip            | A sparkline on each row of the change table                      |
| Dynamic zone visibility   | Opening and closing the detail panel                             |
| Sets / highlight actions  | Click a pillar -> filter the change table                        |

### 6.8 Verifying that the series exist (carried out 2026-08-18)

FRED discontinues and replaces series. Every ID in `config/series.yaml` was measured. **No API key is
needed** — the `fredgraph.csv` endpoint is public.

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

**Result: all 28 exist. But three of them forced a design change.**

| Series                | Measured                       | Decision                                                 |
| --------------------- | ------------------------------ | -------------------------------------------------------- |
| `USSLIND`             | 1982-01 .. **2020-02**         | Six and a half years without an update = effectively discontinued. Replaced by `AWHMAN` and `NEWORDER` |
| `BAMLH0A0HYM2`        | **2023-08-21** .. 2026-08-17   | Only about three years, due to the ICE BofA licensing restriction. Dropped from the scores, moved to display-only |
| `TWEXBGSMTH`          | 2006-01 .. 2026-07 (updating)  | **The initial assumption that it was discontinued was wrong.** Same index and same start as `DTWEXBGS` |

All ranges and the reasons for non-adoption are recorded in the `verification:` block of
`config/series.yaml`.

#### The limit on how far back the panel can go

Measurement showed that **`history_start: 1990-01-01` is unattainable**. The binding constraints are
the following.

| Binding series                   | Start      | Affects        |
| -------------------------------- | ---------- | -------------- |
| `T10YIE` `T5YIFR` `DFII10` (TIPS/BEI) | 2003-01 | Pillars 2 and 3 |
| `DTWEXBGS`                       | 2006-01    | Pillar 5       |

-> **`history_start` was set to 2003-01-01**, with 2006-01-01 for pillar 5 alone. The NBER recessions
covered are **the two of 2007-12..2009-06 and 2020-02..2020-04**. The episodes that the contribution
decomposition of 6.6 can speak to are limited to those two.

This is consistent with `z.min_periods: 60` in 6.5 (five years at monthly frequency, three months at
daily), but **the limitation that "the z-score population contains only two recessions" is stated
explicitly in the README**.

### 6.9 Status of the open questions

| Item                        | Status                                                                    |
| --------------------------- | ------------------------------------------------------------------------- |
| The dashboard's question    | Settled (6.1)                                                             |
| Choice of target series     | Settled (`config/series.yaml`) / existence verified. Three replaced (6.8)  |
| Scope                       | The US only, plus `DEXJPUS`                                               |
| Strategy A / B              | **Start with A.** The expanding-window z secures "computational point-in-time" in Phase 1. B is kept in reserve as the Phase 2 differentiator (6.5) |
| Update frequency and automation | **Staged.** Phase 1 is the three ingest tracks plus manual Tableau refresh, with change columns limited to 1M/3M/1Y. Phase 2 adds automatic refresh via Google Sheets and unlocks "day-over-day" (3.7) |

**Still open**

| Item                                   | Note                                                             |
| -------------------------------------- | ---------------------------------------------------------------- |
| The axis scale of the quadrant maps     | Raw z or historical percentile. The former is vulnerable to outliers, the latter to skew in the distribution |
| Whether history from 2003 is enough     | The z-score population contains only two recessions (6.8). If it is not enough, one option is to drop TIPS/BEI from pillar 2 and extend pillars 1 and 3 further back, but that introduces asymmetry between the pillars |
| **The thresholds for the four-seasons quadrants** | **Pending confirmation against ch. 9 of the original book, "measuring the investment climate by interest rates".** The published summaries did not include the decision rules. The boundary of winter (rates "staying high" = a direction near zero) in particular. For now they are placeholders based on the sign of the z-score, and the thresholds have been factored out into `seasons.thresholds` in `config/series.yaml` (once the original is known, only that needs replacing) |
| The thresholds for the regime labels   | A plain sign, or a dead zone of `abs(z) > 0.5`                    |
| `.gitignore` excludes `*.csv` / `*.gz` | The `response.json.gz` and `fct_observations.csv` of 3.3 would not be committed. A negation rule is needed |

---

## 7. Design decisions to write up in the README

The value of the piece is decided not by how large the structure is but by **whether the judgements
are appropriate to the scale**. State the following explicitly.

- Why the layered structure was adopted (future sources and testability. At the current scale a
  single table would do)
- Why git rather than object storage (at this scale the git history serves as the equivalent of time travel)
- Why no SQL in the BI layer (version control, review and tuning all become impossible)
- Why ingestion is monthly (it matches the publication schedule of macro indicators)
- How revisions to economic statistics were handled (which of A/B was adopted, and why)
- Why two axes rather than a single score (A/B/C in 6.2, especially the context-dependence of the sign)
- How far the source framework (the book's four seasons) was reproduced, and where the implementation
  becomes our own (the thresholds are ours; the World Dollar of ch. 5 was rebuilt on the original definition)
- Why the z-score window is expanding (look-ahead bias, including the point that it is a
  point-in-time problem distinct from the availability of the data)
- Why ingestion was split into three tracks (keeping up with the publication schedule; the waste of
  both fetching a daily series monthly and polling a monthly series daily)

The last three cannot be written without understanding the field, and are where the reasoning
matters more than the implementation.
