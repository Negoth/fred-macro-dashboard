-- Unpacks the raw yfinance response into a typed long format.
-- Deliberately a near-copy of stg_fred_observations: ingest/equities.py writes the same
-- {"observations": [{"date", "value"}]} shape FRED uses, so there is one unpacking idiom
-- rather than two.
--
-- The partition key is `symbol=GSPC`, not `symbol=^GSPC` -- a caret is awkward inside the
-- glob handed to DuckDB. The symbol as requested is recorded in the manifest.

with raw as (
    select
        *,
        regexp_extract(filename, 'symbol=([^/]+)', 1)            as series_id,
        regexp_extract(filename, 'ingested_at=([^/]+)', 1)::date as _ingested_at
    from read_json_auto('{{ var("raw_yf_observations") }}', filename := true)
),

flat as (
    select series_id, _ingested_at, unnest(observations) as obs
    from raw
)

select
    series_id,
    obs.date::date                as observation_date,
    try_cast(obs.value as double) as value,
    _ingested_at
from flat
-- Same strategy-A dedup as the FRED staging model: for a given observation date, keep the
-- most recently fetched version. Yahoo revises the current day's close intraday
qualify row_number() over (
    partition by series_id, observation_date
    order by _ingested_at desc
) = 1
