-- Unpacks the raw FRED observations response into a typed long format.
-- Design note 3.5. Missing values arrive as the string "." so we use try_cast (the trap in chapter 4).

with raw as (
    select
        *,
        regexp_extract(filename, 'series_id=([^/]+)', 1)         as series_id,
        regexp_extract(filename, 'ingested_at=([^/]+)', 1)::date as _ingested_at
    from read_json_auto('{{ var("raw_observations") }}', filename := true)
),

flat as (
    select series_id, _ingested_at, unnest(observations) as obs
    from raw
)

select
    series_id,
    obs.date::date                as observation_date,
    try_cast(obs.value as double) as value,
    obs.realtime_start::date      as realtime_start,
    obs.realtime_end::date        as realtime_end,
    _ingested_at
from flat
-- Deduplication for strategy A (trailing window): for a given observation date, take the
-- most recently fetched version.
-- To switch to strategy B, drop this qualify, keep every vintage, and add a separate
-- "latest view" model filtered on realtime_end = '9999-12-31' (design note 3.2).
qualify row_number() over (
    partition by series_id, observation_date
    order by _ingested_at desc
) = 1
