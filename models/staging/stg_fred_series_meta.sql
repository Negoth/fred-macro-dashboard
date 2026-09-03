-- FRED series metadata. One row arrives per fetch, so keep only the most recent fetch.

with raw as (
    select
        *,
        regexp_extract(filename, 'ingested_at=([^/]+)', 1)::date as _ingested_at
    from read_json_auto('{{ var("raw_series_meta") }}', filename := true)
),

flat as (
    select _ingested_at, unnest(seriess) as s
    from raw
)

select
    s.id                                 as series_id,
    s.title                              as title,
    s.units                              as units,
    s.units_short                        as units_short,
    s.frequency                          as frequency,
    s.frequency_short                    as frequency_short,
    s.seasonal_adjustment                as seasonal_adjustment,
    s.seasonal_adjustment_short          as seasonal_adjustment_short,
    s.observation_start::date            as observation_start,
    s.observation_end::date              as observation_end,
    s.last_updated                       as last_updated,
    _ingested_at
from flat
qualify row_number() over (partition by s.id order by _ingested_at desc) = 1
