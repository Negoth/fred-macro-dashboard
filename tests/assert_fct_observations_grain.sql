-- The grain of fct_observations must be unique on (series_id, observation_date).
-- Verifies that the deduplication for strategy A (trailing window) is working.
-- Any row returned is a failure.

select
    series_id,
    observation_date,
    count(*) as n
from {{ ref('fct_observations') }}
group by 1, 2
having count(*) > 1
