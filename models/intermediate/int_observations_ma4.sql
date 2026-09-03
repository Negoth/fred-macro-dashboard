-- 4-week moving average. Used to damp the noise in ICSA (weekly initial jobless claims).
-- The series is weekly, so "up to 3 rows back" matches 4 calendar weeks.

select
    series_id,
    observation_date,
    avg(value) over (
        partition by series_id
        order by observation_date
        rows between 3 preceding and current row
    ) as value
from {{ ref('int_observations_all') }}
where series_id in (
    select series_id from {{ ref('series_config') }} where transform = 'yoy_4wma'
)
