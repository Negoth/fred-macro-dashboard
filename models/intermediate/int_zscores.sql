-- Expanding-window z-scores. Design note 6.5.
--
-- Standardising by the mean and standard deviation of the whole history amounts to
-- "standardising with knowledge of the future" and makes past scores hindsight
-- (look-ahead bias).
-- `rows between unbounded preceding and current row` uses only the statistics available
-- up to that point in time.
--
-- This is the counterpart to strategy B (ALFRED vintages):
--   strategy B        was it *obtainable* at that point in time -> keep vintages (not implemented)
--   expanding-window z  was it *computable* at that point in time -> here
--
-- The z-score population uses each series' full history, not history_start. Computing from
-- 2003 would burn 12 months on yoy plus 60 months on min_periods, so z-scores would only
-- start around 2009 and the GFC would fall outside the z-score window.

{% set z = var('z_config') %}

-- Convert min_months into an observation count appropriate to each series' frequency.
-- Written as "60 observations", the threshold would mean about 3 months for a daily series
-- and 5 years for a monthly one, so its meaning would change with frequency. This lines it up.
with freq as (
    select
        c.series_id,
        coalesce(nullif(c.freq, ''), m.frequency_short, 'M') as frequency_short
    from {{ ref('series_config') }} c
    left join {{ ref('stg_fred_series_meta') }} m using (series_id)
),

min_obs as (
    select
        series_id,
        ceil({{ z.min_months }} * case frequency_short
            {%- for f, n in z.obs_per_month.items() %}
            when '{{ f }}' then {{ n }}
            {%- endfor %}
            else 1 end) as min_periods
    from freq
),

t as (
    select series_id, observation_date, value, transform, transformed
    from {{ ref('int_observations_transformed') }}
    where transformed is not null
),

stats as (
    select
        *,
        avg(transformed)         over w as mu,
        stddev_samp(transformed) over w as sigma,
        count(transformed)       over w as n_obs
    from t
    window w as (
        partition by series_id
        order by observation_date
        rows between unbounded preceding and current row
    )
),

-- Momentum: the 3-month / 6-month change in the transformed series. Determines the
-- "direction" on the quadrant map
mom as (
    select
        s.series_id,
        s.observation_date,
        s.transformed - m3.transformed as mom3,
        s.transformed - m6.transformed as mom6
    from stats s
    asof left join t m3
        on s.series_id = m3.series_id
       and m3.observation_date <= s.observation_date - interval 3 month
    asof left join t m6
        on s.series_id = m6.series_id
       and m6.observation_date <= s.observation_date - interval 6 month
),

mom_stats as (
    select
        *,
        avg(mom6)         over w as mom_mu,
        stddev_samp(mom6) over w as mom_sigma,
        count(mom6)       over w as mom_n
    from mom
    window w as (
        partition by series_id
        order by observation_date
        rows between unbounded preceding and current row
    )
)

select
    s.series_id,
    s.observation_date,
    s.value,
    s.transform,
    s.transformed,
    s.n_obs,
    -- No z-score is emitted before min_periods is reached, because the statistics are not stable
    case
        when s.n_obs >= mo.min_periods and s.sigma > 0
        then greatest(-{{ z.clip }}, least({{ z.clip }}, (s.transformed - s.mu) / s.sigma))
    end as z_level,
    case
        when m.mom_n >= mo.min_periods and m.mom_sigma > 0
        then greatest(-{{ z.clip }}, least({{ z.clip }}, (m.mom6 - m.mom_mu) / m.mom_sigma))
    end as z_momentum,
    m.mom3,
    m.mom6
from stats s
left join mom_stats m using (series_id, observation_date)
join min_obs mo using (series_id)
