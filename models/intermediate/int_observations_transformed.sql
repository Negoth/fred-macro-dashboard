-- Applies the `transform` vocabulary from config/series.yaml.
--
--   level     the level as-is
--   yoy       year-over-year change (%)
--   yoy_4wma  year-over-year change of a 4-week moving average (%)   damps weekly noise
--   mom3_ann  3-month change, annualised (%)                         catches turning points faster than YoY
--   diff12    12-month difference (the change, not the level)
--   chg6m     6-month change (the rate-direction axis of the four seasons)
--   chg13w    13-week change
--
-- Important: series frequencies are mixed (D/W/M/Q), so we look up "the most recent
-- observation at or before the calendar date N months ago" rather than "N rows back".
-- Applying lag(12) to a daily series would give 12 business days ago rather than 12 months
-- ago, and would break silently.

with obs as (
    select o.series_id, o.observation_date, o.value, c.transform
    from {{ ref('int_observations_all') }} o
    join {{ ref('series_config') }} c using (series_id)
),

-- Look up past values on a calendar basis. ASOF JOIN means "the most recent at or before that date"
lagged as (
    select
        o.series_id,
        o.observation_date,
        o.value,
        o.transform,
        p12.value as value_12m_ago,
        p6.value  as value_6m_ago,
        p3.value  as value_3m_ago,
        p13w.value as value_13w_ago,
        w4.value  as value_4wma,
        w4prev.value as value_4wma_12m_ago
    from obs o
    asof left join obs p12
        on o.series_id = p12.series_id
       and p12.observation_date <= o.observation_date - interval 12 month
    asof left join obs p6
        on o.series_id = p6.series_id
       and p6.observation_date <= o.observation_date - interval 6 month
    asof left join obs p3
        on o.series_id = p3.series_id
       and p3.observation_date <= o.observation_date - interval 3 month
    asof left join obs p13w
        on o.series_id = p13w.series_id
       and p13w.observation_date <= o.observation_date - interval 91 day
    -- The 4-week moving average is built in a separate model (see int_observations_ma4)
    asof left join {{ ref('int_observations_ma4') }} w4
        on o.series_id = w4.series_id
       and w4.observation_date <= o.observation_date
    asof left join {{ ref('int_observations_ma4') }} w4prev
        on o.series_id = w4prev.series_id
       and w4prev.observation_date <= o.observation_date - interval 12 month
)

select
    series_id,
    observation_date,
    value,
    transform,
    case transform
        when 'level'    then value
        when 'yoy'      then case when value_12m_ago is not null and value_12m_ago <> 0
                                  then (value / value_12m_ago - 1) * 100 end
        when 'yoy_4wma' then case when value_4wma_12m_ago is not null and value_4wma_12m_ago <> 0
                                  then (value_4wma / value_4wma_12m_ago - 1) * 100 end
        when 'mom3_ann' then case when value_3m_ago is not null and value_3m_ago > 0
                                  then (pow(value / value_3m_ago, 4.0) - 1) * 100 end
        when 'diff12'   then value - value_12m_ago
        when 'chg6m'    then value - value_6m_ago
        when 'chg13w'   then value - value_13w_ago
    end as transformed
from lagged
