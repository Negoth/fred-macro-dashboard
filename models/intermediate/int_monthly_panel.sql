-- Aligns everything onto a monthly grid. The quadrant maps and the pillar scores are all
-- computed from this. Tableau is not asked to reconcile frequencies
-- (principle 4: no logic in the BI layer).
--
-- Daily -> last value in the month, weekly -> last value in the month, monthly -> as-is,
-- quarterly -> forward-filled.
-- The quarterly DRTSCILM is only observed once a quarter, so the months in between carry
-- the most recent value forward.

with monthly as (
    -- Take the last observation within the month
    select
        series_id,
        date_trunc('month', observation_date)::date as month,
        arg_max(z_level,    observation_date) as z_level,
        arg_max(z_momentum, observation_date) as z_momentum,
        arg_max(transformed, observation_date) as transformed,
        arg_max(value,      observation_date) as value,
        max(observation_date) as last_observation_date
    from {{ ref('int_zscores') }}
    group by 1, 2
),

-- A backing sheet of every series x every month. Needed to forward-fill the gaps in the
-- quarterly series. Clipped to each series' observation range (a correlated subquery would
-- cost series x months, so this is a join instead)
bounds as (
    select series_id, min(month) as first_month, max(month) as last_month
    from monthly
    group by 1
),

spine as (
    select b.series_id, m.month
    from bounds b
    join (select distinct month from monthly) m
        on m.month between b.first_month and b.last_month
)

select
    sp.series_id,
    sp.month,
    -- Forward fill: carry the most recent observation at or before this month forward
    last_value(mo.z_level    ignore nulls) over w as z_level,
    last_value(mo.z_momentum ignore nulls) over w as z_momentum,
    last_value(mo.transformed ignore nulls) over w as transformed,
    last_value(mo.value      ignore nulls) over w as value,
    last_value(mo.last_observation_date ignore nulls) over w as last_observation_date,
    mo.month is null as is_carried_forward
from spine sp
left join monthly mo using (series_id, month)
window w as (
    partition by sp.series_id
    order by sp.month
    rows between unbounded preceding and current row
)
