-- The monthly panel behind both cycles. One row per month, no gaps.
--
-- Aggregation is the MEAN of the daily values for rates and spreads, not the last value in
-- the month. Two reasons:
--   * DFF is the *effective* fed funds rate -- the volume-weighted median of actual overnight
--     transactions -- not the FOMC target. Before the floor system it blew out at year end:
--     1986-12-30 printed 16.17% against a policy stance near 6%, so that month's last value
--     misstates the stance by 7.4pp. The mean suppresses that, and reproduces FEDFUNDS.
--   * FEDFUNDS itself is published only after month-end, so it cannot carry a provisional
--     current month. DFF can.
-- The S&P 500 is the exception and uses the month-end close, because T4/H3 compare it against
-- a 10-month moving average of month-end closes (the standard trend-following form).
-- Both conventions are revisited in issue #8.
--
-- The spine is dense and monthly, so "six months ago" is an exact calendar lookup done with a
-- self-join on `month - interval 6 month`. That is written as a join rather than lag(6) so it
-- reads as the calendar lookup it is -- the ASOF JOIN invariant exists because lag(n) on a
-- *native-grain* mixed-frequency table silently means n observations, not n months.

with last_obs as (
    -- Deterministic end of the panel. current_date would make the build non-reproducible
    select greatest(
        (select max(observation_date) from {{ ref('stg_fred_observations') }}
          where series_id in ('DFF', 'DGS10', 'BAA10Y')),
        (select max(observation_date) from {{ ref('stg_yf_observations') }})
    ) as d
),

spine as (
    select unnest(generate_series(
        date '{{ var("regime_panel_start") }}',
        date_trunc('month', (select d from last_obs))::date,
        interval 1 month
    ))::date as month
),

fred as (
    select series_id, date_trunc('month', observation_date)::date as month,
           avg(value) as mean_value,
           arg_max(value, observation_date) as last_value
    from {{ ref('stg_fred_observations') }}
    where value is not null
    group by 1, 2
),

spx_m as (
    select date_trunc('month', observation_date)::date as month,
           arg_max(value, observation_date) as spx
    from {{ ref('stg_yf_observations') }}
    where series_id = 'GSPC' and value is not null
    group by 1
),

-- SLOOS is dated to the quarter it surveys but published about a month into it, so it applies
-- from the month AFTER its observation month. date_trunc + 1 month, never date_trunc alone
sloos_m as (
    select date_trunc('month', observation_date)::date + interval 1 month as month,
           arg_max(value, observation_date) as sloos
    from {{ ref('stg_fred_observations') }}
    where series_id = 'DRTSCILM' and value is not null
    group by 1
),

joined as (
    select
        s.month,
        max(case when f.series_id = 'DFF'          then f.mean_value end) as ff,
        max(case when f.series_id = 'DGS2'         then f.mean_value end) as y2,
        max(case when f.series_id = 'DGS10'        then f.mean_value end) as y10,
        max(case when f.series_id = 'BAA10Y'       then f.mean_value end) as baa,
        max(case when f.series_id = 'ICSA'         then f.mean_value end) as claims,
        max(case when f.series_id = 'DTWEXBGS'     then f.mean_value end) as usd,
        max(case when f.series_id = 'SAHMREALTIME' then f.mean_value end) as sahm_raw,
        max(case when f.series_id = 'USREC'        then f.mean_value end) as rec_raw,
        -- The longer-run projection is a level, so the latest print in the month wins
        max(case when f.series_id = 'FEDTARMDLR'   then f.last_value end) as lr_ff_raw,
        max(x.spx)   as spx,
        max(sl.sloos) as sloos_raw
    from spine s
    left join fred   f  on f.month  = s.month
    left join spx_m  x  on x.month  = s.month
    left join sloos_m sl on sl.month = s.month
    group by 1
),

filled as (
    select
        month, ff, y2, y10, baa, claims, usd, spx,
        -- Bounded 2-month carry-forward covers the publication lag without inventing data
        -- indefinitely. The spine is dense, so lag(n) here is exactly n months
        coalesce(sahm_raw, lag(sahm_raw, 1) over w, lag(sahm_raw, 2) over w) as sahm,
        coalesce(rec_raw,  lag(rec_raw, 1)  over w, lag(rec_raw, 2)  over w) as rec,
        -- Unbounded carry-forward: a quarterly survey and an annual projection stay in force
        -- until superseded
        last_value(sloos_raw  ignore nulls) over w_all as sloos,
        last_value(lr_ff_raw  ignore nulls) over w_all as lr_ff
    from joined
    window
        w     as (order by month),
        w_all as (order by month rows between unbounded preceding and current row)
)

select
    f.month,
    f.ff, f.y2, f.y10, f.baa, f.spx, f.claims, f.usd, f.sahm, f.rec, f.sloos, f.lr_ff,
    f.y10 - f.ff  as spr,
    f.y2  - f.ff  as y2ff,
    f.ff  - f.lr_ff as ffgap,
    -- Six-month changes: exact calendar lookups on the dense spine
    f.ff  - p6.ff  as d6_ff,
    f.y10 - p6.y10 as d6_10,
    (f.spx / p6.spx - 1) * 100 as eq6,
    (f.baa - p6.baa) * 100     as dsp6,
    f.claims - p3.claims       as claims3,
    -- Twelve-month changes drive the indicator monitor's core five
    f.ff  - p12.ff   as ff12,
    f.y10 - p12.y10  as y1012,
    f.baa - p12.baa  as baa12,
    (f.usd / p12.usd - 1) * 100 as usd12,
    -- Three months back, so Tableau can run the season scenario without arithmetic of its own
    p3.ff  as ff_3m_ago,
    p3.y10 as y10_3m_ago
from filled f
left join filled p3  on p3.month  = f.month - interval 3 month
left join filled p6  on p6.month  = f.month - interval 6 month
left join filled p12 on p12.month = f.month - interval 12 month
