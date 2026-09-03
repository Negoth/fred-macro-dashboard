-- The observation fact table, at its native grain. This is what Tableau reads.
--
-- Columns are only ever added. Existing columns are never renamed or removed, so as not
-- to break Tableau workbooks that have already been built.

with recession as (
    -- USREC is a monthly 0/1. Round to the start of the month so it can also be matched
    -- against daily and weekly observations
    select observation_date as month_start, value as flag
    from {{ ref('stg_fred_observations') }}
    where series_id = 'USREC'
)

select
    z.series_id,
    z.observation_date,
    z.value,
    -- The is_recession of design note 3.6. For shading the NBER recession periods
    coalesce(r.flag = 1, false) as is_recession,
    -- Inside or outside the display window. The z-score population is the full history, but
    -- the dashboard display is limited to history_start onward (the limit on how far back the
    -- panel can go, design note 6.8)
    z.observation_date >= date '{{ var("history_start") }}' as in_display_window,
    -- Columns added in M2
    z.transform,
    z.transformed,
    z.z_level,
    z.z_momentum,
    z.n_obs
from {{ ref('int_zscores') }} z
left join recession r
    on date_trunc('month', z.observation_date) = r.month_start

union all

-- Series outside the z-score scope (context series, components of derived series) are still
-- shown as observations
select
    o.series_id,
    o.observation_date,
    o.value,
    coalesce(r.flag = 1, false) as is_recession,
    o.observation_date >= date '{{ var("history_start") }}' as in_display_window,
    null as transform,
    null as transformed,
    null as z_level,
    null as z_momentum,
    null as n_obs
from {{ ref('int_observations_all') }} o
left join recession r
    on date_trunc('month', o.observation_date) = r.month_start
where o.series_id not in (select distinct series_id from {{ ref('int_zscores') }})
