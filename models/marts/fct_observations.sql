-- The observation fact table, at its native grain. This is what Tableau reads.
--
-- Columns were previously append-only, to protect Tableau workbooks already built against
-- them. That clock restarted when the pillar/z-score layer was retired: no workbook had been
-- built yet, so `transform`, `transformed`, `z_level`, `z_momentum` and `n_obs` were removed
-- along with the models that produced them. From here the rule applies again.

with recession as (
    -- USREC is a monthly 0/1. Round to the start of the month so it can also be matched
    -- against daily and weekly observations
    select observation_date as month_start, value as flag
    from {{ ref('stg_fred_observations') }}
    where series_id = 'USREC'
)

select
    o.series_id,
    o.observation_date,
    o.value,
    -- The is_recession of design note 3.6. For shading the NBER recession periods
    coalesce(r.flag = 1, false) as is_recession,
    -- Inside or outside the display window. The dashboard display is limited to
    -- history_start onward (the limit on how far back the panel can go, design note 6.8)
    o.observation_date >= date '{{ var("history_start") }}' as in_display_window
from {{ ref('int_observations_all') }} o
left join recession r
    on date_trunc('month', o.observation_date) = r.month_start
