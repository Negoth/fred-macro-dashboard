-- The monthly panel. It is the unit of computation for the quadrant maps and the pillar
-- scores, and it is also what Tableau uses to compare series against each other (because
-- the frequencies are aligned).

select
    p.series_id,
    p.month,
    p.value,
    p.transformed,
    p.z_level,
    p.z_momentum,
    p.is_carried_forward,
    p.last_observation_date,
    p.month >= date '{{ var("history_start") }}' as in_display_window,
    c.pillar,
    c.pillar_kind,
    c.sign,
    c.season_axis,
    c.season_role,
    c.is_scored
from {{ ref('int_monthly_panel') }} p
join {{ ref('series_config') }} c using (series_id)
