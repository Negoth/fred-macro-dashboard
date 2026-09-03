-- Series dimension. Overlays FRED's own metadata onto config/series.yaml (the seed).
-- Tableau reads only this and fct_observations.

select
    c.series_id,
    c.title_display,
    coalesce(m.title, c.series_id)  as title_fred,
    m.units,
    m.units_short,
    coalesce(m.frequency_short, c.freq) as frequency_short,
    m.seasonal_adjustment_short,
    c.pillar,
    c.pillar_kind,
    c.transform,
    c.sign,
    c.season_axis,
    c.season_role,
    c.is_scored,
    c.is_derived,
    c.derived_from,
    c.origin,
    m.observation_start,
    m.observation_end,
    m.last_updated
from {{ ref('series_config') }} c
left join {{ ref('stg_fred_series_meta') }} m using (series_id)
