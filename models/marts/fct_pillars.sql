-- Pillar scores: mean_i( sign_i * z_i(t) ). Design note 6.5.
--
-- Emitted as one row per pillar, so that the "stacked contribution by pillar" of 6.6 can
-- break the overall score down. A single line only tells you "it went down"; a stacked
-- chart tells you what made it go down.
--
-- The coordinate pillars (growth / inflation) are positions on an axis and carry no
-- good/bad sign; the score pillars (financial / credit / dollar) are tailwind/headwind for
-- risk assets. Confusing them re-creates the 6.2-C failure of the design note.

select
    p.month,
    c.pillar,
    c.pillar_kind,
    avg(c.sign * p.z_level)    as score,
    avg(c.sign * p.z_momentum) as score_momentum,
    count(p.z_level)           as n_series,
    -- Diffusion index: the share (%) of series pointing in the tailwind direction. A
    -- continuous, intuitive supporting measure
    100.0 * sum(case when c.sign * p.z_level > 0 then 1 else 0 end)
          / nullif(count(p.z_level), 0) as diffusion_pct
from {{ ref('int_monthly_panel') }} p
join {{ ref('series_config') }} c using (series_id)
where c.is_scored = 1
  and c.pillar is not null and c.pillar <> ''
  and p.z_level is not null
group by 1, 2, 3
