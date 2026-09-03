-- Coordinates for the two quadrant maps on the front page. Design note 6.3 / 6.3b.
--
--   seasons          the financial four seasons (direction of rates x credit cycle)  <- the main one
--   growth_inflation growth x inflation                                              <- the second visual
--
-- Emitted in long format. Filtering on `map` lets Tableau reuse the same sheet structure.
--
-- WARNING: the four-seasons quadrant assignment is provisional. Because the decision rules of the
--    original book (Horii, "Kinri o Mireba Toshi wa Umaku Iku", ch. 9, "measuring the investment
--    climate by interest rates") are not available in any published material, they are placeholders
--    based on the sign of the z-score. The thresholds live in seasons.thresholds in
--    config/series.yaml, so once the original can be checked, only that needs replacing
--    (this SQL stays untouched).

{% set dz = var('season_deadzone') %}
{% set sm = var('season_smoothing_months') %}

with panel as (
    select p.series_id, p.month, p.z_level, p.z_momentum, c.sign, c.season_axis, c.pillar
    from {{ ref('int_monthly_panel') }} p
    join {{ ref('series_config') }} c using (series_id)
    where p.z_level is not null
),

-- The two axes of the four seasons. Multiplying by `sign` lines everything up so that
-- "positive = easing / falling rates".
--   rate axis:   the 6-month change in DGS10 and the 12-month difference in FEDFUNDS. Both have
--                sign=-1, so rising rates are negative and falling rates positive. Here we want
--                "rising = positive", so it is inverted
--   credit axis: BAA10Y (the spread) and DRTSCILM (lending standards). Both have sign=-1, so
--                multiplying by sign gives "positive = easing"
season_axes_raw as (
    select
        month,
        -avg(case when season_axis = 'rate'   then sign * z_level end) as x_rate,
         avg(case when season_axis = 'credit' then sign * z_level end) as y_credit
    from panel
    where season_axis in ('rate', 'credit')
    group by 1
),

-- A trailing moving average damps month-to-month reversal noise. It is not centred, in
-- order to avoid look-ahead bias (same reason as the expanding window in int_zscores)
season_axes as (
    select
        month,
        avg(x_rate)   over w as x_rate,
        avg(y_credit) over w as y_credit
    from season_axes_raw
    window w as (order by month rows between {{ sm - 1 }} preceding and current row)
),

-- Inversion of the yield spread. The marker overlaid on the trajectory for chapter 3's
-- "advance notice of a change of season"
inversion as (
    select month, bool_or(value < 0) as is_inverted
    from {{ ref('int_monthly_panel') }}
    where series_id in ('T10YFF', 'T10Y3M') and value is not null
    group by 1
),

-- Growth x inflation. The pillar scores are used directly as coordinates
gi_axes_raw as (
    select
        month,
        max(case when pillar = 'growth'    then score end) as x_growth,
        max(case when pillar = 'inflation' then score end) as y_inflation
    from {{ ref('fct_pillars') }}
    group by 1
),

gi_axes as (
    select
        month,
        avg(x_growth)    over w as x_growth,
        avg(y_inflation) over w as y_inflation
    from gi_axes_raw
    window w as (order by month rows between {{ sm - 1 }} preceding and current row)
),

seasons as (
    select
        a.month,
        'seasons' as map,
        a.x_rate    as x,
        a.y_credit  as y,
        case
            -- No quadrant is assigned near the origin. Judging per axis (OR) would make a point
            -- a boundary merely because one axis is near zero, leaving half the whole history
            -- unclassified
            when sqrt(a.x_rate * a.x_rate + a.y_credit * a.y_credit) < {{ dz }} then 'Boundary'
            when a.x_rate < 0 and a.y_credit > 0 then 'Spring'   -- falling rates x easy credit
            when a.x_rate > 0 and a.y_credit > 0 then 'Summer'   -- rising rates x easy credit
            when a.x_rate > 0 and a.y_credit < 0 then 'Autumn'   -- rising rates x tight credit
            when a.x_rate < 0 and a.y_credit < 0 then 'Winter'   -- falling rates x tight credit
        end as quadrant,
        coalesce(i.is_inverted, false) as is_inverted
    from season_axes a
    left join inversion i using (month)
    where a.x_rate is not null and a.y_credit is not null
),

growth_inflation as (
    select
        g.month,
        'growth_inflation' as map,
        g.x_growth    as x,
        g.y_inflation as y,
        case
            when sqrt(g.x_growth * g.x_growth + g.y_inflation * g.y_inflation) < {{ dz }} then 'Boundary'
            when g.x_growth > 0 and g.y_inflation < 0 then 'Goldilocks'
            when g.x_growth > 0 and g.y_inflation > 0 then 'Overheating'
            when g.x_growth < 0 and g.y_inflation > 0 then 'Stagflation'
            when g.x_growth < 0 and g.y_inflation < 0 then 'Reflation'
        end as quadrant,
        coalesce(i.is_inverted, false) as is_inverted
    from gi_axes g
    left join inversion i using (month)
    where g.x_growth is not null and g.y_inflation is not null
)

select
    month,
    map,
    x,
    y,
    quadrant,
    is_inverted,
    month >= date '{{ var("history_start") }}' as in_display_window
from (select * from seasons union all select * from growth_inflation)
