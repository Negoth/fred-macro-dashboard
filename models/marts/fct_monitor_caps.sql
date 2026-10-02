-- Display-only y-axis caps. This never touches a signal.
--
-- Jobless claims and the Sahm indicator spike so far in 2008-09 and 2020 that every normal
-- move is squashed into a flat line. The cap is median + 6 x MAD of whatever range is being
-- displayed, never below the indicator's own threshold line -- capping below the threshold
-- would hide the very comparison the chart exists to make.
--
-- The cap depends on the displayed range, so one row per (indicator, range) is emitted rather
-- than leaving Tableau to recompute it per period toggle.

{% set ranges = [
    ('all',       "month >= date '" ~ var('regime_display_start') ~ "'"),
    ('since2000', "month >= date '2000-01-01'"),
    ('last10y',   "month > (select max(month) - interval 120 month from " ~ ref('fct_indicator_monitor') | string ~ ")")
] %}

with clipped as (
    select m.indicator, m.month, m.value, m.ref_lines
    from {{ ref('fct_indicator_monitor') }} m
    join {{ ref('regime_indicators') }} d on d.key = m.indicator
    where d.clip = 1 and m.value is not null
),

scoped as (
    {%- for r in ranges %}
    select '{{ r[0] }}' as range_key, indicator, month, value, ref_lines
    from clipped where {{ r[1] }}
    {%- if not loop.last %}
    union all
    {%- endif %}
    {%- endfor %}
),

-- MAD needs the median first, and SQL will not nest a window function inside an aggregate,
-- so the median and the deviations are two passes
meds as (
    select range_key, indicator, median(value) as med, max(value) as observed_max
    from scoped
    group by 1, 2
),

mads as (
    select s.range_key, s.indicator, median(abs(s.value - m.med)) as mad
    from scoped s
    join meds m using (range_key, indicator)
    group by 1, 2
),

-- Highest reference line the chart draws, so the cap never hides the threshold itself
refs as (
    select
        indicator,
        max(coalesce(try_cast(trim(u.unnested) as double), 0)) as max_ref
    from (select distinct indicator, ref_lines from scoped) r,
         unnest(string_split(r.ref_lines, '|')) as u(unnested)
    group by 1
)

select
    m.range_key,
    m.indicator,
    m.med,
    a.mad,
    m.observed_max,
    coalesce(r.max_ref, 0) as max_ref,
    greatest(m.med + 6 * a.mad, coalesce(r.max_ref, 0)) as cap,
    m.observed_max > greatest(m.med + 6 * a.mad, coalesce(r.max_ref, 0)) as is_clipped
from meds m
join mads a using (range_key, indicator)
left join refs r on r.indicator = m.indicator
