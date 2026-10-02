-- Yield curve snapshots: now, six months ago, twelve months ago.
-- Long format so Tableau puts tenor on columns and the snapshot on colour.
--
-- The anchor is the latest date on which EVERY tenor printed. That inner join matters: DGS30
-- has a gap from 2002 to 2006, when the 30-year was not issued, and taking each tenor's own
-- latest value would silently mix dates within one "snapshot".
--
-- The lookbacks are asof, not exact: markets are shut on plenty of calendar dates, so this
-- takes the most recent print at or before the target date.

{% set tenors = [
    ('DGS3MO',  0.25, '3M'),
    ('DGS2',    2,    '2Y'),
    ('DGS5',    5,    '5Y'),
    ('DGS10',  10,    '10Y'),
    ('DGS30',  30,    '30Y')
] %}

with daily as (
    select observation_date, series_id, value
    from {{ ref('stg_fred_observations') }}
    where series_id in ('DFF'{% for t in tenors %}, '{{ t[0] }}'{% endfor %})
      and value is not null
),

complete_days as (
    -- Only dates where all six printed
    select observation_date
    from daily
    group by 1
    having count(distinct series_id) = {{ tenors | length + 1 }}
),

anchors as (
    select 'now' as snapshot, 0 as months_back, max(observation_date) as target from complete_days
    union all
    select 'm6',  6,  max(observation_date) - interval 6 month  from complete_days
    union all
    select 'm12', 12, max(observation_date) - interval 12 month from complete_days
),

resolved as (
    -- Most recent complete date at or before the target
    select a.snapshot, a.months_back, max(c.observation_date) as snapshot_date
    from anchors a
    join complete_days c on c.observation_date <= a.target
    group by 1, 2
)

select
    r.snapshot,
    r.months_back,
    r.snapshot_date,
    x.tenor_years,
    x.tenor_label,
    x.is_policy_rate,
    d.value as yield
from resolved r
join (
    select 'DFF' as series_id, 0.0 as tenor_years, 'FF' as tenor_label, true as is_policy_rate
    {%- for t in tenors %}
    union all select '{{ t[0] }}', {{ t[1] }}, '{{ t[2] }}', false
    {%- endfor %}
) x on true
join daily d on d.series_id = x.series_id and d.observation_date = r.snapshot_date
