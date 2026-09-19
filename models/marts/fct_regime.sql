-- Season, credit phase, the eleven flags and the portfolio stance. One row per month.
--
-- Two cycles, not one circle: the monetary policy cycle runs about five years and the credit
-- cycle about ten, so they are read independently. The retired fct_maps folded them onto one
-- quadrant map, which the data rejects -- of 45 season changes, 22 ran clockwise and 22 ran
-- backwards.
--
-- Ported from the pandas reference implementation. Three things in the port are load-bearing
-- and easy to undo by accident; each is marked below.

{% set gate    = var('regime_season_gate') %}
{% set confirm = var('regime_phase_confirm') %}
{% set s_min   = var('regime_vulnerability_min') %}
{% set t_min   = var('regime_breakdown_min') %}
{% set h_min   = var('regime_recovery_min') %}
{% set memory  = var('regime_memory_months') %}

with recursive
panel as (select * from {{ ref('int_regime_monthly') }}),

-- Season. Which end of the curve moved, and which way: spring is the long end up, summer the
-- short end up, autumn the long end down, winter the short end down
season_raw as (
    select *,
        (d6_ff + d6_10) / 2.0 as lvl,
        d6_10 - d6_ff         as slp,
        case
            when d6_ff is null or d6_10 is null then null
            -- Below one policy step at both ends there is no new reading, and the previous
            -- season carries over rather than the map jumping on noise
            when greatest(abs(d6_ff), abs(d6_10)) < {{ gate }} then null
            when (d6_ff + d6_10) / 2.0 >= 0 and d6_10 - d6_ff >= 0 then 'spring'
            when (d6_ff + d6_10) / 2.0 >= 0                        then 'summer'
            when d6_10 - d6_ff < 0                                 then 'autumn'
            else                                                        'winter'
        end as s_raw
    from panel
),

season as (
    select *,
        last_value(s_raw ignore nulls) over w as season,
        s_raw is null and last_value(s_raw ignore nulls) over w is not null as season_carried
    from season_raw
    window w as (order by month rows between unbounded preceding and current row)
),

-- Credit phase. The reference implementation walks this as a state machine with a `pending`
-- candidate, but it reduces exactly to "switch when the new reading appears in N consecutive
-- non-null readings" -- so two window functions, no recursion. Verified against all 483
-- months of the prototype output
phase_raw as (
    select *,
        case
            when eq6 is null or dsp6 is null then null
            when eq6 > 0 and dsp6 <  0 then 'riskon'
            when eq6 > 0               then 'leverage'
            when dsp6 >= 0             then 'riskoff'
            else                            'repair'
        end as p_raw
    from season
),

phase_confirmed as (
    select *,
        case when p_raw is not null and (
            {%- for i in range(1, confirm) %}
            (lag(p_raw, {{ i }} ignore nulls) over (order by month) is null
             or p_raw = lag(p_raw, {{ i }} ignore nulls) over (order by month))
            {%- if not loop.last %} and {%- endif %}
            {%- endfor %}
        ) then p_raw end as confirmed
    from phase_raw
),

phase as (
    select *,
        last_value(confirmed ignore nulls) over (
            order by month rows between unbounded preceding and current row) as phase
    from phase_confirmed
),

-- Flags. The asymmetry here is deliberate and must not be tidied up: S2 and S3 stay NULL when
-- their input is missing, so SUM skips them and a missing input counts as neither on nor off.
-- Every other flag is coalesced to false, because pandas yields False where SQL yields NULL,
-- and a NULL would silently drop the row from its count
flags as (
    select *,
        coalesce(spr < 0, false)            as f_s1,
        case when lr_ff is null then null else ff > lr_ff end as f_s2,
        case when sloos is null then null else sloos > 0  end as f_s3,
        coalesce(phase = 'leverage', false) as f_s4,
        coalesce(phase = 'riskoff', false)  as f_t1,
        coalesce(y2ff < 0, false)           as f_t2,
        coalesce(sahm >= 0.5, false)        as f_t3,
        coalesce(spx < sma10, false)        as f_t4,
        coalesce(phase = 'repair', false)   as f_h1,
        coalesce(claims3 < 0, false)        as f_h2,
        coalesce(spx > sma10, false)        as f_h3
    from phase
),

counts as (
    select *,
        row_number() over (order by month) as n,
        coalesce(f_s1::int, 0) + coalesce(f_s2::int, 0)
          + coalesce(f_s3::int, 0) + coalesce(f_s4::int, 0) as n_s,
        f_t1::int + f_t2::int + f_t3::int + f_t4::int       as n_t,
        f_h1::int + f_h2::int + f_h3::int                   as n_h
    from flags
),

memo as (
    select *,
        -- Vulnerability flags such as inversion fade once a breakdown starts, so the stance
        -- has to remember them. 24 months is the commonly cited upper bound on the lead from
        -- curve inversion to recession
        max(n_s) over (order by month
                       rows between {{ memory - 1 }} preceding and current row) as s24,
        (n_t >= {{ t_min }}
         and max(n_s) over (order by month
                            rows between {{ memory - 1 }} preceding and current row) >= {{ s_min }}
         {%- if var('regime_require_t1') %}
         and f_t1
         {%- endif %}) as trigger_reduce
    from counts
),

-- Stance. Genuinely sequential -- `reduce` holds until recovery flags line up, so unlike the
-- phase this one does need a recursive walk. Only reduce/restore carry state; normal and
-- nochase are recomputed from scratch each month
walk as (
    select n, month,
        case when trigger_reduce then 'reduce'
             when n_s >= {{ s_min }} then 'nochase' else 'normal' end as stance
    from memo where n = 1
    union all
    select c.n, c.month,
        case
            when w.stance in ('normal', 'nochase') then
                case when c.trigger_reduce then 'reduce'
                     when c.n_s >= {{ s_min }} then 'nochase' else 'normal' end
            when w.stance = 'reduce' then
                case when c.n_h >= {{ h_min }} then 'restore' else 'reduce' end
            else
                case when c.trigger_reduce then 'reduce'
                     when c.n_t = 0 then
                        (case when c.n_s >= {{ s_min }} then 'nochase' else 'normal' end)
                     else 'restore' end
        end
    from walk w join memo c on c.n = w.n + 1
)

select
    m.month,
    m.season,
    m.season_carried,
    m.lvl,
    m.slp,
    m.d6_ff,
    m.d6_10,
    m.phase,
    m.eq6,
    m.dsp6,
    w.stance,
    m.n_s,
    m.n_t,
    m.n_h,
    m.s24,
    m.f_s1, m.f_s2, m.f_s3, m.f_s4,
    m.f_t1, m.f_t2, m.f_t3, m.f_t4,
    -- Recovery flags are only consulted while defensive, so they are masked for display here.
    -- This happens AFTER n_h is counted: masking before it would change the stance path
    m.f_h1 and w.stance in ('reduce', 'restore') as f_h1,
    m.f_h2 and w.stance in ('reduce', 'restore') as f_h2,
    m.f_h3 and w.stance in ('reduce', 'restore') as f_h3,
    -- Carried through for the dashboard: the "what would change the reading" gauges, and the
    -- three-months-ago rates the season scenario needs
    m.ff, m.y2, m.y10, m.baa, m.spx, m.sma10, m.spxgap, m.spr, m.y2ff, m.ffgap, m.claims3,
    m.ff12, m.y1012, m.baa12, m.usd12,
    m.sahm, m.sloos, m.lr_ff, m.claims, m.rec,
    m.ff_3m_ago, m.y10_3m_ago,
    m.month >= date '{{ var("regime_display_start") }}' as in_display_window
from memo m
join walk w using (n)
