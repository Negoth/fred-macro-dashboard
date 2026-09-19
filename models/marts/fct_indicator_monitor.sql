-- What sits inside the signals: one row per indicator per month, with the value, the
-- threshold it is judged against, and the resulting score or flag.
--
-- Rather than adding indicators up into a composite, each is shown against its own threshold.
-- The composite the source framework proposes was tested and rejected: it reads +6 at the
-- January 2022 peak and -10 six months after the December 2018 trough, because indicators
-- with different lead times cancel each other out.
--
-- Definitions come from ref('regime_indicators'), which is generated from the `regime:` block
-- of config/series.yaml, so no threshold is written here.

with panel as (
    select
        month,
        ff12, spr, y1012, baa12, usd12,
        y2ff, ffgap, sloos, sahm, claims, spxgap, baa
    from {{ ref('fct_regime') }}
),

-- Long format. UNPIVOT keeps this honest: adding an indicator to the config and to the
-- select above is enough, with no third place to update
long as (
    unpivot panel
    on ff12, spr, y1012, baa12, usd12, y2ff, ffgap, sloos, sahm, claims, spxgap, baa
    into name indicator value value
),

flags as (
    select month, unnest(['S1','S2','S3','S4','T1','T2','T3','T4','H1','H2','H3']) as flag_key,
           unnest([f_s1, f_s2, f_s3, f_s4, f_t1, f_t2, f_t3, f_t4, f_h1, f_h2, f_h3]) as flag_on
    from {{ ref('fct_regime') }}
)

select
    l.month,
    l.indicator,
    d.monitor_group,
    d.title,
    d.unit,
    -- Initial claims are declared in thousands, so scale once here rather than leaving
    -- Tableau to divide by 1000 on every sheet
    case when d.unit = 'k' then l.value / 1000.0 else l.value end as value,
    -- The +/-2 score the five year-over-year indicators keep individually
    case d.score_rule
        when 'le'   then case when l.value <= d.score_t1 then 2 else -2 end
        when 'ge'   then case when l.value >= d.score_t1 then 2 else -2 end
        when 'band' then case when l.value >= d.score_t1 then 2
                              when l.value >= d.score_t2 then 0
                              else -2 end
    end as score,
    d.flag_key,
    -- Recovery flags read as off outside the defensive stances, because fct_regime masks
    -- them there -- they are only consulted while cutting or rebuilding equities
    f.flag_on,
    -- The threshold to draw as a reference line
    d.score_t1,
    d.score_t2,
    d.flag_threshold,
    d.ref_lines,
    d.clip,
    l.value is not null
      and (d.first_month is null or l.month >= d.first_month) as in_display_window
from long l
join {{ ref('regime_indicators') }} d on d.key = l.indicator
left join flags f on f.month = l.month and f.flag_key = d.flag_key
where l.value is not null
  and (d.first_month is null or l.month >= d.first_month)
