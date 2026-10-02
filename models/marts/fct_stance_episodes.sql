-- Every period the stance spent in Cut equities, with what the S&P 500 did meanwhile.
--
-- This is a falsification table, not a performance record: it exists to show where the rules
-- fire and where they are wrong. The claim being defended is narrow -- step aside for the two
-- credit-cycle busts, ignore the shock-driven declines -- so "missed a rally" is an accepted
-- cost, and listing those costs is the point.
--
-- Publication lags are ignored, as in the reference implementation.

with numbered as (
    select
        month, stance, spx,
        row_number() over (order by month) as n
    from {{ ref('fct_regime') }}
),

-- Gaps and islands over runs of `reduce`
islands as (
    select *,
        n - row_number() over (partition by stance order by month) as grp
    from numbered
),

runs as (
    select
        min(month) as start_month,
        max(month) as last_defensive_month,
        min(n)     as start_n,
        max(n)     as end_n,
        count(*)   as months
    from islands
    where stance = 'reduce'
    group by grp
),

-- The exit month is the first month NOT in the run. It is included in the drawdown but not
-- in the month count, matching the reference implementation
with_exit as (
    select
        r.*,
        e.month as exit_month,
        e.spx   as spx_out,
        s.spx   as spx_in
    from runs r
    join numbered s on s.n = r.start_n
    left join numbered e on e.n = r.end_n + 1
)

select
    w.start_month,
    w.exit_month,
    w.months,
    w.spx_in,
    w.spx_out,
    case when w.spx_out is not null and w.spx_in <> 0
         then (w.spx_out / w.spx_in - 1) * 100 end as spx_change_pct,
    (
        select (min(x.spx) / w.spx_in - 1) * 100
        from numbered x
        where x.n between w.start_n and coalesce(w.end_n + 1, w.end_n)
    ) as worst_drawdown_pct,
    w.exit_month is null as is_open
from with_exit w
order by w.start_month
