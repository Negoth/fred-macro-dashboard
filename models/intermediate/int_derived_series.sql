-- Derived series, computed from several raw series (design note 6.4).
--   NET_LIQUIDITY = WALCL - RRPONTSYD - WTREGEN
--     From the Fed's total assets, subtract the part absorbed by reverse repos and the
--     Treasury General Account that does not circulate in the market. All three series are
--     weekly and share a publication date
--   WORLD_DOLLAR  = BOGMBASE + WMTSECL1
--     The US dollar liquidity measure from chapter 5 of Horii's "Kinri o Mireba Toshi wa
--     Umaku Iku". BOGMBASE is monthly and WMTSECL1 is weekly, so it is forward-filled onto
--     the weekly grid

with obs as (
    select series_id, observation_date, value
    from {{ ref('stg_fred_observations') }}
    where series_id in ('WALCL', 'RRPONTSYD', 'WTREGEN', 'BOGMBASE', 'WMTSECL1')
      and value is not null
),

-- A backing sheet of every observation date across all components. Values are forward-filled onto it
calendar as (
    select distinct observation_date from obs
),

-- The forward fill is done with an ASOF JOIN. A correlated subquery (date x series x observation)
-- would mean over 400 million scans and did not finish in 15 minutes on CI's two-core runner
wide as (
    select
        c.observation_date,
        w.value  as walcl,
        r.value  as rrp,
        t.value  as tga,
        b.value  as monetary_base,
        m.value  as custody
    from calendar c
    asof left join (select * from obs where series_id = 'WALCL')     w on w.observation_date <= c.observation_date
    asof left join (select * from obs where series_id = 'RRPONTSYD') r on r.observation_date <= c.observation_date
    asof left join (select * from obs where series_id = 'WTREGEN')   t on t.observation_date <= c.observation_date
    asof left join (select * from obs where series_id = 'BOGMBASE')  b on b.observation_date <= c.observation_date
    asof left join (select * from obs where series_id = 'WMTSECL1')  m on m.observation_date <= c.observation_date
)

select 'NET_LIQUIDITY' as series_id, observation_date, walcl - rrp - tga as value
from wide
where walcl is not null and rrp is not null and tga is not null

union all

select 'WORLD_DOLLAR' as series_id, observation_date, monetary_base + custody as value
from wide
where monetary_base is not null and custody is not null
