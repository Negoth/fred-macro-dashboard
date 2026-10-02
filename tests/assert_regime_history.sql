-- Regression values from the reference implementation. These sit deep in the past, so FRED
-- revisions do not move them -- unlike the latest month, which is provisional and would make
-- this test fail on data freshness rather than on logic.
--
-- If a refactor quietly changes the season gate, the phase confirmation or the stance memory,
-- this is what catches it.
-- Any row returned is a failure.

with expected as (
    select * from (values
        ('season', 'summer',  date '2004-08-01', date '2006-10-01'),
        ('season', 'winter',  date '2007-09-01', date '2009-03-01'),
        ('season', 'summer',  date '2022-07-01', date '2023-08-01'),
        ('phase',  'riskoff', date '2007-12-01', date '2009-05-01'),
        ('stance', 'reduce',  date '2007-11-01', date '2009-05-01')
    ) as t(kind, value, start_month, end_month)
),

actual as (
    select month, 'season' as kind, season as value from {{ ref('fct_regime') }}
    union all
    select month, 'phase',  phase  from {{ ref('fct_regime') }}
    union all
    select month, 'stance', stance from {{ ref('fct_regime') }}
)

select e.kind, e.value, a.month, a.value as actual_value
from expected e
join actual a
  on a.kind = e.kind
 and a.month between e.start_month and e.end_month
where a.value is distinct from e.value
