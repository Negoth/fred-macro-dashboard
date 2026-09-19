-- The regime panel must have no gaps. Every downstream six-month change is an exact calendar
-- lookup onto this spine, and the 10-month moving average uses a row-count frame, so a single
-- missing month would silently shift both.
-- Any row returned is a failure.

with bounds as (
    select min(month) as lo, max(month) as hi, count(*) as n
    from {{ ref('fct_regime') }}
)

select
    lo, hi, n,
    datediff('month', lo, hi) + 1 as expected
from bounds
where n <> datediff('month', lo, hi) + 1
