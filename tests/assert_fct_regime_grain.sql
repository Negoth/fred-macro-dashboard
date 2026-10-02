-- The grain of fct_regime must be unique on month.
-- Any row returned is a failure.

select month, count(*) as n
from {{ ref('fct_regime') }}
group by 1
having count(*) > 1
