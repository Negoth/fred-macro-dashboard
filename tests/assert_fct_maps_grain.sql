-- The grain of fct_maps must be unique on (month, map).
-- Any row returned is a failure.

select month, map, count(*) as n
from {{ ref('fct_maps') }}
group by 1, 2
having count(*) > 1
