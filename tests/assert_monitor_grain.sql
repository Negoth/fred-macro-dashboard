-- The grain of fct_indicator_monitor must be unique on (indicator, month). The model unpivots
-- one row per month into twelve, so a duplicate here means the join to regime_indicators
-- fanned out -- which the unique test on the seed key should prevent, but this is the
-- downstream guard.
-- Any row returned is a failure.

select indicator, month, count(*) as n
from {{ ref('fct_indicator_monitor') }}
group by 1, 2
having count(*) > 1
