-- A display cap below the indicator's own threshold line would hide the very comparison the
-- chart exists to make -- the reader could not see whether the Sahm indicator had crossed 0.5.
-- Any row returned is a failure.

select range_key, indicator, cap, max_ref
from {{ ref('fct_monitor_caps') }}
where cap < max_ref
