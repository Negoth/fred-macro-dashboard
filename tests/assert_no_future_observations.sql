-- No observations dated in the future may have slipped in.
-- Misreading FRED's date parsing breaks silently here, so we check it explicitly.

select series_id, max(observation_date) as max_date
from {{ ref('fct_observations') }}
group by 1
having max(observation_date) > current_date
