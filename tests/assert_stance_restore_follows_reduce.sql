-- Rebuild equities means rebuilding a position that was cut, so it can only ever follow
-- Cut equities or itself. If it appears out of the blue, the state machine has lost its
-- memory -- the most likely symptom of a broken recursive walk.
-- Any row returned is a failure.

select month, stance, prev_stance
from (
    select month, stance, lag(stance) over (order by month) as prev_stance
    from {{ ref('fct_regime') }}
)
where stance = 'restore'
  and (prev_stance is null or prev_stance not in ('reduce', 'restore'))
