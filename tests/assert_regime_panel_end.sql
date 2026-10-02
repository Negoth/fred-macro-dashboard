-- The panel must not end on a month whose rate observations have not printed yet.
--
-- assert_regime_month_spine guards the other direction: it checks the panel has no gaps, and a
-- panel that runs one month too far is still gapless, so it cannot catch this. The spine end is
-- taken from the rate legs for exactly this reason (design note 6.11) -- the equities leg
-- publishes a day earlier, and on the first day of a month it used to pull in a row holding
-- nothing but the S&P 500, where every rate-derived flag then read "off" instead of "unknown".
--
-- A month still in progress is fine and expected: one printed day is enough to make the mean
-- non-null. What is not allowed is a trailing month with no rate observations at all.
-- Any row returned is a failure.

select month, ff, y10, baa
from {{ ref('fct_regime') }}
where month = (select max(month) from {{ ref('fct_regime') }})
  and (ff is null or y10 is null or baa is null)
