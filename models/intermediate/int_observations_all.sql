-- Bundles the raw and derived series into one stream. Every downstream transform and
-- z-score reads this.

select series_id, observation_date, value
from {{ ref('stg_fred_observations') }}
where value is not null

union all

select series_id, observation_date, value
from {{ ref('int_derived_series') }}
