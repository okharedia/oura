select
    day,
    score as sleep_score,
    contributor_deep_sleep,
    contributor_efficiency,
    contributor_latency,
    contributor_rem_sleep,
    contributor_restfulness,
    contributor_timing,
    contributor_total_sleep
from {{ source('oura', 'daily_sleep') }}
