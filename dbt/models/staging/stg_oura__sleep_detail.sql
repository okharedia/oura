select
    day,
    type as sleep_type,
    bedtime_start,
    bedtime_end,
    total_sleep_duration,
    deep_sleep_duration,
    rem_sleep_duration,
    light_sleep_duration,
    awake_time,
    time_in_bed,
    efficiency as sleep_efficiency,
    latency as sleep_latency,
    average_heart_rate as sleep_avg_hr,
    average_hrv as sleep_avg_hrv,
    lowest_heart_rate as sleep_lowest_hr,
    average_breath as sleep_avg_breath,
    restless_periods
from {{ source('oura', 'sleep') }}
where type = 'long_sleep'
