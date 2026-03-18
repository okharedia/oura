select
    day,
    score as activity_score,
    active_calories,
    total_calories,
    steps,
    equivalent_walking_distance,
    high_activity_time,
    medium_activity_time,
    low_activity_time,
    sedentary_time,
    resting_time,
    high_activity_met_minutes,
    medium_activity_met_minutes,
    low_activity_met_minutes,
    inactivity_alerts
from {{ source('oura', 'daily_activity') }}
