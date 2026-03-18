select
    day,
    stress_high,
    recovery_high,
    day_summary as stress_summary
from {{ source('oura', 'daily_stress') }}
