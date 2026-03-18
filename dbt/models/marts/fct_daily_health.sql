{{
    config(
        materialized='table'
    )
}}

select
    day,

    -- scores
    sleep_score,
    activity_score,
    readiness_score,

    -- sleep detail
    round(total_sleep_duration / 3600.0, 2) as total_sleep_hours,
    round(deep_sleep_duration / 3600.0, 2) as deep_sleep_hours,
    round(rem_sleep_duration / 3600.0, 2) as rem_sleep_hours,
    sleep_efficiency,
    sleep_avg_hr,
    sleep_avg_hrv,
    sleep_lowest_hr,
    bedtime_start,
    bedtime_end,

    -- activity
    steps,
    active_calories,
    total_calories,
    round(equivalent_walking_distance / 1000.0, 2) as walking_distance_km,
    round(high_activity_time / 60.0, 0) as high_activity_minutes,
    round(medium_activity_time / 60.0, 0) as medium_activity_minutes,
    round(sedentary_time / 60.0, 0) as sedentary_minutes,

    -- readiness
    temperature_deviation,
    contributor_hrv_balance,
    contributor_resting_heart_rate,

    -- stress
    stress_high,
    recovery_high,
    stress_summary,

    -- workouts
    workout_count

from {{ ref('int_oura__daily') }}

