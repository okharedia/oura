-- Combines all daily Oura metrics into a single row per day

with sleep_scores as (
    select * from {{ ref('stg_oura__daily_sleep') }}
),

sleep_detail as (
    select * from {{ ref('stg_oura__sleep_detail') }}
),

activity as (
    select * from {{ ref('stg_oura__daily_activity') }}
),

readiness as (
    select * from {{ ref('stg_oura__daily_readiness') }}
),

stress as (
    select * from {{ ref('stg_oura__daily_stress') }}
),

workouts as (
    select
        day,
        count(*) as workout_count,
        sum(workout_calories) as total_workout_calories,
        sum(workout_distance) as total_workout_distance
    from {{ ref('stg_oura__workouts') }}
    group by day
)

select
    coalesce(s.day, a.day, r.day) as day,

    -- sleep scores
    s.sleep_score,
    s.contributor_deep_sleep,
    s.contributor_rem_sleep,
    s.contributor_restfulness,
    s.contributor_timing,

    -- sleep detail
    sd.total_sleep_duration,
    sd.deep_sleep_duration,
    sd.rem_sleep_duration,
    sd.light_sleep_duration,
    sd.awake_time,
    sd.time_in_bed,
    sd.sleep_efficiency,
    sd.sleep_avg_hr,
    sd.sleep_avg_hrv,
    sd.sleep_lowest_hr,
    sd.bedtime_start,
    sd.bedtime_end,

    -- activity
    a.activity_score,
    a.active_calories,
    a.total_calories,
    a.steps,
    a.equivalent_walking_distance,
    a.high_activity_time,
    a.medium_activity_time,
    a.low_activity_time,
    a.sedentary_time,

    -- readiness
    r.readiness_score,
    r.temperature_deviation,
    r.contributor_hrv_balance,
    r.contributor_resting_heart_rate,

    -- stress
    st.stress_high,
    st.recovery_high,
    st.stress_summary,

    -- workouts
    coalesce(w.workout_count, 0) as workout_count,
    w.total_workout_calories,
    w.total_workout_distance

from sleep_scores s
full outer join sleep_detail sd on s.day = sd.day
full outer join activity a on coalesce(s.day, sd.day) = a.day
full outer join readiness r on coalesce(s.day, sd.day, a.day) = r.day
full outer join stress st on coalesce(s.day, sd.day, a.day, r.day) = st.day
left join workouts w on coalesce(s.day, sd.day, a.day, r.day, st.day) = w.day
