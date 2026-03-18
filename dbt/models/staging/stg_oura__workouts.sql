select
    id as workout_id,
    day,
    activity as workout_type,
    label as workout_label,
    intensity,
    start_datetime,
    end_datetime,
    calories as workout_calories,
    distance as workout_distance,
    source as workout_source
from {{ source('oura', 'workout') }}
