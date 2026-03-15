"""BigQuery table schemas and field mappings for Oura data types."""

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

F = bigquery.SchemaField

def _ts():
    return F("_synced_at", "TIMESTAMP", mode="REQUIRED")

# ---------------------------------------------------------------------------
# Schemas  (one dict entry per Oura data type)
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "daily_sleep": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("score", "INTEGER"),
        F("timestamp", "TIMESTAMP"),
        F("contributor_deep_sleep", "INTEGER"),
        F("contributor_efficiency", "INTEGER"),
        F("contributor_latency", "INTEGER"),
        F("contributor_rem_sleep", "INTEGER"),
        F("contributor_restfulness", "INTEGER"),
        F("contributor_timing", "INTEGER"),
        F("contributor_total_sleep", "INTEGER"),
        _ts(),
    ],
    "sleep": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("average_breath", "FLOAT"),
        F("average_heart_rate", "FLOAT"),
        F("average_hrv", "INTEGER"),
        F("awake_time", "INTEGER"),
        F("bedtime_end", "TIMESTAMP"),
        F("bedtime_start", "TIMESTAMP"),
        F("deep_sleep_duration", "INTEGER"),
        F("efficiency", "INTEGER"),
        F("latency", "INTEGER"),
        F("light_sleep_duration", "INTEGER"),
        F("low_battery_alert", "BOOLEAN"),
        F("lowest_heart_rate", "INTEGER"),
        F("movement_30_sec", "STRING"),
        F("period", "INTEGER"),
        F("rem_sleep_duration", "INTEGER"),
        F("restless_periods", "INTEGER"),
        F("sleep_phase_5_min", "STRING"),
        F("sleep_algorithm_version", "STRING"),
        F("sleep_analysis_reason", "STRING"),
        F("time_in_bed", "INTEGER"),
        F("total_sleep_duration", "INTEGER"),
        F("type", "STRING"),
        F("sleep_score_delta", "INTEGER"),
        F("readiness_score_delta", "INTEGER"),
        # Nested SampleModel fields stored as JSON
        F("heart_rate", "STRING"),  # JSON
        F("hrv", "STRING"),  # JSON
        # Nested readiness summary
        F("readiness_score", "INTEGER"),
        F("readiness_temperature_deviation", "FLOAT"),
        F("readiness_temperature_trend_deviation", "FLOAT"),
        _ts(),
    ],
    "daily_activity": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("score", "INTEGER"),
        F("active_calories", "INTEGER"),
        F("average_met_minutes", "FLOAT"),
        F("equivalent_walking_distance", "INTEGER"),
        F("high_activity_met_minutes", "INTEGER"),
        F("high_activity_time", "INTEGER"),
        F("inactivity_alerts", "INTEGER"),
        F("low_activity_met_minutes", "INTEGER"),
        F("low_activity_time", "INTEGER"),
        F("medium_activity_met_minutes", "INTEGER"),
        F("medium_activity_time", "INTEGER"),
        F("meters_to_target", "INTEGER"),
        F("non_wear_time", "INTEGER"),
        F("resting_time", "INTEGER"),
        F("sedentary_met_minutes", "INTEGER"),
        F("sedentary_time", "INTEGER"),
        F("steps", "INTEGER"),
        F("target_calories", "INTEGER"),
        F("target_meters", "INTEGER"),
        F("total_calories", "INTEGER"),
        F("timestamp", "TIMESTAMP"),
        F("class_5_min", "STRING"),
        F("met", "STRING"),  # JSON (SampleModel)
        F("contributor_meet_daily_targets", "INTEGER"),
        F("contributor_move_every_hour", "INTEGER"),
        F("contributor_recovery_time", "INTEGER"),
        F("contributor_stay_active", "INTEGER"),
        F("contributor_training_frequency", "INTEGER"),
        F("contributor_training_volume", "INTEGER"),
        _ts(),
    ],
    "daily_readiness": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("score", "INTEGER"),
        F("temperature_deviation", "FLOAT"),
        F("temperature_trend_deviation", "FLOAT"),
        F("timestamp", "TIMESTAMP"),
        F("contributor_activity_balance", "INTEGER"),
        F("contributor_body_temperature", "INTEGER"),
        F("contributor_hrv_balance", "INTEGER"),
        F("contributor_previous_day_activity", "INTEGER"),
        F("contributor_previous_night", "INTEGER"),
        F("contributor_recovery_index", "INTEGER"),
        F("contributor_resting_heart_rate", "INTEGER"),
        F("contributor_sleep_balance", "INTEGER"),
        F("contributor_sleep_regularity", "INTEGER"),
        _ts(),
    ],
    "daily_stress": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("stress_high", "INTEGER"),
        F("recovery_high", "INTEGER"),
        F("day_summary", "STRING"),
        _ts(),
    ],
    "heartrate": [
        F("bpm", "INTEGER", mode="REQUIRED"),
        F("source", "STRING", mode="REQUIRED"),
        F("timestamp", "TIMESTAMP", mode="REQUIRED"),
        _ts(),
    ],
    "daily_spo2": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("spo2_percentage_average", "FLOAT"),
        F("breathing_disturbance_index", "INTEGER"),
        _ts(),
    ],
    "daily_resilience": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("level", "STRING"),
        F("contributor_sleep_recovery", "FLOAT"),
        F("contributor_daytime_recovery", "FLOAT"),
        F("contributor_stress", "FLOAT"),
        _ts(),
    ],
    "daily_cardiovascular_age": [
        F("day", "DATE", mode="REQUIRED"),
        F("vascular_age", "INTEGER"),
        _ts(),
    ],
    "vo2_max": [
        F("id", "STRING", mode="REQUIRED"),
        F("day", "DATE"),
        F("timestamp", "TIMESTAMP"),
        F("vo2_max", "FLOAT"),
        _ts(),
    ],
    "workout": [
        F("id", "STRING", mode="REQUIRED"),
        F("activity", "STRING"),
        F("calories", "FLOAT"),
        F("day", "DATE"),
        F("distance", "FLOAT"),
        F("end_datetime", "TIMESTAMP"),
        F("intensity", "STRING"),
        F("label", "STRING"),
        F("source", "STRING"),
        F("start_datetime", "TIMESTAMP"),
        _ts(),
    ],
}

# ---------------------------------------------------------------------------
# API endpoint paths & config per data type
# ---------------------------------------------------------------------------

DATA_TYPES: dict[str, dict] = {
    "daily_sleep":              {"path": "daily_sleep",              "key": "id",  "date_param": "date"},
    "sleep":                    {"path": "sleep",                    "key": "id",  "date_param": "date"},
    "daily_activity":           {"path": "daily_activity",           "key": "id",  "date_param": "date"},
    "daily_readiness":          {"path": "daily_readiness",          "key": "id",  "date_param": "date"},
    "daily_stress":             {"path": "daily_stress",             "key": "id",  "date_param": "date"},
    "heartrate":                {"path": "heartrate",                "key": None,  "date_param": "datetime"},
    "daily_spo2":               {"path": "daily_spo2",              "key": "id",  "date_param": "date"},
    "daily_resilience":         {"path": "daily_resilience",         "key": "id",  "date_param": "date"},
    "daily_cardiovascular_age": {"path": "daily_cardiovascular_age", "key": None,  "date_param": "date"},

    "workout":                  {"path": "workout",                  "key": "id",  "date_param": "date"},
}

# Partition config for BigQuery tables
PARTITION_FIELDS: dict[str, str] = {
    "heartrate": "timestamp",
}
# All other tables partition by "day" (default)

# ---------------------------------------------------------------------------
# Sync-state table schema
# ---------------------------------------------------------------------------

SYNC_STATE_SCHEMA = [
    F("data_type", "STRING", mode="REQUIRED"),
    F("last_sync_date", "DATE"),
    F("last_sync_at", "TIMESTAMP"),
    F("status", "STRING"),
    F("records_synced", "INTEGER"),
]
