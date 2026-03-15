"""Cloud Function: Oura Ring → BigQuery sync.

Polls all configured Oura API v2 endpoints and upserts data into BigQuery.
Triggered by Cloud Scheduler every 6 hours.
"""

import json
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import requests
from google.cloud import bigquery, secretmanager

import schemas

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("oura-sync")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "oura-sync")
DATASET = "oura"
BASE_URL = "https://api.ouraring.com/v2/usercollection"
DEFAULT_BACKFILL_DAYS = 730  # ~2 years


# ── Secret Manager helpers ────────────────────────────────────────────────

_sm_client: secretmanager.SecretManagerServiceClient | None = None

def _sm():
    global _sm_client
    if _sm_client is None:
        _sm_client = secretmanager.SecretManagerServiceClient()
    return _sm_client

def _secret_path(name: str) -> str:
    return f"projects/{PROJECT_ID}/secrets/{name}/versions/latest"

def get_secret(name: str) -> str:
    resp = _sm().access_secret_version(request={"name": _secret_path(name)})
    return resp.payload.data.decode("utf-8")

def set_secret(name: str, value: str):
    """Add a new version of an existing secret."""
    parent = f"projects/{PROJECT_ID}/secrets/{name}"
    _sm().add_secret_version(
        request={"parent": parent, "payload": {"data": value.encode("utf-8")}}
    )


# ── OAuth token management ────────────────────────────────────────────────

def get_tokens() -> tuple[str, str]:
    return get_secret("oura-access-token"), get_secret("oura-refresh-token")

def refresh_tokens(refresh_token: str) -> tuple[str, str]:
    """Exchange a refresh token for a new access/refresh pair."""
    client_id = get_secret("oura-client-id")
    client_secret = get_secret("oura-client-secret")
    resp = requests.post(
        "https://api.ouraring.com/oauth/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    new_access = data["access_token"]
    new_refresh = data["refresh_token"]
    set_secret("oura-access-token", new_access)
    set_secret("oura-refresh-token", new_refresh)
    log.info("Tokens refreshed successfully")
    return new_access, new_refresh


# ── BigQuery helpers ──────────────────────────────────────────────────────

_bq_client: bigquery.Client | None = None

def _bq():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=PROJECT_ID)
    return _bq_client

def get_last_sync_date(data_type: str) -> date | None:
    query = f"""
        SELECT last_sync_date FROM `{DATASET}.sync_state`
        WHERE data_type = @dt
    """
    job = _bq().query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("dt", "STRING", data_type)]
        ),
    )
    rows = list(job.result())
    return rows[0].last_sync_date if rows else None

def update_sync_state(data_type: str, sync_date: date, count: int, status: str):
    table = f"{DATASET}.sync_state"
    rows = [{
        "data_type": data_type,
        "last_sync_date": sync_date.isoformat(),
        "last_sync_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "records_synced": count,
    }]
    # Use MERGE via DML for upsert
    tmp = f"_tmp_{data_type}_{int(time.time())}"
    _bq().load_table_from_json(rows, f"{DATASET}.{tmp}",
        job_config=bigquery.LoadJobConfig(schema=schemas.SYNC_STATE_SCHEMA,
                                           write_disposition="WRITE_TRUNCATE")).result()
    merge = f"""
        MERGE `{table}` T USING `{DATASET}.{tmp}` S
        ON T.data_type = S.data_type
        WHEN MATCHED THEN UPDATE SET
            last_sync_date = S.last_sync_date,
            last_sync_at = S.last_sync_at,
            status = S.status,
            records_synced = S.records_synced
        WHEN NOT MATCHED THEN INSERT ROW
    """
    _bq().query(merge).result()
    _bq().delete_table(f"{DATASET}.{tmp}", not_found_ok=True)


# ── Oura API fetching ────────────────────────────────────────────────────

def fetch_oura(path: str, access_token: str, params: dict) -> list[dict]:
    """Fetch all pages from an Oura API endpoint."""
    headers = {"Authorization": f"Bearer {access_token}"}
    all_data: list[dict] = []
    url = f"{BASE_URL}/{path}"

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        all_data.extend(body.get("data", []))

        next_token = body.get("next_token")
        if not next_token:
            break
        params = {"next_token": next_token}

    return all_data


# ── Row transformation ────────────────────────────────────────────────────

def _flatten_contributors(row: dict, prefix: str = "contributor_") -> dict:
    """Move contributors.* to contributor_* top-level keys."""
    contributors = row.pop("contributors", None) or {}
    for k, v in contributors.items():
        row[f"{prefix}{k}"] = v
    return row

def _json_field(row: dict, field: str) -> dict:
    """Convert a complex nested field to a JSON string."""
    val = row.get(field)
    if val is not None and not isinstance(val, str):
        row[field] = json.dumps(val)
    return row

def transform_row(data_type: str, raw: dict) -> dict:
    """Transform a raw API row into a flat dict matching the BQ schema."""
    row = dict(raw)  # shallow copy
    now = datetime.now(timezone.utc).isoformat()

    if data_type == "daily_sleep":
        row = _flatten_contributors(row)

    elif data_type == "sleep":
        # Flatten readiness sub-object
        readiness = row.pop("readiness", None) or {}
        row["readiness_score"] = readiness.get("score")
        row["readiness_temperature_deviation"] = readiness.get("temperature_deviation")
        row["readiness_temperature_trend_deviation"] = readiness.get("temperature_trend_deviation")
        # Store time-series as JSON
        row = _json_field(row, "heart_rate")
        row = _json_field(row, "hrv")

    elif data_type == "daily_activity":
        row = _flatten_contributors(row)
        row = _json_field(row, "met")

    elif data_type == "daily_readiness":
        row = _flatten_contributors(row)

    elif data_type == "daily_spo2":
        spo2 = row.pop("spo2_percentage", None) or {}
        row["spo2_percentage_average"] = spo2.get("average")

    elif data_type == "daily_resilience":
        row = _flatten_contributors(row)

    # Strip fields not in schema
    schema_fields = {f.name for f in schemas.SCHEMAS[data_type]}
    row = {k: v for k, v in row.items() if k in schema_fields}
    row["_synced_at"] = now
    return row


# ── Upsert into BigQuery ─────────────────────────────────────────────────

def upsert(data_type: str, rows: list[dict]):
    if not rows:
        return

    table_id = f"{DATASET}.{data_type}"
    cfg = schemas.DATA_TYPES[data_type]
    schema = schemas.SCHEMAS[data_type]

    # Load into temp table
    tmp = f"{DATASET}._stg_{data_type}_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    _bq().load_table_from_json(rows, tmp, job_config=job_config).result()

    # Build MERGE statement
    key = cfg["key"]
    if key:
        on_clause = f"T.{key} = S.{key}"
    elif data_type == "heartrate":
        on_clause = "T.timestamp = S.timestamp AND T.source = S.source"
    elif data_type == "daily_cardiovascular_age":
        on_clause = "T.day = S.day"
    else:
        on_clause = "FALSE"  # fallback: always insert

    update_cols = [f.name for f in schema if f.name != key and f.name not in ("timestamp", "source") or key]
    set_clause = ", ".join(f"T.{c} = S.{c}" for c in [f.name for f in schema])

    merge_sql = f"""
        MERGE `{table_id}` T USING `{tmp}` S
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ROW
    """
    _bq().query(merge_sql).result()
    _bq().delete_table(tmp, not_found_ok=True)
    log.info(f"{data_type}: upserted {len(rows)} rows")


# ── Main sync logic ──────────────────────────────────────────────────────

HEARTRATE_MAX_DAYS = 30  # Oura limits heartrate queries to 30 days


def sync_data_type(data_type: str, access_token: str) -> int:
    """Sync a single data type. Returns number of records synced."""
    cfg = schemas.DATA_TYPES[data_type]

    last_sync = get_last_sync_date(data_type)
    if last_sync:
        start = last_sync - timedelta(days=1)  # 1-day overlap
    else:
        start = date.today() - timedelta(days=DEFAULT_BACKFILL_DAYS)

    end = date.today()

    # Heartrate needs chunked requests (max 30 days per call)
    if cfg["date_param"] == "datetime":
        raw_rows = []
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=HEARTRATE_MAX_DAYS), end)
            params = {
                "start_datetime": f"{chunk_start}T00:00:00+00:00",
                "end_datetime": f"{chunk_end}T23:59:59+00:00",
            }
            raw_rows.extend(fetch_oura(cfg["path"], access_token, params))
            chunk_start = chunk_end
    else:
        params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
        raw_rows = fetch_oura(cfg["path"], access_token, params)
    if not raw_rows:
        log.info(f"{data_type}: no new data")
        return 0

    rows = [transform_row(data_type, r) for r in raw_rows]
    upsert(data_type, rows)
    update_sync_state(data_type, end, len(rows), "ok")
    return len(rows)


def sync_all():
    """Sync all data types, handling token refresh on 401."""
    access_token, refresh_token = get_tokens()
    results = {}
    sync_all._refreshed = False

    for data_type in schemas.DATA_TYPES:
        try:
            count = sync_data_type(data_type, access_token)
            results[data_type] = {"status": "ok", "records": count}
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                # Only refresh tokens once per sync_all run
                if not getattr(sync_all, "_refreshed", False):
                    log.warning(f"{data_type}: 401 — refreshing tokens")
                    access_token, refresh_token = refresh_tokens(refresh_token)
                    sync_all._refreshed = True
                    try:
                        count = sync_data_type(data_type, access_token)
                        results[data_type] = {"status": "ok", "records": count}
                        continue
                    except Exception as retry_err:
                        log.warning(f"{data_type}: still 401 after refresh — likely not available for this account")
                        results[data_type] = {"status": "skipped", "error": str(retry_err)}
                else:
                    log.warning(f"{data_type}: 401 — skipping (tokens already refreshed)")
                    results[data_type] = {"status": "skipped", "error": "unauthorized"}
            else:
                log.error(f"{data_type}: HTTP {e.response.status_code if e.response else '?'}: {e}")
                results[data_type] = {"status": "error", "error": str(e)}
                update_sync_state(data_type, date.today(), 0, "error")
        except Exception as e:
            log.error(f"{data_type}: {e}")
            results[data_type] = {"status": "error", "error": str(e)}
            update_sync_state(data_type, date.today(), 0, "error")

    return results


# ── Cloud Function entry point ────────────────────────────────────────────

def entry_point(request=None):
    """HTTP Cloud Function entry point."""
    results = sync_all()
    ok = sum(1 for r in results.values() if r["status"] == "ok")
    err = sum(1 for r in results.values() if r["status"] == "error")
    log.info(f"Sync complete: {ok} ok, {err} errors")
    return {"status": "complete", "ok": ok, "errors": err, "details": results}, 200


if __name__ == "__main__":
    # For local testing
    print(json.dumps(entry_point()[0], indent=2))
