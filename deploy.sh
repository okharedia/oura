#!/usr/bin/env bash
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:-oura-sync}"
REGION="us-central1"
DATASET="oura"
FUNCTION_NAME="oura-sync"
SCHEDULER_JOB="oura-sync-schedule"
SERVICE_ACCOUNT="${FUNCTION_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Oura → BigQuery Pipeline Deployment ==="
echo "Project: $PROJECT_ID  Region: $REGION"
echo ""

# ── Step 1: Enable APIs ──────────────────────────────────────────────────
echo "→ Enabling APIs..."
gcloud services enable \
    cloudfunctions.googleapis.com \
    cloudscheduler.googleapis.com \
    secretmanager.googleapis.com \
    bigquery.googleapis.com \
    cloudbuild.googleapis.com \
    run.googleapis.com \
    --project "$PROJECT_ID"

# ── Step 2: Create service account ───────────────────────────────────────
echo "→ Creating service account..."
gcloud iam service-accounts create "$FUNCTION_NAME" \
    --display-name "Oura Sync Function" \
    --project "$PROJECT_ID" 2>/dev/null || echo "  (already exists)"

# Grant BigQuery & Secret Manager access
for role in roles/bigquery.dataEditor roles/bigquery.jobUser roles/secretmanager.secretAccessor roles/secretmanager.secretVersionAdder; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member "serviceAccount:${SERVICE_ACCOUNT}" \
        --role "$role" \
        --quiet > /dev/null
done
echo "  Service account: $SERVICE_ACCOUNT"

# ── Step 3: Create BigQuery dataset + tables ─────────────────────────────
echo "→ Creating BigQuery dataset..."
bq mk --dataset --project_id "$PROJECT_ID" "$DATASET" 2>/dev/null || echo "  (dataset exists)"

echo "→ Creating BigQuery tables..."
python3 -c "
from google.cloud import bigquery
import schemas

client = bigquery.Client(project='$PROJECT_ID')
dataset_ref = f'$PROJECT_ID.$DATASET'

# Create sync_state table
table_id = f'{dataset_ref}.sync_state'
table = bigquery.Table(table_id, schema=schemas.SYNC_STATE_SCHEMA)
try:
    client.create_table(table)
    print(f'  Created: sync_state')
except Exception as e:
    if 'Already Exists' in str(e):
        print(f'  Exists:  sync_state')
    else:
        raise

# Create data tables
for name, schema in schemas.SCHEMAS.items():
    table_id = f'{dataset_ref}.{name}'
    table = bigquery.Table(table_id, schema=schema)

    # Partitioning
    part_field = schemas.PARTITION_FIELDS.get(name, 'day')
    table.time_partitioning = bigquery.TimePartitioning(field=part_field)

    try:
        client.create_table(table)
        print(f'  Created: {name} (partitioned by {part_field})')
    except Exception as e:
        if 'Already Exists' in str(e):
            print(f'  Exists:  {name}')
        else:
            raise
"

# ── Step 4: Deploy Cloud Function ────────────────────────────────────────
echo "→ Deploying Cloud Function..."
gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --runtime python312 \
    --trigger-http \
    --entry-point entry_point \
    --source . \
    --memory 256MB \
    --timeout 300s \
    --region "$REGION" \
    --project "$PROJECT_ID" \
    --service-account "$SERVICE_ACCOUNT" \
    --no-allow-unauthenticated \
    --set-env-vars "GCP_PROJECT_ID=$PROJECT_ID"

# Get the function URL
FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 --region "$REGION" --project "$PROJECT_ID" \
    --format 'value(serviceConfig.uri)')
echo "  URL: $FUNCTION_URL"

# ── Step 5: Create Cloud Scheduler job ───────────────────────────────────
echo "→ Creating Cloud Scheduler job..."
gcloud scheduler jobs delete "$SCHEDULER_JOB" \
    --location "$REGION" --project "$PROJECT_ID" --quiet 2>/dev/null || true

gcloud scheduler jobs create http "$SCHEDULER_JOB" \
    --schedule "0 */6 * * *" \
    --uri "$FUNCTION_URL" \
    --http-method POST \
    --location "$REGION" \
    --project "$PROJECT_ID" \
    --oidc-service-account-email "$SERVICE_ACCOUNT" \
    --oidc-token-audience "$FUNCTION_URL"

# ── Done ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Deployment complete ==="
echo "  Function:  $FUNCTION_URL"
echo "  Schedule:  Every 6 hours (0 */6 * * *)"
echo ""
echo "To trigger manually:"
echo "  gcloud functions call $FUNCTION_NAME --gen2 --region $REGION --project $PROJECT_ID"
echo ""
echo "To check logs:"
echo "  gcloud functions logs read $FUNCTION_NAME --gen2 --region $REGION --project $PROJECT_ID"
