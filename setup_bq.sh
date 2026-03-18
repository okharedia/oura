#!/usr/bin/env bash
# One-time setup: creates BigQuery dataset, tables, and a service account for GH Actions.
set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-oura-sync}"
DATASET="${GCP_BQ_DATASET:-oura_eu}"
SA_NAME="oura-sync"
SA="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
KEY_FILE="gcp-sa-key.json"

echo "=== Oura → BigQuery Setup ==="
echo "Project: $PROJECT_ID"
echo ""

# Enable required APIs
echo "→ Enabling APIs..."
gcloud services enable bigquery.googleapis.com --project "$PROJECT_ID"

# Create service account
echo "→ Creating service account..."
gcloud iam service-accounts create "$SA_NAME" \
    --display-name "Oura Sync (GitHub Actions)" \
    --project "$PROJECT_ID" 2>/dev/null || echo "  (already exists)"

for role in roles/bigquery.dataEditor roles/bigquery.jobUser; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member "serviceAccount:${SA}" \
        --role "$role" --quiet > /dev/null
done
echo "  Service account: $SA"

# Download key for GH Actions secret
echo "→ Generating service account key..."
gcloud iam service-accounts keys create "$KEY_FILE" \
    --iam-account "$SA" --project "$PROJECT_ID"
echo "  Key written to: $KEY_FILE"
echo "  Run: gh secret set GCP_SA_JSON < $KEY_FILE"
echo "  Then delete the local key file."

# Create BigQuery dataset and tables
echo "→ Creating BigQuery dataset and tables..."
bq mk --dataset --location europe-west3 --project_id "$PROJECT_ID" "$DATASET" 2>/dev/null || echo "  (dataset exists)"

python3 -c "
from google.cloud import bigquery
import schemas

client = bigquery.Client(project='$PROJECT_ID')
dataset_ref = '$PROJECT_ID.$DATASET'

table = bigquery.Table(f'{dataset_ref}.sync_state', schema=schemas.SYNC_STATE_SCHEMA)
try:
    client.create_table(table)
    print('  Created: sync_state')
except Exception as e:
    print('  Exists:  sync_state') if 'Already Exists' in str(e) else (_ for _ in ()).throw(e)

for name, schema in schemas.SCHEMAS.items():
    table = bigquery.Table(f'{dataset_ref}.{name}', schema=schema)
    part_field = schemas.PARTITION_FIELDS.get(name, 'day')
    table.time_partitioning = bigquery.TimePartitioning(field=part_field)
    try:
        client.create_table(table)
        print(f'  Created: {name} (partitioned by {part_field})')
    except Exception as e:
        print(f'  Exists:  {name}') if 'Already Exists' in str(e) else (_ for _ in ()).throw(e)
"

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  gh secret set GCP_SA_JSON < $KEY_FILE   # upload key"
echo "  rm $KEY_FILE                              # delete local copy"
echo "  gh variable set GCP_PROJECT_ID --body $PROJECT_ID"
