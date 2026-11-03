#!/usr/bin/env bash
# Prepare BigQuery + the Composer bucket for bigquery_pipeline_dag.py:
#  - dataset "airflow_lab"
#  - products table loaded directly with bq load (manual load, for comparison)
#  - customers.csv / addresses.csv copied to the bucket's data/input/ (the DAG loads them)
#  - Variables gcs_bucket (environment bucket), bq_dataset, gcp_project
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
source "$HERE/../03-composer-environment/scripts/env.sh"

DATASET="${DATASET:-airflow_lab}"
BQ_LOCATION="${BQ_LOCATION:-US}"

DAG_PREFIX=$(gcloud composer environments describe "$ENV_NAME" --location "$REGION" \
  --format="value(config.dagGcsPrefix)")          # gs://<bucket>/dags
BUCKET=$(echo "$DAG_PREFIX" | sed -E 's#gs://([^/]+)/.*#\1#')
echo "Environment bucket: $BUCKET"

echo "Creating dataset ${PROJECT_ID}:${DATASET} (if missing)..."
bq show --dataset "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1 \
  || bq --location="$BQ_LOCATION" mk --dataset "${PROJECT_ID}:${DATASET}"

echo "Loading products table manually..."
bq load --replace --source_format=CSV --skip_leading_rows=1 \
  "${DATASET}.products" "$HERE/data/products.csv" \
  product_id:INT64,name:STRING,unit_of_measure:STRING

echo "Uploading input CSVs to gs://${BUCKET}/data/input/ ..."
gcloud storage cp "$HERE/data/customers.csv" "$HERE/data/addresses.csv" "gs://${BUCKET}/data/input/"

echo "Setting Airflow Variable gcs_bucket=${BUCKET} ..."
airflow_cli variables set gcs_bucket "$BUCKET"
airflow_cli variables set bq_dataset "$DATASET"
airflow_cli variables set gcp_project "$PROJECT_ID"

echo "Done. Now: upload_dag 10-bigquery-pipeline/bigquery_pipeline_dag.py"
