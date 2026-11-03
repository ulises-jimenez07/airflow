#!/usr/bin/env bash
# Create a small Cloud Composer 3 environment for learning.
# Usage:
#   export PROJECT_ID=my-project REGION=us-central1 ENV_NAME=airflow-lab
#   ./create-env.sh
set -euo pipefail

: "${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-us-central1}"
ENV_NAME="${ENV_NAME:-airflow-lab}"
# Pick an Airflow 3 build from the Composer docs ("Composer versions" page),
# e.g. composer-3-airflow-3.1.0-build.N. The DAGs in this repo use Airflow 3 imports.
IMAGE_VERSION="${IMAGE_VERSION:?set IMAGE_VERSION to a composer-3-airflow-3.x build}"
SA_NAME="${SA_NAME:-composer-lab-sa}"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT_ID"

echo "Enabling APIs..."
gcloud services enable \
  composer.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com

echo "Creating service account ${SA_EMAIL} (if missing)..."
if ! gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1; then
  gcloud iam service-accounts create "$SA_NAME" --display-name "Composer lab"
fi

for role in roles/composer.worker roles/bigquery.admin roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member "serviceAccount:${SA_EMAIL}" --role "$role" --condition=None >/dev/null
done

echo "Creating environment ${ENV_NAME} (20-30 min)..."
gcloud composer environments create "$ENV_NAME" \
  --location "$REGION" \
  --image-version "$IMAGE_VERSION" \
  --service-account "$SA_EMAIL" \
  --environment-size small

echo
echo "Airflow UI:"
gcloud composer environments describe "$ENV_NAME" --location "$REGION" \
  --format="value(config.airflowUri)"
echo "DAG folder:"
gcloud composer environments describe "$ENV_NAME" --location "$REGION" \
  --format="value(config.dagGcsPrefix)"
