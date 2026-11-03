#!/usr/bin/env bash
# Delete everything created by this repo's lab: environment, bucket, dataset, service account.
# Asks before each step.
set -euo pipefail
source "$(dirname "$0")/../03-composer-environment/scripts/env.sh"

DATASET="${DATASET:-airflow_lab}"
SA_EMAIL="${SA_NAME:-composer-lab-sa}@${PROJECT_ID}.iam.gserviceaccount.com"

confirm() { read -r -p "$1 [y/N] " ans; [[ "$ans" == "y" || "$ans" == "Y" ]]; }

BUCKET=""
if gcloud composer environments describe "$ENV_NAME" --location "$REGION" >/dev/null 2>&1; then
  BUCKET=$(gcloud composer environments describe "$ENV_NAME" --location "$REGION" \
    --format="value(config.dagGcsPrefix)" | sed -E 's#gs://([^/]+)/.*#\1#')
  if confirm "Delete Composer environment ${ENV_NAME} in ${REGION}?"; then
    gcloud composer environments delete "$ENV_NAME" --location "$REGION" --quiet
  fi
else
  echo "Environment ${ENV_NAME} not found, skipping."
fi

if [[ -n "$BUCKET" ]] && confirm "Delete bucket gs://${BUCKET} and all its contents?"; then
  gcloud storage rm --recursive "gs://${BUCKET}"
fi

if bq show --dataset "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1 \
  && confirm "Delete BigQuery dataset ${DATASET} and all its tables?"; then
  bq rm -r -f --dataset "${PROJECT_ID}:${DATASET}"
fi

if gcloud iam service-accounts describe "$SA_EMAIL" >/dev/null 2>&1 \
  && confirm "Delete service account ${SA_EMAIL}?"; then
  for role in roles/composer.worker roles/bigquery.admin roles/storage.objectAdmin; do
    gcloud projects remove-iam-policy-binding "$PROJECT_ID" \
      --member "serviceAccount:${SA_EMAIL}" --role "$role" --condition=None >/dev/null || true
  done
  gcloud iam service-accounts delete "$SA_EMAIL" --quiet
fi

echo "Cleanup finished."
