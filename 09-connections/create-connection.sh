#!/usr/bin/env bash
# Create the custom GCP connection "my_gcp_conn" inside the Composer environment.
# Optional: IMPERSONATE_SA=bq-reader@PROJECT.iam.gserviceaccount.com to act as another SA
# (the Composer SA needs roles/iam.serviceAccountTokenCreator on it).
set -euo pipefail
source "$(dirname "$0")/../03-composer-environment/scripts/env.sh"

EXTRA="{\"project\": \"${PROJECT_ID}\""
if [[ -n "${IMPERSONATE_SA:-}" ]]; then
  EXTRA+=", \"impersonation_chain\": \"${IMPERSONATE_SA}\""
fi
EXTRA+="}"

airflow_cli connections add my_gcp_conn \
  --conn-type google_cloud_platform \
  --conn-extra "$EXTRA"

airflow_cli connections get my_gcp_conn
