#!/usr/bin/env bash
# Source this file to get the shared settings used by every script in this repo:
#   source 03-composer-environment/scripts/env.sh
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
export REGION="${REGION:-us-central1}"
export ENV_NAME="${ENV_NAME:-airflow-lab}"

# Upload one DAG file:   upload_dag 05-dag-structure/hello_dag.py
upload_dag() {
  gcloud composer environments storage dags import \
    --environment "$ENV_NAME" --location "$REGION" --source "$1"
}

# Upload into the data/ folder:   upload_data 08-bash-scripts/scripts/greet.py scripts
upload_data() {
  local dest="${2:-}"
  gcloud composer environments storage data import \
    --environment "$ENV_NAME" --location "$REGION" --source "$1" \
    ${dest:+--destination "$dest"}
}

# Run any airflow CLI command inside the environment:   airflow_cli dags list
airflow_cli() {
  local sub="$1"; shift
  gcloud composer environments run "$ENV_NAME" --location "$REGION" "$sub" -- "$@"
}
