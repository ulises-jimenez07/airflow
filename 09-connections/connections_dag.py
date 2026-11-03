"""Same BigQuery query with: implicit default connection, explicit default, custom connection.

q_custom fails until the connection "my_gcp_conn" exists (see create-connection.sh).
Needs apache-airflow-providers-google (preinstalled in Composer).
"""

from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG

QUERY = {"query": {"query": "SELECT 1 AS ok", "useLegacySql": False}}

with DAG(
    dag_id="connections_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["09", "connections", "gcp"],
):
    # 1. No gcp_conn_id: uses google_cloud_default (environment service account).
    q_default = BigQueryInsertJobOperator(task_id="q_default", configuration=QUERY)

    # 2. Same connection, explicitly.
    q_explicit = BigQueryInsertJobOperator(
        task_id="q_explicit",
        configuration=QUERY,
        gcp_conn_id="google_cloud_default",
    )

    # 3. A connection you define yourself.
    q_custom = BigQueryInsertJobOperator(
        task_id="q_custom",
        configuration=QUERY,
        gcp_conn_id="my_gcp_conn",
    )
