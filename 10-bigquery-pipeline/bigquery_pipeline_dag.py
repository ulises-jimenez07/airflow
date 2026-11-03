"""ELT with BigQuery: load CSVs from GCS, join them in SQL, export the result back to GCS.

    GCS data/input/*.csv --> BQ customers, addresses --> BQ customer_addresses --> GCS data/output/*.csv

Run bq-setup.sh first (creates the dataset, uploads the CSVs, sets Variables
gcs_bucket, bq_dataset, gcp_project).
"""

from datetime import datetime

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import DAG

BUCKET = "{{ var.value.gcs_bucket }}"
DATASET = "{{ var.value.get('bq_dataset', 'airflow_lab') }}"

JOIN_SQL = f"""
SELECT
  c.customer_id,
  c.name,
  c.location,
  a.street,
  a.postal_code
FROM `{DATASET}.customers` AS c
LEFT JOIN `{DATASET}.addresses` AS a USING (customer_id)
ORDER BY c.customer_id
"""

with DAG(
    dag_id="bigquery_pipeline_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["10", "bigquery", "gcp"],
):
    load_customers = GCSToBigQueryOperator(
        task_id="load_customers",
        bucket=BUCKET,
        source_objects=["data/input/customers.csv"],
        destination_project_dataset_table=f"{DATASET}.customers",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING"},
            {"name": "location", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    load_addresses = GCSToBigQueryOperator(
        task_id="load_addresses",
        bucket=BUCKET,
        source_objects=["data/input/addresses.csv"],
        destination_project_dataset_table=f"{DATASET}.addresses",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    join_tables = BigQueryInsertJobOperator(
        task_id="join_tables",
        configuration={
            "query": {
                "query": JOIN_SQL,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": "{{ var.value.gcp_project }}",
                    "datasetId": DATASET,
                    "tableId": "customer_addresses",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    export_csv = BigQueryToGCSOperator(
        task_id="export_csv",
        source_project_dataset_table=f"{DATASET}.customer_addresses",
        destination_cloud_storage_uris=[f"gs://{BUCKET}/data/output/customer_addresses_{{{{ ds_nodash }}}}.csv"],
        export_format="CSV",
        print_header=True,
    )

    [load_customers, load_addresses] >> join_tables >> export_csv
