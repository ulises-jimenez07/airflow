"""Wait (deferrably) for a partner file in GCS, then load it into BigQuery.

Uses Variables gcs_bucket and bq_dataset (set by 10-bigquery-pipeline/bq-setup.sh).
Drop a file at gs://<bucket>/data/incoming/orders_<YYYYMMDD>.csv to release the sensor.
"""

from datetime import datetime, timedelta

from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import DAG

BUCKET = "{{ var.value.gcs_bucket }}"
DATASET = "{{ var.value.get('bq_dataset', 'airflow_lab') }}"
OBJECT = "data/incoming/orders_{{ ds_nodash }}.csv"

with DAG(
    dag_id="gcs_sensor_dag",
    start_date=datetime(2025, 1, 1),
    schedule="0 22 * * *",  # start waiting at 22:00 UTC
    catchup=False,
    tags=["16", "sensors", "gcp"],
):
    wait_for_partner_file = GCSObjectExistenceSensor(
        task_id="wait_for_partner_file",
        bucket=BUCKET,
        object=OBJECT,
        deferrable=True,  # waits in the triggerer, no worker slot used
        poke_interval=300,
        timeout=int(timedelta(hours=4).total_seconds()),
    )

    load_orders = GCSToBigQueryOperator(
        task_id="load_orders",
        bucket=BUCKET,
        source_objects=[OBJECT],
        destination_project_dataset_table=DATASET + ".orders_{{ ds_nodash }}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    wait_for_partner_file >> load_orders
