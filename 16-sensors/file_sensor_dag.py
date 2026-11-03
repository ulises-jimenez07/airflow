"""Wait for a local file to arrive, then process it. Runs on a laptop.

Create the file while the sensor is waiting:
    mkdir -p /tmp/airflow-incoming
    printf 'id\\n1\\n2\\n' > /tmp/airflow-incoming/orders_<YYYYMMDD>.csv
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG

INCOMING = "/tmp/airflow-incoming"

with DAG(
    dag_id="file_sensor_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["16", "sensors"],
):
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=INCOMING + "/orders_{{ ds_nodash }}.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=int(timedelta(minutes=10).total_seconds()),
        mode="reschedule",
    )

    process_file = BashOperator(
        task_id="process_file",
        bash_command="wc -l " + INCOMING + "/orders_{{ ds_nodash }}.csv",
    )

    wait_for_file >> process_file
