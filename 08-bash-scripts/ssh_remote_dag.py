"""Run scripts on two remote machines over SSH, one after the other.

Needs:
  pip install apache-airflow-providers-ssh   (Composer: add as PyPI package)
  Connections "machine_a" and "machine_b" of type SSH, e.g.
    airflow connections add machine_a --conn-type ssh --conn-host 203.0.113.10 \
        --conn-login etl --conn-port 22 --conn-extra '{"key_file": "/path/to/id_ed25519"}'
"""

from datetime import datetime, timedelta

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.sdk import DAG

with DAG(
    dag_id="ssh_remote_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["08", "ssh"],
):
    extract_on_a = SSHOperator(
        task_id="extract_on_a",
        ssh_conn_id="machine_a",
        command="bash /opt/jobs/extract.sh {{ ds }}",
        cmd_timeout=600,
    )

    load_on_b = SSHOperator(
        task_id="load_on_b",
        ssh_conn_id="machine_b",
        command="bash /opt/jobs/load.sh {{ ds }}",
        cmd_timeout=600,
    )

    extract_on_a >> load_on_b
