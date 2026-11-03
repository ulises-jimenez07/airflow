"""Timeouts + failure callbacks: the Airflow 3 way to catch slow or stuck work."""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


def alert(context):
    """Failure callback. Replace the print with Slack, email, PagerDuty..."""
    ti = context["task_instance"]
    print(
        "ALERT: "
        f"dag={ti.dag_id} task={ti.task_id} run={context['run_id']} "
        f"try={ti.try_number} error={context.get('exception')!r}"
    )


with DAG(
    dag_id="timeouts_alerts_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(minutes=2),
        "on_failure_callback": alert,
    },
    tags=["14", "alerting"],
):
    quick = BashOperator(task_id="quick", bash_command="echo fast enough")

    slow = BashOperator(
        task_id="slow",
        bash_command="sleep 20 && echo never printed",
        execution_timeout=timedelta(seconds=10),  # overrides the default: fails after 10 s
    )

    quick >> slow
