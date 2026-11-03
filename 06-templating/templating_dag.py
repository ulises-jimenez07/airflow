"""Jinja templating: built-in variables, macros and params."""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

templated_command = dedent(
    """
    {% for i in range(3) %}
      echo "iteration {{ i }}"
      echo "ds = {{ ds }}"
      echo "ds + 7 = {{ macros.ds_add(ds, 7) }}"
      echo "param = {{ params.my_param }}"
    {% endfor %}
    """
)

with DAG(
    dag_id="templating_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    params={"my_param": Param("Parameter I passed in", type="string")},
    tags=["06", "templating"],
):
    print_date = BashOperator(task_id="print_date", bash_command="date")

    sleep = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3)

    templated = BashOperator(task_id="templated", bash_command=templated_command)

    print_date >> [sleep, templated]
