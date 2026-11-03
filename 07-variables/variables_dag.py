"""Variables: read one with a Jinja template, another inside a Python task.

Requires variables var2 and var3:
    airflow variables import 07-variables/variables.json
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, task

with DAG(
    dag_id="variables_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["07", "variables"],
):
    start = EmptyOperator(task_id="start")

    # Templated: resolved only when the task runs.
    get_var = BashOperator(task_id="get_var", bash_command="echo {{ var.value.var3 }}")

    @task
    def add_five():
        # Read inside the task, never at module top level.
        value = int(Variable.get("var2"))
        result = value + 5
        print(f"var2 + 5 = {result}")
        return result

    start >> [get_var, add_five()]
