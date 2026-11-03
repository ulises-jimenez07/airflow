"""First DAG: a Python task followed by a Bash task (classic operator style)."""

# 1. Imports
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# 2. Default arguments (applied to every task)
default_args = {
    "owner": "airflow-notes",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def greet():
    print("Hello from Python!")


# 3. DAG definition
with DAG(
    dag_id="hello_dag",
    description="Python task then Bash task",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["05", "basics"],
):
    # 4. Tasks
    hello_python = PythonOperator(task_id="hello_python", python_callable=greet)

    goodbye_bash = BashOperator(task_id="goodbye_bash", bash_command="echo 'Goodbye from Bash'")

    # 5. Dependencies
    hello_python >> goodbye_bash
