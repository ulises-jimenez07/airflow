"""Same DAG as hello_dag.py, written with the TaskFlow (decorator) API."""

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="hello_dag_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "airflow-notes", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["05", "basics", "taskflow"],
)
def hello_dag_taskflow():
    @task
    def hello_python():
        print("Hello from Python!")

    @task.bash
    def goodbye_bash() -> str:
        # A @task.bash function returns the command to run.
        return "echo 'Goodbye from Bash'"

    hello_python() >> goodbye_bash()


hello_dag_taskflow()
