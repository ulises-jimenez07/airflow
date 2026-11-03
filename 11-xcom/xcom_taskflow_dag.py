"""XCom with TaskFlow: return values flow between tasks as function arguments."""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="xcom_taskflow_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["11", "xcom", "taskflow"],
)
def xcom_taskflow_dag():
    @task
    def extract() -> dict:
        # Pretend we read a file and counted rows; pass a reference, not the data.
        return {"path": "gs://bucket/data/input/customers.csv", "rows": 4}

    @task
    def validate(stats: dict) -> int:
        if stats["rows"] == 0:
            raise ValueError("empty input")
        return stats["rows"]

    @task
    def report(rows: int, stats: dict):
        print(f"{rows} rows validated from {stats['path']}")

    stats = extract()
    rows = validate(stats)
    report(rows, stats)


xcom_taskflow_dag()
