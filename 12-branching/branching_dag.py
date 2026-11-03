"""Branch on the weekday of the run: only one of seven tasks runs, the rest are skipped."""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task

DAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]

with DAG(
    dag_id="branching_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["12", "branching"],
):

    @task
    def get_weekday(**context) -> str:
        # Use the run's logical date so re-runs are reproducible.
        day = DAYS[context["logical_date"].weekday()]
        print(f"logical date {context['ds']} is a {day}")
        return day

    @task.branch
    def branch(day: str) -> str:
        return f"task_for_{day}"

    day_tasks = [EmptyOperator(task_id=f"task_for_{d}") for d in DAYS]

    # Without this trigger rule the join would be skipped together with the 6 skipped branches.
    join = EmptyOperator(task_id="join", trigger_rule="none_failed_min_one_success")

    branch(get_weekday()) >> day_tasks >> join
