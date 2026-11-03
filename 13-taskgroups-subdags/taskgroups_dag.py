"""TaskGroups: two collapsible sections of five tasks each (replacement for SubDAGs)."""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, TaskGroup, task_group

with DAG(
    dag_id="taskgroups_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["13", "taskgroup"],
):
    start = EmptyOperator(task_id="start")

    # Context-manager style.
    with TaskGroup("section_1", tooltip="first block of parallel tasks") as section_1:
        for i in range(1, 6):
            EmptyOperator(task_id=f"task_{i}")  # full id: section_1.task_i

    some_other_task = EmptyOperator(task_id="some_other_task")

    # Decorator style: a reusable group factory.
    @task_group
    def section(n: int):
        for i in range(1, n + 1):
            EmptyOperator(task_id=f"task_{i}")

    section_2 = section.override(group_id="section_2")(5)

    end = EmptyOperator(task_id="end")

    start >> section_1 >> some_other_task >> section_2 >> end
