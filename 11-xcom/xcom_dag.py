"""XCom with classic operators: explicit push, push by return, pull both."""

from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

VALUE_1 = [1, 2, 3]
VALUE_2 = {"a": "b"}


def push_explicit(ti):
    """Pushes an XCom with a custom key. Returns nothing."""
    ti.xcom_push(key="value from pusher 1", value=VALUE_1)


def push_by_return():
    """The return value is pushed automatically under key 'return_value'."""
    return VALUE_2


def puller(ti, **kwargs):
    """Pulls both values and checks them."""
    v1 = ti.xcom_pull(task_ids="push_explicit", key="value from pusher 1")
    v2 = ti.xcom_pull(task_ids="push_by_return")
    print(f"pulled v1={v1} v2={v2} (run {kwargs['run_id']})")
    if v1 != VALUE_1:
        raise ValueError(f"unexpected v1: {v1}")
    if v2 != VALUE_2:
        raise ValueError(f"unexpected v2: {v2}")


with DAG(
    dag_id="xcom_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["11", "xcom"],
):
    t_push_explicit = PythonOperator(task_id="push_explicit", python_callable=push_explicit)
    t_push_by_return = PythonOperator(task_id="push_by_return", python_callable=push_by_return)
    t_puller = PythonOperator(task_id="puller", python_callable=puller)

    [t_push_explicit, t_push_by_return] >> t_puller
