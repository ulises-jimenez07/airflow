"""KubernetesPodOperator: run arbitrary container images as tasks.

Composer: works out of the box (pods run in the environment's cluster).
Local: needs a cluster (kind/minikube) and apache-airflow-providers-cncf-kubernetes;
set IN_CLUSTER=False below and point at your kubeconfig.
"""

from datetime import datetime

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG, task

IN_CLUSTER = True

common = {
    "in_cluster": IN_CLUSTER,
    "get_logs": True,
    "on_finish_action": "delete_pod",
}

with DAG(
    dag_id="k8s_pod_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["15", "kubernetes"],
):
    hello_pod = KubernetesPodOperator(
        task_id="hello_pod",
        name="hello-pod",
        image="python:3.12-slim",
        cmds=["python", "-c"],
        arguments=["import platform; print('hello from pod', platform.node())"],
        **common,
    )

    sized_pod = KubernetesPodOperator(
        task_id="sized_pod",
        name="sized-pod",
        image="busybox:1.36",
        cmds=["sh", "-c"],
        arguments=["echo run date {{ ds }}; nproc; free -m"],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "512Mi"},
            limits={"cpu": "1", "memory": "1Gi"},
        ),
        **common,
    )

    pod_xcom = KubernetesPodOperator(
        task_id="pod_xcom",
        name="pod-xcom",
        image="busybox:1.36",
        cmds=["sh", "-c"],
        # Anything written here becomes the task's XCom return value.
        arguments=["mkdir -p /airflow/xcom && echo '{\"rows\": 42}' > /airflow/xcom/return.json"],
        do_xcom_push=True,
        **common,
    )

    @task
    def read_xcom(payload: dict):
        print(f"pod reported {payload['rows']} rows")

    hello_pod >> sized_pod >> pod_xcom
    read_xcom(pod_xcom.output)
