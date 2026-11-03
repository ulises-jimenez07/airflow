# 18 · Review questions

Self-check questions with short answers. Cover the answer, try first.

---

## Concepts

<details><summary>1. What is a data pipeline, and what makes Airflow better than cron for one?</summary>

Several tasks that must run (often in order) to produce a result. cron only runs things on a timer; Airflow adds explicit dependencies, parallelism, retries, skipping downstream on failure, alerts, history/logs in a UI, re-running only failed parts, and backfills.
</details>

<details><summary>2. What does each word of "Directed Acyclic Graph" mean?</summary>

Graph: tasks connected by dependencies. Directed: each edge has a direction (a before b). Acyclic: no path leads back to a task already visited, so no loops within a run.
</details>

<details><summary>3. A DAG runs every day. Isn't that a cycle?</summary>

No. Each day is a separate DAG run. "Acyclic" is about dependencies inside one run.
</details>

<details><summary>4. Operator vs task vs task instance?</summary>

Operator: the class / template (BashOperator). Task: an instance of it in a DAG (`task_id="extract"`). Task instance: that task in one specific DAG run, with a state.
</details>

<details><summary>5. What are sensors and when do you need one instead of a normal dependency?</summary>

Operators that wait for a condition (file, table, external task, time). Needed when the thing you wait for is outside your Airflow's control (another company or system). Inside the same Airflow, use dependencies (or Assets).
</details>

## Architecture

<details><summary>6. Name the core components and their roles.</summary>

DAG processor (parses files), scheduler (decides what runs), executor (how/where it runs), workers (run tasks), metadata DB (state), API server + UI (monitor/trigger), triggerer (deferred tasks), and for Celery a queue.
</details>

<details><summary>7. Single node vs multi-node?</summary>

Single node: everything on one machine, LocalExecutor, simple but a single point of failure. Multi-node: workers on several machines (Celery/Kubernetes executor), fault tolerant and horizontally scalable. Production uses multi-node; Composer always is.
</details>

<details><summary>8. KubernetesExecutor vs KubernetesPodOperator?</summary>

Executor: every task of the deployment runs in its own pod with the Airflow image. Operator: one specific task launches a pod with any image you choose. They're independent; the operator works with any executor.
</details>

## Writing DAGs

<details><summary>9. List the five sections of a DAG file.</summary>

Imports, default arguments, DAG definition (id, start_date, schedule, catchup), tasks, dependencies.
</details>

<details><summary>10. You upload a DAG with start_date one month ago, daily schedule, catchup=True. What happens?</summary>

About 30 DAG runs are created to backfill every missed interval. With catchup=False (default in Airflow 3) only the latest one.
</details>

<details><summary>11. What do `t1 >> t2`, `t2 << t1` and `[a, b] >> c` mean?</summary>

First two are identical: t1 runs before t2. Last: c runs after both a and b finish successfully.
</details>

<details><summary>12. What is rendered in `echo {{ macros.ds_add(ds, 7) }}` for logical date 2025-04-04?</summary>

`echo 2025-04-11`.
</details>

<details><summary>13. Why shouldn't you call Variable.get() at the top level of a DAG file?</summary>

The file is parsed repeatedly (every ~30 s); every top-level call hits the DB or secrets backend on every parse. Read variables in templates or inside tasks.
</details>

<details><summary>14. Variable vs Connection vs XCom vs params?</summary>

Variable: shared configuration. Connection: how to reach and authenticate to a system (credentials). XCom: small runtime values passed between tasks of a run. Params: per-run inputs, overridable at trigger time.
</details>

<details><summary>15. Your BashOperator `bash_command="run.sh"` fails with TemplateNotFound. Why?</summary>

Strings ending in .sh are treated as a Jinja template file to load, searched relative to the DAG folder. Add a trailing space or pass arguments after the script name (or use `@task.bash`).
</details>

## Advanced features

<details><summary>16. Two ways to push an XCom? Where is it stored? What shouldn't you put in it?</summary>

Return a value (key `return_value`) or `ti.xcom_push(key, value)`. Stored in the metadata DB by default. Not large data: pass references like GCS paths.
</details>

<details><summary>17. After a BranchPythonOperator, the join task is always skipped. Why and how to fix?</summary>

Default trigger rule `all_success` treats skipped parents as not satisfied. Use `trigger_rule="none_failed_min_one_success"` on the join.
</details>

<details><summary>18. Why were SubDAGs removed and what replaces them?</summary>

They were separate DAGs inside a task: deadlocks, ignored parallelism, confusing to manage. Replaced by TaskGroups (pure UI/naming grouping in the same DAG), or TriggerDagRunOperator / Assets for truly separate DAGs.
</details>

<details><summary>19. In Airflow 3 how do you get alerted when a task runs too long?</summary>

`execution_timeout` (task fails), `dagrun_timeout` (run fails), failure callbacks / notifiers, and Deadline Alerts (3.1+) for alert-without-failing. The 2.x `sla` parameter is gone.
</details>

<details><summary>20. Sensor modes: poke vs reschedule vs deferrable?</summary>

Poke holds a worker slot while sleeping. Reschedule frees the slot between checks. Deferrable hands the wait to the async triggerer, using no worker slot. Prefer deferrable for long waits.
</details>

## Cloud Composer / GCP

<details><summary>21. How do you deploy a DAG to Composer?</summary>

Copy the .py file into the `dags/` folder of the environment's bucket (console upload, `gcloud composer environments storage dags import`, or `gcloud storage cp`). The DAG processor picks it up.
</details>

<details><summary>22. Where do helper scripts and data files go, and what's their path on workers?</summary>

The bucket's `data/` folder, mounted at `/home/airflow/gcs/data/`.
</details>

<details><summary>23. How does a BigQuery task in Composer authenticate without any connection configured?</summary>

`google_cloud_default` uses the environment's service account (ADC). Use impersonation_chain for other identities instead of JSON keys.
</details>

<details><summary>24. Which operator replaces the old BigQueryOperator?</summary>

`BigQueryInsertJobOperator` with a job `configuration` dict.
</details>

<details><summary>25. Airflow vs Beam vs Spark in one sentence each?</summary>

Airflow orchestrates (when, order, retries); Beam is a unified batch/streaming programming model run by engines like Dataflow; Spark is a fast distributed processing engine (Dataproc on GCP).
</details>
