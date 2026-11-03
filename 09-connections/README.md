# 09 · Connections

---

## Part 1 — Why connections

Pipelines touch systems **outside** Airflow: an ERP on a Linux box, a Postgres DB, BigQuery, an HTTP API. The data can be anywhere, so Airflow needs to know **how to reach and authenticate** to each one.

A **Connection** = a named record with everything needed to reach a system:

| Field | Example |
|---|---|
| `conn_id` | `erp_server`, `my_gcp_conn` |
| `conn_type` | `ssh`, `postgres`, `google_cloud_platform`, `http` |
| `host`, `port` | `203.0.113.10`, `22` |
| `login`, `password` | `etl`, `********` (stored encrypted with the Fernet key) |
| `schema` | DB name |
| `extra` | JSON with type-specific options (key file, scopes, project, timeout...) |

Tasks only reference the **id**: `ssh_conn_id="erp_server"`, `gcp_conn_id="my_gcp_conn"`. Credentials stay out of DAG code and out of Git.

```mermaid
flowchart LR
    t1["Task 1<br/>SSHOperator<br/>ssh_conn_id=erp_server"] --> t2["Task 2<br/>PostgresOperator<br/>conn_id=dwh"]
    t2 --> t3["Task 3<br/>BigQueryInsertJobOperator<br/>gcp_conn_id=google_cloud_default"]
    t1 -.-> erp["ERP Linux box"]
    t2 -.-> pg[("Data warehouse")]
    t3 -.-> bq[("BigQuery")]
    conns[("Connections<br/>(DB / secrets backend)")] -.-> t1 & t2 & t3
```

Airflow monitors each step and only starts the next one when the previous succeeded, across different machines.

---

## Part 2 — Creating connections

### UI

**Admin -> Connections -> +**. Pick the type; the form shows the relevant fields. Many defaults already exist (`google_cloud_default`, `ssh_default`, `bigquery_default`...). The **Test** button checks it (disabled by default, enable with `AIRFLOW__CORE__TEST_CONNECTION=Enabled`).

### CLI

```bash
airflow connections add erp_server \
  --conn-type ssh --conn-host 203.0.113.10 --conn-login etl --conn-port 22 \
  --conn-extra '{"key_file": "/home/airflow/gcs/data/keys/id_ed25519"}'

airflow connections add my_gcp_conn \
  --conn-type google_cloud_platform \
  --conn-extra '{"project": "my-project", "impersonation_chain": "bq-reader@my-project.iam.gserviceaccount.com"}'

airflow connections list
airflow connections get my_gcp_conn
airflow connections delete erp_server
```

### Environment variable (URI or JSON)

```bash
export AIRFLOW_CONN_ERP_SERVER='ssh://etl@203.0.113.10:22'
export AIRFLOW_CONN_DWH='{"conn_type": "postgres", "host": "db", "login": "u", "password": "p", "schema": "dwh"}'
```

> **Tip:** In production prefer a **secrets backend** (Secret Manager on GCP: secret `airflow-connections-<conn_id>`), so credentials aren't even in the Airflow DB.

---

## Part 3 — Google Cloud connections

Inside Composer, `google_cloud_default` uses the **environment's service account** automatically (Application Default Credentials). Three ways a GCP task can authenticate:

| Style | How | When |
|---|---|---|
| **Default** | don't pass `gcp_conn_id` (uses `google_cloud_default`) | most of the time |
| **Explicit default** | `gcp_conn_id="google_cloud_default"` | same, just readable |
| **Custom** | `gcp_conn_id="my_gcp_conn"` defined by you | different project, or different identity |

For a different identity, **don't download JSON keys**. Use impersonation:

1. Create the target SA (e.g. `bq-reader`) with only the roles it needs.
2. Grant the Composer SA `roles/iam.serviceAccountTokenCreator` on it.
3. In the connection extra (or directly on the operator) set `impersonation_chain`.

```bash
gcloud iam service-accounts add-iam-policy-binding \
  bq-reader@$PROJECT_ID.iam.gserviceaccount.com \
  --member "serviceAccount:composer-lab-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role roles/iam.serviceAccountTokenCreator
```

> **Warning:** The old approach (create a SA key in IAM, download the JSON, upload it to `data/`, put its path in the connection's "Keyfile Path") still works but is a security risk: long-lived keys leak. Many orgs block key creation entirely by policy.

### Example

[`connections_dag.py`](connections_dag.py) runs the same trivial BigQuery query three times:

```mermaid
flowchart LR
    q_default["q_default<br/>no conn id"]
    q_explicit["q_explicit<br/>google_cloud_default"]
    q_custom["q_custom<br/>my_gcp_conn"]
```

1. Upload and trigger **before** creating `my_gcp_conn`: the first two succeed, `q_custom` fails with `The conn_id my_gcp_conn isn't defined`.
2. Create the connection ([`create-connection.sh`](create-connection.sh)), **Clear** `q_custom`: it succeeds.

That failure-then-fix loop is exactly how you debug connection problems: read the task log, fix the connection, clear the task.

---

## Cheat sheet

```bash
airflow connections add ID --conn-type TYPE --conn-host H --conn-login L --conn-password P --conn-port N --conn-extra JSON
airflow connections list | get ID | delete ID | export conns.json | import conns.json
export AIRFLOW_CONN_<ID>='type://login:password@host:port/schema?param=x'
```
