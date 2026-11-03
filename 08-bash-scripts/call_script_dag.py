"""Call scripts that live in another folder of the environment.

In Composer, upload scripts/ to the bucket's data/ folder first:
    upload_data 08-bash-scripts/scripts/greet.py scripts
    upload_data 08-bash-scripts/scripts/report.sh scripts

Locally, point the Variable at this repo:
    airflow variables set scripts_dir "$PWD/08-bash-scripts/scripts"
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

# Default = Composer's data/ mount. Overridable with the scripts_dir Variable.
SCRIPTS_DIR = "{{ var.value.get('scripts_dir', '/home/airflow/gcs/data/scripts') }}"

with DAG(
    dag_id="call_script_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["08", "bash"],
):
    run_python_script = BashOperator(
        task_id="run_python_script",
        bash_command=f"python3 {SCRIPTS_DIR}/greet.py",
    )

    # Passing an argument after the .sh avoids the "TemplateNotFound" gotcha.
    run_bash_script = BashOperator(
        task_id="run_bash_script",
        bash_command=f"bash {SCRIPTS_DIR}/report.sh {{{{ ds }}}}",
    )

    run_python_script >> run_bash_script
