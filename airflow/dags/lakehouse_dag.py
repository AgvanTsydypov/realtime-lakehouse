# lakehouse_dag.py — orchestrate the Kubernetes Spark pipeline from Airflow
from datetime import datetime
from pathlib import Path
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# repo root, computed from this file: dags -> airflow -> repo
REPO = Path(__file__).resolve().parents[2]
MANIFEST = REPO / "k8s" / "pipeline-sparkapp.yaml"
NS = "lakehouse"
APP = "lakehouse-pipeline"

with DAG(
    dag_id="lakehouse_pipeline",
    description="Run the bronze/silver/gold Spark job on Kubernetes",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger for now
    catchup=False,
    tags=["lakehouse"],
) as dag:

    # 1) (re)submit the SparkApplication — delete the old run first, so it is idempotent
    submit = BashOperator(
        task_id="submit_pipeline",
        bash_command=(
            f"kubectl delete sparkapplication {APP} -n {NS} --ignore-not-found && "
            f"kubectl apply -f {MANIFEST}"
        ),
    )

    # 2) poll until the job reaches COMPLETED (or fail fast on FAILED)
    wait = BashOperator(
        task_id="wait_for_completion",
        bash_command=(
            "for i in $(seq 1 120); do "
            f"state=$(kubectl get sparkapplication {APP} -n {NS} "
            "-o jsonpath='{.status.applicationState.state}' 2>/dev/null); "
            'echo "state: $state"; '
            'if [ "$state" = "COMPLETED" ]; then exit 0; fi; '
            'if [ "$state" = "FAILED" ]; then echo "pipeline FAILED"; exit 1; fi; '
            "sleep 5; "
            "done; "
            'echo "timed out"; exit 1'
        ),
    )

    # 3) print the row counts from the driver logs
    show = BashOperator(
        task_id="show_results",
        bash_command=(
            f"kubectl logs -n {NS} {APP}-driver "
            '| grep -E "bronze rows|silver rows|gold zones"'
        ),
    )

    submit >> wait >> show
