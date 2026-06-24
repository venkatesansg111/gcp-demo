from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


DEFAULT_CONFIG_PATH = "/home/airflow/gcs/dags/config/runtime_config.yaml"


def load_runtime_config(config_path: str = DEFAULT_CONFIG_PATH) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Runtime config not found at {config_path}. "
            "Upload config/dev.yaml or config/prod.yaml to dags/config/runtime_config.yaml"
        )

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


CONFIG = load_runtime_config()
CLUSTER_NAME = "sales-ephemeral-{{ ts_nodash | lower }}"

with DAG(
    dag_id="sales_pipeline_dag",
    description="Minimal Composer -> Dataproc -> GCS -> BigQuery pipeline demo",
    start_date=datetime(2024, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["demo", "gcp", "cicd"],
) as dag:
    start = EmptyOperator(task_id="start")

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_ephemeral_cluster",
        project_id=CONFIG["project_id"],
        region=CONFIG["region"],
        cluster_name=CLUSTER_NAME,
        cluster_config={
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": CONFIG["dataproc_master_machine_type"],
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": CONFIG["dataproc_master_disk_size_gb"],
                },
            },
            "worker_config": {"num_instances": 0},
            "software_config": {
                "image_version": CONFIG["dataproc_image_version"],
                "properties": {
                    "dataproc:dataproc.allow.zero.workers": "true",
                },
            },
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": 1200},
            },
        },
    )

    dataproc_job = {
        "reference": {"project_id": CONFIG["project_id"]},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{CONFIG['dataproc_job_bucket']}/jobs/sales_transform.py",
            "args": [
                "--input",
                f"gs://{CONFIG['input_bucket']}/sales.csv",
                "--output",
                f"gs://{CONFIG['output_bucket']}/processed_sales.csv",
            ],
        },
    }

    submit_dataproc = DataprocSubmitJobOperator(
        task_id="submit_dataproc_spark_job",
        project_id=CONFIG["project_id"],
        region=CONFIG["region"],
        job=dataproc_job,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_ephemeral_cluster",
        project_id=CONFIG["project_id"],
        region=CONFIG["region"],
        cluster_name=CLUSTER_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_processed_csv_to_bigquery",
        bucket=CONFIG["output_bucket"],
        source_objects=["processed_sales.csv/part-*.csv"],
        destination_project_dataset_table=(
            f"{CONFIG['bq_dataset']}.{CONFIG['bq_table']}"
        ),
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        schema_fields=[
            {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "product", "type": "STRING", "mode": "NULLABLE"},
            {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    end = EmptyOperator(task_id="end")

    start >> create_cluster >> submit_dataproc >> load_to_bigquery >> delete_cluster >> end
