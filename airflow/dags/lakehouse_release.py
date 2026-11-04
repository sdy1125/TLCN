"""Ordered Bronze → Silver → Gold release orchestration."""

import hashlib
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id="lakehouse_release",
     schedule=os.environ.get("LAKEHOUSE_RELEASE_SCHEDULE", "@daily"),
     start_date=datetime(2026, 11, 4, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, default_args={"retries": 1,
                                      "retry_delay": timedelta(minutes=2)},
     tags=["bronze", "silver", "gold", "release"])
def lakehouse_release():
    @task
    def release_ids():
        run_id = get_current_context()["run_id"]
        value = hashlib.sha256(run_id.encode()).hexdigest()[:24]
        silver_run_id = "pipeline-silver-" + value
        return {"bronze_release_id": "bronze-" + value,
                "bronze_dag_run_id": "pipeline-bronze-" + value,
                "silver_dag_run_id": silver_run_id,
                "silver_batch_id": "silver-" + hashlib.sha256(
                    silver_run_id.encode()).hexdigest()[:24],
                "gold_dag_run_id": "pipeline-gold-" + value}

    ids = release_ids()
    bronze = TriggerDagRunOperator(
        task_id="bronze_verified_release", trigger_dag_id="bronze_ingestion",
        trigger_run_id="{{ ti.xcom_pull(task_ids='release_ids')['bronze_dag_run_id'] }}",
        conf={"publish_release": True,
              "release_id": "{{ ti.xcom_pull(task_ids='release_ids')['bronze_release_id'] }}"},
        wait_for_completion=True, poke_interval=15, reset_dag_run=True)
    silver = TriggerDagRunOperator(
        task_id="silver_validated_release", trigger_dag_id="silver_release",
        trigger_run_id="{{ ti.xcom_pull(task_ids='release_ids')['silver_dag_run_id'] }}",
        conf={"bronze_release_id":
              "{{ ti.xcom_pull(task_ids='release_ids')['bronze_release_id'] }}"},
        wait_for_completion=True, poke_interval=15, reset_dag_run=True)
    gold = TriggerDagRunOperator(
        task_id="gold_published_release", trigger_dag_id="gold_build",
        trigger_run_id="{{ ti.xcom_pull(task_ids='release_ids')['gold_dag_run_id'] }}",
        conf={"expected_silver_batch_id":
              "{{ ti.xcom_pull(task_ids='release_ids').get('silver_batch_id', '') }}"},
        wait_for_completion=True, poke_interval=15, reset_dag_run=True)
    ids >> bronze >> silver >> gold


lakehouse_release()
