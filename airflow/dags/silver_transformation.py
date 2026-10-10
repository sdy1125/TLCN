"""Submit and monitor the real local Spark runner over the internal network."""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException

ENDPOINT = os.environ.get("SILVER_RUNNER_URL", "http://spark-iceberg:8090")
TOKEN_ENV = "SILVER_RUNNER_TOKEN"


def request(path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    token = os.environ.get(TOKEN_ENV)
    if not token or len(token) < 32:
        raise AirflowFailException(f"{TOKEN_ENV} must contain at least 32 characters")
    try:
        with urllib.request.urlopen(
            urllib.request.Request(
                ENDPOINT + path,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + token,
                },
            ),
            timeout=30,
        ) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 400:
            raise AirflowFailException("Invalid Spark submission request") from exc
        if exc.code == 401:
            raise AirflowFailException("Spark runner rejected authentication") from exc
        raise


@dag(
    dag_id="silver_transformation",
    schedule=os.environ.get("SILVER_DAG_SCHEDULE") or None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "spark", "iceberg"],
)
def silver_transformation():
    @task
    def select_datasets():
        from silver.registry import select_datasets as select

        return select(get_current_context()["dag_run"].conf.get("dataset_id"))

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def submit_spark_jobs(datasets):
        return request(
            "/jobs", {"dataset_id": datasets, "run_id": get_current_context()["run_id"]}
        )["job_id"]

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(hours=3),
    )
    def wait_for_completion(job):
        deadline = time.monotonic() + 7500
        while time.monotonic() < deadline:
            result = request("/jobs/" + job)
            if result["status"] != "RUNNING":
                return result
            time.sleep(15)
        raise AirflowFailException("Spark job exceeded monitor deadline")

    @task
    def publish_summary(result):
        print(json.dumps(result, ensure_ascii=False))
        if result["status"] != "SUCCESS":
            raise AirflowFailException(
                "Spark contract/job failure; inspect job log and Silver DQ results"
            )
        return result

    publish_summary(wait_for_completion(submit_spark_jobs(select_datasets())))


silver_transformation()
