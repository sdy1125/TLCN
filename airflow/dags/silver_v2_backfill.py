"""Isolated Silver V2 backfill. It does not change the published V1 interface."""

import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException


def _request(path, payload=None):
    token = os.environ.get("SILVER_RUNNER_TOKEN")
    if not token or len(token) < 32:
        raise AirflowFailException("SILVER_RUNNER_TOKEN is not configured")
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        os.environ.get("SILVER_RUNNER_URL", "http://spark-iceberg:8090") + path,
        data=data,
        headers={"Authorization": "Bearer " + token,
                 "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise AirflowFailException(f"Silver runner returned HTTP {exc.code}") from exc


@dag(
    dag_id="silver_v2_backfill", schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
    max_active_runs=1, tags=["silver-v2", "spark", "iceberg"],
)
def silver_v2_backfill():
    @task
    def select():
        from silver.registry import select_datasets

        return select_datasets(get_current_context()["dag_run"].conf.get("dataset_id"))

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def submit(datasets):
        return _request("/jobs", {
            "dataset_id": datasets,
            "run_id": get_current_context()["run_id"],
            "pipeline": "v2",
        })["job_id"]

    @task(execution_timeout=timedelta(hours=3))
    def verify(job_id):
        deadline = time.monotonic() + 7500
        while time.monotonic() < deadline:
            status = _request("/jobs/" + job_id)
            if status["status"] == "SUCCESS":
                if any(item["status"] == "FAILED" for item in status.get("results", [])):
                    raise AirflowFailException("Silver V2 dataset failed; inspect Spark job log")
                print(json.dumps(status, ensure_ascii=False))
                return status
            if status["status"] == "FAILED":
                raise AirflowFailException("Silver V2 Spark job failed; inspect Spark job log")
            time.sleep(15)
        raise AirflowFailException("Silver V2 job exceeded monitor deadline")

    verify(submit(select()))


silver_v2_backfill()
