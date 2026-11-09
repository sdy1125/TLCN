"""Complete Silver build and publication over one pinned Bronze release."""

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException


STAGES = ("foundation", "backfill", "repair_metadata", "reconcile_weather_units", "refs", "policy",
          "canonical", "validate", "gold_smoke", "prepare", "activate")


def _request(path, payload=None):
    token = os.environ.get("SILVER_RUNNER_TOKEN")
    if not token or len(token) < 32:
        raise AirflowFailException("Silver runner token is not configured")
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        os.environ.get("SILVER_RUNNER_URL", "http://spark-iceberg:8090") + path,
        data=data, headers={"Authorization": "Bearer " + token,
                            "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise AirflowFailException(f"Silver runner returned HTTP {exc.code}") from exc


@dag(dag_id="silver_release", schedule=None,
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, default_args={"retries": 2,
                                      "retry_delay": timedelta(minutes=1)},
     tags=["silver", "release", "iceberg"])
def silver_release():
    @task(execution_timeout=timedelta(hours=3), retries=2,
          retry_delay=timedelta(minutes=1))
    def run_stage(stage):
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        bronze_release = conf.get("bronze_release_id")
        if not bronze_release:
            raise AirflowFailException("bronze_release_id is required")
        identity = hashlib.sha256(context["run_id"].encode()).hexdigest()[:24]
        request = {"pipeline": "silver_release", "stage": stage,
                   "run_id": f"silver-{identity}-{stage}-{context['ti'].try_number}",
                   "bronze_release_id": bronze_release,
                   "batch_id": "silver-" + identity,
                   "dq_run_id": "silver-dq-" + identity}
        if stage == "backfill" and conf.get("dataset_id") is not None:
            request["dataset_id"] = conf["dataset_id"]
        job_id = _request("/jobs", request)["job_id"]
        deadline = time.monotonic() + 7500
        while time.monotonic() < deadline:
            result = _request("/jobs/" + job_id)
            if result["status"] == "SUCCESS":
                return result
            if result["status"] == "FAILED":
                raise AirflowFailException(
                    f"Silver {stage} failed; inspect {result.get('log_path')}")
            time.sleep(15)
        raise AirflowFailException(f"Silver {stage} exceeded monitor deadline")

    previous = None
    for stage in STAGES:
        current = run_stage.override(task_id=stage)(stage)
        if previous is not None:
            previous >> current
        previous = current


silver_release()
