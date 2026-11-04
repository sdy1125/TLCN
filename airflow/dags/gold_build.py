"""Publication-gated Gold build over the bounded internal Spark runner."""

import json
import hashlib
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
        raise AirflowFailException("Runner token is not configured")
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        os.environ.get("SILVER_RUNNER_URL", "http://spark-iceberg:8090") + path,
        data=data, headers={"Authorization": "Bearer " + token,
                            "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise AirflowFailException(f"Gold runner returned HTTP {exc.code}") from exc


@dag(dag_id="gold_build", schedule=None,
     start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
     max_active_runs=1, tags=["gold", "spark", "iceberg"])
def gold_build():
    @task(execution_timeout=timedelta(hours=3))
    def run_stage(stage):
        context = get_current_context()
        run_id = f"{context['run_id']}:{stage}:{context['ti'].try_number}"
        manifest_id = "gold-" + hashlib.sha256(
            context["run_id"].encode()).hexdigest()[:24]
        job_id = _request("/jobs", {"pipeline": "gold", "stage": stage,
                                    "run_id": run_id,
                                    "manifest_id": manifest_id})["job_id"]
        deadline = time.monotonic() + 7500
        while time.monotonic() < deadline:
            result = _request("/jobs/" + job_id)
            if result["status"] == "SUCCESS":
                return result
            if result["status"] == "FAILED":
                raise AirflowFailException(
                    f"Gold {stage} failed; inspect {result.get('log_path')}")
            time.sleep(15)
        raise AirflowFailException(f"Gold {stage} exceeded monitor deadline")

    ready = run_stage.override(task_id="resolve_silver")("inspect_input")
    foundation = run_stage.override(task_id="create_gold_storage")("foundation")
    dimensions = run_stage.override(task_id="build_dimensions_bridge")("dimensions")
    facts = run_stage.override(task_id="build_facts_lineage")("facts")
    publication = run_stage.override(task_id="validate_publish")("publish")
    ready >> foundation >> dimensions >> facts >> publication


gold_build()
