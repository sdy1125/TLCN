"""Week 7 local ``data/raw`` to MinIO Bronze ingestion DAG."""

from __future__ import annotations

import logging
import os
from collections import Counter
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task
from airflow.task.trigger_rule import TriggerRule

from bronze_ingestion import (
    IngestionService,
    SourceSpec,
    build_minio_client,
    list_run_results,
    validate_manifest_rows,
)
from bronze_ingestion.manifest import read_manifest_rows

LOGGER = logging.getLogger(__name__)
MANIFEST_PATH = os.environ.get(
    "BRONZE_MANIFEST_PATH", "/opt/airflow/data/raw_manifest.csv"
)
RAW_ROOT = os.environ.get("BRONZE_RAW_ROOT", "/opt/airflow/data/raw")
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "bronze")
SCHEDULE = os.environ.get("BRONZE_DAG_SCHEDULE") or None


@dag(
    dag_id="bronze_ingestion",
    description="Ingest local raw source files unchanged into versioned MinIO Bronze",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
    },
    tags=["tlcn", "bronze", "local-raw", "week-7"],
)
def bronze_ingestion():
    @task
    def load_manifest() -> list[dict[str, str]]:
        rows = read_manifest_rows(MANIFEST_PATH)
        LOGGER.info("Loaded %s manifest rows from %s", len(rows), MANIFEST_PATH)
        return rows

    @task
    def validate_manifest(rows: list[dict[str, str]]) -> list[dict[str, object]]:
        sources = validate_manifest_rows(rows)
        enabled = [source for source in sources if source.enabled]
        LOGGER.info(
            "Validated local raw manifest configured=%s enabled=%s raw_root=%s",
            len(sources),
            len(enabled),
            RAW_ROOT,
        )
        return [source.to_dict() for source in enabled]

    @task
    def select_sources(sources: list[dict[str, object]]) -> list[dict[str, object]]:
        """Optionally select one or more dataset IDs from manual run config."""

        context = get_current_context()
        dag_run = context.get("dag_run")
        requested = dag_run.conf.get("dataset_id") if dag_run else None
        if requested is None:
            return sources
        if isinstance(requested, str):
            requested_ids = {requested}
        elif isinstance(requested, list) and all(
            isinstance(value, str) for value in requested
        ):
            requested_ids = set(requested)
        else:
            raise ValueError("dataset_id must be a string or a list of strings")
        available_ids = {str(source["dataset_id"]) for source in sources}
        unknown = requested_ids.difference(available_ids)
        if unknown:
            raise ValueError(f"Unknown or disabled dataset_id(s): {sorted(unknown)}")
        selected = [
            source for source in sources if source["dataset_id"] in requested_ids
        ]
        LOGGER.info("Selected dataset IDs: %s", sorted(requested_ids))
        return selected

    @task(execution_timeout=timedelta(minutes=30))
    def ingest_source(source: dict[str, object]) -> dict[str, object]:
        context = get_current_context()
        dag_run = context["dag_run"]
        logical_date = dag_run.logical_date or datetime.now(timezone.utc)
        service = IngestionService(
            build_minio_client(), raw_root=RAW_ROOT, bucket=BRONZE_BUCKET
        )
        return service.ingest(
            SourceSpec.from_dict(source),
            airflow_run_id=dag_run.run_id,
            ingestion_date=logical_date.date(),
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(sources: list[dict[str, object]]) -> dict[str, object]:
        context = get_current_context()
        run_id = context["dag_run"].run_id
        records = list_run_results(build_minio_client(), BRONZE_BUCKET, run_id)
        statuses = Counter(str(record.get("status")) for record in records)
        states = Counter(str(record.get("change_state")) for record in records)
        load_types = Counter(str(record.get("load_type")) for record in records)
        expected = {str(source["dataset_id"]) for source in sources}
        observed = {str(record.get("dataset_id")) for record in records}
        missing = sorted(expected.difference(observed))
        summary = {
            "airflow_run_id": run_id,
            "expected_sources": len(expected),
            "result_records": len(records),
            "statuses": dict(statuses),
            "change_states": dict(states),
            "load_types": dict(load_types),
            "missing_results": missing,
        }
        LOGGER.info("Bronze ingestion summary: %s", summary)
        if missing or statuses.get("FAILED", 0):
            raise RuntimeError(f"Bronze ingestion completed with failures: {summary}")
        if len(records) != len(expected):
            raise RuntimeError(f"Bronze ingestion result count mismatch: {summary}")
        return summary

    rows = load_manifest()
    valid_sources = validate_manifest(rows)
    selected_sources = select_sources(valid_sources)
    mapped_ingestion = ingest_source.expand(source=selected_sources)
    summary = summarize(selected_sources)
    mapped_ingestion >> summary


bronze_ingestion()
