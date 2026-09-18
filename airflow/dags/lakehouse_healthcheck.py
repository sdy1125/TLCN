"""Airflow DAG that verifies the local lakehouse services are reachable."""

from datetime import datetime, timezone
from urllib.request import urlopen

from airflow.sdk import dag, task


@dag(
    dag_id="lakehouse_healthcheck",
    description="Check MinIO, Iceberg REST and Trino from the Airflow network",
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["tlcn", "lakehouse"],
)
def lakehouse_healthcheck():
    @task
    def verify_services() -> dict[str, str]:
        endpoints = {
            "minio": "http://minio:9000/minio/health/live",
            "iceberg_rest": "http://iceberg-rest:8181/v1/config",
            "trino": "http://trino:8080/v1/info",
        }
        statuses = {}
        for service, endpoint in endpoints.items():
            with urlopen(endpoint, timeout=10) as response:  # noqa: S310 - local services only
                statuses[service] = f"HTTP {response.status}"
        return statuses

    @task
    def print_contract(statuses: dict[str, str]) -> None:
        print(f"Service status: {statuses}")
        print("Pipeline contract: sources -> bronze -> silver -> gold -> Trino/Power BI")

    print_contract(verify_services())


lakehouse_healthcheck()

