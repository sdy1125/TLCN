# Repository Guidelines

## Project Structure & Module Organization

`airflow/dags/` contains Bronze ingestion, `silver_release`, `gold_build`, and the ordered `lakehouse_release` workflow; `silver_backfill` remains available for targeted repair. Reusable Bronze code lives in `airflow/include/bronze_ingestion/`. Silver parsers, contracts, selection rules, and Iceberg logic are in `spark/silver/`, with runnable entrypoints in `spark/jobs/silver/`. Gold candidate, scope, DQ, publication and semantic-view code lives in `spark/gold/` and `spark/jobs/gold/`. `data/raw_manifest.csv` lists the 26 source files under `data/raw/`. Tests are in `tests/`, operational guidance is in `docs/silver/` and `docs/gold/`, and reconciliation evidence is in `reports/`. Local services are configured in `compose.yaml`, `minio/`, and `trino/etc/`.

## Build, Test & Development Commands

Run these from the repository root:

```bash
cp .env.example .env                 # Configure local service credentials.
docker compose config --quiet       # Validate Compose configuration.
docker compose up -d --build        # Build and start the lakehouse services.
PYTHONPATH=spark:airflow/include python3 -m unittest discover -s tests -t . -v
PYTHONPYCACHEPREFIX=/tmp/tlcn-pycache python3 -m compileall -q spark airflow/dags tests
docker compose exec -T airflow-apiserver airflow dags list-import-errors
```

For a full release, trigger `lakehouse_release`; for manual repair, follow the ordered commands in `docs/silver/RUNBOOK.md` and use unique run and batch IDs. Read `docs/PIPELINE_RELEASE_AUDIT_20261104.md` before claiming production readiness.

## Coding Style & Naming Conventions

Use four-space indentation and PEP 8 conventions for Python. Name modules and functions with `snake_case`, classes with `PascalCase`, and constants with descriptive uppercase names. Keep source-specific parsing in `spark/silver/parsers/` and reviewed mappings in `spark/silver/config/`. Define Iceberg schemas and natural keys explicitly. No repository-wide formatter or linter is configured; match nearby code and run `git diff --check`.

## Testing Guidelines

Tests use `unittest` and files named `test_*.py`. Add focused cases for source parsing, stable identities, missing values, lineage, and canonical conflicts when changing Silver behavior. Host tests can skip Spark-dependent checks. For pipeline changes, run the container validation and Gold input smoke jobs from the runbook, and reconcile row counts before activation.

## Commits, Pull Requests & Data Safety

History includes short imperative subjects and scoped subjects such as `refactor:`; use a specific subject, for example `Validate Silver price conflicts`. PRs should name affected datasets and tables, report test results and migration impact, describe rollback, and link an issue when available. Add screenshots for UI changes only. Never commit `.env` or credentials, alter immutable Bronze objects, or delete Iceberg files directly from MinIO. Preserve MinIO and catalog volumes during container recreation.
