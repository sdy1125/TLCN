# Silver V1 inventory — 2026-10-21

## Running environment

- Docker Compose: MinIO, Apache Iceberg REST fixture 1.8.1, Spark 3.5.5, Trino 483, Airflow 3.3.2, PostgreSQL. Airflow service containers were stopped at audit start because their old bind mounts pointed at a previous workspace path. Trino and Spark were recreated from the current Compose file; Airflow has not yet been restarted.
- Git baseline: `f9e76a0` on `main`. The worktree already contained a modification to `docs/silver/RUNBOOK.md` and untracked `.agents/` and `skills-lock.json`; these are not migration edits. Branch creation failed because `.git` is read-only in this workspace.
- Bronze: 26 registered source datasets. Verified latest-success metadata, file size and object presence through the existing Bronze reader. Full paths and SHA-256 digests: `reports/silver_bronze_manifest.csv`. Bronze objects have not been written.
- Five XLSX sources each contain one `data` sheet: area 533 data rows, production 292, local price 240, export 203, stock 450.

## Iceberg catalog recovery and baseline

The REST fixture used `jdbc:sqlite::memory:`. Its catalog registrations had disappeared after container restart, while the table objects remained in MinIO. Before restoration, the complete `warehouse` bucket was mirrored to `silver-v1-backup-20261021`; both source and backup measured 1,492 objects and 86 MiB. The Compose configuration now uses a named persistent SQLite volume. Nine V1 tables were registered from their highest numbered metadata JSON files. Trino lists all nine again. This restores access but does not prove every historic snapshot is valid; the latest read succeeded for all six domain tables.

| V1 table | Rows at audit | Disposition |
|---|---:|---|
| `silver.production_national` | 28,409 | Rebuild as `silver_core.production_observation`; preserve per-element observations and flags. |
| `silver.production_provincial` | 3,298 | Rebuild with geography, coverage and record-status grain. |
| `silver.market_indicator` | 17,029 | Split into price, stock and macro domains. |
| `silver.trade_total` | 44,751 | Rebuild with classification, product and scope fields. |
| `silver.trade_partner` | 23,038 | Rebuild; V1 filtered detail transport/customs/secondary-partner rows. |
| `silver.weather_daily` | 614,754 | Alter/rebuild with per-measure validity and source location lineage. |
| `silver.processing_state` | control | Replace with observation-set/run state. |
| `silver.dq_results` | control | Replace with rule-level DQ result. |
| `silver.quarantine_records` | control | Replace with source-locator quarantine. |

## Code and consumer map

- `spark/silver/readers.py`: KEEP verified Bronze resolve/download; REBUILD row readers to retain source file/sheet/row position and avoid early Comtrade filtering.
- `spark/silver/transforms/*`: KEEP tested numeric/date parsing and weather sentinel ideas; REBUILD targets, price series, coverage, trade valuation and canonical handling.
- `spark/silver/pipeline.py`, `writer.py`, `quality.py`, `state.py`: REBUILD V2 contracts, staging, DQ and publish state. V1 code stays available during validation.
- `airflow/dags/silver_transformation.py`: ALTER after V2 runner is tested; the current DAG submits V1 jobs.
- `dbt/models/gold/commodity_year_summary.sql`: DEPRECATE old Gold model dependency, but no Gold code will be built as part of this migration.
- `docs/silver/RUNBOOK.md`: existing user changes are preserved.

## Current cutover state

V1 remains the published namespace. V2 has not been promoted. The referenced higher-priority Gold design `Thiet_ke_Gold_hoan_chinh_va_xu_ly_Silver_tu_Bronze(1).md` was not found in the workspace or Downloads directory; exact eight-dimension handoff requires that document. The migration specification itself defines eight Fact-facing Silver processes.
