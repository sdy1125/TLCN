# Silver runbook

## Automated release workflow (2026-11-04)

Use `lakehouse_release` for an ordered Bronze → Silver → Gold run. It creates a
verified Bronze release manifest, then runs the ten Silver stages and the five
Gold stages. Its default schedule is daily; set `LAKEHOUSE_RELEASE_SCHEDULE`
before deploying Airflow to change the cadence. Drive download remains an
independent handoff. Its in-progress marker blocks Bronze during download;
after completion Bronze verifies all source SHA256 values from
`data/raw/.drive-handoff.json` before ingestion.

```bash
docker compose exec -T airflow-apiserver airflow dags trigger lakehouse_release
docker compose exec -T airflow-apiserver airflow dags list-runs lakehouse_release -o plain
docker compose exec -T airflow-apiserver airflow dags list-runs silver_release -o plain
docker compose exec -T airflow-apiserver airflow dags list-runs gold_build -o plain
```

The Silver candidate stores a checksum protected manifest with Bronze release
ID, policy version, DQ run ID, all core/ref/meta Iceberg snapshots and domain
row counts. `silver_meta.publication_pointer` selects the serving Silver batch
with a one row generation guarded update. Gold resolves this pointer and reads
the candidate's pinned snapshots. The old `publish_batch.status` remains useful
for audit but is no longer the serving selector. To roll back to a candidate
with a complete manifest, run
`spark-submit /home/iceberg/jobs/silver/activate.py --rollback-to <batch-id>`.
The legacy batch can also be selected if its historical snapshots still exist.

Silver validation uses the configured source registry and observed data for
nulls and exceptions. A candidate can publish without warnings; accepted
warning categories are recorded per batch in `silver_meta.release_exception`.
Any new warning category blocks publication until its policy and evidence are
reviewed. Do not edit parser, mapping, DQ or publication code during a running
release: the content based policy version intentionally rejects code drift.

The following sections describe the historical October recovery and manual
commands. Their fixed batch counts are evidence from that run, not current
acceptance criteria.

## Contract and migration status

The [migration plan](SILVER_V2_IMPLEMENTATION_CONTRACT.md) defines the Silver handoff. The code uses `silver_backfill` and a catalog whose tables reside under `warehouse/silver/`. On 2026-10-30 all 26 Bronze releases were rebuilt into eight atomic `silver_core` tables with matching `silver_stage` rows.

The historical October batch `silver-layered-20261030-1650` was
`ACTIVE_WITH_EXCEPTIONS`, with eight core snapshot IDs and policy
`source-local-v5-3cb854afd9b7f156`. Its DQ recorded 52 PASS and two known
WARN; all 21 Gold input smoke checks passed. The current Silver release is
selected by `silver_meta.publication_pointer`.

## Storage layout

Bronze continues to use the separate `bronze` bucket. The `warehouse` bucket
holds Iceberg tables under `silver/silver_core/`, `silver/silver_stage/`,
`silver/silver_meta/`, and `silver/silver_ref/`. The `gold/` prefix contains
published Gold Iceberg tables. Silver code is under `spark/silver/` and
`spark/jobs/silver/`; Gold code is under `spark/gold/` and `spark/jobs/gold/`.

## Run and verify

Run from the repository root. Keep Bronze read-only.

```bash
docker compose config --quiet
docker compose up -d --no-build
docker compose exec -T airflow-apiserver airflow dags trigger silver_backfill --conf '{"dataset_id":"world_bank_wdi"}'
docker compose exec -T airflow-apiserver airflow dags list-runs silver_backfill -o json
docker compose exec -T airflow-apiserver airflow dags list-import-errors
docker compose exec -T trino trino --execute 'SHOW SCHEMAS FROM iceberg'
docker compose exec -T trino trino --execute "SELECT batch_id,status,policy_version FROM iceberg.silver_meta.publish_batch WHERE status LIKE 'ACTIVE%'"
```

For a new successful Bronze release, run the ordered pipeline. Use unique run, DQ and batch IDs. Review the DQ and Gold smoke output before activation.

```bash
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/foundation.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/backfill.py --run-id new-run-id
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/repair_metadata.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/refs.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/policy.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/canonical.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/validate.py --run-id new-dq-id
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/gold_contract_smoke.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/prepare_publish.py --batch-id new-batch-id --dq-run-id new-dq-id
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver/activate.py --batch-id new-batch-id
```

The current activation job requires the complete configured DQ rule set, zero
failed BLOCK checks, reviewed warning categories, the candidate policy version,
and unchanged pinned snapshots and row counts. Consumers needing a stable
release should follow `silver_meta.publication_pointer` and use Iceberg time
travel. Re-running an unchanged Bronze release skips its existing observation
set.

## Known source limitations

- Six Đắk Lắk 2022 local price groups contain conflicting source values. All 12 candidate rows remain in core and the six groups have zero canonical winners. Resolve them through reviewed source evidence before adding a winner.
- The Bronze files lack official district codes for 2,142 observations. Silver uses stable source-scoped geography keys, retains the raw names and marks 44 aliases pending official mapping. The keys preserve district grain; they are not official administrative codes.
- The consolidated NASA CSV lacks the original API community and unit request. Native solar values are retained; canonical `solar_radiation_mj_m2_day` remains NULL pending source verification.
- Comtrade variants are retained atomically. Their canonical policy remains inactive until classification, partner scope, transport/customs, quantity and valuation rules are approved. FAOSTAT matrix observations have source-local decisions.
- Source aggregate FAO code 159 is preserved as a source code and excluded from canonical Fact keys because it is not an M49 country code.

The recovered release has 52 PASS and two WARN DQ results. The two WARN counts are six open price conflicts and 2,142 source-scoped district rows. The 21 Gold input smoke checks passed. These exceptions are explicit in the active batch, not silently filled.

## Backup, recovery and deployment

On 2026-10-22 the MinIO `warehouse` objects and the old V1 backup objects were deleted from the Console. Neither bucket had versioning at deletion time; those objects cannot be restored from MinIO. Bronze stayed intact. Silver was rebuilt from all 26 verified Bronze releases in a fresh Iceberg catalog file. The previous SQLite catalog file is retained for diagnosis, but its table pointers refer to missing files. The empty V1 backup bucket was removed. Historical V1 inventory and snapshot CSVs remain as records only; they are **not a usable backup**.

The rebuilt `warehouse` bucket has versioning enabled, and Compose enables it at initialization. Do not delete Iceberg objects directly through MinIO: catalog pointers and data/metadata files must change together. `recover_silver_catalog.py` scans the new `warehouse/silver/` layout only. For accidental object deletion, inspect versions/delete markers and restore the required Iceberg metadata and data objects before querying. A separate physical backup remains advisable because bucket versioning shares the same MinIO volume.

### Incident and how to run the DAG now

The failed Airflow run at 2026-10-22 13:21 UTC reported `Location does not exist` for `s3://warehouse/silver_meta/dataset_registry/metadata/...metadata.json`; the entire warehouse was empty. Recovery loaded all 26 Bronze releases, rebuilt canonical and references, passed 54 DQ checks (52 PASS, two known WARN) and 21 Gold input checks, and activated `silver-recovered-20261022`. A WDI-only DAG run and a full 26-dataset DAG run without configuration both finished `success` after recovery.

In the MinIO Console, refresh `warehouse` to see only `silver/` and `gold/` at the bucket root. The obsolete top-level `silver_*` prefixes were removed after the 2026-10-30 layered rebuild passed publication checks. In Airflow, trigger `silver_backfill` with no configuration to check all 26 latest Bronze releases, or provide `{"dataset_id":"world_bank_wdi"}` to check one source. Existing releases return `SKIPPED` by design. When a new Bronze release exists, follow the ordered policy, canonical, DQ and activation steps above after backfill.

The Iceberg REST fixture uses a persistent SQLite volume with WAL and a busy timeout. Keep the `iceberg-catalog` and MinIO named volumes when recreating containers. Compose publishes MinIO, Trino, Iceberg REST and Airflow only on localhost. `.env` currently holds development credentials; rotate MinIO, PostgreSQL, Airflow JWT/admin and runner token before exposing the stack beyond the local host. PostgreSQL rotation must update the initialized database role and matching Compose value together. A production deployment should replace the single-node SQLite REST fixture with a supported durable catalog and protect its credentials and backups.
