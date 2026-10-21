# Silver V2 runbook (Docker Compose / Ubuntu)

## Current state

The 26 Bronze releases were backfilled to eight Iceberg tables under `silver_core`, with matching copies under `silver_stage`. Metadata lives in `silver_meta` and references in `silver_ref`. The legacy `silver` namespace and DAG remain available. Gold V2 is a design contract only; no Gold model is deployed. The old dbt Gold model referencing `silver.agri_trade` was removed.

The Iceberg REST fixture now uses `jdbc:sqlite:/catalog/iceberg.db` on the named Docker volume `iceberg-catalog`. A one-shot init service grants the fixture user write access. Do not delete this volume. A restart drill on 2026-10-21 preserved all catalog registrations.

## Backups and recovery

- Silver V1 warehouse copy: MinIO bucket `silver-v1-backup-20261021`, 1,492 objects / 86 MiB. `reports/silver_v1_snapshots.csv` records the nine last-known snapshot IDs. `reports/silver_v1_schema.csv` records the V1 schema.
- Bronze: `bronze` bucket, 130 objects / 98 MiB at the audit start and after V2 backfill. Never write to this bucket from Silver jobs.
- If a catalog starts empty after a reset, inspect the backup and run `docker compose exec -T spark-iceberg python3 /home/iceberg/jobs/recover_silver_catalog.py` to preview paths. It finds V1/V2 table metadata paths in the current warehouse. Use `--apply` only if registrations are absent. This registers existing metadata and does not rewrite data. Use a separate Iceberg catalog/database for a destructive restore drill; the production data paths in metadata remain `s3://warehouse/...`.

## Re-run and validation

From the repository root:

```bash
docker compose config --quiet
docker compose up -d
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_foundation.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_backfill.py --run-id new-unique-run-id
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_repair_metadata.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_refs.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_policy.py
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_canonical.py --policy-version source-local-v3
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_validate.py --run-id new-unique-acceptance-id
docker compose exec -T spark-iceberg spark-submit /home/iceberg/jobs/silver_v2_prepare_publish.py --batch-id new-review-batch-id --dq-run-id new-unique-acceptance-id
docker compose exec -T trino trino --execute 'SHOW SCHEMAS FROM iceberg'
docker compose exec -T airflow-apiserver airflow dags list-import-errors
```

`silver_v2_backfill` is a manual Airflow DAG (`schedule=None`). It calls the authenticated Spark runner with `pipeline=v2` and can accept `--conf '{"dataset_id":"world_bank_wdi"}'`. A manual run for WDI succeeded on 2026-10-21. The runner defaults to V1 for existing clients until cutover, so old Airflow requests do not change behavior unexpectedly.

Backfill is idempotent at `observation_set_id` derived from dataset, source release timestamp and SHA-256. The reader verifies file size/hash against Bronze, attaches `csv:record:N` or `sheet:data:row:N`, and fails on schema or row-width drift. Same release already present in core is skipped. For corrected source files, Bronze must first publish a new successful release; Silver then sees a new observation set.

## Selection and known exceptions

The active technical policy `source-local-v3` compares rows only within one dataset and one release. It uses deterministic `observation_id` order for identical values and zero winners for conflicting values. Cross-source ranking is not configured. Comtrade variants remain in core with `quality_status=WARN` and inactive canonical policy until product hierarchy, transport/customs/partner2 and flow-specific value rules are approved. The 5,920 FAOSTAT matrix rows have source-local winners.

Six Đắk Lắk 2022 price groups have conflicting values; the 12 candidate rows are `EXCLUDED` and six `canonical_conflict` rows remain `OPEN/BLOCK` in policy version `source-local-v3`. The 18 groups with identical values have one winner each. Do not reinstate V1's `maximum_value_within_tolerance` choice without a business decision.

2,142 district observations retain raw district names but have no approved district code. Their `quality_status=WARN` and canonical decision is `EXCLUDED`. The 44 distinct district aliases are `PENDING` in `silver_ref.geography_alias`. NASA's consolidated Bronze CSV lacks the original API community/unit metadata; `solar_native_value` is retained but `solar_radiation_mj_m2_day` is NULL. Do not infer a conversion from a generic NASA parameter page without the exact source request.

## Cutover and rollback gate

Before switching consumers or retiring V1, obtain the referenced Gold design file and verify every one of its eight Dimension and eight Fact field-level requirements. Resolve or approve the district, solar and Comtrade policy exceptions, and decide the six local price conflicts. Confirm `silver_v2_validate.py` reports zero BLOCK failures, check all 26 source accounts, then update consumers as one reviewed release. Batch `silver-v2-20261021` in `silver_meta.publish_batch` records all eight current V2 snapshot IDs and has status `BLOCKED_BUSINESS_REVIEW`. Avoid relying on a multi-table Iceberg transaction: record the active policy version and all eight snapshot IDs before the switch. To roll back, route consumers to `silver` and the snapshot IDs in `reports/silver_v1_snapshots.csv`; keep the V1 bucket copy intact until Gold V2 acceptance is complete.

The V2 core is already queryable through Trino for source-level validation. It is not yet the default consumer interface. The missing Gold design prevents a defensible field-by-field sign-off for the eight Dimensions.
