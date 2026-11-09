# Catalog contention and Silver DAG refactor — 05/11/2026

## Decision and cause

The local Iceberg REST fixture uses a SQLite JDBC catalog in WAL mode. The
05/11 release initially failed at Silver `backfill` with
`SQLITE_BUSY_SNAPSHOT`: a deferred transaction had read an older catalog
snapshot before trying to write. Increasing `busy_timeout` alone cannot make
that stale read transaction writable. The SQLite [result code reference]
(https://sqlite.org/rescode.html#busy_snapshot) and [transaction isolation
guide](https://sqlite.org/isolation.html) describe this failure and the
`BEGIN IMMEDIATE` remedy.

`compose.yaml` now uses the existing catalog file with
`transaction_mode=IMMEDIATE`, WAL, and a 60-second busy timeout. The bundled
SQLite JDBC driver was checked with an isolated Java connection probe; it
reported `IMMEDIATE` for the configured URI. The change did not migrate,
delete, or re-register Iceberg tables.

Before container recreation, catalog writers were stopped and the catalog
volume was copied to `/tmp/tlcn-catalog-before-immediate-20261105`. The SQLite
copy passed `PRAGMA integrity_check` and contained 234 table registrations,
35 view registrations, and 18 namespaces. This is a local rollback copy, not
an independent backup of MinIO and PostgreSQL.

## Silver orchestration and naming

`silver_release` is now the only active Silver DAG file. It accepts optional
`dataset_id` for the backfill stage while still running the full Silver
publication gates. The old `silver_backfill.py` DAG file was retired; its
historical Airflow metadata remains marked `stale` and paused.
`silver_v2_backfill` is also stale and paused. Neither is parsed from the
current DAG folder.
The Airflow image tag is `tlcn-airflow:local`; the unused `week7` image tag was
removed. The Bronze DAG no longer has a `week-7` tag or description.

The first targeted runner probe exposed a request-normalization defect: the
`dataset_id` field was dropped and all 26 datasets were scanned. All returned
`SKIPPED`; no Silver or Gold pointer changed. The runner now normalizes the
request before removing transient fields, and a regression test covers the
single-source, all-source, and invalid-source cases. A second probe returned
`SUCCESS` for only `world_bank_wdi` (396 existing rows, `SKIPPED`). It did not
publish a new Silver batch.

## Runtime validation

The complete `lakehouse_release` run `manual__catalog_immediate_20261105`
finished `success` at 06:41:03 UTC with Trino running throughout. Bronze,
Silver, Gold build, DQ, input smoke, and publication all completed without
restarting the catalog or stopping Trino. No `SQLITE_BUSY` or catalog
`Unknown failure` appeared in catalog logs from the start of this run.

| Check | Result |
| --- | --- |
| Active Silver pointer | `silver-703648d5384bfe4c29e50359`, generation 9 |
| Silver DQ | 55 PASS, 1 accepted WARN, 0 FAIL |
| Active Gold pointer | `gold-23f22e1971dacb5230c12a50`, generation 14 |
| Gold DQ | 225 PASS, 0 FAIL |
| Gold lineage | 732,186 observation contributions |
| Gold input snapshot pins | 26 |
| Production KPI view | 9,262 rows |
| Export SOURCE_TOTAL KPI view | 22,008 rows |
| HS total-mode country view | 7,661 rows |
| Verified official district view | 705 rows |
| Host tests | 65 run: 54 passed, 11 skipped for optional Spark dependencies |

The new Gold release has the same 17 business-table row counts as the previous
release. Its four published exception groups remain: one unresolved annual
district boundary, 16 EU aggregate observations outside country stock, 26
provincial area conflicts, and eight provincial production conflicts. These
are business-scope limitations, not catalog commit failures.

## Operational boundary

This change addresses the observed SQLite read-to-write contention in the
local fixture and passed one complete concurrent Trino/Spark release. It is
not a proof of production-grade catalog durability or arbitrary multi-writer
load. A production catalog migration still needs a separately tested metadata
and MinIO backup, an isolated restore, and one-writer cutover. The SQLite
catalog file and prior Gold candidate remain available for rollback; revert
the `CATALOG_URI` option and recreate only the affected containers if the
transaction-mode change itself must be rolled back.
