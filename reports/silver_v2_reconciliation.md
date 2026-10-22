# Silver V2 release evidence — 2026-10-22

The current release is `silver-v2-recovered-20261022` in `silver_meta.publish_batch`, status `ACTIVE_WITH_EXCEPTIONS`, policy `source-local-v5-3cb854afd9b7f156`. It records all eight core snapshot IDs. The V1 DAG, code, live tables and namespace were removed. On 2026-10-22 the MinIO warehouse and V1 backup objects were deleted; V2 was rebuilt from the unchanged Bronze bucket in a fresh catalog.

| Domain | Core and stage rows | Canonical winner rows |
| --- | ---: | ---: |
| production_observation | 28,409 | 26,578 |
| provincial_agri_observation | 3,298 | 3,055 |
| price_observation | 16,207 | 15,709 |
| trade_total_observation | 44,751 | 44,367 |
| trade_partner_observation | 27,182 | 5,920 |
| weather_daily | 614,754 | 614,628 |
| coffee_stock_observation | 450 | 450 |
| macro_indicator_observation | 396 | 258 |

All 26 Bronze observation sets were republished; WDI-only and full 26-dataset Airflow reruns skipped existing sets without duplicate rows. Dataset-level counts and source identities are in [the reconciliation CSV](silver_v2_dataset_reconciliation.csv). `acceptance-v2-recovery-20261022` recorded 54 checks: 52 PASS, two WARN, zero BLOCK failures. The warnings are six open price conflicts and 2,142 district rows using source-scoped geography keys pending official codes. All 21 Gold input smoke checks passed; parser smoke exercised 11 real source adapters before the rebuild. Airflow's WDI recovery run at 13:34 UTC and full run at 13:36 UTC both finished `success`. Gold Facts and Dimensions have not been built, so the smoke checks are an input contract check only.

The old V1 backup bucket lost all objects before versioning was enabled; its empty bucket was removed. The [inventory](silver_v1_inventory.md), schema and snapshot CSVs are historical records, not a restorable backup. Bronze retained 130 objects during recovery. The rebuilt warehouse contains the four Silver V2 prefixes and now has versioning enabled. The active namespace list is `silver_core`, `silver_meta`, `silver_ref`, and `silver_stage` plus Trino system schemas.

Known source limitations and operational steps are in the [runbook](../docs/silver/V2_RUNBOOK.md). Local development credentials and the single-node SQLite catalog are deployment limits; this release should not be exposed as a public production service until those are replaced.
