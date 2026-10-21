# Silver V2 migration evidence — 2026-10-21

## Phase outcome

| Phase | Outcome | Evidence / remaining gate |
|---|---|---|
| P0 inventory/backup | Complete | 26 Bronze releases, nine V1 tables; `silver_bronze_manifest.csv`, `silver_v1_schema.csv`, `silver_v1_snapshots.csv`; warehouse backup 1,492 objects / 86 MiB. |
| P1 gap matrix | Complete | `silver_v2_gap_matrix.csv`. All six V1 domains require alteration/rebuild; market indicator split into price/stock/macro. |
| P2 metadata/reference | Implemented with pending mappings | 26 registry rows and 26 published observation sets; 250 countries, 63 provinces, 63 weather points, 10,640 periods, 33 metric definitions, 452 price series, 11 trade products/bridges, 44 pending district aliases. |
| P3 domain backfill | Complete for current Bronze releases | All 26 releases have data in eight `silver_stage` and eight `silver_core` tables, with source locator and hash. |
| P4 canonical/DQ | Technical policy implemented | 60 policy rows (56 active); source-local-v3 decisions for 735,447 atomic observations. Six open price conflicts, 0 multiple winner keys. Comtrade remains excluded from curated canonical. |
| P5 reconciliation | Technical gates pass | 34 blocking DQ checks PASS; 2 WARN, 0 BLOCK failures. Read-after-write counts and V1 comparable sums match. |
| P6 publish/cutover | Review batch prepared; consumer cutover withheld | `silver_meta.publish_batch` records `silver-v2-20261021` with eight snapshot IDs and `BLOCKED_BUSINESS_REVIEW`. V1 namespace and DAG remain available. Exact Gold 8 Dimension contract file was not supplied; district/solar/Comtrade and six price conflicts need decisions. |
| P7 handover | Complete for current state | `docs/silver/V2_RUNBOOK.md`, code, logs, manifest, DQ results and rollback paths. |

## Domain counts and source reconciliation

Per-dataset Bronze→V2 counts, exclusions, expansion and observation-set IDs are in `silver_v2_dataset_reconciliation.csv`.

| Silver V2 table | V2 core/stage rows | V1 comparable rows | Explanation |
|---|---:|---:|---|
| `production_observation` | 28,409 | 28,409 | Three FAOSTAT production files; 1,303 source missing values retained. |
| `provincial_agri_observation` | 3,298 | 3,298 | 2,473 CSV + 533 area XLSX + 292 production XLSX; 2,142 district rows retained, mapping pending. |
| `price_observation` | 16,207 | Part of 17,029 mixed V1 market rows | 12,527 FAOSTAT price + 3,200 Pink Sheet atomic prices + 480 local price observations. Pink Sheet: 1,963 raw records, 1,163 non-observation rows excluded, 800 monthly records expanded to four series. |
| `trade_total_observation` | 44,751 | 44,751 | 44,345 FAOSTAT + 203 XLSX records × two measures. |
| `trade_partner_observation` | 27,182 | 23,038 | 5,920 FAOSTAT + 2 × (2,807 annual + 7,824 monthly) Comtrade. V2 retains 4,144 atomic Comtrade observations dropped by V1's early scope filter. |
| `weather_daily` | 614,754 | 614,754 | 63 locations. Each of six common measures has 126 NULL after sentinel replacement; solar has 5,040 NULL native values. |
| `coffee_stock_observation` | 450 | Part of mixed V1 market rows | 431 actual, 19 forecast. Source declares 60 kg/bag on all 450 rows; zero conversion discrepancies. |
| `macro_indicator_observation` | 396 | Part of mixed V1 market rows | Six WDI indicators; all 138 source NULL values retained. |

The three V2 domains replacing V1 `market_indicator` total 17,053 rows, 24 more than V1's 17,029. These are the 24 local-price observations suppressed by V1 duplicate handling. Source-local canonical marks 18 identical duplicate observations LOSER and all 12 rows from six conflicting groups EXCLUDED. It does not guess a winner for the six conflicts.

Comparable V1/V2 totals agree exactly: production `2,119,191,442.03`, provincial `86,285,856.10`, trade total `1,716,951,000,914.31`; daily mean temperature sum agrees to three decimal places (`15,253,060.34`). FAOSTAT matrix row count is 5,920 in both versions. Native source units are not combined across incompatible metric types when computing a Gold measure.

## Quality, lineage and orchestration

- Acceptance run `acceptance-v2-20261021-final`: 34 BLOCK/PASS, 2 WARN, 0 BLOCK/FAIL. All eight tables have unique atomic IDs, complete source locator/hash and an existing observation-set FK. All 26 observation sets are `PUBLISHED` and have non-NULL source record counts, including five XLSX counts read from verified Bronze workbooks. Source-local-v3 canonical has zero keys with more than one winner. Price XOR has zero failures among nonmissing observations; 468 source-missing prices legitimately have both amount and index NULL.
- Weather has zero `-999` values in measurement columns and zero `valid_measure_count` mismatches. Solar native values are preserved, but canonical MJ/m²/day remains NULL because Bronze does not carry the original NASA API request or unit metadata.
- Trade partner: 21,262 Comtrade atomic observations retained with `quality_status=WARN` and no canonical winner. FAOSTAT matrix has 5,920 source-local winners. Product bridge entries for HS are `PENDING`, without allocated commodity weights.
- Airflow `silver_v2_backfill` imported with no errors and manual WDI runs at `02:59:05` and `03:21:23` UTC both finished `success`. The second run recorded `SKIPPED`, `rows_read=0`, `rows_published=396`, proving idempotent routing through Airflow→Spark runner. Re-running the 26-set metadata seed left all 26 statuses `PUBLISHED`. Restarting Iceberg REST preserved V1 and V2 table registrations and read counts.
- Review batch `silver-v2-20261021` captured immutable snapshot IDs and row counts for all eight V2 domains. It is `BLOCKED_BUSINESS_REVIEW`; no consumer pointer was switched.
- Bronze bucket remained 130 objects / 98 MiB before and after backfill. The backup bucket remained 1,492 objects / 86 MiB. The warehouse measured 2,802 objects / 357 MiB after final DQ and publish-batch writes.
- The recovery tool discovered 46 current V1/V2 table metadata paths and, with `--apply` on the intact catalog, reported each already registered. No table was re-registered or overwritten in this idempotence drill.

## Cutover blockers and decisions

1. The migration specification references `Thiet_ke_Gold_hoan_chinh_va_xu_ly_Silver_tu_Bronze(1).md` as the higher-priority Gold/Silver contract. It was not found in the workspace or Downloads path. The supplied implementation document defines eight Fact-facing processes but not enough detail to sign off all eight Dimensions.
2. The source has 2,142 district observations without approved district codes. They remain in core with `quality_status=WARN`; 44 distinct alias records are `PENDING`. Mapping district to province would change grain and is prohibited.
3. The consolidated NASA Bronze CSV omits source community/unit metadata for `ALLSKY_SFC_SW_DWN`. Its native value remains available, but `solar_radiation_mj_m2_day` is NULL until the exact source request and conversion are verified.
4. Comtrade canonical requires approved classification hierarchy, partner2/customs/transport scope, quantity-unit and flow-specific valuation rules. No cross-source trade merge or HS allocation has been made.
5. Six conflicting 2022 Đắk Lắk price groups need a business resolution or an accepted exclusion for downstream Gold.

The legacy `silver` namespace has not been deleted and consumers have not been switched. The old dbt Gold model/source declaration referring to `silver.agri_trade` was removed so it cannot silently continue the superseded Gold logic. No new Gold code was added.
