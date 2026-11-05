# Gold release runbook

Gold consumes the Silver release selected by `silver_meta.publication_pointer` and
pins its core, reference and metadata snapshots. Bronze is immutable and Silver
is published using `docs/silver/RUNBOOK.md`. Run Gold from the repository root
after Silver activation, or trigger the `lakehouse_release` Airflow DAG for the
ordered Bronze → Silver → Gold workflow.

## Contract and analytical boundary

`spark/gold/scope_contract.json` lists every accepted Silver domain and source dataset.
`spark/gold/scope.py` classifies each published WINNER during a run. DQ compares the
complete WINNER set with Fact lineage and approved exclusions; there is no persisted
per-WINNER status table. An unknown source or unknown trade scope blocks publication.
Current approved exclusions are European Union stock aggregates without a verified
M49 member. `NATIONAL_REPORTED` and `SOURCE_TOTAL` share `fact_trade_total` but
are separated by `statistical_scope_code`. FAOSTAT partner rows and Comtrade HS
rows share `fact_trade_partner` but retain classification, partner, customs,
transport and valuation scopes. All eight original Facts remain.
Source values and mappings are never invented to force coverage.

## Build and publish

Use a new manifest ID for each independent release. The Airflow `gold_build` DAG
generates one ID from its DAG run ID and runs the five stages in order through the
authenticated runner. For direct operations:

```bash
GOLD_ID=gold-example-20261104-a
for stage in inspect foundation dimensions facts validate; do
  docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' \
    /opt/silver/jobs/gold/candidate.py --manifest-id "$GOLD_ID" --stage "$stage"
done
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/silver/jobs/gold/publish_candidate.py --manifest-id "$GOLD_ID"
```

Inspect writes an immutable manifest containing the Silver batch, policy, transform
and scope versions, all core/reference/metadata snapshot IDs and a checksum. Later
stages reuse that manifest. A code or scope change requires a new ID. The candidate
namespace is private to the manifest. No stage writes the serving Fact or Dimension
tables. Publication requires zero failed Gold DQ checks and records exceptions for
that batch. The registry stays `PREPARED` until semantic views have passed and
the pointer has moved; a failed view update can be retried with the same candidate.
BI reads `iceberg.gold_semantic.*`, whose views are pinned to release
snapshots and filtered through `gold_meta.publication_pointer`. The pointer is updated
as one Iceberg row only after the views are ready.
`fact_production_provincial_canonical` collapses overlapping source rows at the
analytic grain and returns NULL for measures with conflicting source values.

## Verify and roll back

```sql
SELECT * FROM iceberg.gold_meta.publication_pointer;
SELECT status, count(*) FROM iceberg.gold_candidate_<hash>.dq_result GROUP BY 1;
SELECT exception_code, affected_count, approval_status
FROM iceberg.gold_meta.release_exception WHERE gold_batch_id = '<Gold ID>';
SELECT count(*) FROM iceberg.gold_semantic.fact_weather_daily;
SELECT * FROM iceberg.gold_semantic.kpi_production_tonnes_annual LIMIT 10;
```

Get the candidate namespace from the foundation stage output; do not type the
placeholder `<hash>` literally. To restore a previously published release:

```bash
docker compose exec -T spark-iceberg /opt/spark/bin/spark-submit --master 'local[2]' \
  /opt/silver/jobs/gold/publish_candidate.py --rollback-to <previous-Gold-ID>
```

Rollback updates the same pointer. Keep candidate tables, release metadata, MinIO
objects and catalog volumes. Never delete Iceberg files directly from MinIO.

## Current evidence and limits

The 2026-11-04 Airflow E2E run `manual__e2e_20261104_fixed` published
`gold-84bab056530b551e14627720` from Silver
`silver-b2a36b2e5c4a6e6018aabac5`. It passed 222 DQ checks, wrote 710,093
lineage rows and recorded eleven exception groups. The Gold pointer was rolled
back to the prior release and then restored; its generation is 12. The Silver
pointer was similarly restored and is generation 7. The local SQLite REST
fixture has produced `SQLITE_BUSY_SNAPSHOT` during concurrent access, so this
is validation evidence rather than a production deployment. See
`docs/PIPELINE_RELEASE_AUDIT_20261104.md` for remaining blockers.

The 2026-11-04 candidate `gold-candidate-20261104-f` (`gold-v3`,
`scope-5100fb5c64927fd2`) pinned Silver batch
`silver-layered-20261030-1650`, built 8 Dimensions, 1 Bridge and 8 Facts, and passed
222 DQ checks with zero failures. It classified all 710,965 Silver WINNER rows:
710,093 contributed to Fact lineage; 450 local prices, 406 NATIONAL_REPORTED trade
observations and 16 European Union stock observations were approved exclusions.
The release recorded eleven exception groups, including six open price conflicts,
eight pending product mappings, 2,142 district source observations, inactive
Comtrade and unverified solar conversion. The provincial canonical view handles
26 conflicting area values and eight conflicting production values by leaving
those measures NULL until source precedence is reviewed.
These data remain unavailable to affected KPI queries until evidence is approved
and a new Silver/Gold release is built. The existing source files do not establish
all missing administrative crosswalks, Comtrade harmonization, or solar units.

On 2026-11-05, the source workbook established a bounded rule for six local
price conflicts, and the as-published province keys allowed all 456 local
price winners into `fact_price` with full lineage. The eight HS 0901/0904
bridges are classified `NOT_EQUIVALENT` with no allocation weight: HS 0904
also includes Capsicum/Pimenta, so it cannot be treated as pepper alone.
Independent NASA POWER AG/UTC API comparisons verified MJ/m²/day and enabled
canonical solar values. The historical 2026-11-04 counts above describe only
that older release; use the active Gold pointer and `release_exception` for the
current scope.

The 2026-11-05 active Gold release `gold-6d7ac0c1e3c4e507d13a7079`
(pointer generation 13) passed 223 DQ checks and serves 8 Facts. Query
`trade_total_canonical` for SOURCE_TOTAL and `trade_national_reported` for
the distinct local monthly series. Query `trade_partner_canonical` for
FAOSTAT and `trade_hs_source` for raw HS scope; the narrower
`trade_hs_total_mode_country` view excludes transport breakdowns and
unmapped/aggregate partners. Do not add annual and monthly rows together,
or infer COFFEE/PEPPER from HS 0901/0904. See
`docs/PIPELINE_REVIEW_20261105.md` for current counts and remaining limits.

Gold DQ now rejects NULL `is_canonical`, rows with both/neither native price and
index, and missing stock conversion inputs or output. A candidate built with the
older transform fingerprint cannot be published with the revised code; use a new
Gold manifest ID and run all stages again.
On a fresh catalog, the first candidate creates `gold_meta` and its run manifest;
after DQ and semantic view validation it creates the initial pointer at generation
zero. Later publications advance that pointer. A missing pointer alongside an
already published release is treated as an error instead of silently selecting
another batch.

## District and provincial analysis after the 2026-11-05 crosswalk release

The active release `gold-4e7c029665c8969015748a0d` (pointer generation 14)
passed 225 Gold DQ checks. The official NSO district crosswalk is reviewed by
province, source name and source calendar year in
`spark/gold/config/nso_district_crosswalk.json`. Query
`gold_semantic.fact_production_provincial_district_official` for rows with a
verified historical district code. It serves 705 Fact rows and 41 distinct
official district codes. Three Fact rows for Đạ Huoai in 2024 remain in the
source Fact because the annual record cannot safely be assigned to either
boundary that existed during that year. They are intentionally absent from the
official district view. Gold DQ blocks any new district Fact row without an
explicit source-name/year crosswalk entry.

`gold_semantic.fact_production_provincial_canonical` returns NULL per measure
when different sources disagree at the same business grain. Currently 26
area-planted and eight production groups conflict. Analyze the corresponding
`fact_production_provincial` rows with `record_source_code` when source-specific
results are needed; do not sum those alternative rows together. The 16 European
Union stock observations remain excluded from `fact_coffee_stock` because the
source aggregate is not a country and no verified M49 member mapping exists.

These rules make the declared Gold scopes available for analysis without
inventing district boundaries, source precedence or aggregate country codes.
The local Iceberg REST fixture still persists metadata in SQLite and is not a
production catalog. A catalog migration must retain all published snapshots,
candidate namespaces and serving views; do not register the same writable
tables in two catalogs and continue writes through both.
