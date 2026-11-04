# Gold release runbook

Gold consumes an ACTIVE Silver release. Bronze is immutable and Silver is published using
`docs/silver/RUNBOOK.md`. Run Gold from the repository root after Silver activation.

## Contract and analytical boundary

`spark/gold/scope_contract.json` lists every accepted Silver domain and source dataset.
`spark/gold/scope.py` classifies each published WINNER during a run. DQ compares the
complete WINNER set with Fact lineage and approved exclusions; there is no persisted
per-WINNER status table. An unknown source, unknown trade scope, or unmapped country
blocks publication. Current approved exclusions are local VND prices without a verified
time-aware geography, NATIONAL_REPORTED trade without an equivalence rule to
SOURCE_TOTAL, and European Union stock aggregates without a verified M49 member.
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
that batch. BI reads `iceberg.gold_semantic.*`, whose views are pinned to release
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
