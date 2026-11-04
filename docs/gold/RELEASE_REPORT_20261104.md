# Bronze → Silver → Gold implementation and release evidence (2026-11-04)

## Deployment state

- Bronze: 26 raw files remain controlled by `data/raw_manifest.csv` and immutable
  MinIO Bronze objects. The Gold work did not alter Bronze data or service volumes.
- Silver v2: active batch `silver-layered-20261030-1650`; 710,965 canonical WINNER
  observations across eight domains. Gold pins its core, reference and metadata
  snapshots from that release, including conflict and geography alias evidence.
- Gold v2: active candidate `gold-candidate-20261104-c`, scope version
  `scope-5100fb5c64927fd2`, status `ACTIVE_WITH_EXCEPTIONS`. The first candidate
  caught a required country key defect in 16 EU stock observations; a later candidate
  was rebuilt with the reviewed exclusion. The original Gold release and candidate
  `gold-candidate-20261104-b` remain available to the publication pointer.

## Work completed

1. Added a versioned scope contract for all eight Facts: source datasets, grain,
   measures, periods, geography, units, analytic scope and approved exclusions.
   Unknown data blocks publication. A transient scope rule classifies every Silver
   WINNER; DQ anti-joins the full set with Fact lineage and approved exclusions.
2. Excluded 450 local price observations until time-aware official province mapping
   is verified, 406 NATIONAL_REPORTED trade observations until equivalence with
   SOURCE_TOTAL is proven, and 16 EU stock aggregates lacking a verified M49 member.
   No country, price conversion, geography or bridge weight was fabricated.
3. Replaced Fact hashing of `(code, NULL)` for price series and weather locations
   with a temporal join against the built Dimension. Full observation periods must
   fit exactly one nonoverlapping Dimension version. Added checks for required FK
   NULL, orphan keys, business grain duplicates, lineage and bridge weights.
4. Added immutable Gold run manifests, isolated candidate tables, 206 DQ gates,
   batch-specific exception registry and a one-row publication pointer. Trino
   semantic views pin Iceberg snapshots for each release and expose only the
   pointed release. Four guarded KPI views constrain period, scenario, source,
   measure and units. Rollback uses the same pointer.
5. Rewired `gold_build` and the authenticated Spark runner to the candidate flow.
   Removed the five old Gold job entrypoints that wrote to shared serving tables.
   Published candidates reject later build stages.

## Reconciliation and test evidence

| Check | Result |
| --- | ---: |
| Silver canonical WINNER | 710,965 |
| Gold Fact lineage | 710,093 |
| Approved exclusions: local price | 450 |
| Approved exclusions: NATIONAL_REPORTED trade | 406 |
| Approved exclusions: EU stock aggregate | 16 |
| Unclassified Silver WINNER | 0 |
| Gold DQ | 206 PASS, 0 FAIL |
| Gold model | 8 Dimension, 1 Bridge, 8 Fact |
| Gold Fact coffee stock | 434 rows |
| Gold Fact daily weather | 614,628 rows |
| Gold release exception groups | 9 |

The host suite passed 52 tests (ten skipped because optional runtime packages
are absent on the host). Nine synthetic Spark tests passed inside the Spark
container: six temporal version cases and three scope inclusion/exclusion/blocking
cases. Python compilation, Compose configuration, `git diff --check`, Airflow DAG
import validation, and an authenticated runner API smoke check passed. The semantic
views returned 434 stock rows, 614,628 weather rows, 416 actual stock KPI groups,
9,262 national production KPI groups, 22,008 export value KPI groups and 11,574
native price-series KPI groups for the first validated candidate. Views include
`FOR VERSION AS OF` snapshot pins. The publication pointer was switched to the
legacy release and back to candidate B; the stock Fact count changed 450 → 434
without modifying serving tables. Candidate C was then published independently.

## Accepted limitations and next evidence

The nine release exception groups are measured from this Silver batch, not fixed
thresholds. They include 21,262 inactive Comtrade observations, 2,142 district
observations with source identity only, 44 pending geography aliases, eight pending
product mappings, six open price conflicts, and 614,754 weather observations with
unverified solar conversion. The excluded 450/406/16 observations are also listed
in the exception registry. Analysis views omit unverified or incompatible measures.
Official district/province crosswalks, Comtrade HS/valuation rules, product allocation
weights, price conflict evidence and NASA POWER unit provenance still require source
documentation and review. A Gold build from a *new Silver release with changed source
data* has not yet been exercised because no such published Silver batch is present.
The demonstrated multi-batch cutover covers two independent Gold candidates built
from the same pinned Silver input with a revised scope contract.

Use `docs/gold/RUNBOOK.md` for normal build, verification and rollback commands.
