# Gold

The Gold model has 8 Dimensions, 1 trade product Bridge and 8 Facts. `contracts.py` defines Iceberg schemas and guards all locations under `s3://warehouse/gold/`. `source.py` resolves an active Silver batch under `s3://warehouse/silver/`, verifies eight published core snapshots, pins reference/decision snapshots at activation, and reads published canonical WINNER observations.

`build_dimensions.py` loads conformed Dimensions and the Bridge. `build_facts.py` pivots compatible measures, writes canonical Facts and observation-level lineage. `quality.py` validates 121 rules, records input/output snapshot manifests and activates the Gold pointer only after all blocking checks pass. See `reports/gold/` for counts, decisions, exceptions and recovery guidance.
