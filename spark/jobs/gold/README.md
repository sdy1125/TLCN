# Gold Spark jobs

Run `candidate.py` stages `inspect`, `foundation`, `dimensions`, `facts`, and `validate` with one manifest ID, then use `publish_candidate.py`. The `gold_build` Airflow DAG executes the same release flow through the authenticated runner. See `docs/gold/RUNBOOK.md` for commands, checks, and rollback.
