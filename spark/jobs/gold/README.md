# Gold Spark jobs

Run in order after a published Silver release: `inspect_input.py`, `foundation.py`, `dimensions.py`, `facts.py`, `publish.py`. The `gold_build` Airflow DAG executes the same fixed sequence through the authenticated runner. Reruns use stable keys and a pinned Silver release. See `reports/gold/GOLD_RUNBOOK.md` for exact Compose commands and publication checks.
