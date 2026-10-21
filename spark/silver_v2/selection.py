"""Policy-driven canonical decisions; conflicting source values never win."""

from collections import defaultdict
from decimal import Decimal


def decide(candidates, policy):
    """Return decisions/conflicts for one policy context.

    Policy fields: source_priority mapping, allowed_statuses, and key_fields.
    A duplicate with the same value keeps one deterministic locator and marks
    all other rows LOSER. Different values at the best rank have zero winners.
    """
    priority = policy["source_priority"]
    allowed = set(policy["allowed_statuses"])
    keys = tuple(policy["key_fields"])
    grouped = defaultdict(list)
    for row in candidates:
        grouped[tuple(row.get(k) for k in keys)].append(row)
    decisions, conflicts = [], []
    for key, group in grouped.items():
        eligible = [
            row for row in group
            if row["dataset_id"] in priority
            and row.get("value_status") in allowed
            and row.get("quality_status") == "VALID"
        ]
        if not eligible:
            decisions.extend((row["observation_id"], "EXCLUDED", "ineligible") for row in group)
            continue
        best_rank = min(priority[row["dataset_id"]] for row in eligible)
        best = [row for row in eligible if priority[row["dataset_id"]] == best_rank]
        values = {Decimal(str(row["value_canonical"])) if row.get("value_canonical") is not None else None for row in best}
        if len(values) > 1:
            conflicts.append({"key": key, "candidate_ids": sorted(row["observation_id"] for row in best), "reason": "CONFLICTING_VALUES"})
            decisions.extend((row["observation_id"], "EXCLUDED", "unresolved_conflict") for row in group)
            continue
        winner = min(best, key=lambda row: (row["observation_id"], row["source_row_locator"]))
        for row in group:
            status = "WINNER" if row["observation_id"] == winner["observation_id"] else "LOSER" if row in eligible else "EXCLUDED"
            decisions.append((row["observation_id"], status, "approved_policy" if status == "WINNER" else "not_selected"))
    return decisions, conflicts
