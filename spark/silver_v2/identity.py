"""Stable identities for a Bronze release and each atomic observation."""

import hashlib
import json
import re


def digest(*parts):
    return hashlib.sha256(
        json.dumps(parts, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def observation_set_id(dataset_id, release_identifier, file_sha256):
    if not re.fullmatch(r"[a-f0-9]{64}", file_sha256):
        raise ValueError("Invalid source SHA-256")
    return digest(dataset_id, release_identifier or "UNVERSIONED", file_sha256)


def observation_id(dataset_id, set_id, source_row_locator, metric_code):
    if not source_row_locator or not metric_code:
        raise ValueError("Source row locator and metric are required")
    return digest(dataset_id, set_id, source_row_locator, metric_code)


def canonical_business_key(**parts):
    """Caller supplies the complete approved natural grain, including scope."""
    return digest(*sorted(parts.items()))
