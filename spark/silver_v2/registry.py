"""Reviewed Bronze source definitions for the Silver V2 pipeline."""

import json
from pathlib import Path


CONFIG = Path(__file__).parent / "config"


def load(name):
    return json.loads((CONFIG / (name + ".json")).read_text(encoding="utf-8"))


SOURCES = load("sources")["datasets"]


def select_datasets(requested=None):
    if requested is None:
        return list(SOURCES)
    if isinstance(requested, str):
        requested = [requested]
    if (not isinstance(requested, list) or not requested
            or not all(isinstance(item, str) for item in requested)):
        raise ValueError("dataset_id must be a nonempty string or list of strings")
    if set(requested) - SOURCES.keys():
        raise ValueError("Unknown dataset_id")
    return list(dict.fromkeys(requested))
