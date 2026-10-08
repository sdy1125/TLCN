"""Load reviewed, versioned rules. Their digest participates in processing state."""

import hashlib
import json
from pathlib import Path

CONFIG = Path(__file__).parent / "config"


def load(name):
    return json.loads((CONFIG / (name + ".json")).read_text(encoding="utf-8"))


SOURCES = load("sources")["datasets"]
CONTRACTS = load("contracts")


def transform_version():
    root = Path(__file__).parent
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.suffix in {".py", ".json"}):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    for name in ["requirements.txt", "Dockerfile"]:
        dependency = root.parent / name
        if dependency.exists():
            digest.update(dependency.read_bytes())
    return "silver-v1-" + digest.hexdigest()[:20]


def select_datasets(requested=None):
    if requested is None:
        return list(SOURCES)
    if isinstance(requested, str):
        requested = [requested]
    if (
        not isinstance(requested, list)
        or not requested
        or not all(isinstance(x, str) for x in requested)
    ):
        raise ValueError("dataset_id must be a nonempty string or list of strings")
    if set(requested) - SOURCES.keys():
        raise ValueError("Unknown dataset_id")
    return list(dict.fromkeys(requested))
