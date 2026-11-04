"""Validate an optional complete Drive-to-local source handoff."""

import hashlib
import json
from pathlib import Path


def verify_handoff(raw_root, sources):
    root = Path(raw_root).resolve()
    if (root / ".drive-handoff.in-progress").exists():
        raise ValueError("Drive handoff is still in progress")
    path = root / ".drive-handoff.json"
    if not path.exists():
        return False  # Legacy local data predates the Drive handoff script.
    state = json.loads(path.read_text(encoding="utf-8-sig"))
    expected = {str(source["relative_path"]) for source in sources}
    if not isinstance(state, dict) or set(state) != expected:
        raise ValueError("Drive handoff does not cover the complete Bronze manifest")
    for relative in sorted(expected):
        file = (root / relative).resolve()
        if not file.is_relative_to(root) or not file.is_file():
            raise ValueError(f"Drive handoff source is missing: {relative}")
        recorded = state[relative]
        if not isinstance(recorded, dict) or not isinstance(
                recorded.get("sha256"), str):
            raise ValueError(f"Drive handoff checksum is missing: {relative}")
        digest = hashlib.sha256()
        with file.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        if digest.hexdigest() != recorded["sha256"].lower():
            raise ValueError(f"Drive handoff source changed: {relative}")
    return True
