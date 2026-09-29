"""Manifest contract for local ``data/raw`` Bronze sources."""

from __future__ import annotations

import csv
import re
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable, Mapping


class ManifestError(ValueError):
    """Raised when the local raw manifest is unsafe or inconsistent."""


_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9_]{2,127}$")
_SOURCE_SYSTEM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,63}$")
_SUPPORTED_FORMATS = {"csv", "xlsx"}
_REQUIRED_FIELDS = {
    "dataset_id",
    "source_system",
    "relative_path",
    "source_file_name",
    "file_format",
    "enabled",
}


@dataclass(frozen=True, slots=True)
class SourceSpec:
    """One independently ingestible file underneath the configured raw root."""

    dataset_id: str
    source_system: str
    relative_path: str
    source_file_name: str
    file_format: str
    enabled: bool
    source_group: str | None = None
    commodity: str | None = None
    frequency: str | None = None
    bronze_dataset: str | None = None
    notes: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "SourceSpec":
        return cls(**value)  # type: ignore[arg-type]


def read_manifest_rows(path: str | Path) -> list[dict[str, str]]:
    """Read manifest CSV rows without touching any source files."""

    manifest_path = Path(path)
    if not manifest_path.is_file():
        raise ManifestError(f"Manifest does not exist: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ManifestError("Manifest has no header")
        missing = _REQUIRED_FIELDS.difference(reader.fieldnames)
        if missing:
            raise ManifestError(
                f"Manifest is missing required columns: {', '.join(sorted(missing))}"
            )
        return [dict(row) for row in reader]


def validate_manifest_rows(rows: Iterable[Mapping[str, object]]) -> list[SourceSpec]:
    """Validate control metadata and reject unsafe/ambiguous source identities."""

    sources: list[SourceSpec] = []
    dataset_ids: set[str] = set()
    relative_paths: set[str] = set()

    for row_number, row in enumerate(rows, start=2):
        values = {key: _clean(row.get(key)) for key in _REQUIRED_FIELDS}
        missing = [key for key, value in values.items() if not value]
        if missing:
            raise ManifestError(
                f"Manifest row {row_number} has empty required fields: "
                f"{', '.join(sorted(missing))}"
            )

        dataset_id = values["dataset_id"]
        if not _IDENTIFIER_RE.fullmatch(dataset_id):
            raise ManifestError(
                f"Manifest row {row_number} has invalid dataset_id {dataset_id!r}"
            )
        source_system = values["source_system"]
        if not _SOURCE_SYSTEM_RE.fullmatch(source_system):
            raise ManifestError(
                f"Manifest row {row_number} has invalid source_system {source_system!r}"
            )

        relative_path = values["relative_path"].replace("\\", "/")
        path = PurePosixPath(relative_path)
        if path.is_absolute() or ".." in path.parts or relative_path.endswith("/"):
            raise ManifestError(
                f"Manifest row {row_number} has unsafe relative_path {relative_path!r}"
            )

        source_file_name = values["source_file_name"]
        if PurePosixPath(source_file_name).name != source_file_name:
            raise ManifestError(
                f"Manifest row {row_number} has unsafe source_file_name "
                f"{source_file_name!r}"
            )
        if path.name != source_file_name:
            raise ManifestError(
                f"Manifest row {row_number} source_file_name does not match "
                f"relative_path basename"
            )

        file_format = values["file_format"].lower().lstrip(".")
        if file_format not in _SUPPORTED_FORMATS:
            raise ManifestError(
                f"Manifest row {row_number} has unsupported file_format {file_format!r}"
            )
        if path.suffix.lower() != f".{file_format}":
            raise ManifestError(
                f"Manifest row {row_number} format {file_format!r} does not match "
                f"relative_path {relative_path!r}"
            )

        if dataset_id in dataset_ids:
            raise ManifestError(f"Duplicate dataset_id: {dataset_id}")
        if relative_path in relative_paths:
            raise ManifestError(f"Duplicate relative_path: {relative_path}")
        dataset_ids.add(dataset_id)
        relative_paths.add(relative_path)

        bronze_dataset = _optional(row.get("bronze_dataset"))
        if bronze_dataset and not _IDENTIFIER_RE.fullmatch(bronze_dataset):
            raise ManifestError(
                f"Manifest row {row_number} has invalid bronze_dataset "
                f"{bronze_dataset!r}"
            )
        sources.append(
            SourceSpec(
                dataset_id=dataset_id,
                source_system=source_system,
                relative_path=relative_path,
                source_file_name=source_file_name,
                file_format=file_format,
                enabled=_parse_bool(values["enabled"], row_number),
                source_group=_optional(row.get("source_group")),
                commodity=_optional(row.get("commodity")),
                frequency=_optional(row.get("frequency")),
                bronze_dataset=bronze_dataset,
                notes=_optional(row.get("notes")),
            )
        )

    if not sources:
        raise ManifestError("Manifest contains no source rows")
    return sources


def load_manifest(path: str | Path) -> list[SourceSpec]:
    return validate_manifest_rows(read_manifest_rows(path))


def _clean(value: object | None) -> str:
    return "" if value is None else str(value).strip()


def _optional(value: object | None) -> str | None:
    cleaned = _clean(value)
    return cleaned or None


def _parse_bool(value: object | None, row_number: int) -> bool:
    cleaned = _clean(value).lower()
    if cleaned in {"true", "1", "yes"}:
        return True
    if cleaned in {"false", "0", "no"}:
        return False
    raise ManifestError(
        f"Manifest row {row_number} has invalid enabled value {value!r}"
    )
