"""Idempotent local-raw to MinIO Bronze ingestion service."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import re
import uuid
import zipfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .manifest import SourceSpec

LOGGER = logging.getLogger(__name__)
_CHUNK_SIZE = 1024 * 1024
_NOT_FOUND_CODES = {"NoSuchKey", "NoSuchObject", "NoSuchBucket", "NotFound"}


class IngestionError(RuntimeError):
    """Base ingestion failure."""


class TechnicalValidationError(IngestionError):
    """A local source failed a technical, non-business validation."""


@dataclass(frozen=True, slots=True)
class TechnicalProfile:
    file_size_bytes: int
    source_modified_time: str
    source_mtime_ns: int
    checksum_sha256: str
    row_count: int | None
    schema_hash: str | None


@dataclass(slots=True)
class IngestionRecord:
    ingestion_id: str
    dataset_id: str
    source_system: str
    source_file_path: str
    source_file_name: str
    file_format: str
    file_size_bytes: int | None
    source_modified_time: str | None
    checksum_sha256: str | None
    bronze_path: str | None
    load_type: str
    change_state: str
    ingested_at: str
    airflow_run_id: str
    status: str
    error_message: str | None
    row_count: int | None = None
    schema_hash: str | None = None
    source_group: str | None = None
    commodity: str | None = None
    frequency: str | None = None
    error_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class IngestionService:
    """Validate and publish one configured local source file unchanged."""

    def __init__(
        self,
        minio_client: Any,
        *,
        raw_root: str | Path,
        bucket: str = "bronze",
    ) -> None:
        self.client = minio_client
        self.raw_root = Path(raw_root)
        self.bucket = bucket

    def ingest(
        self,
        source: SourceSpec,
        *,
        airflow_run_id: str,
        ingestion_date: date | None = None,
    ) -> dict[str, object]:
        """Ingest one manifest entry using file-version incremental semantics."""

        if not source.enabled:
            raise IngestionError(f"Dataset is disabled: {source.dataset_id}")

        run_date = ingestion_date or datetime.now(timezone.utc).date()
        ingested_at = datetime.now(timezone.utc).isoformat()
        ingestion_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"tlcn:{airflow_run_id}:{source.dataset_id}",
            )
        )
        record = IngestionRecord(
            ingestion_id=ingestion_id,
            dataset_id=source.dataset_id,
            source_system=source.source_system,
            source_file_path=source.relative_path,
            source_file_name=source.source_file_name,
            file_format=source.file_format,
            file_size_bytes=None,
            source_modified_time=None,
            checksum_sha256=None,
            bronze_path=None,
            load_type="FULL",
            change_state="NEW",
            ingested_at=ingested_at,
            airflow_run_id=airflow_run_id,
            status="FAILED",
            error_message=None,
            source_group=source.source_group,
            commodity=source.commodity,
            frequency=source.frequency,
        )
        staging_key = f"_staging/{ingestion_id}/{source.source_file_name}.part"
        staged_in_minio = False

        LOGGER.info(
            "bronze_ingestion event=START dataset_id=%s source_path=%s "
            "airflow_run_id=%s ingestion_id=%s",
            source.dataset_id,
            source.relative_path,
            airflow_run_id,
            ingestion_id,
        )

        try:
            latest = self._read_json_if_exists(_latest_success_key(source.dataset_id))
            if latest is not None:
                record.load_type = "INCREMENTAL"
                record.change_state = "CHANGED"

            source_path = resolve_source_path(self.raw_root, source)
            profile = inspect_source(source_path, source.file_format)
            record.file_size_bytes = profile.file_size_bytes
            record.source_modified_time = profile.source_modified_time
            record.checksum_sha256 = profile.checksum_sha256
            record.row_count = profile.row_count
            record.schema_hash = profile.schema_hash

            checksum_key = _success_index_key(
                source.dataset_id, profile.checksum_sha256
            )
            if (
                latest is not None
                and latest.get("ingestion_id") == ingestion_id
                and latest.get("checksum_sha256") == profile.checksum_sha256
                and latest.get("status") == "SUCCESS"
            ):
                self._write_json(checksum_key, latest)
                self._record_value(latest)
                LOGGER.info(
                    "bronze_ingestion event=RETRY_REPAIR dataset_id=%s "
                    "status=SUCCESS bronze_path=%s checksum=%s",
                    source.dataset_id,
                    latest.get("bronze_path"),
                    profile.checksum_sha256,
                )
                return latest

            prior_version = self._read_json_if_exists(checksum_key)
            if prior_version is not None:
                if prior_version.get("ingestion_id") == ingestion_id:
                    self._write_json(
                        _latest_success_key(source.dataset_id), prior_version
                    )
                    self._record_value(prior_version)
                    LOGGER.info(
                        "bronze_ingestion event=RETRY_RESUME dataset_id=%s "
                        "state=%s status=%s bronze_path=%s checksum=%s",
                        source.dataset_id,
                        prior_version.get("change_state"),
                        prior_version.get("status"),
                        prior_version.get("bronze_path"),
                        profile.checksum_sha256,
                    )
                    return prior_version

                record.change_state = "UNCHANGED"
                record.load_type = "INCREMENTAL"
                record.status = "SKIPPED"
                record.bronze_path = _optional_string(
                    prior_version.get("bronze_path")
                )
                self._record_result(record)
                LOGGER.info(
                    "bronze_ingestion event=COMPLETE dataset_id=%s source_path=%s "
                    "state=UNCHANGED load_type=INCREMENTAL status=SKIPPED "
                    "bronze_path=%s checksum=%s",
                    source.dataset_id,
                    source.relative_path,
                    record.bronze_path,
                    profile.checksum_sha256,
                )
                return record.to_dict()

            final_key = _final_object_key(source, run_date, ingestion_id)
            record.bronze_path = f"s3://{self.bucket}/{final_key}"
            self.client.fput_object(self.bucket, staging_key, str(source_path))
            staged_in_minio = True
            _ensure_source_unchanged(source_path, profile)
            self._copy_object(staging_key, final_key)
            self.client.remove_object(self.bucket, staging_key)
            staged_in_minio = False

            record.status = "SUCCESS"
            success_value = record.to_dict()
            self._write_json(checksum_key, success_value)
            self._write_json(_latest_success_key(source.dataset_id), success_value)
            self._record_result(record)
            LOGGER.info(
                "bronze_ingestion event=COMPLETE dataset_id=%s source_path=%s "
                "state=%s load_type=%s status=SUCCESS bronze_path=%s size=%s "
                "checksum=%s",
                source.dataset_id,
                source.relative_path,
                record.change_state,
                record.load_type,
                record.bronze_path,
                profile.file_size_bytes,
                profile.checksum_sha256,
            )
            return success_value
        except Exception as exc:
            if staged_in_minio:
                try:
                    self.client.remove_object(self.bucket, staging_key)
                except Exception:
                    LOGGER.exception(
                        "Could not clean staging object bucket=%s key=%s",
                        self.bucket,
                        staging_key,
                    )
            record.status = "FAILED"
            record.error_type = type(exc).__name__
            record.error_message = str(exc)[:2000]
            try:
                self._record_result(record)
            except Exception:
                LOGGER.exception(
                    "Could not persist FAILED result dataset_id=%s run_id=%s",
                    source.dataset_id,
                    airflow_run_id,
                )
            LOGGER.exception(
                "bronze_ingestion event=COMPLETE dataset_id=%s source_path=%s "
                "state=%s load_type=%s status=FAILED checksum=%s reason=%s",
                source.dataset_id,
                source.relative_path,
                record.change_state,
                record.load_type,
                record.checksum_sha256,
                record.error_message,
            )
            raise

    def _copy_object(self, source_key: str, destination_key: str) -> None:
        from minio.commonconfig import CopySource

        self.client.copy_object(
            self.bucket,
            destination_key,
            CopySource(self.bucket, source_key),
        )

    def _record_result(self, record: IngestionRecord) -> None:
        self._record_value(record.to_dict())

    def _record_value(self, value: dict[str, object]) -> None:
        self._write_json(_history_key(value), value)
        self._write_json(
            _run_result_key(
                str(value["airflow_run_id"]), str(value["dataset_id"])
            ),
            value,
        )

    def _write_json(self, object_key: str, value: dict[str, object]) -> None:
        payload = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        self.client.put_object(
            self.bucket,
            object_key,
            io.BytesIO(payload),
            len(payload),
            content_type="application/json",
        )

    def _read_json_if_exists(self, object_key: str) -> dict[str, object] | None:
        try:
            response = self.client.get_object(self.bucket, object_key)
        except KeyError:
            return None
        except Exception as exc:
            if getattr(exc, "code", None) in _NOT_FOUND_CODES:
                return None
            raise
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            if hasattr(response, "release_conn"):
                response.release_conn()


def resolve_source_path(raw_root: str | Path, source: SourceSpec) -> Path:
    """Resolve a manifest path and guarantee it remains below ``raw_root``."""

    root = Path(raw_root).resolve(strict=True)
    candidate = (root / source.relative_path).resolve(strict=False)
    if not candidate.is_relative_to(root):
        raise TechnicalValidationError(
            f"Source path escapes configured raw root: {source.relative_path}"
        )
    if not candidate.exists():
        raise FileNotFoundError(
            f"Configured source file does not exist: {source.relative_path}"
        )
    if not candidate.is_file():
        raise TechnicalValidationError(
            f"Configured source is not a regular file: {source.relative_path}"
        )
    return candidate


def inspect_source(path: Path, file_format: str) -> TechnicalProfile:
    """Stream technical checks without modifying or fully buffering the source."""

    before = path.stat()
    if before.st_size == 0:
        raise TechnicalValidationError("Source file is empty")
    size, checksum = _size_and_checksum(path)
    if file_format == "csv":
        row_count, schema_hash = _inspect_csv(path)
    elif file_format == "xlsx":
        row_count, schema_hash = _inspect_xlsx(path)
    else:
        raise TechnicalValidationError(f"Unsupported file format: {file_format}")
    after = path.stat()
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or size != after.st_size
    ):
        raise TechnicalValidationError("Source file changed during inspection")
    modified_at = datetime.fromtimestamp(after.st_mtime, tz=timezone.utc).isoformat()
    return TechnicalProfile(
        file_size_bytes=size,
        source_modified_time=modified_at,
        source_mtime_ns=after.st_mtime_ns,
        checksum_sha256=checksum,
        row_count=row_count,
        schema_hash=schema_hash,
    )


def _ensure_source_unchanged(path: Path, profile: TechnicalProfile) -> None:
    current = path.stat()
    if (
        current.st_size != profile.file_size_bytes
        or current.st_mtime_ns != profile.source_mtime_ns
    ):
        raise TechnicalValidationError("Source file changed during upload")


def _size_and_checksum(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK_SIZE):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _inspect_csv(path: Path) -> tuple[int, str]:
    csv.field_size_limit(16 * 1024 * 1024)
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header or not any(field.strip() for field in header):
                raise TechnicalValidationError("CSV has no usable header")
            first_cell = header[0].lstrip().lower()
            if first_cell.startswith(("<!doctype html", "<html")):
                raise TechnicalValidationError("CSV source contains an HTML page")
            row_count = sum(1 for _ in reader)
    except UnicodeDecodeError as exc:
        raise TechnicalValidationError("CSV is not valid UTF-8") from exc
    except csv.Error as exc:
        raise TechnicalValidationError(f"CSV cannot be parsed: {exc}") from exc
    schema_payload = json.dumps(header, ensure_ascii=False, separators=(",", ":"))
    return row_count, hashlib.sha256(schema_payload.encode("utf-8")).hexdigest()


def _inspect_xlsx(path: Path) -> tuple[None, str]:
    try:
        with zipfile.ZipFile(path) as workbook:
            bad_member = workbook.testzip()
            if bad_member is not None:
                raise TechnicalValidationError(
                    f"XLSX contains corrupt member: {bad_member}"
                )
            names = set(workbook.namelist())
            required = {"[Content_Types].xml", "xl/workbook.xml"}
            if required.difference(names) or not any(
                name.startswith("xl/worksheets/") and name.endswith(".xml")
                for name in names
            ):
                raise TechnicalValidationError("XLSX workbook structure is incomplete")
    except zipfile.BadZipFile as exc:
        raise TechnicalValidationError("XLSX is not a valid ZIP workbook") from exc
    structure = "\n".join(sorted(names))
    return None, hashlib.sha256(structure.encode("utf-8")).hexdigest()


def build_minio_client() -> Any:
    """Build a MinIO client exclusively from environment secret pointers."""

    from minio import Minio

    endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    if not access_key or not secret_key:
        raise IngestionError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY are required")
    secure = os.environ.get("MINIO_SECURE", "false").lower() in {"1", "true", "yes"}
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def list_run_results(
    client: Any, bucket: str, airflow_run_id: str
) -> list[dict[str, object]]:
    """Return the latest final result for each dataset selected in one DAG run."""

    prefix = f"_metadata/run_results/{_run_key(airflow_run_id)}/"
    records: list[dict[str, object]] = []
    for item in client.list_objects(bucket, prefix=prefix, recursive=True):
        response = client.get_object(bucket, item.object_name)
        try:
            records.append(json.loads(response.read().decode("utf-8")))
        finally:
            response.close()
            if hasattr(response, "release_conn"):
                response.release_conn()
    return records


def _history_key(value: dict[str, object]) -> str:
    event_time = re.sub(
        r"[^0-9A-Za-z_.-]+", "-", str(value["ingested_at"])
    )
    return (
        f"_metadata/ingestion_history/{value['dataset_id']}/"
        f"{event_time}-{value['ingestion_id']}-{str(value['status']).lower()}.json"
    )


def _run_result_key(airflow_run_id: str, dataset_id: str) -> str:
    return f"_metadata/run_results/{_run_key(airflow_run_id)}/{dataset_id}.json"


def _run_key(airflow_run_id: str) -> str:
    readable = re.sub(r"[^A-Za-z0-9_.-]+", "_", airflow_run_id).strip("._")[:80]
    digest = hashlib.sha256(airflow_run_id.encode("utf-8")).hexdigest()[:12]
    return f"{readable or 'run'}-{digest}"


def _success_index_key(dataset_id: str, checksum: str) -> str:
    return f"_metadata/success_index/{dataset_id}/{checksum}.json"


def _latest_success_key(dataset_id: str) -> str:
    return f"_metadata/latest_success/{dataset_id}.json"


def _final_object_key(
    source: SourceSpec, run_date: date, ingestion_id: str
) -> str:
    bronze_dataset = source.bronze_dataset or source.dataset_id
    return (
        f"{source.source_system}/{bronze_dataset}/"
        f"ingestion_date={run_date.isoformat()}/ingestion_id={ingestion_id}/"
        f"{source.source_file_name}"
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
