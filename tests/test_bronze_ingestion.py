from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
import zipfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace

from bronze_ingestion.manifest import (
    ManifestError,
    SourceSpec,
    load_manifest,
    validate_manifest_rows,
)
from bronze_ingestion.service import (
    IngestionService,
    TechnicalValidationError,
    inspect_source,
    list_run_results,
    resolve_source_path,
)


ROOT = Path(__file__).resolve().parents[1]


class MemoryResponse(io.BytesIO):
    def release_conn(self) -> None:
        pass


class FakeMinio:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.fail_copies = 0
        self.fail_put_contains_once: str | None = None

    def put_object(
        self,
        bucket: str,
        key: str,
        data: io.BytesIO,
        length: int,
        **_: object,
    ) -> None:
        if self.fail_put_contains_once and self.fail_put_contains_once in key:
            marker = self.fail_put_contains_once
            self.fail_put_contains_once = None
            raise OSError(f"simulated metadata write failure: {marker}")
        self.objects[(bucket, key)] = data.read(length)

    def get_object(self, bucket: str, key: str) -> MemoryResponse:
        try:
            return MemoryResponse(self.objects[(bucket, key)])
        except KeyError as exc:
            raise KeyError(key) from exc

    def fput_object(self, bucket: str, key: str, path: str) -> None:
        self.objects[(bucket, key)] = Path(path).read_bytes()

    def copy_key(self, bucket: str, destination: str, source: str) -> None:
        if self.fail_copies:
            self.fail_copies -= 1
            raise OSError("simulated transient copy failure")
        self.objects[(bucket, destination)] = self.objects[(bucket, source)]

    def remove_object(self, bucket: str, key: str) -> None:
        self.objects.pop((bucket, key), None)

    def list_objects(self, bucket: str, *, prefix: str, recursive: bool):
        del recursive
        return [
            SimpleNamespace(object_name=key)
            for item_bucket, key in self.objects
            if item_bucket == bucket and key.startswith(prefix)
        ]


class LocalTestService(IngestionService):
    """Use in-memory object storage without importing the MinIO package."""

    def _copy_object(self, source_key: str, destination_key: str) -> None:
        self.client.copy_key(self.bucket, destination_key, source_key)


def source_spec(
    *,
    dataset_id: str = "faostat_production_coffee",
    relative_path: str = "faostat/production/01-coffee.csv",
) -> SourceSpec:
    return SourceSpec(
        dataset_id=dataset_id,
        source_system="faostat",
        relative_path=relative_path,
        source_file_name=Path(relative_path).name,
        file_format="csv",
        enabled=True,
        source_group="01",
        commodity="coffee",
        frequency="annual",
        bronze_dataset=dataset_id,
    )


class ManifestTests(unittest.TestCase):
    def test_project_manifest_maps_all_26_existing_raw_sources(self) -> None:
        sources = load_manifest(ROOT / "data" / "raw_manifest.csv")
        self.assertEqual(len(sources), 26)
        self.assertEqual(len({source.dataset_id for source in sources}), 26)
        self.assertTrue(all(source.enabled for source in sources))
        missing = [
            source.relative_path
            for source in sources
            if not (ROOT / "data" / "raw" / source.relative_path).is_file()
        ]
        self.assertEqual(missing, [])

    def test_drive_and_ingestion_manifests_share_paths(self) -> None:
        sources = load_manifest(ROOT / "data" / "raw_manifest.csv")
        expected = {source.relative_path for source in sources}
        with (ROOT / "data" / "drive_manifest.csv").open(encoding="utf-8") as handle:
            actual = {row["relative_path"] for row in csv.DictReader(handle)}
        self.assertEqual(actual, expected)

    def test_duplicate_dataset_id_is_rejected(self) -> None:
        row = {
            "dataset_id": "valid_key",
            "source_system": "faostat",
            "relative_path": "one.csv",
            "source_file_name": "one.csv",
            "file_format": "csv",
            "enabled": "true",
        }
        duplicate = dict(row, relative_path="two.csv", source_file_name="two.csv")
        with self.assertRaisesRegex(ManifestError, "Duplicate dataset_id"):
            validate_manifest_rows([row, duplicate])

    def test_unsafe_path_and_format_mismatch_are_rejected(self) -> None:
        row = {
            "dataset_id": "valid_key",
            "source_system": "faostat",
            "relative_path": "../one.csv",
            "source_file_name": "one.csv",
            "file_format": "csv",
            "enabled": "true",
        }
        with self.assertRaisesRegex(ManifestError, "unsafe relative_path"):
            validate_manifest_rows([row])
        mismatch = dict(
            row,
            relative_path="one.xlsx",
            source_file_name="one.xlsx",
        )
        with self.assertRaisesRegex(ManifestError, "does not match"):
            validate_manifest_rows([mismatch])

    def test_disabled_entry_is_valid_but_not_enabled(self) -> None:
        row = {
            "dataset_id": "valid_key",
            "source_system": "faostat",
            "relative_path": "one.csv",
            "source_file_name": "one.csv",
            "file_format": "csv",
            "enabled": "false",
        }
        self.assertFalse(validate_manifest_rows([row])[0].enabled)


class TechnicalValidationTests(unittest.TestCase):
    def test_csv_profile_is_streamed_and_fingerprinted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "source.csv"
            path.write_bytes(b"name,value\ncoffee,10\npepper,20\n")
            profile = inspect_source(path, "csv")
        self.assertEqual(profile.row_count, 2)
        self.assertEqual(profile.file_size_bytes, 31)
        self.assertEqual(len(profile.checksum_sha256), 64)
        self.assertEqual(len(profile.schema_hash or ""), 64)
        self.assertTrue(profile.source_modified_time.endswith("+00:00"))

    def test_empty_html_and_corrupt_xlsx_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            empty = Path(temp_dir) / "empty.csv"
            empty.touch()
            with self.assertRaisesRegex(TechnicalValidationError, "empty"):
                inspect_source(empty, "csv")
            html = Path(temp_dir) / "html.csv"
            html.write_bytes(b"<!DOCTYPE html><html><body>denied</body></html>")
            with self.assertRaisesRegex(TechnicalValidationError, "HTML"):
                inspect_source(html, "csv")
            bad_xlsx = Path(temp_dir) / "bad.xlsx"
            bad_xlsx.write_bytes(b"not a workbook")
            with self.assertRaisesRegex(TechnicalValidationError, "valid ZIP"):
                inspect_source(bad_xlsx, "xlsx")

    def test_minimal_xlsx_structure_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "valid.xlsx"
            with zipfile.ZipFile(path, "w") as workbook:
                workbook.writestr("[Content_Types].xml", "<Types/>")
                workbook.writestr("xl/workbook.xml", "<workbook/>")
                workbook.writestr("xl/worksheets/sheet1.xml", "<worksheet/>")
            profile = inspect_source(path, "xlsx")
        self.assertIsNone(profile.row_count)
        self.assertEqual(len(profile.schema_hash or ""), 64)

    def test_large_nasa_file_is_validated_with_streaming_code(self) -> None:
        path = (
            ROOT
            / "data"
            / "raw"
            / "nasa_power/nasa_power_weather_daily_63.csv"
        )
        profile = inspect_source(path, "csv")
        self.assertEqual(profile.row_count, 614754)
        self.assertEqual(profile.file_size_bytes, 83581646)

    def test_resolved_symlink_may_not_escape_raw_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            raw_root = base / "raw"
            raw_root.mkdir()
            outside = base / "outside.csv"
            outside.write_text("a,b\n1,2\n", encoding="utf-8")
            (raw_root / "link.csv").symlink_to(outside)
            with self.assertRaisesRegex(TechnicalValidationError, "escapes"):
                resolve_source_path(
                    raw_root,
                    source_spec(relative_path="link.csv"),
                )


class IngestionServiceTests(unittest.TestCase):
    payload = b"item,value\ncoffee,123\n"

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.raw_root = Path(self.temporary.name) / "raw"
        self.source = source_spec()
        self.client = FakeMinio()
        self.service = LocalTestService(self.client, raw_root=self.raw_root)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_source(self, payload: bytes | None = None) -> Path:
        path = self.raw_root / self.source.relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(self.payload if payload is None else payload)
        return path

    def final_keys(self) -> list[str]:
        return [
            key
            for bucket, key in self.client.objects
            if bucket == "bronze" and key.startswith("faostat/")
        ]

    def test_new_source_is_full_load_success(self) -> None:
        self.write_source()
        result = self.service.ingest(
            self.source,
            airflow_run_id="manual__first",
            ingestion_date=date(2026, 9, 29),
        )
        self.assertEqual(result["change_state"], "NEW")
        self.assertEqual(result["load_type"], "FULL")
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(len(self.final_keys()), 1)

    def test_unchanged_rerun_skips_without_duplicate(self) -> None:
        self.write_source()
        first = self.service.ingest(self.source, airflow_run_id="run-1")
        second = self.service.ingest(self.source, airflow_run_id="run-2")
        self.assertEqual(second["change_state"], "UNCHANGED")
        self.assertEqual(second["load_type"], "INCREMENTAL")
        self.assertEqual(second["status"], "SKIPPED")
        self.assertEqual(first["bronze_path"], second["bronze_path"])
        self.assertEqual(len(self.final_keys()), 1)

    def test_changed_source_is_incremental_and_preserves_old_version(self) -> None:
        path = self.write_source()
        first = self.service.ingest(self.source, airflow_run_id="run-1")
        path.write_bytes(b"item,value\ncoffee,456\n")
        second = self.service.ingest(self.source, airflow_run_id="run-2")
        self.assertEqual(second["change_state"], "CHANGED")
        self.assertEqual(second["load_type"], "INCREMENTAL")
        self.assertEqual(second["status"], "SUCCESS")
        self.assertNotEqual(first["bronze_path"], second["bronze_path"])
        self.assertEqual(len(self.final_keys()), 2)

    def test_missing_source_is_failed_with_diagnostic(self) -> None:
        self.raw_root.mkdir()
        with self.assertRaisesRegex(FileNotFoundError, "does not exist"):
            self.service.ingest(self.source, airflow_run_id="run-missing")
        records = list_run_results(self.client, "bronze", "run-missing")
        self.assertEqual(records[0]["status"], "FAILED")
        self.assertIn("does not exist", str(records[0]["error_message"]))

    def test_empty_source_is_failed_and_not_published(self) -> None:
        self.write_source(b"")
        with self.assertRaisesRegex(TechnicalValidationError, "empty"):
            self.service.ingest(self.source, airflow_run_id="run-empty")
        self.assertEqual(self.final_keys(), [])

    def test_retry_reuses_identity_without_duplicate_final_object(self) -> None:
        self.write_source()
        self.client.fail_copies = 1
        with self.assertRaisesRegex(OSError, "transient"):
            self.service.ingest(
                self.source,
                airflow_run_id="same-run",
                ingestion_date=date(2026, 9, 29),
            )
        result = self.service.ingest(
            self.source,
            airflow_run_id="same-run",
            ingestion_date=date(2026, 9, 29),
        )
        self.assertEqual(result["status"], "SUCCESS")
        self.assertFalse(
            any(key.startswith("_staging/") for _, key in self.client.objects)
        )
        self.assertEqual(len(self.final_keys()), 1)

    def test_retry_repairs_metadata_index_after_publish(self) -> None:
        self.write_source()
        self.client.fail_put_contains_once = "_metadata/latest_success/"
        with self.assertRaisesRegex(OSError, "metadata write failure"):
            self.service.ingest(self.source, airflow_run_id="metadata-retry")
        result = self.service.ingest(
            self.source, airflow_run_id="metadata-retry"
        )
        self.assertEqual(result["status"], "SUCCESS")
        self.assertEqual(result["load_type"], "FULL")
        self.assertEqual(len(self.final_keys()), 1)
        self.assertIn(
            ("bronze", f"_metadata/latest_success/{self.source.dataset_id}.json"),
            self.client.objects,
        )

    def test_metadata_contains_required_local_lineage_fields(self) -> None:
        self.write_source()
        result = self.service.ingest(self.source, airflow_run_id="lineage-run")
        required = {
            "ingestion_id",
            "dataset_id",
            "source_system",
            "source_file_path",
            "source_file_name",
            "file_size_bytes",
            "source_modified_time",
            "checksum_sha256",
            "bronze_path",
            "load_type",
            "ingested_at",
            "airflow_run_id",
            "status",
            "error_message",
        }
        self.assertTrue(required.issubset(result))
        encoded = json.dumps(result)
        self.assertNotIn("MINIO_SECRET", encoded)
        self.assertNotIn("drive", encoded.lower())


if __name__ == "__main__":
    unittest.main()
