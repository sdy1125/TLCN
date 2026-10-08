"""Read-only landing baseline; production uses readers.resolve/download instead.

Usage: python -m silver.profile --raw-root /source --output /reports/baseline.json
"""

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path
from datetime import datetime
from .registry import SOURCES, load
from .common import stable_id, period
from .readers import iter_workbook
from .transforms import rows


def profile(root, manifest):
    result = {}
    for entry in manifest:
        dataset = entry["dataset_id"]
        spec = dict(SOURCES[dataset])
        path = root / entry["relative_path"]
        digest = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1048576), b""):
                digest.update(chunk)
        if spec["format"] == "xlsx":
            it = iter_workbook(path)
            header = next(it)
            records = (dict(zip(header, r)) for r in it)
        else:
            f = path.open(encoding="utf-8-sig", newline="")
            records = csv.DictReader(f)
            header = records.fieldnames
        transport_totals = set()
        if dataset.startswith("un_comtrade_"):
            records = list(records)  # bounded Comtrade files; never the NASA file
            transport_totals = {
                tuple(
                    r[k]
                    for k in [
                        "reporterCode",
                        "partnerCode",
                        "cmdCode",
                        "period",
                        "flowCode",
                    ]
                )
                for r in records
                if r["motCode"] == "0"
                and r["partner2Code"] == "0"
                and r["customsCode"] == "C00"
            }
        counts = Counter()
        values = defaultdict(Counter)
        errors = Counter()
        keys = {}
        conflicts = Counter()
        earliest = None
        latest = None
        metadata = {
            "dataset_id": dataset,
            "source_system": spec["source_system"],
            "source_file_name": path.name,
            "bronze_path": "profile-only",
            "ingestion_id": "profile-only",
            "checksum_sha256": digest.hexdigest(),
        }
        for r in records:
            counts["input_rows"] += 1
            for k, v in r.items():
                if not v:
                    counts["null:" + k] += 1
                if v and v.strip() in {"-999", "-999.0", "-999.00"}:
                    counts["sentinel:" + k] += 1
                if k in {
                    "Unit",
                    "unit",
                    "ĐVT",
                    "Element Code",
                    "Flag",
                    "commodity",
                    "metric",
                    "geographic_level",
                    "indicator.id",
                    "cmdCode",
                    "motCode",
                    "province_name_normalized",
                }:
                    values[k][v] += 1
            if dataset == "nasa_power_weather_daily_63":
                observed_period = str(period(r["YEAR"], "daily", doy=r["DOY"])[0])
                earliest = min(earliest or observed_period, observed_period)
                latest = max(latest or observed_period, observed_period)
                continue
            if (
                dataset.startswith("un_comtrade_")
                and r["motCode"] != "0"
                and tuple(
                    r[k]
                    for k in [
                        "reporterCode",
                        "partnerCode",
                        "cmdCode",
                        "period",
                        "flowCode",
                    ]
                )
                in transport_totals
            ):
                counts["excluded_rows"] += 1
                continue
            if dataset == "world_bank_pink_sheet":
                if r["source_sheet"] != "Monthly Prices":
                    counts["excluded_rows"] += 1
                    continue
                if r["column_12"] == "($/kg)":
                    spec["pink_units"] = {k: r[k] for k in load("rules")["pink"]}
                import re

                if not re.fullmatch(r"\d{4}M\d{2}", r["column_0"]):
                    counts["excluded_rows"] += 1
                    continue
            for out in rows(
                [r], spec, metadata, "profile", "profile", datetime(2000, 1, 1)
            ):
                counts["candidate_rows"] += 1
                if out["error_code"]:
                    errors[out["error_code"] + ":" + out["error_message"]] += 1
                    continue
                record = out["record"]
                key = record["source_record_id"]
                payload = stable_id(
                    dataset,
                    {
                        k: v
                        for k, v in record.items()
                        if k
                        not in {
                            "raw_payload_json",
                            "processed_at",
                            "airflow_run_id",
                            "dedupe_reason",
                        }
                    },
                )
                if key in keys:
                    counts["duplicate_rows"] += 1
                    if keys[key] != payload:
                        conflicts[key] += 1
                keys[key] = payload
                period_text = str(record["period_start"])
                earliest = min(earliest or period_text, period_text)
                latest = max(latest or period_text, period_text)
        if spec["format"] == "csv":
            f.close()
        result[dataset] = {
            "sha256": digest.hexdigest(),
            "headers": header,
            "schema_hash": hashlib.sha256(json.dumps(header).encode()).hexdigest(),
            "counts": dict(counts),
            "observed": {k: dict(v) for k, v in values.items()},
            "mapping_errors": dict(errors),
            "conflicting_keys": len(conflicts),
            "period_start_min": earliest,
            "period_start_max": latest,
        }
    return result


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--raw-root", required=True)
    p.add_argument("--manifest", default="/manifest.csv")
    p.add_argument("--output", required=True)
    args = p.parse_args()
    with open(args.manifest, encoding="utf-8-sig") as f:
        manifest = list(csv.DictReader(f))
    result = profile(Path(args.raw_root), manifest)
    Path(args.output).write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        json.dumps(
            {
                k: {
                    "rows": v["counts"]["input_rows"],
                    "errors": v["mapping_errors"],
                    "conflicts": v["conflicting_keys"],
                }
                for k, v in result.items()
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
