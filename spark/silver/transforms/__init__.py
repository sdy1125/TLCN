"""Small source parsers run per Spark partition; NASA uses native SQL expressions."""

import json
from ..common import RowError, stable_id, RULES
from ..contracts import fields, REQUIRED, OPTIONAL_KEYS
from ..registry import CONTRACTS
from . import faostat, provincial, market_indicator, trade_partner


def rows(iterator, spec, metadata, version, run_id, processed_at):
    target = spec["target"]
    names = fields(target)
    dataset = metadata["dataset_id"]
    for raw in iterator:
        raw = raw.asDict() if hasattr(raw, "asDict") else raw
        raw = {k: ("" if v is None else str(v)) for k, v in raw.items()}
        payload = json.dumps(
            raw, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
        base = dict(
            dataset_id=dataset,
            source_system=metadata["source_system"],
            source_file=metadata["source_file_name"],
            bronze_path=metadata["bronze_path"],
            bronze_ingestion_id=metadata["ingestion_id"],
            bronze_checksum_sha256=metadata["checksum_sha256"],
            airflow_run_id=run_id,
            processed_at=processed_at,
            transform_version=version,
            raw_payload_json=payload,
        )
        try:
            if dataset.startswith("faostat_"):
                result = faostat.transform(raw, spec)
            elif dataset.startswith("provincial_"):
                result = provincial.transform(raw, spec, dataset)
            elif dataset.startswith("world_bank_"):
                result = market_indicator.transform(raw, spec, dataset)
            else:
                result = trade_partner.transform(raw, spec)
            validated = []
            for o in result:
                o.update(base)
                priority = RULES["source_priority"].get(dataset, {})
                o.update(
                    source_priority=priority.get("priority"),
                    is_preferred=priority.get("is_preferred"),
                )
                if o.get("value_standard") is None:
                    o["observation_status"] = "missing"
                keys = CONTRACTS[target]["keys"]
                for key in keys:
                    if o.get(key) is None and key not in OPTIONAL_KEYS:
                        raise RowError("MISSING_KEY", key)
                if o.get("commodity_code") is None and target != "market_indicator":
                    raise RowError("MISSING_KEY", "commodity_code")
                o["source_record_id"] = stable_id(dataset, {k: o.get(k) for k in keys})
                validated.append(
                    {
                        "record": {k: o.get(k) for k in names},
                        "error_code": None,
                        "error_message": None,
                        "raw_payload_json": payload,
                        "raw_id": stable_id(dataset, raw),
                    }
                )
            yield from validated
        except RowError as exc:
            yield {
                "record": None,
                "error_code": exc.code,
                "error_message": str(exc),
                "raw_payload_json": payload,
                "raw_id": stable_id(dataset, raw),
            }
