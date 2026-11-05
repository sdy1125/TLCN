"""Reviewed, year-specific district links to the official NSO register."""

import json
import re
from pathlib import Path


PATH = Path(__file__).parent / "config" / "nso_district_crosswalk.json"
CONTRACT = json.loads(PATH.read_text())
ROWS = CONTRACT["rows"]


def validate():
    if "danhmuchanhchinh.nso.gov.vn" not in CONTRACT["source"]:
        raise ValueError("District crosswalk lacks official source")
    seen = set()
    for row in ROWS:
        key = (row["province_code"], row["raw_name"], row["year"])
        if key in seen:
            raise ValueError(f"Duplicate district crosswalk: {key}")
        seen.add(key)
        if not re.fullmatch(r"\d{2}", row["province_code"]):
            raise ValueError(f"Invalid province code: {key}")
        if not 2015 <= row["year"] <= 2024:
            raise ValueError(f"Unreviewed district year: {key}")
        if row["mapping_status"] == "VERIFIED":
            if not re.fullmatch(r"\d{3}", row["official_district_code"] or ""):
                raise ValueError(f"Missing official district code: {key}")
        elif row["mapping_status"] == "UNRESOLVED_ANNUAL_BOUNDARY":
            if row["official_district_code"] is not None:
                raise ValueError(f"Unresolved district has code: {key}")
        else:
            raise ValueError(f"Unknown district status: {key}")
    return len(seen)


def trino_values():
    """Generate a bounded literal table for the Gold semantic view."""
    validate()

    def quote(value):
        return "'" + value.replace("'", "''") + "'"

    values = []
    for row in ROWS:
        code = (quote(row["official_district_code"])
                if row["official_district_code"] else "CAST(NULL AS varchar)")
        values.append(
            f"({quote(row['province_code'])}, {quote(row['raw_name'])}, "
            f"{row['year']}, {code}, {quote(row['mapping_status'])})")
    return ", ".join(values)
