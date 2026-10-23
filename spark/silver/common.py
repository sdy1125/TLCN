"""Strict, independently testable parsing and mapping rules."""

import calendar
import hashlib
import json
import re
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from .registry import load

RULES = load("rules")
GEO = load("geography")


class RowError(ValueError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


def fail(code, message):
    raise RowError(code, message)


def number(raw):
    text = "" if raw is None else str(raw).strip()
    if text in RULES["missing"]:
        return None
    try:
        n = Decimal(text)
    except InvalidOperation:
        fail("PARSE_ERROR", "Invalid numeric value")
    if not n.is_finite() or abs(n) >= Decimal("1e20"):
        fail("PARSE_ERROR", "Nonfinite or decimal overflow")
    return n


def convert(raw, unit, allow_negative=False):
    if unit not in RULES["units"]:
        fail("UNKNOWN_UNIT", str(unit))
    value = number(raw)
    standard, factor = RULES["units"][unit]
    if value is not None:
        value *= Decimal(factor)
        if not allow_negative and value < 0:
            fail("IMPOSSIBLE_VALUE", "Negative measure")
        if abs(value) >= Decimal("1e20"):
            fail("PARSE_ERROR", "Decimal overflow after conversion")
        value = value.quantize(Decimal("0.00000001"))
    return value, standard


def period(label, frequency="annual", month=None, doy=None):
    text = str(label).strip()
    status = (
        "forecast"
        if text.startswith("Dự báo ")
        else "provisional" if text.startswith("Sơ bộ") else "official"
    )
    text = re.sub(r"^(Dự báo |Sơ bộ năm |Sơ bộ )", "", text)
    try:
        if frequency == "seasonal":
            if not re.fullmatch(r"\d{4}/\d{4}", text):
                raise ValueError()
            y, end = map(int, text.split("/"))
            if end != y + 1:
                raise ValueError()
            # Calendar envelope, not an assumed agricultural season start.
            return date(y, 1, 1), date(end, 12, 31), y, None, text, status
        if frequency == "monthly" and month is None:
            match = re.fullmatch(r"(\d{4})M?(\d{2})", text)
            if not match:
                raise ValueError()
            text, month = match.groups()
        if not re.fullmatch(r"\d{4}", text):
            raise ValueError()
        y = int(text)
        if frequency == "daily":
            day = int(doy)
            if str(day) != str(doy) or not 1 <= day <= 365 + calendar.isleap(y):
                raise ValueError()
            start = date(y, 1, 1) + timedelta(days=day - 1)
            return start, start, y, start.month, None, status
        m = int(month) if frequency == "monthly" else 1
        start = date(y, m, 1)
        end = (
            date(y, m, calendar.monthrange(y, m)[1])
            if frequency == "monthly"
            else date(y, 12, 31)
        )
        return start, end, y, m if frequency == "monthly" else None, None, status
    except (ValueError, TypeError, OverflowError):
        fail("INVALID_PERIOD", f"Invalid {frequency} period")


def time_fields(label, frequency, month=None, doy=None):
    p = period(label, frequency, month, doy)
    return dict(
        zip(
            [
                "period_start",
                "period_end",
                "year",
                "month",
                "season_label",
                "observation_status",
            ],
            p,
        ),
        frequency=frequency,
    )


def stable_id(dataset, keys):
    return hashlib.sha256(
        json.dumps(
            [dataset, keys],
            ensure_ascii=False,
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def commodity(code):
    return RULES["commodity"].get(str(code)) or fail("UNKNOWN_COMMODITY", str(code))


@lru_cache(maxsize=1024)
def country(code):
    import pycountry

    text = str(code).strip()
    text = RULES["country_aliases"].get(text, text)
    special = RULES.get("country_special", {})
    if text in special:
        return special[text]
    if text in {"0", "000", "WORLD"}:
        return {"code": "001", "m49": "001", "iso3": None, "kind": "world"}
    if text in {"EU", "OTHER"}:
        return {"code": text, "m49": None, "iso3": None, "kind": "aggregate"}
    if text.isdigit():
        c = pycountry.countries.get(numeric=text.zfill(3))
    else:
        try:
            c = pycountry.countries.lookup(text)
        except LookupError:
            c = None
    if c is None:
        fail("UNKNOWN_COUNTRY", text)
    return {"code": c.numeric, "m49": c.numeric, "iso3": c.alpha_3, "kind": "country"}


def province(name):
    name = GEO["aliases"].get(str(name).strip(), str(name).strip())
    if name not in GEO["provinces"]:
        fail("UNKNOWN_PROVINCE", name)
    return GEO["provinces"][name], name


def measure(raw, unit, code, allow_negative=False):
    v, u = convert(raw, unit, allow_negative)
    return {
        "raw_value": None if raw is None else str(raw),
        "unit_raw": unit,
        "value_standard": v,
        "unit_standard": u,
        "measure_code": code,
        "conversion_factor": Decimal(RULES["units"][unit][1]).quantize(
            Decimal("0.00000001")
        ),
        "unit_rule": unit,
    }
