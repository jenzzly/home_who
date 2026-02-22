import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock
from etl import transform, lifebirthrecord, fetch_data, save_data


# ── transform() tests ─────────────────────────────────────────────────────────

VALID_RAW = {
    "IndicatorCode": "WHOSIS_000001",
    "SpatialDim": "KEN",
    "Country": "AFRICA",
    "TimeDim": 2020,
    "Dim1": "BTSX",
    "NumericValue": 66.5,
    "Low": 64.0,
    "High": 69.0,
    "DateModified": "2023-01-15T00:00:00Z",
}


def test_transform_valid_record():
    r = transform(VALID_RAW)
    assert r is not None
    assert r.spatial_dim == "KEN"
    assert r.time_dim == 2020
    assert r.numeric_value == 66.5
    assert r.dim1 == "BTSX"


def test_transform_missing_numeric_value():
    raw = {**VALID_RAW, "NumericValue": None}
    r = transform(raw)
    assert r is not None
    assert r.numeric_value is None


def test_transform_missing_date_ok():
    raw = {**VALID_RAW, "Date": None}
    r = transform(raw)
    assert r is not None
    assert r.date_modified is None

def test_transform_string_numeric_value():
    raw = {**VALID_RAW, "NumericValue": "72.3"}
    r = transform(raw)
    assert r is not None
    assert r.numeric_value == pytest.approx(72.3)
