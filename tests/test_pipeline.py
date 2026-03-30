"""
Unit tests for core pipeline logic.
Run with: pytest tests/
"""

import pandas as pd
import pytest

from kaggle_uploader import KaggleUploader


@pytest.fixture
def uploader():
    return KaggleUploader()


# ------------------------------------------------------------------ #
# merge_data tests                                                     #
# ------------------------------------------------------------------ #

def test_merge_empty_existing(uploader):
    new_df = pd.DataFrame([{"event_id": "A", "magnitude": 5.0}])
    result = uploader.merge_data(pd.DataFrame(), new_df, ["event_id"])
    assert len(result) == 1
    assert result.iloc[0]["event_id"] == "A"


def test_merge_empty_new(uploader):
    existing = pd.DataFrame([{"event_id": "A", "magnitude": 5.0}])
    result = uploader.merge_data(existing, pd.DataFrame(), ["event_id"])
    assert len(result) == 1


def test_merge_deduplication_new_wins(uploader):
    existing = pd.DataFrame([
        {"event_id": "A", "magnitude": 4.0},
        {"event_id": "B", "magnitude": 3.0},
    ])
    new_df = pd.DataFrame([
        {"event_id": "A", "magnitude": 5.5},  # duplicate — new value should win
        {"event_id": "C", "magnitude": 2.0},
    ])
    result = uploader.merge_data(existing, new_df, ["event_id"])
    assert len(result) == 3
    row_a = result[result["event_id"] == "A"].iloc[0]
    assert row_a["magnitude"] == 5.5


def test_merge_missing_key_columns(uploader):
    existing = pd.DataFrame([{"foo": 1}])
    new_df = pd.DataFrame([{"foo": 2}])
    # merge_keys not in columns — should still return combined rows without dedup
    result = uploader.merge_data(existing, new_df, ["nonexistent_key"])
    assert len(result) == 2


def test_merge_multiple_keys(uploader):
    existing = pd.DataFrame([
        {"year": 2023, "station_no": "47401", "bloom_date": "2023-03-25"},
    ])
    new_df = pd.DataFrame([
        {"year": 2023, "station_no": "47401", "bloom_date": "2023-03-26"},  # dup
        {"year": 2024, "station_no": "47401", "bloom_date": "2024-03-20"},  # new
    ])
    result = uploader.merge_data(existing, new_df, ["year", "station_no"])
    assert len(result) == 2
    row_2023 = result[result["year"] == 2023].iloc[0]
    assert row_2023["bloom_date"] == "2023-03-26"
