"""
Unit tests for core pipeline logic.
Run with: pytest tests/
"""

import json
import os
import tempfile
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# config must be imported before jma_api_client to resolve circular import
import config  # noqa: F401
from kaggle_uploader import KaggleUploader
from jma_api_client.base import strip_ns, find_text, find_all_text, get_feed, iter_feed_entries


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


def test_merge_deduplication_existing_wins(uploader):
    existing = pd.DataFrame([
        {"event_id": "A", "magnitude": 4.0},
        {"event_id": "B", "magnitude": 3.0},
    ])
    new_df = pd.DataFrame([
        {"event_id": "A", "magnitude": 5.5},  # duplicate — existing value should win
        {"event_id": "C", "magnitude": 2.0},
    ])
    result = uploader.merge_data(existing, new_df, ["event_id"])
    assert len(result) == 3
    row_a = result[result["event_id"] == "A"].iloc[0]
    assert row_a["magnitude"] == 4.0  # Keep existing data


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
    assert row_2023["bloom_date"] == "2023-03-25"  # Keep existing data


# ------------------------------------------------------------------ #
# strip_ns / find_text / find_all_text tests                          #
# ------------------------------------------------------------------ #

def test_strip_ns_with_namespace():
    assert strip_ns("{http://example.com/ns}MyTag") == "MyTag"


def test_strip_ns_without_namespace():
    assert strip_ns("MyTag") == "MyTag"


def test_strip_ns_empty():
    assert strip_ns("") == ""


def test_find_text_found():
    root = ET.fromstring("<root><child><name>Tokyo</name></child></root>")
    assert find_text(root, "name") == "Tokyo"


def test_find_text_not_found():
    root = ET.fromstring("<root><child><name>Tokyo</name></child></root>")
    assert find_text(root, "missing") is None


def test_find_text_multiple_candidates_returns_first():
    root = ET.fromstring("<root><a>first</a><b>second</b></root>")
    assert find_text(root, "a", "b") == "first"


def test_find_all_text():
    root = ET.fromstring(
        "<root><pref><name>Tokyo</name></pref><pref><name>Osaka</name></pref></root>"
    )
    result = find_all_text(root, "name")
    assert result == ["Tokyo", "Osaka"]


def test_find_all_text_no_matches():
    root = ET.fromstring("<root><a>x</a></root>")
    assert find_all_text(root, "missing") == []


# ------------------------------------------------------------------ #
# get_feed / iter_feed_entries tests                                  #
# ------------------------------------------------------------------ #

SAMPLE_ATOM_FEED = """\
<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Test Feed</title>
  <entry>
    <title>Earthquake Info</title>
    <id>https://example.com/data/20240101_0_VXSE53_000000.xml</id>
    <link type="application/xml" href="https://example.com/data/20240101_0_VXSE53_000000.xml"/>
  </entry>
  <entry>
    <title>Volcano Info</title>
    <id>https://example.com/data/20240101_0_VFVO53_000000.xml</id>
    <link type="application/xml" href="https://example.com/data/20240101_0_VFVO53_000000.xml"/>
  </entry>
</feed>
"""


def test_get_feed_missing_file():
    result = get_feed("nonexistent_feed_xyz.xml")
    assert result is None


def test_get_feed_returns_element(tmp_path, monkeypatch):
    feed_file = tmp_path / "test_feed.xml"
    feed_file.write_bytes(SAMPLE_ATOM_FEED.encode("utf-8"))

    import config
    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    result = get_feed("test_feed.xml")
    assert result is not None
    assert result.tag.endswith("feed")


def test_iter_feed_entries_filters_by_type_code(tmp_path, monkeypatch):
    feed_file = tmp_path / "eqvol_l.xml"
    feed_file.write_bytes(SAMPLE_ATOM_FEED.encode("utf-8"))

    import config
    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    entries = list(iter_feed_entries("eqvol_l.xml", "VXSE53"))
    assert len(entries) == 1
    url, _ = entries[0]
    assert "VXSE53" in url


def test_iter_feed_entries_no_match(tmp_path, monkeypatch):
    feed_file = tmp_path / "eqvol_l.xml"
    feed_file.write_bytes(SAMPLE_ATOM_FEED.encode("utf-8"))

    import config
    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    entries = list(iter_feed_entries("eqvol_l.xml", "VXSE99"))
    assert len(entries) == 0


def test_iter_feed_entries_multiple_type_codes(tmp_path, monkeypatch):
    feed_file = tmp_path / "eqvol_l.xml"
    feed_file.write_bytes(SAMPLE_ATOM_FEED.encode("utf-8"))

    import config
    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    entries = list(iter_feed_entries("eqvol_l.xml", "VXSE53", "VFVO53"))
    assert len(entries) == 2


# ------------------------------------------------------------------ #
# fetch_all_feeds tests                                               #
# ------------------------------------------------------------------ #

def test_fetch_all_feeds_raises_on_failure(tmp_path, monkeypatch):
    import config
    from jma_api_client.base import fetch_all_feeds

    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    with patch("jma_api_client.utils.get", side_effect=RuntimeError("network error")):
        with pytest.raises(RuntimeError, match="Failed to fetch feeds"):
            fetch_all_feeds()


def test_fetch_all_feeds_saves_files(tmp_path, monkeypatch):
    import config
    from jma_api_client.base import fetch_all_feeds, JMA_FEED_URLS

    monkeypatch.setattr(config, "RAW_DATA_DIR", str(tmp_path))

    mock_resp = MagicMock()
    mock_resp.content = SAMPLE_ATOM_FEED.encode("utf-8")

    with patch("jma_api_client.utils.get", return_value=mock_resp):
        fetch_all_feeds()

    for feed_name in JMA_FEED_URLS:
        assert (tmp_path / feed_name).exists()
