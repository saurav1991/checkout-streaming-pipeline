from datetime import datetime

import pytest

from src.producer import PAGES, POSTCODES, WEBSITE
from tests.integration.conftest import collect_jsonl_records

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def agg_records(pipeline):
    return collect_jsonl_records(pipeline["agg_path"])


def test_agg_files_created(pipeline):
    files = list(pipeline["agg_path"].rglob("*.jsonl"))
    assert len(files) > 0, "No aggregate JSONL files were created"


def test_agg_record_schema(agg_records):
    expected_keys = {
        "webpage",
        "postcode",
        "window_start",
        "window_end",
        "pageview_count",
    }
    for record in agg_records:
        assert set(record.keys()) == expected_keys, f"Unexpected keys: {record.keys()}"


def test_agg_window_duration(agg_records):
    for record in agg_records:
        ws = datetime.fromisoformat(record["window_start"])
        we = datetime.fromisoformat(record["window_end"])
        delta = (we - ws).total_seconds()
        assert delta == 60, f"Window duration {delta}s != 60s for {record}"


def test_agg_postcodes_valid(agg_records):
    postcodes_set = set(POSTCODES)
    for record in agg_records:
        assert record["postcode"] in postcodes_set, (
            f"Unknown postcode: {record['postcode']}"
        )


def test_agg_webpages_valid(agg_records):
    valid_webpages = {f"{WEBSITE}{page}" for page in PAGES}
    for record in agg_records:
        assert record["webpage"] in valid_webpages, (
            f"Unknown webpage: {record['webpage']}"
        )


def test_agg_counts_positive(agg_records):
    for record in agg_records:
        assert record["pageview_count"] > 0, f"Non-positive count: {record}"


def test_agg_timestamps_iso(agg_records):
    for record in agg_records:
        ws = datetime.fromisoformat(record["window_start"])
        we = datetime.fromisoformat(record["window_end"])
        assert ws.tzinfo is not None, f"window_start missing timezone: {record}"
        assert we.tzinfo is not None, f"window_end missing timezone: {record}"
