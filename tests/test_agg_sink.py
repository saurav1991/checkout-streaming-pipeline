import json

import pytest

from src.agg_sink import agg_to_file_key, parse_agg_record
from src.util.io import BatchBuffer

pytestmark = pytest.mark.unit


def test_parse_agg_record(sample_agg_record):
    result = parse_agg_record(json.dumps(sample_agg_record))
    assert result["postcode"] == "SW19"
    assert result["window_start"] == "2021-01-26T12:04:00+00:00"
    assert result["window_end"] == "2021-01-26T12:05:00+00:00"
    assert result["pageview_count"] == 42


def test_agg_to_file_key():
    record = {"window_start": "2021-01-26T14:24:00+00:00"}
    assert agg_to_file_key(record) == "2021-01-26/14-24"


def test_agg_to_file_key_midnight():
    record = {"window_start": "2021-01-26T00:00:00+00:00"}
    assert agg_to_file_key(record) == "2021-01-26/00-00"


def test_agg_flush(tmp_output_dir):
    buf = BatchBuffer(str(tmp_output_dir), batch_size=100, interval_secs=30)
    record = {
        "postcode": "SW19",
        "window_start": "2021-01-26T14:24:00+00:00",
        "window_end": "2021-01-26T14:25:00+00:00",
        "pageview_count": 42,
    }
    buf.add("2021-01-26/14-24", json.dumps(record))
    flushed = buf.flush()
    assert flushed == 1

    file_path = tmp_output_dir / "2021-01-26" / "14-24.jsonl"
    assert file_path.exists()
    parsed = json.loads(file_path.read_text().strip())
    assert parsed["pageview_count"] == 42
