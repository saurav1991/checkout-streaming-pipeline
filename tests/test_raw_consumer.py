import json

import pytest

from src.raw_consumer import event_to_file_key, flush_buffer

pytestmark = pytest.mark.unit


def test_event_to_file_key(sample_event):
    key = event_to_file_key(sample_event)
    # 1611662684 = 2021-01-26T12:04:44Z
    assert key == "2021-01-26/12-04"


def test_event_to_file_key_midnight():
    event = {"event_time": 1611619200000}  # 2021-01-26T00:00:00Z in ms
    assert event_to_file_key(event) == "2021-01-26/00-00"


def test_flush_buffer_creates_files(tmp_output_dir):
    ev1 = json.dumps(
        {
            "user_id": 1,
            "postcode": "SW19",
            "webpage": "www.website1.com/index.html",
            "event_time": 1611662684,
        }
    )
    ev2 = json.dumps(
        {
            "user_id": 2,
            "postcode": "EC1A",
            "webpage": "www.website2.com/about.html",
            "event_time": 1611662685,
        }
    )
    buffer = {"2021-01-26/14-24": [ev1, ev2]}
    flushed = flush_buffer(buffer, str(tmp_output_dir))
    assert flushed == 2

    file_path = tmp_output_dir / "2021-01-26" / "14-24.jsonl"
    assert file_path.exists()

    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert "user_id" in parsed


def test_flush_buffer_appends(tmp_output_dir):
    buffer1 = {"2021-01-26/14-24": ['{"user_id": 1}']}
    buffer2 = {"2021-01-26/14-24": ['{"user_id": 2}']}

    flush_buffer(buffer1, str(tmp_output_dir))
    flush_buffer(buffer2, str(tmp_output_dir))

    file_path = tmp_output_dir / "2021-01-26" / "14-24.jsonl"
    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 2


def test_flush_buffer_multiple_keys(tmp_output_dir):
    buffer = {
        "2021-01-26/14-24": ['{"a": 1}'],
        "2021-01-26/14-25": ['{"b": 2}'],
        "2021-01-27/00-00": ['{"c": 3}'],
    }
    flushed = flush_buffer(buffer, str(tmp_output_dir))
    assert flushed == 3

    assert (tmp_output_dir / "2021-01-26" / "14-24.jsonl").exists()
    assert (tmp_output_dir / "2021-01-26" / "14-25.jsonl").exists()
    assert (tmp_output_dir / "2021-01-27" / "00-00.jsonl").exists()
