import json

import pytest

from src.raw_consumer import event_to_file_key
from src.util.io import BatchBuffer

pytestmark = pytest.mark.unit


def test_event_to_file_key(sample_event):
    key = event_to_file_key(sample_event)
    # 1611662684 = 2021-01-26T12:04:44Z
    assert key == "2021-01-26/12-04"


def test_event_to_file_key_midnight():
    event = {"event_time": 1611619200000}  # 2021-01-26T00:00:00Z in ms
    assert event_to_file_key(event) == "2021-01-26/00-00"


def test_flush_creates_files(tmp_output_dir):
    buf = BatchBuffer(str(tmp_output_dir), batch_size=100, interval_secs=10)
    buf.add(
        "2021-01-26/14-24",
        json.dumps(
            {
                "user_id": 1,
                "postcode": "SW19",
                "webpage": "www.website1.com/index.html",
                "event_time": 1611662684,
            }
        ),
    )
    buf.add(
        "2021-01-26/14-24",
        json.dumps(
            {
                "user_id": 2,
                "postcode": "EC1A",
                "webpage": "www.website2.com/about.html",
                "event_time": 1611662685,
            }
        ),
    )
    assert len(buf) == 2
    flushed = buf.flush()
    assert flushed == 2
    assert len(buf) == 0

    file_path = tmp_output_dir / "2021-01-26" / "14-24.jsonl"
    assert file_path.exists()

    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 2
    for line in lines:
        parsed = json.loads(line)
        assert "user_id" in parsed


def test_flush_appends(tmp_output_dir):
    buf = BatchBuffer(str(tmp_output_dir), batch_size=100, interval_secs=10)
    buf.add("2021-01-26/14-24", '{"user_id": 1}')
    buf.flush()

    buf.add("2021-01-26/14-24", '{"user_id": 2}')
    buf.flush()

    file_path = tmp_output_dir / "2021-01-26" / "14-24.jsonl"
    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 2


def test_flush_multiple_keys(tmp_output_dir):
    buf = BatchBuffer(str(tmp_output_dir), batch_size=100, interval_secs=10)
    buf.add("2021-01-26/14-24", '{"a": 1}')
    buf.add("2021-01-26/14-25", '{"b": 2}')
    buf.add("2021-01-27/00-00", '{"c": 3}')
    flushed = buf.flush()
    assert flushed == 3

    assert (tmp_output_dir / "2021-01-26" / "14-24.jsonl").exists()
    assert (tmp_output_dir / "2021-01-26" / "14-25.jsonl").exists()
    assert (tmp_output_dir / "2021-01-27" / "00-00.jsonl").exists()


def test_is_flush_due_batch_size():
    buf = BatchBuffer("/tmp", batch_size=2, interval_secs=60)
    assert not buf.is_flush_due()
    buf.add("key", "line1")
    assert not buf.is_flush_due()
    buf.add("key", "line2")
    assert buf.is_flush_due()
