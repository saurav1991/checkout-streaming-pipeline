import re

import pytest

from src.producer import PAGES, POSTCODES, USER_IDS, WEBSITE
from tests.integration.conftest import collect_jsonl_records

pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def raw_records(pipeline):
    return collect_jsonl_records(pipeline["raw_path"])


def test_raw_files_created(pipeline):
    files = list(pipeline["raw_path"].rglob("*.jsonl"))
    assert len(files) > 0, "No raw JSONL files were created"


def test_raw_directory_structure(pipeline):
    for jsonl_file in pipeline["raw_path"].rglob("*.jsonl"):
        rel = jsonl_file.relative_to(pipeline["raw_path"])
        assert re.match(r"\d{4}-\d{2}-\d{2}/\d{2}-\d{2}\.jsonl$", str(rel)), (
            f"Unexpected path structure: {rel}"
        )


def test_raw_event_schema(raw_records):
    expected_keys = {"user_id", "postcode", "webpage", "event_time"}
    for record in raw_records:
        assert set(record.keys()) == expected_keys, f"Unexpected keys: {record.keys()}"


def test_raw_event_types(raw_records):
    for record in raw_records:
        assert isinstance(record["user_id"], int)
        assert isinstance(record["postcode"], str)
        assert isinstance(record["webpage"], str)
        assert isinstance(record["event_time"], int)


def test_raw_event_values(raw_records):
    postcodes_set = set(POSTCODES)
    pages_set = set(PAGES)
    user_ids_set = set(USER_IDS)

    for record in raw_records:
        assert record["postcode"] in postcodes_set
        assert record["user_id"] in user_ids_set
        parts = record["webpage"].split("/", 1)
        assert parts[0] == WEBSITE, f"Unknown website: {parts[0]}"
        assert f"/{parts[1]}" in pages_set, f"Unknown page: /{parts[1]}"
