import pytest

from src.producer import PAGES, POSTCODES, USER_IDS, WEBSITE, generate_event

pytestmark = pytest.mark.unit


def test_generate_event_has_required_fields():
    event = generate_event()
    assert "user_id" in event
    assert "postcode" in event
    assert "webpage" in event
    assert "event_time" in event


def test_generate_event_field_types():
    event = generate_event()
    assert isinstance(event["user_id"], int)
    assert isinstance(event["postcode"], str)
    assert isinstance(event["webpage"], str)
    assert isinstance(event["event_time"], int)


def test_generate_event_values_in_expected_ranges():
    event = generate_event()
    assert event["user_id"] in USER_IDS
    assert event["postcode"] in POSTCODES
    parts = event["webpage"].split("/", 1)
    assert parts[0] == WEBSITE
    assert f"/{parts[1]}" in PAGES


def test_generate_event_randomness():
    """Multiple calls should produce varying events."""
    events = [generate_event() for _ in range(100)]
    user_ids = {e["user_id"] for e in events}
    postcodes = {e["postcode"] for e in events}
    assert len(user_ids) > 1
    assert len(postcodes) > 1


def test_website_and_postcode_counts():
    assert WEBSITE == "www.example.com"
    assert len(POSTCODES) == 50
    assert len(USER_IDS) == 1000
