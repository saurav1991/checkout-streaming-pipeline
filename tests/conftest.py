import pytest


@pytest.fixture
def sample_event():
    return {
        "user_id": 42,
        "postcode": "SW19",
        "webpage": "www.website1.com/index.html",
        "event_time": 1611662684000,
    }


@pytest.fixture
def sample_agg_record():
    """Raw ksqlDB aggregate record (as it comes from the output topic)."""
    return {
        "POSTCODE_VALUE": "SW19",
        "WINDOW_START": 1611662640000,  # 2021-01-26T12:04:00Z in ms
        "WINDOW_END": 1611662700000,  # 2021-01-26T12:05:00Z in ms
        "PAGEVIEW_COUNT": 42,
    }


@pytest.fixture
def tmp_output_dir(tmp_path):
    return tmp_path
