import json
import os
import shutil
import time
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_OUTPUT = PROJECT_ROOT / "output" / "raw"
AGG_OUTPUT = PROJECT_ROOT / "output" / "agg"

DOCKER_CMD = os.path.join(os.path.expanduser("~"), ".rd", "bin", "docker")


def collect_jsonl_records(base_path: Path) -> list[dict]:
    records = []
    for jsonl_file in base_path.rglob("*.jsonl"):
        for line in jsonl_file.read_text().strip().splitlines():
            if line:
                records.append(json.loads(line))
    return records


def _wait_for_output(path: Path, timeout: float):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if list(path.rglob("*.jsonl")):
            return
        time.sleep(2)
    pytest.fail(f"No .jsonl files appeared in {path} within {timeout}s")


def _clean_output():
    for d in (RAW_OUTPUT, AGG_OUTPUT):
        if d.exists():
            shutil.rmtree(d)
        d.mkdir(parents=True, exist_ok=True)


@pytest.fixture(scope="session")
def pipeline():
    _clean_output()

    compose = DockerCompose(
        context=str(PROJECT_ROOT),
        build=True,
        env_file=".env.test",
        docker_command_path=DOCKER_CMD if Path(DOCKER_CMD).exists() else None,
    )
    compose.start()

    try:
        # Wait for the ksqlDB server to be healthy before checking for output.
        compose.wait_for("http://localhost:8088/info")
        _wait_for_output(RAW_OUTPUT, timeout=60)
        _wait_for_output(AGG_OUTPUT, timeout=180)
        yield {"raw_path": RAW_OUTPUT, "agg_path": AGG_OUTPUT}
    finally:
        compose.stop(down=True)
        _clean_output()
