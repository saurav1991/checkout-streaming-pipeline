import json
import logging
import os
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from src.config import (
    CONSUMER_FLUSH_BATCH_SIZE,
    CONSUMER_FLUSH_INTERVAL_SECS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_AGGREGATES,
    OUTPUT_AGG_PATH,
)
from src.metrics import (
    agg_sink_buffer_size,
    agg_sink_flush_size,
    agg_sink_flushes_total,
    agg_sink_records_total,
    start_metrics_server,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def parse_agg_record(raw: str) -> dict:
    """Parse a ksqlDB aggregate record into output format."""
    record = json.loads(raw)
    window_start_ms = record["WINDOW_START"]
    window_end_ms = record["WINDOW_END"]
    ws = datetime.fromtimestamp(window_start_ms / 1000, tz=timezone.utc)
    we = datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc)
    return {
        "postcode": record["POSTCODE_VALUE"],
        "window_start": ws.isoformat(),
        "window_end": we.isoformat(),
        "pageview_count": record["PAGEVIEW_COUNT"],
    }


def agg_to_file_key(record: dict) -> str:
    """Derive YYYY-MM-DD/HH-MM file key from window_start ISO string."""
    ws = datetime.fromisoformat(record["window_start"])
    return ws.strftime("%Y-%m-%d/%H-%M")


def flush_buffer(buffer: dict[str, list[str]], base_path: str) -> int:
    total = 0
    for file_key, lines in buffer.items():
        dir_path = os.path.join(base_path, os.path.dirname(file_key))
        os.makedirs(dir_path, exist_ok=True)
        file_path = os.path.join(base_path, f"{file_key}.jsonl")
        with open(file_path, "a") as f:
            for line in lines:
                f.write(line + "\n")
        total += len(lines)
    return total


def run():
    start_metrics_server()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "agg-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC_AGGREGATES])
    running = True

    def shutdown(signum, frame):
        nonlocal running
        logger.info("Shutting down agg sink...")
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    buffer: dict[str, list[str]] = defaultdict(list)
    buffer_size = 0
    last_flush = time.monotonic()
    total_written = 0

    logger.info("Agg sink started, writing to %s", OUTPUT_AGG_PATH)

    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error("Consumer error: %s", msg.error())
            continue
        else:
            raw_value = msg.value().decode("utf-8")
            record = parse_agg_record(raw_value)
            file_key = agg_to_file_key(record)
            buffer[file_key].append(json.dumps(record))
            buffer_size += 1
            agg_sink_records_total.inc()
            agg_sink_buffer_size.set(buffer_size)

        elapsed = time.monotonic() - last_flush
        if buffer_size >= CONSUMER_FLUSH_BATCH_SIZE or (
            buffer_size > 0 and elapsed >= CONSUMER_FLUSH_INTERVAL_SECS
        ):
            flushed = flush_buffer(buffer, OUTPUT_AGG_PATH)
            consumer.commit(asynchronous=False)
            total_written += flushed
            agg_sink_flushes_total.inc()
            agg_sink_flush_size.observe(flushed)
            logger.info("Flushed %d aggregates (total: %d)", flushed, total_written)
            buffer.clear()
            buffer_size = 0
            agg_sink_buffer_size.set(0)
            last_flush = time.monotonic()

    if buffer_size > 0:
        flushed = flush_buffer(buffer, OUTPUT_AGG_PATH)
        consumer.commit(asynchronous=False)
        total_written += flushed
        logger.info("Final flush: %d aggregates (total: %d)", flushed, total_written)

    consumer.close()
    logger.info("Agg sink stopped.")


if __name__ == "__main__":
    run()
