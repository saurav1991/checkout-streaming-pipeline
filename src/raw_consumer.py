import json
import logging
import signal
import time
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from src.config import (
    CONSUMER_FLUSH_BATCH_SIZE,
    CONSUMER_FLUSH_INTERVAL_SECS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_PAGEVIEWS,
    OUTPUT_RAW_PATH,
)
from src.metrics import (
    raw_consumer_buffer_size,
    raw_consumer_events_total,
    raw_consumer_flush_size,
    raw_consumer_flushes_total,
    start_metrics_server,
)
from src.util.io import flush_buffer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def event_to_file_key(event: dict) -> str:
    """Derive YYYY-MM-DD/HH-MM file key from event timestamp."""
    ts_ms = event.get("event_time", int(time.time() * 1000))
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d/%H-%M")


def run():
    start_metrics_server()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "raw-sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([KAFKA_TOPIC_PAGEVIEWS])
    running = True

    def shutdown(signum, frame):
        nonlocal running
        logger.info("Shutting down raw consumer...")
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    buffer: dict[str, list[str]] = defaultdict(list)
    buffer_size = 0
    last_flush = time.monotonic()
    total_written = 0

    logger.info("Raw consumer started, writing to %s", OUTPUT_RAW_PATH)

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
            if msg.value() is None:
                continue
            try:
                raw_value = msg.value().decode("utf-8")
                event = json.loads(raw_value)
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                logger.warning(
                    "Skipping malformed message at offset %d: %s",
                    msg.offset(),
                    e,
                )
                continue
            file_key = event_to_file_key(event)
            buffer[file_key].append(raw_value)
            buffer_size += 1
            raw_consumer_events_total.inc()
            raw_consumer_buffer_size.set(buffer_size)

        elapsed = time.monotonic() - last_flush
        if buffer_size >= CONSUMER_FLUSH_BATCH_SIZE or (
            buffer_size > 0 and elapsed >= CONSUMER_FLUSH_INTERVAL_SECS
        ):
            flushed = flush_buffer(buffer, OUTPUT_RAW_PATH)
            consumer.commit(asynchronous=False)
            total_written += flushed
            raw_consumer_flushes_total.inc()
            raw_consumer_flush_size.observe(flushed)
            logger.info("Flushed %d events (total: %d)", flushed, total_written)
            buffer.clear()
            buffer_size = 0
            raw_consumer_buffer_size.set(0)
            last_flush = time.monotonic()

    # Final flush on shutdown
    if buffer_size > 0:
        flushed = flush_buffer(buffer, OUTPUT_RAW_PATH)
        consumer.commit(asynchronous=False)
        total_written += flushed
        logger.info("Final flush: %d events (total: %d)", flushed, total_written)

    consumer.close()
    logger.info("Raw consumer stopped.")


if __name__ == "__main__":
    run()
