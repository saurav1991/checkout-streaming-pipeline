import json
import logging
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_PAGEVIEWS,
    OUTPUT_RAW_PATH,
    RAW_CONSUMER_FLUSH_BATCH_SIZE,
    RAW_CONSUMER_FLUSH_INTERVAL_SECS,
)
from src.metrics import (
    raw_consumer_buffer_size,
    raw_consumer_events_total,
    raw_consumer_flush_size,
    raw_consumer_flushes_total,
    start_metrics_server,
)
from src.util.io import BatchBuffer
from src.util.shutdown import GracefulShutdown

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
    shutdown = GracefulShutdown("raw consumer")

    buffer = BatchBuffer(
        OUTPUT_RAW_PATH,
        batch_size=RAW_CONSUMER_FLUSH_BATCH_SIZE,
        interval_secs=RAW_CONSUMER_FLUSH_INTERVAL_SECS,
    )
    total_written = 0

    logger.info("Raw consumer started, writing to %s", OUTPUT_RAW_PATH)

    while shutdown.running:
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
            buffer.add(file_key, raw_value)
            raw_consumer_events_total.inc()
            raw_consumer_buffer_size.set(len(buffer))

        if buffer.is_flush_due():
            flushed = buffer.flush()
            consumer.commit(asynchronous=False)
            total_written += flushed
            raw_consumer_flushes_total.inc()
            raw_consumer_flush_size.observe(flushed)
            logger.info("Flushed %d events (total: %d)", flushed, total_written)
            raw_consumer_buffer_size.set(0)

    # Final flush on shutdown
    if len(buffer) > 0:
        flushed = buffer.flush()
        consumer.commit(asynchronous=False)
        total_written += flushed
        logger.info("Final flush: %d events (total: %d)", flushed, total_written)

    consumer.close()
    logger.info("Raw consumer stopped.")


if __name__ == "__main__":
    run()
