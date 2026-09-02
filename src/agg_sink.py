import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

from src.config import (
    AGG_SINK_FLUSH_BATCH_SIZE,
    AGG_SINK_FLUSH_INTERVAL_SECS,
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
from src.util.io import BatchBuffer
from src.util.shutdown import GracefulShutdown

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
    shutdown = GracefulShutdown("agg sink")

    buffer = BatchBuffer(
        OUTPUT_AGG_PATH,
        batch_size=AGG_SINK_FLUSH_BATCH_SIZE,
        interval_secs=AGG_SINK_FLUSH_INTERVAL_SECS,
    )
    total_written = 0

    logger.info("Agg sink started, writing to %s", OUTPUT_AGG_PATH)

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
                record = parse_agg_record(raw_value)
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError) as e:
                logger.warning(
                    "Skipping malformed message at offset %d: %s",
                    msg.offset(),
                    e,
                )
                continue
            file_key = agg_to_file_key(record)
            buffer.add(file_key, json.dumps(record))
            agg_sink_records_total.inc()
            agg_sink_buffer_size.set(len(buffer))

        if buffer.is_flush_due():
            flushed = buffer.flush()
            consumer.commit(asynchronous=False)
            total_written += flushed
            agg_sink_flushes_total.inc()
            agg_sink_flush_size.observe(flushed)
            logger.info("Flushed %d aggregates (total: %d)", flushed, total_written)
            agg_sink_buffer_size.set(0)

    if len(buffer) > 0:
        flushed = buffer.flush()
        consumer.commit(asynchronous=False)
        total_written += flushed
        logger.info("Final flush: %d aggregates (total: %d)", flushed, total_written)

    consumer.close()
    logger.info("Agg sink stopped.")


if __name__ == "__main__":
    run()
