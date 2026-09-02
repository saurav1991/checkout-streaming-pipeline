import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC_PAGEVIEWS = os.getenv("KAFKA_TOPIC_PAGEVIEWS", "pageviews")
KAFKA_TOPIC_AGGREGATES = os.getenv("KAFKA_TOPIC_AGGREGATES", "pageview_aggregates")

OUTPUT_RAW_PATH = os.getenv("OUTPUT_RAW_PATH", "/app/output/raw")
OUTPUT_AGG_PATH = os.getenv("OUTPUT_AGG_PATH", "/app/output/agg")

PRODUCER_RATE = max(1, int(os.getenv("PRODUCER_RATE", "100")))
# Raw consumer: a continuous stream at PRODUCER_RATE events/sec, so the batch
# fills quickly and drives most flushes.
RAW_CONSUMER_FLUSH_INTERVAL_SECS = int(
    os.getenv("RAW_CONSUMER_FLUSH_INTERVAL_SECS", "10")
)
RAW_CONSUMER_FLUSH_BATCH_SIZE = int(os.getenv("RAW_CONSUMER_FLUSH_BATCH_SIZE", "500"))

# Agg sink: low-volume and bursty. ksqlDB emits at most one record per postcode
# per 1-minute window (EMIT FINAL), so a burst is bounded by postcode
# cardinality and arrives once a minute rather than continuously.
AGG_SINK_FLUSH_INTERVAL_SECS = int(os.getenv("AGG_SINK_FLUSH_INTERVAL_SECS", "30"))
AGG_SINK_FLUSH_BATCH_SIZE = int(os.getenv("AGG_SINK_FLUSH_BATCH_SIZE", "50"))
