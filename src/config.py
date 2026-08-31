import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_PAGEVIEWS = os.getenv("KAFKA_TOPIC_PAGEVIEWS", "pageviews")
KAFKA_TOPIC_AGGREGATES = os.getenv("KAFKA_TOPIC_AGGREGATES", "pageview_aggregates")

OUTPUT_RAW_PATH = os.getenv("OUTPUT_RAW_PATH", "/app/output/raw")
OUTPUT_AGG_PATH = os.getenv("OUTPUT_AGG_PATH", "/app/output/agg")

PRODUCER_RATE = int(os.getenv("PRODUCER_RATE", "100"))  # events per second
CONSUMER_FLUSH_INTERVAL_SECS = int(os.getenv("CONSUMER_FLUSH_INTERVAL_SECS", "10"))
CONSUMER_FLUSH_BATCH_SIZE = int(os.getenv("CONSUMER_FLUSH_BATCH_SIZE", "500"))
