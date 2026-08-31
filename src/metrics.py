import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

# Producer
producer_events_total = Counter("producer_events_total", "Total events produced")
producer_errors_total = Counter("producer_errors_total", "Total delivery failures")

# Raw consumer
raw_consumer_events_total = Counter(
    "raw_consumer_events_total", "Total raw events consumed"
)
raw_consumer_flushes_total = Counter(
    "raw_consumer_flushes_total", "Total raw consumer flush operations"
)
raw_consumer_flush_size = Histogram(
    "raw_consumer_flush_size",
    "Events per raw consumer flush",
    buckets=[10, 50, 100, 250, 500, 1000],
)
raw_consumer_buffer_size = Gauge(
    "raw_consumer_buffer_size", "Current raw consumer buffer size"
)

# Agg sink
agg_sink_records_total = Counter(
    "agg_sink_records_total", "Total aggregate records consumed"
)
agg_sink_flushes_total = Counter(
    "agg_sink_flushes_total", "Total agg sink flush operations"
)
agg_sink_flush_size = Histogram(
    "agg_sink_flush_size",
    "Records per agg sink flush",
    buckets=[10, 25, 50, 100, 250],
)
agg_sink_buffer_size = Gauge("agg_sink_buffer_size", "Current agg sink buffer size")


def start_metrics_server(port: int | None = None):
    start_http_server(port or METRICS_PORT)
