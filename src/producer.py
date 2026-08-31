import json
import logging
import random
import signal
import time

from confluent_kafka import Producer

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PAGEVIEWS, PRODUCER_RATE
from src.metrics import (
    producer_errors_total,
    producer_events_total,
    start_metrics_server,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

WEBSITE = "www.example.com"
PAGES = ["/index.html", "/about.html", "/contact.html", "/products.html", "/blog.html"]
POSTCODES = [
    "SW1A",
    "SW1B",
    "SW19",
    "EC1A",
    "EC2A",
    "EC3A",
    "EC4A",
    "WC1A",
    "WC2A",
    "SE1",
    "SE10",
    "SE11",
    "N1",
    "N7",
    "N19",
    "E1",
    "E14",
    "E16",
    "W1",
    "W2",
    "W8",
    "W11",
    "NW1",
    "NW3",
    "NW10",
    "CR0",
    "BR1",
    "DA1",
    "EN1",
    "HA0",
    "IG1",
    "KT1",
    "RM1",
    "SM1",
    "TW1",
    "UB1",
    "GU1",
    "RG1",
    "SL1",
    "HP1",
    "AL1",
    "SG1",
    "CM1",
    "SS1",
    "CO1",
    "CB1",
    "IP1",
    "NR1",
    "PE1",
    "LN1",
]
USER_IDS = list(range(1, 1001))


def generate_event() -> dict:
    page = random.choice(PAGES)
    return {
        "user_id": random.choice(USER_IDS),
        "postcode": random.choice(POSTCODES),
        "webpage": f"{WEBSITE}{page}",
        "event_time": int(time.time() * 1000),
    }


def delivery_callback(err, msg):
    if err:
        producer_errors_total.inc()
        logger.error("Delivery failed: %s", err)


def run():
    start_metrics_server()
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    running = True

    def shutdown(signum, frame):
        nonlocal running
        logger.info("Shutting down producer...")
        running = False

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    interval = 1.0 / PRODUCER_RATE
    sent = 0
    logger.info(
        "Producer started: %d events/sec -> topic '%s'",
        PRODUCER_RATE,
        KAFKA_TOPIC_PAGEVIEWS,
    )

    while running:
        event = generate_event()
        producer.produce(
            KAFKA_TOPIC_PAGEVIEWS,
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_callback,
        )
        producer.poll(0)
        producer_events_total.inc()
        sent += 1
        if sent % 1000 == 0:
            logger.info("Sent %d events", sent)
        time.sleep(interval)

    producer.flush(timeout=10)
    logger.info("Producer stopped. Total sent: %d", sent)


if __name__ == "__main__":
    run()
