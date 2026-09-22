import json
import logging
import os
import sys
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("check_connection")

TOPIC = "f1.telemetry"


def kafka_config() -> dict:
    load_dotenv()
    required = ["CONFLUENT_BOOTSTRAP_SERVER", "CONFLUENT_API_KEY", "CONFLUENT_API_SECRET"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        sys.exit(f"Missing environment variables: {', '.join(missing)} (see .env.example)")
    return {
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
    }


def main() -> None:
    producer = Producer(kafka_config())
    message = {
        "event_type": "connectivity_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    delivered = {}

    def on_delivery(err, msg):
        if err is not None:
            delivered["error"] = err
        else:
            delivered["ok"] = (msg.topic(), msg.partition(), msg.offset())

    producer.produce(TOPIC, value=json.dumps(message), on_delivery=on_delivery)
    remaining = producer.flush(timeout=15)

    if "ok" in delivered:
        topic, partition, offset = delivered["ok"]
        log.info("SUCCESS: delivered to %s [partition %d] at offset %d", topic, partition, offset)
    elif "error" in delivered:
        sys.exit(f"FAILED: {delivered['error']}")
    else:
        sys.exit(f"FAILED: no delivery report within 15s ({remaining} message(s) unflushed)")


if __name__ == "__main__":
    main()
