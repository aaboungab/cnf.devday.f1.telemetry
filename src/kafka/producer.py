import logging
import os
import socket
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from src.kafka.check_connection import kafka_config
from src.telemetry import parser
from src.telemetry.events import TelemetryNormalizer

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("producer")

TOPIC = "f1.telemetry"
SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "telemetry.schema.json"
UDP_IP = "0.0.0.0"
UDP_PORT = int(os.environ.get("F1_UDP_PORT", "20777"))
EMIT_HZ = float(os.environ.get("F1_EMIT_HZ", "0"))
STATUS_INTERVAL_S = 1.0


class Stats:
    received = 0
    published = 0
    delivered = 0
    failed = 0


def on_delivery(err, msg) -> None:
    if err is None:
        Stats.delivered += 1
    else:
        Stats.failed += 1
        log.error("delivery failed: %s", err)


def make_serializer() -> JSONSerializer:
    """Schema-aware serializer; registers the schema on first use."""
    registry = SchemaRegistryClient({
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    })
    return JSONSerializer(SCHEMA_PATH.read_text(), registry)


def main() -> None:
    producer = Producer({**kafka_config(), "linger.ms": 50})
    serializer = make_serializer()
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    sock.settimeout(1.0)
    log.info("Listening on UDP %d, publishing to %s", UDP_PORT, TOPIC)

    normalizer = TelemetryNormalizer()
    min_interval = 1.0 / EMIT_HZ if EMIT_HZ > 0 else 0.0
    last_emit = 0.0
    last_status = time.monotonic()

    try:
        while True:
            try:
                data, _ = sock.recvfrom(2048)
                Stats.received += 1
                try:
                    event = normalizer.handle(parser.parse_packet(data))
                except ValueError as exc:
                    log.warning("decode error: %s", exc)
                    event = None

                now = time.monotonic()
                if event is not None and now - last_emit >= min_interval:
                    last_emit = now
                    producer.produce(
                        TOPIC,
                        key=str(event["car_index"]),
                        value=serializer(event, ctx),
                        on_delivery=on_delivery,
                    )
                    Stats.published += 1
            except socket.timeout:
                pass

            producer.poll(0)
            now = time.monotonic()
            if now - last_status >= STATUS_INTERVAL_S and Stats.received:
                log.info(
                    "packets: %d | published: %d | delivered: %d | failed: %d",
                    Stats.received, Stats.published, Stats.delivered, Stats.failed,
                )
                last_status = now
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
        remaining = producer.flush(timeout=10)
        log.info(
            "Stopped. packets: %d | published: %d | delivered: %d | failed: %d | unflushed: %d",
            Stats.received, Stats.published, Stats.delivered, Stats.failed, remaining,
        )


if __name__ == "__main__":
    main()
