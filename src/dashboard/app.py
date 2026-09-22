import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import streamlit as st
from confluent_kafka import Consumer, DeserializingConsumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

ALERTS_TOPIC = "f1.alerts"
TELEMETRY_TOPIC = "f1.telemetry"
REFRESH_SECONDS = 2

SEVERITY_ICON = {"CRITICAL": "🔴", "WARNING": "🟡"}


def base_config() -> dict:
    return {
        "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVER"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["CONFLUENT_API_KEY"],
        "sasl.password": os.environ["CONFLUENT_API_SECRET"],
    }


@st.cache_resource
def alert_consumer() -> DeserializingConsumer:
    registry = SchemaRegistryClient({
        "url": os.environ["SCHEMA_REGISTRY_URL"],
        "basic.auth.user.info": (
            f"{os.environ['SCHEMA_REGISTRY_API_KEY']}:"
            f"{os.environ['SCHEMA_REGISTRY_API_SECRET']}"
        ),
    })
    consumer = DeserializingConsumer({
        **base_config(),
        "group.id": "dashboard-" + uuid.uuid4().hex[:8],
        "auto.offset.reset": "earliest",
        "value.deserializer": AvroDeserializer(registry),
    })
    consumer.subscribe([ALERTS_TOPIC])
    return consumer


@st.cache_resource
def meta_consumer() -> Consumer:
    return Consumer({**base_config(), "group.id": "dashboard-meta"})


def telemetry_event_count() -> int:
    consumer = meta_consumer()
    metadata = consumer.list_topics(TELEMETRY_TOPIC, timeout=10)
    total = 0
    for p in metadata.topics[TELEMETRY_TOPIC].partitions:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(TELEMETRY_TOPIC, p), timeout=10
        )
        total += high - low
    return total


def poll_new_alerts() -> None:
    consumer = alert_consumer()
    while True:
        msg = consumer.poll(0.2)
        if msg is None:
            break
        if msg.error() or msg.value() is None:
            continue
        st.session_state.alerts.append(dict(msg.value()))
    st.session_state.alerts.sort(key=lambda a: str(a.get("detected_at")))


def fmt_value(alert: dict) -> tuple[str, str]:
    """Returns (current, previous) display strings for an alert."""
    if alert["event_type"] == "pace_degradation":
        return fmt_lap_ms(alert["value"]), fmt_lap_ms(alert["previous_value"])
    return f"{alert['value']} km/h", f"{alert['previous_value']} km/h"


def fmt_lap_ms(ms: int) -> str:
    minutes, rem = divmod(int(ms), 60_000)
    return f"{minutes}:{rem / 1000:06.3f}"


def fmt_time(dt) -> str:
    if isinstance(dt, datetime):
        return dt.strftime("%H:%M:%S")
    return str(dt)


st.set_page_config(page_title="F1 Stream Alert", page_icon="🏎️", layout="wide")

if "alerts" not in st.session_state:
    st.session_state.alerts = []

poll_new_alerts()
alerts = st.session_state.alerts

st.title("🏎️ F1 Stream Alert")
st.caption("Real-time event detection on live F1 25 telemetry — "
           "Kafka + Flink + Stream Governance on Confluent Cloud")

col1, col2, col3 = st.columns(3)
col1.metric("Events processed", f"{telemetry_event_count():,}")
col2.metric("Alerts generated", len(alerts))
col3.metric(
    "Latest lap seen",
    max((a.get("lap_number") or 0) for a in alerts) if alerts else "—",
)

st.divider()

if not alerts:
    st.info("No alerts yet — waiting for the stream…")
else:
    latest = alerts[-1]
    icon = SEVERITY_ICON.get(latest["severity"], "⚪")
    current, previous = fmt_value(latest)

    st.subheader("Latest alert")
    box = st.error if latest["severity"] == "CRITICAL" else st.warning
    box(
        f"### {icon} {latest['event_type'].replace('_', ' ').upper()}\n"
        f"**Car:** {latest['car_index']} &nbsp;|&nbsp; "
        f"**Lap:** {latest.get('lap_number', '—')} &nbsp;|&nbsp; "
        f"**Now:** {current} &nbsp;|&nbsp; "
        f"**Before:** {previous} &nbsp;|&nbsp; "
        f"**Detected:** {fmt_time(latest['detected_at'])}"
    )

    st.subheader("Alert history")
    for alert in reversed(alerts[-25:]):
        icon = SEVERITY_ICON.get(alert["severity"], "⚪")
        current, previous = fmt_value(alert)
        st.text(
            f"{fmt_time(alert['detected_at'])}  {icon} "
            f"{alert['event_type'].replace('_', ' ').upper():<20} "
            f"car {alert['car_index']:>2}  lap {alert.get('lap_number') or '—':>3}  "
            f"{previous} → {current}"
        )

time.sleep(REFRESH_SECONDS)
st.rerun()
