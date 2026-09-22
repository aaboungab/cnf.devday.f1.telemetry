from datetime import datetime, timezone
from typing import Any, Optional

from src.telemetry import parser


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


class TelemetryNormalizer:
    """Turns parsed packets into normalized telemetry events.

    Lap Data packets update internal state; Car Telemetry packets emit an
    event carrying both the live car values and the latest lap state.
    """

    def __init__(self) -> None:
        self._latest_lap: dict[str, Any] = {}

    def handle(self, parsed: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        """Process one parsed packet; return an event to emit, or None."""
        if parsed is None:
            return None

        if parsed["packet_id"] == parser.PACKET_ID_LAP_DATA:
            self._latest_lap = parsed
            return None

        if parsed["packet_id"] != parser.PACKET_ID_CAR_TELEMETRY:
            return None

        lap = self._latest_lap
        return {
            "event_type": "telemetry",
            "timestamp": utc_now_iso(),
            "session_time": parsed["session_time"],
            "car_index": parsed["car_index"],
            "speed": parsed["speed"],
            "throttle": parsed["throttle"],
            "brake": parsed["brake"],
            "gear": parsed["gear"],
            "engine_rpm": parsed["engine_rpm"],
            "drs": parsed["drs"],
            "lap_number": lap.get("lap_number"),
            "car_position": lap.get("car_position"),
            "current_lap_time_ms": lap.get("current_lap_time_ms"),
            "last_lap_time_ms": lap.get("last_lap_time_ms"),
        }
