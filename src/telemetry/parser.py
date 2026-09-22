import struct
from typing import Any, Optional

PACKET_ID_LAP_DATA = 2
PACKET_ID_CAR_TELEMETRY = 6

# --- PacketHeader: 29 bytes ---
HEADER_FORMAT = "<HBBBBBQfIIBB"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 29

NUM_CARS = 24

# --- CarTelemetryData: 59 bytes per car, 24 cars ---
# speed, throttle, steer, brake, clutch, gear, engineRPM, drs,
# revLightsPercent, revLightsBitValue, brakesTemp[4], tyresSurfaceTemp[4],
# tyresInnerTemp[4], engineTemp (uint8), tyresPressure[4], surfaceType[4]
CAR_TELEMETRY_FORMAT = "<HfffBbHBBH4H4B4BB4f4B"
CAR_TELEMETRY_SIZE = struct.calcsize(CAR_TELEMETRY_FORMAT)  # 59
CAR_TELEMETRY_PACKET_SIZE = 1448  # 29 + 24*59 + 3

# --- LapData: 57 bytes per car, 24 cars ---
# lastLapTimeMS, currentLapTimeMS, sector1(ms,min), sector2(ms,min),
# deltaCarInFront(ms,min), deltaRaceLeader(ms,min), lapDistance,
# totalDistance, safetyCarDelta, carPosition, currentLapNum, pitStatus,
# numPitStops, sector, currentLapInvalid, penalties, totalWarnings,
# cornerCuttingWarnings, unservedDriveThrough, unservedStopGo,
# gridPosition, driverStatus, resultStatus, pitLaneTimerActive,
# pitLaneTimeInLaneMS, pitStopTimerMS, pitStopShouldServePen,
# speedTrapFastestSpeed, speedTrapFastestLap
LAP_DATA_FORMAT = "<IIHBHBHBHBfffBBBBBBBBBBBBBBBHHBfB"
LAP_DATA_SIZE = struct.calcsize(LAP_DATA_FORMAT)  # 57
LAP_DATA_PACKET_SIZE = 1399  # 29 + 24*57 + 2


def parse_header(data: bytes) -> dict[str, Any]:
    """Decode the 29-byte packet header present in every F1 25 packet."""
    if len(data) < HEADER_SIZE:
        raise ValueError(f"Packet too short for header: {len(data)} bytes")
    fields = struct.unpack_from(HEADER_FORMAT, data, 0)
    return {
        "packet_format": fields[0],
        "game_year": fields[1],
        "packet_id": fields[5],
        "session_uid": fields[6],
        "session_time": fields[7],
        "frame_identifier": fields[8],
        "player_car_index": fields[10],
    }


def parse_car_telemetry(data: bytes, car_index: int) -> dict[str, Any]:
    """Decode one car's telemetry from a Car Telemetry packet (ID 6)."""
    offset = HEADER_SIZE + car_index * CAR_TELEMETRY_SIZE
    f = struct.unpack_from(CAR_TELEMETRY_FORMAT, data, offset)
    return {
        "speed": f[0],           # km/h
        "throttle": round(f[1], 3),   # 0.0 - 1.0
        "brake": round(f[3], 3),      # 0.0 - 1.0
        "gear": f[5],            # -1 reverse, 0 neutral, 1-8
        "engine_rpm": f[6],
        "drs": f[7],             # 0 off, 1 on
    }


def parse_lap_data(data: bytes, car_index: int) -> dict[str, Any]:
    """Decode one car's lap data from a Lap Data packet (ID 2)."""
    offset = HEADER_SIZE + car_index * LAP_DATA_SIZE
    f = struct.unpack_from(LAP_DATA_FORMAT, data, offset)
    return {
        "last_lap_time_ms": f[0],
        "current_lap_time_ms": f[1],
        "lap_distance": round(f[10], 1),  # metres into current lap
        "car_position": f[13],
        "lap_number": f[14],
        "pit_status": f[15],     # 0 none, 1 pitting, 2 in pit area
        "driver_status": f[25],  # 0 in garage, 1 flying lap, 2 in lap, 3 out lap, 4 on track
    }


def parse_packet(data: bytes) -> Optional[dict[str, Any]]:
    """Decode a raw UDP packet into a dict for the player's car.

    Returns None for packet types we don't handle yet.
    """
    header = parse_header(data)
    packet_id = header["packet_id"]
    car_index = header["player_car_index"]

    if packet_id == PACKET_ID_CAR_TELEMETRY:
        if len(data) != CAR_TELEMETRY_PACKET_SIZE:
            raise ValueError(
                f"Unexpected Car Telemetry packet size: {len(data)} "
                f"(expected {CAR_TELEMETRY_PACKET_SIZE})"
            )
        body = parse_car_telemetry(data, car_index)
    elif packet_id == PACKET_ID_LAP_DATA:
        if len(data) != LAP_DATA_PACKET_SIZE:
            raise ValueError(
                f"Unexpected Lap Data packet size: {len(data)} "
                f"(expected {LAP_DATA_PACKET_SIZE})"
            )
        body = parse_lap_data(data, car_index)
    else:
        return None

    return {
        "packet_id": packet_id,
        "session_time": round(header["session_time"], 3),
        "car_index": car_index,
        **body,
    }
