import os
import socket
import time

from src.telemetry import parser

UDP_IP = "0.0.0.0"
UDP_PORT = int(os.environ.get("F1_UDP_PORT", "20777"))
PRINT_INTERVAL_S = 0.5


def format_lap_time(ms: int) -> str:
    if ms == 0:
        return "--:--.---"
    minutes, rem = divmod(ms, 60_000)
    return f"{minutes}:{rem / 1000:06.3f}"


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    print(f"Listening on UDP port {UDP_PORT}... decoding (Ctrl+C to stop)\n")

    latest_telemetry: dict = {}
    latest_lap: dict = {}
    decode_errors = 0
    last_print = time.monotonic()

    try:
        while True:
            data, _ = sock.recvfrom(2048)
            try:
                event = parser.parse_packet(data)
            except ValueError as exc:
                decode_errors += 1
                print(f"DECODE ERROR: {exc}")
                continue

            if event is None:
                continue
            if event["packet_id"] == parser.PACKET_ID_CAR_TELEMETRY:
                latest_telemetry = event
            elif event["packet_id"] == parser.PACKET_ID_LAP_DATA:
                latest_lap = event

            now = time.monotonic()
            if now - last_print >= PRINT_INTERVAL_S and latest_telemetry:
                t = latest_telemetry
                lap = latest_lap
                print(
                    f"speed: {t['speed']:>3} km/h | "
                    f"gear: {t['gear']:>2} | "
                    f"rpm: {t['engine_rpm']:>5} | "
                    f"throttle: {t['throttle']:4.2f} | "
                    f"brake: {t['brake']:4.2f} | "
                    f"lap: {lap.get('lap_number', '?'):>2} | "
                    f"pos: {lap.get('car_position', '?'):>2} | "
                    f"lap time: {format_lap_time(lap.get('current_lap_time_ms', 0))}"
                )
                last_print = now
    except KeyboardInterrupt:
        print(f"\nStopped. Decode errors: {decode_errors}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
