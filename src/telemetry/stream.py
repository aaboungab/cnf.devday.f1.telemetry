import json
import logging
import os
import socket
import time

from src.telemetry import parser
from src.telemetry.events import TelemetryNormalizer

UDP_IP = "0.0.0.0"
UDP_PORT = int(os.environ.get("F1_UDP_PORT", "20777"))
EMIT_HZ = float(os.environ.get("F1_EMIT_HZ", "10"))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("stream")


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    log.info("Listening on UDP port %d, emitting max %s events/s", UDP_PORT, EMIT_HZ or "unlimited")

    normalizer = TelemetryNormalizer()
    min_interval = 1.0 / EMIT_HZ if EMIT_HZ > 0 else 0.0
    last_emit = 0.0
    emitted = 0
    received = 0

    try:
        while True:
            data, _ = sock.recvfrom(2048)
            received += 1
            try:
                event = normalizer.handle(parser.parse_packet(data))
            except ValueError as exc:
                log.warning("decode error: %s", exc)
                continue
            if event is None:
                continue

            now = time.monotonic()
            if now - last_emit < min_interval:
                continue
            last_emit = now
            emitted += 1
            print(json.dumps(event), flush=True)
    except KeyboardInterrupt:
        log.info("Stopped. Packets received: %d, events emitted: %d", received, emitted)
    finally:
        sock.close()


if __name__ == "__main__":
    main()
