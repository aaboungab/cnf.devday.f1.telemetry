import socket
import time

UDP_IP = "0.0.0.0"
UDP_PORT = 20777


def main() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((UDP_IP, UDP_PORT))
    print(f"Listening on UDP port {UDP_PORT}... (Ctrl+C to stop)")

    packet_count = 0
    window_start = time.monotonic()
    window_packets = 0

    try:
        while True:
            data, addr = sock.recvfrom(2048)
            packet_count += 1
            window_packets += 1

            now = time.monotonic()
            elapsed = now - window_start
            if elapsed >= 1.0:
                rate = window_packets / elapsed
                first_bytes = data[:8].hex(" ")
                print(
                    f"Packets: {packet_count:>7} | "
                    f"rate: {rate:6.1f}/s | "
                    f"last size: {len(data):>4} bytes | "
                    f"from {addr[0]} | "
                    f"first bytes: {first_bytes}"
                )
                window_start = now
                window_packets = 0
    except KeyboardInterrupt:
        print(f"\nStopped. Total packets received: {packet_count}")
    finally:
        sock.close()


if __name__ == "__main__":
    main()
