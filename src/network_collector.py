"""
Real network metrics collector.
Measures inter-node latency, throughput, packet drops, TCP retransmits.
Runs on each GPU node alongside gpu_collector.

Usage:
    NODE_ID=node-1 PEER_IP=10.0.1.11 python -m src.network_collector
"""

import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

try:
    import psutil
except ImportError:
    print("ERROR: psutil not installed. Run: pip install psutil", file=sys.stderr)
    sys.exit(1)

NODE_ID = os.getenv("NODE_ID", socket.gethostname())
PEER_IP = os.getenv("PEER_IP", "")
COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "5"))
OUTPUT_FILE = os.getenv("NET_METRICS_OUTPUT", "./data/net_metrics_raw/metrics.jsonl")

# State for delta computation
_prev_tx_bytes = None
_prev_rx_bytes = None
_prev_drops = None
_prev_retransmits = None
_prev_time = None


def _ping_latency_us(peer_ip: str) -> float:
    """Measure round-trip latency to peer node via ping. Returns microseconds."""
    if not peer_ip:
        return -1.0
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", "1", peer_ip],
            capture_output=True,
            text=True,
            timeout=3,
        )
        for line in result.stdout.split("\n"):
            if "time=" in line:
                # Parse "time=0.432 ms" → 432.0 µs
                ms_str = line.split("time=")[1].split(" ")[0]
                return float(ms_str) * 1000.0
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError, IndexError):
        pass
    return -1.0


def _get_tcp_retransmits() -> int:
    """Read cumulative TCP retransmit count from /proc/net/netstat."""
    try:
        with open("/proc/net/netstat") as f:
            lines = f.readlines()
        for i, line in enumerate(lines):
            if line.startswith("TcpExt:") and i + 1 < len(lines):
                keys = line.split()
                vals = lines[i + 1].split()
                if "TCPRetransSegs" in keys:
                    idx = keys.index("TCPRetransSegs")
                    return int(vals[idx])
    except (FileNotFoundError, ValueError, IndexError):
        pass
    return 0


def collect_network_metrics() -> dict | None:
    """Collect one sample of network metrics. Returns None on first call (need delta)."""
    global _prev_tx_bytes, _prev_rx_bytes, _prev_drops, _prev_retransmits, _prev_time

    now = time.time()
    ingestion_time = datetime.now(timezone.utc)
    net = psutil.net_io_counters()
    retransmits = _get_tcp_retransmits()
    latency_us = _ping_latency_us(PEER_IP)

    if _prev_time is None:
        # First call — store baselines
        _prev_tx_bytes = net.bytes_sent
        _prev_rx_bytes = net.bytes_recv
        _prev_drops = net.dropin + net.dropout
        _prev_retransmits = retransmits
        _prev_time = now
        return None

    elapsed = now - _prev_time
    if elapsed <= 0:
        return None

    # Compute deltas
    tx_mbps = round(((net.bytes_sent - _prev_tx_bytes) * 8) / elapsed / 1_000_000, 4)
    rx_mbps = round(((net.bytes_recv - _prev_rx_bytes) * 8) / elapsed / 1_000_000, 4)
    drop_delta = (net.dropin + net.dropout) - _prev_drops
    retransmit_delta = retransmits - _prev_retransmits

    # Update state
    _prev_tx_bytes = net.bytes_sent
    _prev_rx_bytes = net.bytes_recv
    _prev_drops = net.dropin + net.dropout
    _prev_retransmits = retransmits
    _prev_time = now

    return {
        "timestamp_ms": int(now * 1000),
        "node_id": NODE_ID,
        "inter_node_latency_us": round(latency_us, 2),
        "throughput_tx_mbps": tx_mbps,
        "throughput_rx_mbps": rx_mbps,
        "packet_drop_count": max(0, drop_delta),
        "tcp_retransmit_count": max(0, retransmit_delta),
        "ingestion_date": ingestion_time.strftime("%Y-%m-%d"),
        "ingestion_hour": ingestion_time.hour,
    }


def main():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    print(
        f"Network Collector: {NODE_ID} → peer {PEER_IP or '(none)'}, interval={COLLECT_INTERVAL}s",
        file=sys.stderr,
    )

    # Push mode: POST to remote server if INGEST_URL is set
    ingest_url = os.getenv("INGEST_URL", "")
    session = None
    if ingest_url:
        import requests
        session = requests.Session()
        print(f"Push mode: sending to {ingest_url}/ingest/network_metrics", file=sys.stderr)

    with open(OUTPUT_FILE, "a") as f:
        while True:
            row = collect_network_metrics()
            if row:
                line = json.dumps(row)
                print(line, flush=True)
                f.write(line + "\n")
                f.flush()

                # Push to remote server
                if session and ingest_url:
                    try:
                        session.post(
                            f"{ingest_url}/ingest/network_metrics",
                            json=row,
                            timeout=5,
                        )
                    except Exception as e:
                        print(f"Push failed: {e}", file=sys.stderr)

            time.sleep(COLLECT_INTERVAL)


if __name__ == "__main__":
    main()
