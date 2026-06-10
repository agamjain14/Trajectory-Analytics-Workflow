#!/usr/bin/env python3
"""
Synthetic data generator for LOCAL mode.
Produces JSONL files identical in format to real gpu_collector and network_collector.
No GPU, no Vast.ai, no Azure needed. Just run this → then run streaming jobs → done.

Usage:
    python3 -m src.synthetic_collector --duration 300 --interval 5
"""
import argparse
import json
import math
import os
import random
import time
from pathlib import Path

from src.contention import compute_contention_index


def generate_gpu_metrics(node_id: str, timestamp_ms: int, t: float, contention_event: bool):
    """Generate one GPU metrics row matching GPU_METRICS_SCHEMA."""
    base_util = 45 + 20 * math.sin(t / 30)  # oscillating load
    if contention_event:
        base_util = min(98, base_util + random.uniform(25, 45))

    gpu_util = max(0, min(100, base_util + random.gauss(0, 5)))
    mem_ctrl = max(0, min(100, gpu_util * 0.7 + random.gauss(0, 3)))
    mem_used = max(10, min(95, 40 + gpu_util * 0.4 + random.gauss(0, 2)))
    temp = max(30, min(92, 35 + gpu_util * 0.5 + random.gauss(0, 2)))
    power = max(20, min(100, 30 + gpu_util * 0.6 + random.gauss(0, 3)))
    clock_sm = int(max(210, min(1800, 1200 + (gpu_util - 50) * 10 + random.gauss(0, 50))))
    clock_mem = int(max(400, min(7000, 5000 + random.gauss(0, 200))))
    throttle = 1 if temp > 83 else 0
    pcie_tx = max(0, min(16000, gpu_util * 80 + random.gauss(0, 500)))
    pcie_rx = max(0, min(16000, gpu_util * 60 + random.gauss(0, 400)))
    ecc = random.randint(0, 1) if random.random() < 0.02 else 0

    # Contention index (canonical shared formula — matches real collector)
    contention = compute_contention_index(
        gpu_util, mem_ctrl, mem_used, temp, power, throttle, pcie_tx, pcie_rx
    )

    return {
        "timestamp_ms": timestamp_ms,
        "node_id": node_id,
        "gpu_id": "gpu-0",
        "gpu_uuid": f"GPU-SIM-{node_id.upper()}-0000",
        "gpu_utilization": round(gpu_util, 1),
        "memory_controller_util": round(mem_ctrl, 1),
        "memory_used_pct": round(mem_used, 1),
        "temperature_c": round(temp, 1),
        "power_draw_pct": round(power, 1),
        "clock_sm_mhz": clock_sm,
        "clock_mem_mhz": clock_mem,
        "throttle_active": throttle,
        "pcie_tx_mbps": round(pcie_tx, 1),
        "pcie_rx_mbps": round(pcie_rx, 1),
        "ecc_errors_total": ecc,
        "contention_index": round(contention, 4),
    }


def generate_network_metrics(node_id: str, peer_id: str, timestamp_ms: int, t: float, contention_event: bool):
    """Generate one network metrics row matching NETWORK_METRICS_SCHEMA."""
    base_latency = 150 + 50 * math.sin(t / 60)
    if contention_event:
        base_latency += random.uniform(200, 800)

    latency = max(50, base_latency + random.gauss(0, 20))
    tx = max(10, min(10000, 2000 + random.gauss(0, 500)))
    rx = max(10, min(10000, 1800 + random.gauss(0, 500)))
    drops = random.randint(0, 3) if contention_event else (1 if random.random() < 0.05 else 0)
    retransmits = random.randint(0, 5) if contention_event else (1 if random.random() < 0.08 else 0)

    return {
        "timestamp_ms": timestamp_ms,
        "node_id": node_id,
        "peer_node_id": peer_id,
        "inter_node_latency_us": round(latency, 1),
        "throughput_tx_mbps": round(tx, 1),
        "throughput_rx_mbps": round(rx, 1),
        "packet_drop_count": drops,
        "tcp_retransmit_count": retransmits,
    }


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic GPU/network JSONL for local mode")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds of simulated data")
    parser.add_argument("--interval", type=int, default=5, help="Seconds between samples")
    parser.add_argument("--output-dir", default="./data", help="Base output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)

    gpu_dir = Path(args.output_dir) / "gpu_metrics_raw"
    net_dir = Path(args.output_dir) / "net_metrics_raw"
    gpu_dir.mkdir(parents=True, exist_ok=True)
    net_dir.mkdir(parents=True, exist_ok=True)

    nodes = ["node-1", "node-2"]
    base_ts = int(time.time() * 1000) - (args.duration * 1000)  # start in the past

    # Generate contention events (bursts at random intervals)
    contention_windows = []
    t = 30
    while t < args.duration:
        contention_windows.append((t, t + random.randint(15, 45)))
        t += random.randint(60, 120)

    def is_contention(t_sec):
        return any(start <= t_sec <= end for start, end in contention_windows)

    gpu_count = 0
    net_count = 0

    # Write JSONL files per node
    for node in nodes:
        peer = [n for n in nodes if n != node][0]
        gpu_file = gpu_dir / f"{node}.jsonl"
        net_file = net_dir / f"{node}.jsonl"

        with open(gpu_file, "w") as gf, open(net_file, "w") as nf:
            for step in range(0, args.duration, args.interval):
                ts = base_ts + step * 1000
                contention = is_contention(step)

                gpu_row = generate_gpu_metrics(node, ts, step, contention)
                gf.write(json.dumps(gpu_row) + "\n")
                gpu_count += 1

                net_row = generate_network_metrics(node, peer, ts, step, contention)
                nf.write(json.dumps(net_row) + "\n")
                net_count += 1

    print(f"Generated {gpu_count} GPU metrics rows → {gpu_dir}/")
    print(f"Generated {net_count} network metrics rows → {net_dir}/")
    print(f"Contention events: {len(contention_windows)} bursts")
    print(f"Duration simulated: {args.duration}s ({args.duration // 60}m)")
    print(f"\nNext: run streaming jobs to process into Delta tables.")


if __name__ == "__main__":
    main()
