"""
Real GPU metrics collector using NVIDIA NVML.
Runs on each GPU node, polls every COLLECT_INTERVAL seconds,
writes JSONL to stdout and optionally to a local file for ingestion.

Usage:
    NODE_ID=node-1 PEER_IP=10.0.1.11 python -m src.gpu_collector
"""

import json
import os
import socket
import sys
import time
from datetime import datetime, timezone

try:
    import pynvml
except ImportError:
    print("ERROR: pynvml not installed. Run: pip install pynvml", file=sys.stderr)
    sys.exit(1)

NODE_ID = os.getenv("NODE_ID", socket.gethostname())
COLLECT_INTERVAL = int(os.getenv("COLLECT_INTERVAL", "5"))
OUTPUT_FILE = os.getenv("GPU_METRICS_OUTPUT", "./data/gpu_metrics_raw/metrics.jsonl")


def init_nvml():
    """Initialize NVML and return device handles."""
    pynvml.nvmlInit()
    device_count = pynvml.nvmlDeviceGetCount()
    handles = [pynvml.nvmlDeviceGetHandleByIndex(i) for i in range(device_count)]
    driver_version = pynvml.nvmlSystemGetDriverVersion()
    print(f"NVML initialized: {device_count} GPU(s), driver {driver_version}", file=sys.stderr)
    return handles


def collect_gpu_metrics(handles):
    """Collect one sample of GPU metrics from all devices."""
    ts = int(time.time() * 1000)
    ingestion_time = datetime.now(timezone.utc)
    rows = []

    for i, handle in enumerate(handles):
        try:
            util = pynvml.nvmlDeviceGetUtilizationRates(handle)
            mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
            power = pynvml.nvmlDeviceGetPowerUsage(handle)  # milliwatts
            power_limit = pynvml.nvmlDeviceGetPowerManagementLimit(handle)
            temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            clock_sm = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_SM)
            clock_mem = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_MEM)
            throttle = pynvml.nvmlDeviceGetCurrentClocksThrottleReasons(handle)
            uuid = pynvml.nvmlDeviceGetUUID(handle)

            # PCIe throughput (returns KB/s)
            try:
                pcie_tx = pynvml.nvmlDeviceGetPcieThroughput(
                    handle, pynvml.NVML_PCIE_UTIL_TX_BYTES
                )
                pcie_rx = pynvml.nvmlDeviceGetPcieThroughput(
                    handle, pynvml.NVML_PCIE_UTIL_RX_BYTES
                )
            except pynvml.NVMLError:
                pcie_tx = 0
                pcie_rx = 0

            # ECC errors (not all GPUs support this)
            try:
                ecc = pynvml.nvmlDeviceGetTotalEccErrors(
                    handle,
                    pynvml.NVML_MEMORY_ERROR_TYPE_UNCORRECTED,
                    pynvml.NVML_VOLATILE_ECC,
                )
            except pynvml.NVMLError:
                ecc = 0

            gpu_utilization = float(util.gpu)
            memory_controller_util = float(util.memory)
            memory_used_pct = round((mem.used / mem.total) * 100, 2) if mem.total > 0 else 0.0
            temperature_c = float(temp)
            power_draw_pct = round((power / power_limit) * 100, 2) if power_limit > 0 else 0.0
            throttle_active = 1 if throttle != 0 else 0
            pcie_tx_mbps = round(pcie_tx / 1000.0, 2)  # KB/s → MB/s
            pcie_rx_mbps = round(pcie_rx / 1000.0, 2)

            # Contention index: weighted composite
            contention_index = round(
                0.20 * (gpu_utilization / 100.0)
                + 0.15 * (memory_controller_util / 100.0)
                + 0.15 * (memory_used_pct / 100.0)
                + 0.15 * min(temperature_c / 90.0, 1.0)
                + 0.15 * (power_draw_pct / 100.0)
                + 0.10 * float(throttle_active)
                + 0.10 * min((pcie_tx_mbps + pcie_rx_mbps) / 15000.0, 1.0),
                4,
            )

            rows.append({
                "timestamp_ms": ts,
                "node_id": NODE_ID,
                "gpu_id": f"gpu-{i}",
                "gpu_uuid": uuid,
                "gpu_utilization": gpu_utilization,
                "memory_controller_util": memory_controller_util,
                "memory_used_pct": memory_used_pct,
                "temperature_c": temperature_c,
                "power_draw_pct": power_draw_pct,
                "clock_sm_mhz": float(clock_sm),
                "clock_mem_mhz": float(clock_mem),
                "throttle_active": throttle_active,
                "pcie_tx_mbps": pcie_tx_mbps,
                "pcie_rx_mbps": pcie_rx_mbps,
                "ecc_errors_total": int(ecc),
                "contention_index": contention_index,
                "ingestion_date": ingestion_time.strftime("%Y-%m-%d"),
                "ingestion_hour": ingestion_time.hour,
            })

        except pynvml.NVMLError as e:
            print(f"ERROR reading GPU {i}: {e}", file=sys.stderr)

    return rows


def main():
    handles = init_nvml()
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    print(f"GPU Collector: {len(handles)} GPU(s) on {NODE_ID}, interval={COLLECT_INTERVAL}s", file=sys.stderr)

    # Push mode: POST to remote server if INGEST_URL is set
    ingest_url = os.getenv("INGEST_URL", "")
    session = None
    if ingest_url:
        import requests
        session = requests.Session()
        print(f"Push mode: sending to {ingest_url}/ingest/gpu_metrics", file=sys.stderr)

    with open(OUTPUT_FILE, "a") as f:
        while True:
            rows = collect_gpu_metrics(handles)
            for row in rows:
                line = json.dumps(row)
                print(line, flush=True)
                f.write(line + "\n")
                f.flush()

                # Push to remote server
                if session and ingest_url:
                    try:
                        session.post(
                            f"{ingest_url}/ingest/gpu_metrics",
                            json=row,
                            timeout=5,
                        )
                    except Exception as e:
                        print(f"Push failed: {e}", file=sys.stderr)

            time.sleep(COLLECT_INTERVAL)


if __name__ == "__main__":
    main()
