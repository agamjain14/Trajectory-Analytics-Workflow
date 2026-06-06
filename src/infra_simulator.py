"""
Phase 3: Infrastructure Simulator + Request Routing.
Generates synthetic GPU metrics, network metrics, and topology-aware
request routing for agent trace spans.

Components:
1. Static topology definition (nodes, GPUs, NICs, switches, pods)
2. GPU metrics time-series generator with contention events
3. Network metrics time-series generator
4. Request routing assignment (REASON spans → pods)
"""

import hashlib
import json
import logging
import math
import os
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("infra_simulator")

# --- Configuration ---
AGENT_STEPS_PATH = os.getenv("AGENT_STEPS_PATH", "./data/agent_steps")
GPU_METRICS_PATH = os.getenv("GPU_METRICS_PATH", "./data/gpu_metrics")
NETWORK_METRICS_PATH = os.getenv("NETWORK_METRICS_PATH", "./data/network_metrics")
ROUTING_PATH = os.getenv("ROUTING_PATH", "./data/request_routing")
TOPOLOGY_PATH = os.getenv("TOPOLOGY_PATH", "./data/topology")

# Simulation config
METRIC_INTERVAL_MS = 5000  # one sample every 5 seconds
RANDOM_SEED = int(os.getenv("SIM_SEED", "42"))

# --- Static Topology ---
TOPOLOGY = {
    "cluster": "gpu-cluster-west",
    "switches": [
        {
            "id": "tor-rack-01",
            "ports": {
                "port-1": {"connected_node": "node-1"},
                "port-2": {"connected_node": "node-2"},
                "port-3": {"connected_node": "node-3"},
            },
        }
    ],
    "nodes": [
        {
            "id": "node-1",
            "gpus": [
                {"id": "gpu-0", "uuid": "GPU-a1b2c3d4", "memory_gb": 24},
                {"id": "gpu-1", "uuid": "GPU-e5f6g7h8", "memory_gb": 24},
            ],
            "nic": {"id": "nic-1", "bandwidth_gbps": 100},
            "switch_port": "tor-rack-01:port-1",
        },
        {
            "id": "node-2",
            "gpus": [
                {"id": "gpu-0", "uuid": "GPU-i9j0k1l2", "memory_gb": 24},
                {"id": "gpu-1", "uuid": "GPU-m3n4o5p6", "memory_gb": 24},
            ],
            "nic": {"id": "nic-2", "bandwidth_gbps": 100},
            "switch_port": "tor-rack-01:port-2",
        },
        {
            "id": "node-3",
            "gpus": [
                {"id": "gpu-0", "uuid": "GPU-q7r8s9t0", "memory_gb": 24},
                {"id": "gpu-1", "uuid": "GPU-u1v2w3x4", "memory_gb": 24},
            ],
            "nic": {"id": "nic-3", "bandwidth_gbps": 100},
            "switch_port": "tor-rack-01:port-3",
        },
    ],
    "pods": [
        {"id": "vllm-1", "node": "node-1", "gpu": "gpu-0"},
        {"id": "vllm-2", "node": "node-1", "gpu": "gpu-1"},
        {"id": "vllm-3", "node": "node-2", "gpu": "gpu-0"},
        {"id": "vllm-4", "node": "node-2", "gpu": "gpu-1"},
        {"id": "vllm-5", "node": "node-3", "gpu": "gpu-0"},
        {"id": "vllm-6", "node": "node-3", "gpu": "gpu-1"},
    ],
}

# Contention scenarios that get injected
CONTENTION_SCENARIOS = [
    {
        "name": "training_job_node2",
        "description": "A training job lands on node-2, saturating both GPUs and NIC",
        "affected_components": {
            "gpus": [("node-2", "gpu-0"), ("node-2", "gpu-1")],
            "nics": ["node-2"],
            "switch_ports": [],
        },
        "duration_minutes": 8,
        "gpu_intensity": 0.85,
        "nic_intensity": 0.75,
    },
    {
        "name": "switch_congestion",
        "description": "Network congestion on switch port-2 affecting node-2",
        "affected_components": {
            "gpus": [],
            "nics": ["node-2"],
            "switch_ports": ["tor-rack-01:port-2"],
        },
        "duration_minutes": 4,
        "gpu_intensity": 0.0,
        "nic_intensity": 0.80,
    },
    {
        "name": "thermal_throttle_node1",
        "description": "Thermal throttling on node-1 gpu-1",
        "affected_components": {
            "gpus": [("node-1", "gpu-1")],
            "nics": [],
            "switch_ports": [],
        },
        "duration_minutes": 5,
        "gpu_intensity": 0.65,
        "nic_intensity": 0.0,
    },
    {
        "name": "multi_tenant_burst_node3",
        "description": "Burst of inference requests from another tenant on node-3",
        "affected_components": {
            "gpus": [("node-3", "gpu-0"), ("node-3", "gpu-1")],
            "nics": ["node-3"],
            "switch_ports": ["tor-rack-01:port-3"],
        },
        "duration_minutes": 6,
        "gpu_intensity": 0.78,
        "nic_intensity": 0.60,
    },
]

# --- Schemas ---
GPU_METRICS_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("node_id", pa.string()),
    ("gpu_id", pa.string()),
    ("gpu_uuid", pa.string()),
    ("gpu_utilization", pa.float64()),
    ("sm_occupancy", pa.float64()),
    ("memory_used_pct", pa.float64()),
    ("queue_delay_ms", pa.float64()),
    ("power_draw_pct", pa.float64()),
    ("pcie_bandwidth_pct", pa.float64()),
    ("contention_index", pa.float64()),
])

NETWORK_METRICS_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("node_id", pa.string()),
    ("nic_id", pa.string()),
    ("switch_port", pa.string()),
    ("nic_bandwidth_pct", pa.float64()),
    ("packet_drop_rate", pa.float64()),
    ("switch_port_util", pa.float64()),
    ("latency_us", pa.float64()),
])

ROUTING_SCHEMA = pa.schema([
    ("trace_id", pa.string()),
    ("span_id", pa.string()),
    ("vllm_pod_id", pa.string()),
    ("node_id", pa.string()),
    ("gpu_id", pa.string()),
    ("gpu_uuid", pa.string()),
    ("nic_id", pa.string()),
    ("switch_port", pa.string()),
    ("timestamp_ms", pa.int64()),
])

TOPOLOGY_SCHEMA = pa.schema([
    ("entity_type", pa.string()),  # switch, node, gpu, nic, pod
    ("entity_id", pa.string()),
    ("parent_id", pa.string()),
    ("properties", pa.string()),   # JSON
])


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _compute_contention_index(gpu_util: float, sm_occ: float, mem_pct: float,
                               queue_delay: float, power_pct: float) -> float:
    """Composite contention index (0-1). Weighted average of normalized signals."""
    # Normalize queue delay (0-500ms → 0-1)
    norm_queue = min(queue_delay / 500.0, 1.0)
    return _clamp(
        0.25 * (gpu_util / 100.0) +
        0.20 * (sm_occ / 100.0) +
        0.20 * (mem_pct / 100.0) +
        0.25 * norm_queue +
        0.10 * (power_pct / 100.0),
        0.0, 1.0
    )


@dataclass
class ContentionEvent:
    """A scheduled contention event."""
    scenario_name: str
    start_ms: int
    end_ms: int
    affected_gpus: list  # [(node_id, gpu_id), ...]
    affected_nics: list  # [node_id, ...]
    affected_switch_ports: list  # [switch_port, ...]
    gpu_intensity: float
    nic_intensity: float


def _schedule_contention_events(start_ms: int, end_ms: int, rng: random.Random) -> list[ContentionEvent]:
    """
    Schedule contention events across the time range.
    Places events with gaps, ensuring realistic spacing.
    """
    events = []
    duration_ms = end_ms - start_ms

    # Place 2-4 contention events spread across the time range
    num_events = min(len(CONTENTION_SCENARIOS), max(2, int(duration_ms / (10 * 60 * 1000))))

    # Divide time into segments, place one event per segment
    segment_size = duration_ms // (num_events + 1)

    selected_scenarios = rng.sample(CONTENTION_SCENARIOS, min(num_events, len(CONTENTION_SCENARIOS)))

    for i, scenario in enumerate(selected_scenarios):
        # Place event in the (i+1)th segment with some jitter
        segment_start = start_ms + (i + 1) * segment_size
        jitter = rng.randint(-int(segment_size * 0.2), int(segment_size * 0.2))
        event_start = segment_start + jitter
        event_duration_ms = scenario["duration_minutes"] * 60 * 1000
        event_end = min(event_start + event_duration_ms, end_ms)

        events.append(ContentionEvent(
            scenario_name=scenario["name"],
            start_ms=event_start,
            end_ms=event_end,
            affected_gpus=scenario["affected_components"]["gpus"],
            affected_nics=scenario["affected_components"]["nics"],
            affected_switch_ports=scenario["affected_components"]["switch_ports"],
            gpu_intensity=scenario["gpu_intensity"],
            nic_intensity=scenario["nic_intensity"],
        ))

    return sorted(events, key=lambda e: e.start_ms)


def _is_gpu_contended(node_id: str, gpu_id: str, timestamp_ms: int,
                       events: list[ContentionEvent]) -> tuple[bool, float]:
    """Check if a specific GPU is under contention at a given time."""
    for event in events:
        if event.start_ms <= timestamp_ms <= event.end_ms:
            if (node_id, gpu_id) in event.affected_gpus:
                return True, event.gpu_intensity
    return False, 0.0


def _is_nic_contended(node_id: str, timestamp_ms: int,
                       events: list[ContentionEvent]) -> tuple[bool, float]:
    """Check if a node's NIC is under contention at a given time."""
    for event in events:
        if event.start_ms <= timestamp_ms <= event.end_ms:
            if node_id in event.affected_nics:
                return True, event.nic_intensity
    return False, 0.0


def _is_switch_contended(switch_port: str, timestamp_ms: int,
                          events: list[ContentionEvent]) -> tuple[bool, float]:
    """Check if a switch port is under contention at a given time."""
    for event in events:
        if event.start_ms <= timestamp_ms <= event.end_ms:
            if switch_port in event.affected_switch_ports:
                return True, event.nic_intensity
    return False, 0.0


def generate_gpu_metrics(start_ms: int, end_ms: int,
                          events: list[ContentionEvent],
                          rng: random.Random) -> list[dict]:
    """Generate GPU metric samples for all GPUs across the time range."""
    rows = []
    timestamp = start_ms

    while timestamp <= end_ms:
        for node in TOPOLOGY["nodes"]:
            node_id = node["id"]
            for gpu in node["gpus"]:
                gpu_id = gpu["id"]
                gpu_uuid = gpu["uuid"]

                contended, intensity = _is_gpu_contended(node_id, gpu_id, timestamp, events)

                if contended:
                    # Under contention: high utilization, long queues
                    gpu_util = _clamp(rng.gauss(50 + intensity * 48, 4), 0, 100)
                    sm_occ = _clamp(rng.gauss(45 + intensity * 50, 5), 0, 100)
                    mem_pct = _clamp(rng.gauss(50 + intensity * 42, 4), 0, 100)
                    queue_delay = max(0, rng.gauss(intensity * 300, 60))
                    power_pct = _clamp(rng.gauss(55 + intensity * 40, 5), 0, 100)
                    pcie_bw = _clamp(rng.gauss(40 + intensity * 50, 8), 0, 100)
                else:
                    # Normal operation
                    gpu_util = _clamp(rng.gauss(40, 12), 5, 75)
                    sm_occ = _clamp(rng.gauss(45, 10), 10, 70)
                    mem_pct = _clamp(rng.gauss(48, 8), 20, 65)
                    queue_delay = max(0, rng.gauss(3, 2))
                    power_pct = _clamp(rng.gauss(55, 8), 30, 75)
                    pcie_bw = _clamp(rng.gauss(25, 8), 5, 50)

                contention_idx = _compute_contention_index(
                    gpu_util, sm_occ, mem_pct, queue_delay, power_pct
                )

                rows.append({
                    "timestamp_ms": timestamp,
                    "node_id": node_id,
                    "gpu_id": gpu_id,
                    "gpu_uuid": gpu_uuid,
                    "gpu_utilization": round(gpu_util, 2),
                    "sm_occupancy": round(sm_occ, 2),
                    "memory_used_pct": round(mem_pct, 2),
                    "queue_delay_ms": round(queue_delay, 2),
                    "power_draw_pct": round(power_pct, 2),
                    "pcie_bandwidth_pct": round(pcie_bw, 2),
                    "contention_index": round(contention_idx, 4),
                })

        timestamp += METRIC_INTERVAL_MS

    return rows


def generate_network_metrics(start_ms: int, end_ms: int,
                              events: list[ContentionEvent],
                              rng: random.Random) -> list[dict]:
    """Generate network metric samples for all NICs/switch ports."""
    rows = []
    timestamp = start_ms

    while timestamp <= end_ms:
        for node in TOPOLOGY["nodes"]:
            node_id = node["id"]
            nic_id = node["nic"]["id"]
            switch_port = node["switch_port"]

            nic_contended, nic_intensity = _is_nic_contended(node_id, timestamp, events)
            sw_contended, sw_intensity = _is_switch_contended(switch_port, timestamp, events)

            combined_intensity = max(nic_intensity if nic_contended else 0,
                                     sw_intensity if sw_contended else 0)

            if combined_intensity > 0:
                nic_bw = _clamp(rng.gauss(30 + combined_intensity * 65, 8), 0, 100)
                drop_rate = max(0, rng.gauss(combined_intensity * 0.05, 0.01))
                sw_util = _clamp(rng.gauss(30 + combined_intensity * 60, 8), 0, 100)
                latency = max(50, rng.gauss(100 + combined_intensity * 800, 100))
            else:
                nic_bw = _clamp(rng.gauss(20, 8), 2, 45)
                drop_rate = max(0, rng.gauss(0.001, 0.0005))
                sw_util = _clamp(rng.gauss(18, 6), 2, 40)
                latency = max(20, rng.gauss(80, 15))

            rows.append({
                "timestamp_ms": timestamp,
                "node_id": node_id,
                "nic_id": nic_id,
                "switch_port": switch_port,
                "nic_bandwidth_pct": round(nic_bw, 2),
                "packet_drop_rate": round(drop_rate, 6),
                "switch_port_util": round(sw_util, 2),
                "latency_us": round(latency, 2),
            })

        timestamp += METRIC_INTERVAL_MS

    return rows


def generate_request_routing(agent_steps_path: str,
                              events: list[ContentionEvent],
                              rng: random.Random) -> list[dict]:
    """
    Assign each REASON span to a vLLM pod based on simulated load balancing.
    Considers pod queue depth and avoids heavily contended pods.
    """
    if not DeltaTable.is_deltatable(agent_steps_path):
        logger.error(f"agent_steps not found at {agent_steps_path}")
        return []

    dt = DeltaTable(agent_steps_path)
    table = dt.to_pyarrow_table()

    # Get REASON spans only (these are LLM inference requests)
    reason_spans = []
    for i in range(table.num_rows):
        if table.column("span_kind")[i].as_py() == "REASON":
            reason_spans.append({
                "trace_id": table.column("trace_id")[i].as_py(),
                "span_id": table.column("span_id")[i].as_py(),
                "start_ts_ms": table.column("start_ts_ms")[i].as_py(),
            })

    logger.info(f"Routing {len(reason_spans)} REASON spans to pods")

    # Simple weighted round-robin with queue simulation
    pod_queue = {pod["id"]: 0 for pod in TOPOLOGY["pods"]}
    pods = TOPOLOGY["pods"]

    rows = []
    for span in sorted(reason_spans, key=lambda s: s["start_ts_ms"] or 0):
        # Weighted selection: prefer pods with shorter queues
        weights = []
        for pod in pods:
            queue = pod_queue[pod["id"]]
            # Also penalize pods on contended GPUs
            node_id = pod["node"]
            gpu_id = pod["gpu"]
            contended, _ = _is_gpu_contended(node_id, gpu_id, span["start_ts_ms"] or 0, events)
            # Higher weight = more likely to be selected
            weight = 1.0 / (queue + 1)
            if contended:
                weight *= 0.3  # still gets some traffic (realistic LB doesn't know about GPU state)
            weights.append(weight)

        selected_pod = rng.choices(pods, weights=weights, k=1)[0]

        # Increment queue, decay over time
        pod_queue[selected_pod["id"]] += 1
        # Decay all queues slightly (simulates completion)
        for pid in pod_queue:
            pod_queue[pid] = max(0, pod_queue[pid] - 0.3)

        # Resolve topology path
        node = next(n for n in TOPOLOGY["nodes"] if n["id"] == selected_pod["node"])
        gpu = next(g for g in node["gpus"] if g["id"] == selected_pod["gpu"])

        rows.append({
            "trace_id": span["trace_id"],
            "span_id": span["span_id"],
            "vllm_pod_id": selected_pod["id"],
            "node_id": node["id"],
            "gpu_id": selected_pod["gpu"],
            "gpu_uuid": gpu["uuid"],
            "nic_id": node["nic"]["id"],
            "switch_port": node["switch_port"],
            "timestamp_ms": span["start_ts_ms"],
        })

    return rows


def save_topology() -> None:
    """Save topology as a Delta table for API access."""
    rows = []

    # Switches
    for switch in TOPOLOGY["switches"]:
        rows.append({
            "entity_type": "switch",
            "entity_id": switch["id"],
            "parent_id": TOPOLOGY["cluster"],
            "properties": json.dumps({"ports": list(switch["ports"].keys())}),
        })

    # Nodes
    for node in TOPOLOGY["nodes"]:
        rows.append({
            "entity_type": "node",
            "entity_id": node["id"],
            "parent_id": node["switch_port"].split(":")[0],  # switch id
            "properties": json.dumps({
                "switch_port": node["switch_port"],
                "nic": node["nic"]["id"],
                "gpu_count": len(node["gpus"]),
            }),
        })

        # GPUs
        for gpu in node["gpus"]:
            rows.append({
                "entity_type": "gpu",
                "entity_id": f"{node['id']}:{gpu['id']}",
                "parent_id": node["id"],
                "properties": json.dumps({
                    "uuid": gpu["uuid"],
                    "memory_gb": gpu["memory_gb"],
                }),
            })

        # NIC
        rows.append({
            "entity_type": "nic",
            "entity_id": node["nic"]["id"],
            "parent_id": node["id"],
            "properties": json.dumps({
                "bandwidth_gbps": node["nic"]["bandwidth_gbps"],
                "switch_port": node["switch_port"],
            }),
        })

    # Pods
    for pod in TOPOLOGY["pods"]:
        rows.append({
            "entity_type": "pod",
            "entity_id": pod["id"],
            "parent_id": pod["node"],
            "properties": json.dumps({"gpu": pod["gpu"]}),
        })

    out_table = pa.Table.from_pylist(rows, schema=TOPOLOGY_SCHEMA)
    write_deltalake(TOPOLOGY_PATH, out_table, mode="overwrite")
    logger.info(f"Saved topology ({len(rows)} entities) to {TOPOLOGY_PATH}")


def run_simulation() -> None:
    """Run the full infrastructure simulation."""
    rng = random.Random(RANDOM_SEED)

    # Determine time range from agent_steps
    if not DeltaTable.is_deltatable(AGENT_STEPS_PATH):
        logger.error(f"agent_steps not found at {AGENT_STEPS_PATH}")
        return

    dt = DeltaTable(AGENT_STEPS_PATH)
    table = dt.to_pyarrow_table()

    # Get time range
    timestamps = []
    for i in range(table.num_rows):
        ts = table.column("start_ts_ms")[i].as_py()
        if ts:
            timestamps.append(ts)

    if not timestamps:
        logger.error("No timestamps in agent_steps")
        return

    start_ms = min(timestamps) - (5 * 60 * 1000)   # 5 min before first span
    end_ms = max(timestamps) + (5 * 60 * 1000)      # 5 min after last span
    duration_min = (end_ms - start_ms) / 60000

    logger.info(f"Simulation time range: {duration_min:.1f} minutes")
    logger.info(f"  Start: {start_ms} ms")
    logger.info(f"  End:   {end_ms} ms")

    # 1. Save topology
    save_topology()

    # 2. Schedule contention events
    events = _schedule_contention_events(start_ms, end_ms, rng)
    logger.info(f"Scheduled {len(events)} contention events:")
    for e in events:
        dur = (e.end_ms - e.start_ms) / 60000
        logger.info(f"  {e.scenario_name}: {dur:.1f}min, GPU intensity={e.gpu_intensity}, "
                    f"NIC intensity={e.nic_intensity}")

    # 3. Generate GPU metrics
    logger.info("Generating GPU metrics...")
    gpu_rows = generate_gpu_metrics(start_ms, end_ms, events, rng)
    gpu_table = pa.Table.from_pylist(gpu_rows, schema=GPU_METRICS_SCHEMA)
    write_deltalake(GPU_METRICS_PATH, gpu_table, mode="overwrite")
    logger.info(f"Wrote {len(gpu_rows)} GPU metric samples to {GPU_METRICS_PATH}")

    # 4. Generate network metrics
    logger.info("Generating network metrics...")
    net_rows = generate_network_metrics(start_ms, end_ms, events, rng)
    net_table = pa.Table.from_pylist(net_rows, schema=NETWORK_METRICS_SCHEMA)
    write_deltalake(NETWORK_METRICS_PATH, net_table, mode="overwrite")
    logger.info(f"Wrote {len(net_rows)} network metric samples to {NETWORK_METRICS_PATH}")

    # 5. Generate request routing
    logger.info("Generating request routing...")
    routing_rows = generate_request_routing(AGENT_STEPS_PATH, events, rng)
    if routing_rows:
        routing_table = pa.Table.from_pylist(routing_rows, schema=ROUTING_SCHEMA)
        write_deltalake(ROUTING_PATH, routing_table, mode="overwrite")
        logger.info(f"Wrote {len(routing_rows)} routing assignments to {ROUTING_PATH}")

    # Print summary
    print_summary(events, gpu_rows, net_rows, routing_rows)


def print_summary(events, gpu_rows, net_rows, routing_rows):
    """Print simulation summary."""
    print(f"\n{'='*70}")
    print(f" Infrastructure Simulation Complete")
    print(f"{'='*70}")
    print(f"\n  Topology: {len(TOPOLOGY['nodes'])} nodes, "
          f"{sum(len(n['gpus']) for n in TOPOLOGY['nodes'])} GPUs, "
          f"{len(TOPOLOGY['pods'])} pods")
    print(f"\n  Contention events: {len(events)}")
    for e in events:
        dur = (e.end_ms - e.start_ms) / 60000
        print(f"    [{e.scenario_name}] {dur:.1f}min | GPUs: {e.affected_gpus} | NICs: {e.affected_nics}")

    print(f"\n  GPU metrics:     {len(gpu_rows)} samples")
    print(f"  Network metrics: {len(net_rows)} samples")
    print(f"  Request routing: {len(routing_rows)} assignments")

    # Contention distribution
    contended = sum(1 for r in gpu_rows if r["contention_index"] > 0.7)
    print(f"\n  GPU samples with contention > 0.7: {contended}/{len(gpu_rows)} "
          f"({contended/max(len(gpu_rows),1)*100:.1f}%)")

    # Routing distribution
    pod_counts = defaultdict(int)
    for r in routing_rows:
        pod_counts[r["vllm_pod_id"]] += 1
    print(f"\n  Routing distribution:")
    for pod_id, count in sorted(pod_counts.items()):
        print(f"    {pod_id}: {count} requests")

    print(f"{'='*70}\n")


if __name__ == "__main__":
    run_simulation()
