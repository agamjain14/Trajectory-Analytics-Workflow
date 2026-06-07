"""
Live Metrics Service.
Runs as background tasks inside FastAPI. Provides:
1. /ingest/gpu_metrics + /ingest/network_metrics endpoints (Vast.ai pushes here)
2. Synthetic fallback: if no real data arrives in 30s, generates synthetic metrics
3. Lightweight correlation: periodically correlates new traces with latest metrics

Works identically on local (Ollama) and cloud (Azure OpenAI).
No Spark. No JVM. Uses deltalake (Rust-based) for Delta table writes.
"""

import asyncio
import json
import logging
import math
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

logger = logging.getLogger("live_metrics")

# --- Config ---
DATA_DIR = os.getenv("DATA_DIR", "./data")
GPU_METRICS_PATH = os.path.join(DATA_DIR, "gpu_metrics")
NET_METRICS_PATH = os.path.join(DATA_DIR, "network_metrics")
ROUTING_PATH = os.path.join(DATA_DIR, "request_routing")
CORRELATED_PATH = os.path.join(DATA_DIR, "trace_correlated")
AGENT_STEPS_PATH = os.path.join(DATA_DIR, "agent_steps")

SYNTHETIC_INTERVAL = int(os.getenv("SYNTHETIC_INTERVAL", "5"))  # seconds
CORRELATION_INTERVAL = int(os.getenv("CORRELATION_INTERVAL", "30"))  # seconds
REAL_DATA_TIMEOUT = int(os.getenv("REAL_DATA_TIMEOUT", "30"))  # seconds before fallback

# Track when real data last arrived
_last_real_gpu_ts = 0.0
_last_real_net_ts = 0.0

router = APIRouter()


# --- Schemas ---

class GPUMetricRow(BaseModel):
    timestamp_ms: int
    node_id: str
    gpu_id: str
    gpu_uuid: Optional[str] = ""
    gpu_utilization: float
    memory_controller_util: float
    memory_used_pct: float
    temperature_c: float
    power_draw_pct: float
    clock_sm_mhz: int
    clock_mem_mhz: int
    throttle_active: int
    pcie_tx_mbps: float
    pcie_rx_mbps: float
    ecc_errors_total: int
    contention_index: float


class NetworkMetricRow(BaseModel):
    timestamp_ms: int
    node_id: str
    peer_node_id: Optional[str] = ""
    inter_node_latency_us: float
    throughput_tx_mbps: float
    throughput_rx_mbps: float
    packet_drop_count: int
    tcp_retransmit_count: int


# --- PyArrow schemas ---

GPU_PA_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("node_id", pa.string()),
    ("gpu_id", pa.string()),
    ("gpu_uuid", pa.string()),
    ("gpu_utilization", pa.float64()),
    ("memory_controller_util", pa.float64()),
    ("memory_used_pct", pa.float64()),
    ("temperature_c", pa.float64()),
    ("power_draw_pct", pa.float64()),
    ("clock_sm_mhz", pa.int32()),
    ("clock_mem_mhz", pa.int32()),
    ("throttle_active", pa.int32()),
    ("pcie_tx_mbps", pa.float64()),
    ("pcie_rx_mbps", pa.float64()),
    ("ecc_errors_total", pa.int32()),
    ("contention_index", pa.float64()),
])

NET_PA_SCHEMA = pa.schema([
    ("timestamp_ms", pa.int64()),
    ("node_id", pa.string()),
    ("peer_node_id", pa.string()),
    ("inter_node_latency_us", pa.float64()),
    ("throughput_tx_mbps", pa.float64()),
    ("throughput_rx_mbps", pa.float64()),
    ("packet_drop_count", pa.int32()),
    ("tcp_retransmit_count", pa.int32()),
])


# --- Delta write helpers ---

def _ensure_delta(path: str, schema: pa.Schema):
    """Create empty Delta table if it doesn't exist."""
    if not DeltaTable.is_deltatable(path):
        Path(path).mkdir(parents=True, exist_ok=True)
        empty = pa.table({f.name: pa.array([], type=f.type) for f in schema}, schema=schema)
        write_deltalake(path, empty, mode="overwrite")
        logger.info(f"Created Delta table: {path}")


def _append_gpu(rows: list[dict]):
    """Append GPU metric rows to Delta table."""
    _ensure_delta(GPU_METRICS_PATH, GPU_PA_SCHEMA)
    arrays = {f.name: pa.array([r.get(f.name) for r in rows], type=f.type) for f in GPU_PA_SCHEMA}
    table = pa.table(arrays, schema=GPU_PA_SCHEMA)
    write_deltalake(GPU_METRICS_PATH, table, mode="append")


def _append_net(rows: list[dict]):
    """Append network metric rows to Delta table."""
    _ensure_delta(NET_METRICS_PATH, NET_PA_SCHEMA)
    arrays = {f.name: pa.array([r.get(f.name) for r in rows], type=f.type) for f in NET_PA_SCHEMA}
    table = pa.table(arrays, schema=NET_PA_SCHEMA)
    write_deltalake(NET_METRICS_PATH, table, mode="append")


# --- Ingest endpoints (Vast.ai pushes here) ---

@router.post("/ingest/gpu_metrics")
async def ingest_gpu(row: GPUMetricRow):
    """Receive GPU metrics from remote collector (Vast.ai node)."""
    global _last_real_gpu_ts
    _last_real_gpu_ts = time.time()
    _append_gpu([row.model_dump()])
    return {"status": "ok"}


@router.post("/ingest/network_metrics")
async def ingest_network(row: NetworkMetricRow):
    """Receive network metrics from remote collector (Vast.ai node)."""
    global _last_real_net_ts
    _last_real_net_ts = time.time()
    _append_net([row.model_dump()])
    return {"status": "ok"}


@router.post("/ingest/gpu_metrics/batch")
async def ingest_gpu_batch(rows: list[GPUMetricRow]):
    """Batch receive GPU metrics."""
    global _last_real_gpu_ts
    _last_real_gpu_ts = time.time()
    _append_gpu([r.model_dump() for r in rows])
    return {"status": "ok", "count": len(rows)}


@router.post("/ingest/network_metrics/batch")
async def ingest_network_batch(rows: list[NetworkMetricRow]):
    """Batch receive network metrics."""
    global _last_real_net_ts
    _last_real_net_ts = time.time()
    _append_net([r.model_dump() for r in rows])
    return {"status": "ok", "count": len(rows)}


@router.get("/ingest/status")
async def ingest_status():
    """Check if real data is flowing or synthetic fallback is active."""
    now = time.time()
    gpu_real = (now - _last_real_gpu_ts) < REAL_DATA_TIMEOUT if _last_real_gpu_ts else False
    net_real = (now - _last_real_net_ts) < REAL_DATA_TIMEOUT if _last_real_net_ts else False
    return {
        "gpu_source": "real" if gpu_real else "synthetic",
        "net_source": "real" if net_real else "synthetic",
        "last_real_gpu_ago_s": round(now - _last_real_gpu_ts, 1) if _last_real_gpu_ts else None,
        "last_real_net_ago_s": round(now - _last_real_net_ts, 1) if _last_real_net_ts else None,
    }


# --- Synthetic fallback generator ---

def _generate_synthetic_gpu(node_id: str, t: float) -> dict:
    """Generate one synthetic GPU metric row."""
    contention_event = random.random() < 0.15  # 15% chance of spike
    base_util = 45 + 20 * math.sin(t / 30)
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
    contention = (gpu_util * 0.3 + mem_ctrl * 0.25 + mem_used * 0.2 + (temp / 100) * 0.15 + power * 0.1) / 100.0

    return {
        "timestamp_ms": int(time.time() * 1000),
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


def _generate_synthetic_net(node_id: str, peer_id: str, t: float) -> dict:
    """Generate one synthetic network metric row."""
    contention_event = random.random() < 0.1
    base_latency = 150 + 50 * math.sin(t / 60)
    if contention_event:
        base_latency += random.uniform(200, 800)

    return {
        "timestamp_ms": int(time.time() * 1000),
        "node_id": node_id,
        "peer_node_id": peer_id,
        "inter_node_latency_us": round(max(50, base_latency + random.gauss(0, 20)), 1),
        "throughput_tx_mbps": round(max(10, 2000 + random.gauss(0, 500)), 1),
        "throughput_rx_mbps": round(max(10, 1800 + random.gauss(0, 500)), 1),
        "packet_drop_count": random.randint(0, 3) if contention_event else 0,
        "tcp_retransmit_count": random.randint(0, 5) if contention_event else 0,
    }


async def synthetic_metrics_loop():
    """Background loop: generates synthetic metrics when no real data is flowing."""
    t = 0.0
    logger.info("Synthetic metrics fallback started")
    while True:
        await asyncio.sleep(SYNTHETIC_INTERVAL)
        t += SYNTHETIC_INTERVAL

        now = time.time()
        gpu_real_active = (_last_real_gpu_ts and (now - _last_real_gpu_ts) < REAL_DATA_TIMEOUT)
        net_real_active = (_last_real_net_ts and (now - _last_real_net_ts) < REAL_DATA_TIMEOUT)

        try:
            if not gpu_real_active:
                rows = [
                    _generate_synthetic_gpu("node-1", t),
                    _generate_synthetic_gpu("node-2", t),
                ]
                _append_gpu(rows)

            if not net_real_active:
                rows = [
                    _generate_synthetic_net("node-1", "node-2", t),
                    _generate_synthetic_net("node-2", "node-1", t),
                ]
                _append_net(rows)
        except Exception as e:
            logger.error(f"Synthetic metrics write failed: {e}")


# --- Lightweight correlation (no Spark) ---

async def correlation_loop():
    """Background loop: correlates recent traces with latest metrics every 30s."""
    logger.info("Correlation loop started")
    while True:
        await asyncio.sleep(CORRELATION_INTERVAL)
        try:
            _run_correlation()
        except Exception as e:
            logger.error(f"Correlation failed: {e}")


def _run_correlation():
    """Simple correlation: join latest agent_steps with recent GPU metrics."""
    if not DeltaTable.is_deltatable(AGENT_STEPS_PATH):
        return
    if not DeltaTable.is_deltatable(GPU_METRICS_PATH):
        return

    # Read recent traces (last 5 minutes)
    cutoff_ms = int((time.time() - 300) * 1000)
    steps_dt = DeltaTable(AGENT_STEPS_PATH)
    steps_table = steps_dt.to_pyarrow_table()

    if steps_table.num_rows == 0:
        return

    # Get unique trace_ids from recent steps
    all_trace_ids = steps_table.column("trace_id").to_pylist()
    recent_trace_ids = set(all_trace_ids[-100:])  # last 100 traces

    # Already correlated?
    existing_ids = set()
    if DeltaTable.is_deltatable(CORRELATED_PATH):
        corr_dt = DeltaTable(CORRELATED_PATH)
        existing_ids = set(corr_dt.to_pyarrow_table().column("trace_id").to_pylist())

    new_ids = recent_trace_ids - existing_ids
    if not new_ids:
        return

    # Get GPU metrics summary
    gpu_dt = DeltaTable(GPU_METRICS_PATH)
    gpu_table = gpu_dt.to_pyarrow_table()
    recent_gpu = [
        {col: gpu_table.column(col)[i].as_py() for col in gpu_table.column_names}
        for i in range(max(0, gpu_table.num_rows - 100), gpu_table.num_rows)
    ]

    avg_contention = sum(r["contention_index"] for r in recent_gpu) / max(len(recent_gpu), 1) if recent_gpu else 0
    avg_temp = sum(r["temperature_c"] for r in recent_gpu) / max(len(recent_gpu), 1) if recent_gpu else 0
    max_temp = max((r["temperature_c"] for r in recent_gpu), default=0)

    # Build correlated rows for new traces
    correlated_rows = []
    for trace_id in list(new_ids)[:20]:  # batch limit
        # Get trace steps
        trace_steps = [
            {col: steps_table.column(col)[i].as_py() for col in steps_table.column_names}
            for i in range(steps_table.num_rows)
            if steps_table.column("trace_id")[i].as_py() == trace_id
        ]
        if not trace_steps:
            continue

        step_count = len(trace_steps)
        start_ts = min(s.get("start_ts_ms", 0) or 0 for s in trace_steps)
        end_ts = max(s.get("end_ts_ms", 0) or 0 for s in trace_steps)

        # Count span types
        llm_calls = sum(1 for s in trace_steps if s.get("span_kind") == "REASON")
        tool_calls = sum(1 for s in trace_steps if s.get("span_kind") == "TOOL")
        retrieve_calls = sum(1 for s in trace_steps if s.get("span_kind") == "RETRIEVE")

        ingestion_time = datetime.now(timezone.utc)

        correlated_rows.append({
            "trace_id": trace_id,
            "session_id": trace_steps[0].get("session_id", ""),
            "start_ts_ms": start_ts,
            "end_ts_ms": end_ts,
            "total_duration_ms": float(end_ts - start_ts) if end_ts and start_ts else 0.0,
            "trajectory_signature": "",
            "step_sequence": "",
            "step_count": step_count,
            "llm_call_count": llm_calls,
            "tool_call_count": tool_calls,
            "retrieve_count": retrieve_calls,
            "retry_count": 0,
            "quality_overall": 0.0,
            "quality_completeness": 0.0,
            "quality_coherence": 0.0,
            "quality_hallucination": 0.0,
            "quality_groundedness": 0.0,
            "quality_relevance": 0.0,
            "quality_explanation": "",
            "gpu_contention_avg": round(avg_contention, 4),
            "gpu_contention_max": round(avg_contention, 4),
            "gpu_temperature_avg": round(avg_temp, 1),
            "gpu_temperature_max": round(max_temp, 1),
            "gpu_memory_pressure_avg": 0.0,
            "gpu_throttle_count": sum(1 for r in recent_gpu if r.get("throttle_active", 0)),
            "gpu_power_avg": sum(r.get("power_draw_pct", 0) for r in recent_gpu) / max(len(recent_gpu), 1),
            "inter_node_latency_avg": 0.0,
            "inter_node_latency_max": 0.0,
            "packet_drop_total": 0,
            "tcp_retransmit_total": 0,
            "queue_wait_avg": 0.0,
            "queue_wait_max": 0.0,
            "primary_pod_id": "",
            "primary_node_id": trace_steps[0].get("node_id", "local"),
            "primary_gpu_id": "",
            "ingestion_date": ingestion_time.strftime("%Y-%m-%d"),
            "ingestion_hour": ingestion_time.hour,
        })

    if not correlated_rows:
        return

    # Write to Delta
    schema = pa.schema([
        ("trace_id", pa.string()),
        ("session_id", pa.string()),
        ("start_ts_ms", pa.int64()),
        ("end_ts_ms", pa.int64()),
        ("total_duration_ms", pa.float64()),
        ("trajectory_signature", pa.string()),
        ("step_sequence", pa.string()),
        ("step_count", pa.int32()),
        ("llm_call_count", pa.int32()),
        ("tool_call_count", pa.int32()),
        ("retrieve_count", pa.int32()),
        ("retry_count", pa.int32()),
        ("quality_overall", pa.float64()),
        ("quality_completeness", pa.float64()),
        ("quality_coherence", pa.float64()),
        ("quality_hallucination", pa.float64()),
        ("quality_groundedness", pa.float64()),
        ("quality_relevance", pa.float64()),
        ("quality_explanation", pa.string()),
        ("gpu_contention_avg", pa.float64()),
        ("gpu_contention_max", pa.float64()),
        ("gpu_temperature_avg", pa.float64()),
        ("gpu_temperature_max", pa.float64()),
        ("gpu_memory_pressure_avg", pa.float64()),
        ("gpu_throttle_count", pa.int32()),
        ("gpu_power_avg", pa.float64()),
        ("inter_node_latency_avg", pa.float64()),
        ("inter_node_latency_max", pa.float64()),
        ("packet_drop_total", pa.int32()),
        ("tcp_retransmit_total", pa.int32()),
        ("queue_wait_avg", pa.float64()),
        ("queue_wait_max", pa.float64()),
        ("primary_pod_id", pa.string()),
        ("primary_node_id", pa.string()),
        ("primary_gpu_id", pa.string()),
        ("ingestion_date", pa.string()),
        ("ingestion_hour", pa.int32()),
    ])
    _ensure_delta(CORRELATED_PATH, schema)
    arrays = {f.name: pa.array([r.get(f.name) for r in correlated_rows], type=f.type) for f in schema}
    table = pa.table(arrays, schema=schema)
    write_deltalake(CORRELATED_PATH, table, mode="append")
    logger.info(f"Correlated {len(correlated_rows)} new traces")
