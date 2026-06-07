"""
Phase 5: Analytics API.
FastAPI server that reads Delta tables directly and serves precomputed
analytics results for the dashboard and LLM summarization.
"""

import json
import logging
import os
import time
from collections import defaultdict
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from deltalake import DeltaTable
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("analytics_api")

# --- Configuration ---
DATA_DIR = os.getenv("DATA_DIR", "./data")
LLM_BACKEND = os.getenv("LLM_BACKEND", "ollama")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
SUMMARY_MODEL = os.getenv("SUMMARY_MODEL", "llama3.2")

app = FastAPI(title="Trajectory Analytics API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Helpers ---

def _load_table(name: str) -> list[dict]:
    """Load a Delta table as list of dicts."""
    path = os.path.join(DATA_DIR, name)
    if not DeltaTable.is_deltatable(path):
        return []
    dt = DeltaTable(path)
    table = dt.to_pyarrow_table()
    return [
        {col: table.column(col)[i].as_py() for col in table.column_names}
        for i in range(table.num_rows)
    ]


# --- Endpoints ---

@app.get("/analytics/trajectories")
def get_trajectories():
    """Get all trajectory templates with distribution stats."""
    rows = _load_table("trajectory_templates")
    if not rows:
        raise HTTPException(404, "trajectory_templates table not found")

    # Compute distribution
    sig_counts = defaultdict(int)
    sig_examples = {}
    for r in rows:
        sig = r["trajectory_signature"]
        sig_counts[sig] += 1
        if sig not in sig_examples:
            sig_examples[sig] = r

    distribution = []
    for sig, count in sorted(sig_counts.items(), key=lambda x: -x[1]):
        ex = sig_examples[sig]
        distribution.append({
            "signature": sig,
            "count": count,
            "share": round(count / len(rows), 4),
            "step_count": ex["step_count"],
            "llm_call_count": ex["llm_call_count"],
            "tool_call_count": ex["tool_call_count"],
            "retrieve_count": ex["retrieve_count"],
            "step_sequence": ex["step_sequence"],
            "step_sequence_detailed": ex["step_sequence_detailed"],
        })

    return {
        "total_traces": len(rows),
        "unique_templates": len(sig_counts),
        "distribution": distribution,
        "traces": rows,
    }


@app.get("/analytics/trajectories/{trace_id}")
def get_trajectory_detail(trace_id: str):
    """Get step-by-step trajectory for a specific trace."""
    # Get from agent_steps for full detail
    steps = _load_table("agent_steps")
    trace_steps = [s for s in steps if s.get("trace_id") == trace_id]
    if not trace_steps:
        raise HTTPException(404, f"Trace {trace_id} not found")

    trace_steps.sort(key=lambda s: s.get("start_ts_ms") or 0)

    # Also get template info
    templates = _load_table("trajectory_templates")
    template = next((t for t in templates if t["trace_id"] == trace_id), None)

    return {
        "trace_id": trace_id,
        "template": template,
        "steps": trace_steps,
        "step_count": len(trace_steps),
    }


@app.get("/analytics/quality")
def get_quality_scores():
    """Get quality score time-series and summary."""
    rows = _load_table("quality_scores")
    if not rows:
        raise HTTPException(404, "quality_scores table not found")

    # Sort by time
    rows.sort(key=lambda r: r.get("start_ts_ms") or 0)

    dims = ["completeness", "coherence", "hallucination", "groundedness", "relevance", "overall"]
    summary = {}
    for dim in dims:
        vals = [r[dim] for r in rows if r.get(dim) is not None]
        summary[dim] = {
            "avg": round(sum(vals) / max(len(vals), 1), 3),
            "min": min(vals) if vals else 0,
            "max": max(vals) if vals else 0,
        }

    return {
        "total_evaluated": len(rows),
        "eval_model": rows[0].get("eval_model", "") if rows else "",
        "summary": summary,
        "scores": rows,
    }


@app.get("/analytics/quality/{trace_id}")
def get_quality_detail(trace_id: str):
    """Get quality breakdown for one trace."""
    rows = _load_table("quality_scores")
    score = next((r for r in rows if r["trace_id"] == trace_id), None)
    if not score:
        raise HTTPException(404, f"Quality score for trace {trace_id} not found")
    return score


@app.get("/analytics/gpu")
def get_gpu_metrics(
    start_ms: Optional[int] = Query(None),
    end_ms: Optional[int] = Query(None),
    node_id: Optional[str] = Query(None),
    gpu_id: Optional[str] = Query(None),
    downsample: int = Query(1, description="Take every Nth sample"),
):
    """Get GPU metrics time-series."""
    rows = _load_table("gpu_metrics")
    if not rows:
        raise HTTPException(404, "gpu_metrics table not found")

    # Filter
    if start_ms:
        rows = [r for r in rows if r["timestamp_ms"] >= start_ms]
    if end_ms:
        rows = [r for r in rows if r["timestamp_ms"] <= end_ms]
    if node_id:
        rows = [r for r in rows if r["node_id"] == node_id]
    if gpu_id:
        rows = [r for r in rows if r["gpu_id"] == gpu_id]

    # Sort by time
    rows.sort(key=lambda r: r["timestamp_ms"])

    # Downsample
    if downsample > 1:
        rows = rows[::downsample]

    # Summary
    contention_vals = [r["contention_index"] for r in rows]
    summary = {
        "total_samples": len(rows),
        "avg_contention": round(sum(contention_vals) / max(len(contention_vals), 1), 4),
        "max_contention": max(contention_vals) if contention_vals else 0,
        "high_contention_pct": round(
            sum(1 for v in contention_vals if v > 0.7) / max(len(contention_vals), 1) * 100, 1
        ),
    }

    return {"summary": summary, "metrics": rows}


@app.get("/analytics/network")
def get_network_metrics(
    start_ms: Optional[int] = Query(None),
    end_ms: Optional[int] = Query(None),
    node_id: Optional[str] = Query(None),
    downsample: int = Query(1),
):
    """Get network metrics time-series."""
    rows = _load_table("network_metrics")
    if not rows:
        raise HTTPException(404, "network_metrics table not found")

    if start_ms:
        rows = [r for r in rows if r["timestamp_ms"] >= start_ms]
    if end_ms:
        rows = [r for r in rows if r["timestamp_ms"] <= end_ms]
    if node_id:
        rows = [r for r in rows if r["node_id"] == node_id]

    rows.sort(key=lambda r: r["timestamp_ms"])
    if downsample > 1:
        rows = rows[::downsample]

    return {"total_samples": len(rows), "metrics": rows}


@app.get("/analytics/topology")
def get_topology():
    """Get infrastructure topology."""
    rows = _load_table("topology")
    if not rows:
        raise HTTPException(404, "topology table not found")

    # Group by entity type
    by_type = defaultdict(list)
    for r in rows:
        r["properties"] = json.loads(r["properties"]) if r.get("properties") else {}
        by_type[r["entity_type"]].append(r)

    return {
        "entities": rows,
        "by_type": dict(by_type),
        "summary": {t: len(items) for t, items in by_type.items()},
    }


@app.get("/analytics/correlation/traces")
def get_correlated_traces():
    """Get per-trace correlated view with all signals."""
    rows = _load_table("trace_correlated")
    if not rows:
        raise HTTPException(404, "trace_correlated table not found")

    rows.sort(key=lambda r: r.get("start_ts_ms") or 0)

    return {
        "total_traces": len(rows),
        "traces": rows,
    }


@app.get("/analytics/correlation/windows")
def get_analytics_windows(
    window_size: Optional[str] = Query(None, description="5min, 30min, or 1h"),
):
    """Get windowed analytics with correlation verdicts."""
    rows = _load_table("analytics_windows")
    if not rows:
        raise HTTPException(404, "analytics_windows table not found")

    if window_size:
        rows = [r for r in rows if r["window_size"] == window_size]

    rows.sort(key=lambda r: r["window_start_ms"])

    return {"total_windows": len(rows), "windows": rows}


@app.get("/analytics/correlation/alerts")
def get_correlation_alerts():
    """Get windows with non-normal correlation flags."""
    rows = _load_table("analytics_windows")
    if not rows:
        raise HTTPException(404, "analytics_windows table not found")

    alerts = [r for r in rows if r.get("correlation_flag") != "normal"]
    alerts.sort(key=lambda r: r["window_start_ms"])

    # Group by flag type
    by_flag = defaultdict(int)
    for a in alerts:
        by_flag[a["correlation_flag"]] += 1

    return {
        "total_alerts": len(alerts),
        "by_flag": dict(by_flag),
        "alerts": alerts,
    }


@app.get("/analytics/microstructure")
def get_microstructure():
    """Get per-trace step microstructure (counts of each span type)."""
    rows = _load_table("trajectory_templates")
    if not rows:
        raise HTTPException(404, "trajectory_templates table not found")

    result = []
    for r in rows:
        result.append({
            "trace_id": r["trace_id"],
            "session_id": r["session_id"],
            "step_count": r["step_count"],
            "llm_call_count": r["llm_call_count"],
            "tool_call_count": r["tool_call_count"],
            "retrieve_count": r["retrieve_count"],
            "agent_count": r["agent_count"],
            "retry_count": r["retry_count"],
            "error_count": r["error_count"],
            "total_duration_ms": r["total_duration_ms"],
            "trajectory_signature": r["trajectory_signature"],
        })

    return {"total_traces": len(result), "microstructure": result}


@app.get("/analytics/routing")
def get_routing():
    """Get request routing assignments."""
    rows = _load_table("request_routing")
    if not rows:
        raise HTTPException(404, "request_routing table not found")

    # Distribution by pod
    pod_counts = defaultdict(int)
    node_counts = defaultdict(int)
    for r in rows:
        pod_counts[r["vllm_pod_id"]] += 1
        node_counts[r["node_id"]] += 1

    return {
        "total_requests": len(rows),
        "by_pod": dict(pod_counts),
        "by_node": dict(node_counts),
        "assignments": rows,
    }


@app.get("/analytics/summary")
def get_llm_summary():
    """Generate LLM-powered natural language summary of current analytics state."""
    # Gather data for summary
    correlated = _load_table("trace_correlated")
    windows = _load_table("analytics_windows")
    trajectories = _load_table("trajectory_templates")

    if not correlated:
        raise HTTPException(404, "No analytics data available")

    # Build context for LLM
    avg_quality = sum(r["quality_overall"] for r in correlated) / max(len(correlated), 1)
    avg_contention = sum(r["gpu_contention_avg"] for r in correlated) / max(len(correlated), 1)
    avg_hallucination = sum(r["quality_hallucination"] for r in correlated) / max(len(correlated), 1)

    # Trajectory distribution
    sig_counts = defaultdict(int)
    for t in trajectories:
        sig_counts[t["trajectory_signature"]] += 1

    # Alerts
    alerts = [w for w in windows if w.get("correlation_flag") != "normal"]

    context = f"""Analytics State:
- Total traces analyzed: {len(correlated)}
- Average quality score: {avg_quality:.2f}/5
- Average hallucination score: {avg_hallucination:.2f}/5 (5=no hallucination, 1=severe)
- Average GPU contention index: {avg_contention:.3f} (0=idle, 1=saturated)
- Unique trajectory templates: {len(sig_counts)}
- Trajectory distribution: {json.dumps({k: v for k, v in sorted(sig_counts.items(), key=lambda x: -x[1])})}
- Correlation alerts: {len(alerts)}
- Alert types: {json.dumps({a['correlation_flag']: a.get('correlation_details','') for a in alerts[:5]})}

Traces with highest GPU contention:
"""
    high_cont = sorted(correlated, key=lambda r: -r["gpu_contention_avg"])[:3]
    for r in high_cont:
        context += f"  - trace {r['trace_id'][:12]}: quality={r['quality_overall']:.1f}, gpu_contention={r['gpu_contention_avg']:.3f}, steps={r['step_count']}\n"

    prompt = f"""You are an observability analyst. Summarize the following agent trajectory analytics data in 3-5 concise sentences. Focus on:
1. Overall agent quality and any concerns
2. Trajectory patterns (are agents following consistent paths?)
3. Infrastructure impact (is GPU contention affecting quality?)
4. Actionable recommendations if any

{context}

Provide a concise summary:"""

    try:
        if LLM_BACKEND == "azure":
            from openai import AzureOpenAI
            client = AzureOpenAI(
                azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT", ""),
                api_key=os.getenv("AZURE_OPENAI_API_KEY", ""),
                api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-08-01-preview"),
            )
            response = client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini"),
                messages=[
                    {"role": "system", "content": "You are a concise observability analyst. Provide brief, actionable summaries."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,
            )
            summary_text = response.choices[0].message.content
        else:
            import ollama
            client = ollama.Client(host=OLLAMA_BASE_URL)
            response = client.chat(
                model=SUMMARY_MODEL,
                messages=[
                    {"role": "system", "content": "You are a concise observability analyst. Provide brief, actionable summaries."},
                    {"role": "user", "content": prompt},
                ],
                options={"temperature": 0.3},
            )
            summary_text = response["message"]["content"]
    except Exception as e:
        logger.error(f"LLM summary failed: {e}")
        summary_text = (
            f"Analytics summary (auto-generated): {len(correlated)} traces analyzed. "
            f"Avg quality: {avg_quality:.2f}/5. Avg GPU contention: {avg_contention:.3f}. "
            f"{len(alerts)} correlation alerts detected."
        )

    return {
        "summary": summary_text,
        "stats": {
            "total_traces": len(correlated),
            "avg_quality": round(avg_quality, 3),
            "avg_contention": round(avg_contention, 4),
            "avg_hallucination": round(avg_hallucination, 3),
            "unique_templates": len(sig_counts),
            "alert_count": len(alerts),
        },
        "generated_at_ms": int(time.time() * 1000),
        "model": SUMMARY_MODEL,
    }


# --- Static files for dashboard ---
static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/analytics")
def analytics_dashboard():
    """Serve the analytics dashboard."""
    dashboard_path = os.path.join(static_dir, "analytics.html")
    if os.path.exists(dashboard_path):
        return FileResponse(dashboard_path)
    raise HTTPException(404, "Dashboard not found")
