"""
Phase 2: Quality Evaluation Job (LLM-as-Judge).
Reads agent_steps Delta Table, reconstructs per-trace context
(user request, reasoning chain, tool results, final response),
then calls an LLM to score quality on multiple dimensions.
"""

import json
import logging
import os
from collections import defaultdict

import ollama
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("quality_eval_job")

# --- Configuration ---
INPUT_DELTA_PATH = os.getenv("AGENT_STEPS_PATH", "./data/agent_steps")
OUTPUT_DELTA_PATH = os.getenv("QUALITY_SCORES_PATH", "./data/quality_scores")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
EVAL_MODEL = os.getenv("EVAL_MODEL", "llama3.2")

# --- Output Schema ---
QUALITY_SCHEMA = pa.schema([
    ("trace_id", pa.string()),
    ("session_id", pa.string()),
    ("completeness", pa.float64()),
    ("coherence", pa.float64()),
    ("hallucination", pa.float64()),
    ("groundedness", pa.float64()),
    ("relevance", pa.float64()),
    ("overall", pa.float64()),
    ("explanation", pa.string()),
    ("eval_model", pa.string()),
    ("eval_timestamp_ms", pa.int64()),
    ("start_ts_ms", pa.int64()),
    ("end_ts_ms", pa.int64()),
])

# --- Evaluation Prompt ---
EVAL_PROMPT_TEMPLATE = """You are an expert evaluator for AI agent responses. Score the agent's response on each dimension using a 1-5 scale.

SCORING GUIDE:
- 5 = Excellent, no issues
- 4 = Good, minor issues
- 3 = Acceptable, some notable issues
- 2 = Poor, significant issues
- 1 = Very poor, critical issues

DIMENSIONS:
- COMPLETENESS: Does the response address ALL parts of the user request?
- COHERENCE: Is the response logically structured, consistent, and well-organized?
- HALLUCINATION: Does the response contain fabricated/unsupported claims NOT backed by tool or retrieval results? (5 = no hallucination, 1 = severe hallucination)
- GROUNDEDNESS: Is the response grounded in the actual tool results and retrieved information?
- RELEVANCE: Does the response stay on-topic and directly answer what was asked?

---

USER REQUEST:
{user_request}

---

AGENT REASONING CHAIN (steps the agent took):
{reasoning_chain}

---

TOOL/RETRIEVAL RESULTS (evidence available to the agent):
{evidence}

---

FINAL AGENT RESPONSE:
{final_response}

---

Score each dimension (1-5) and provide a brief explanation.
You MUST respond with valid JSON only, no other text:
{{"completeness": <int>, "coherence": <int>, "hallucination": <int>, "groundedness": <int>, "relevance": <int>, "overall": <float>, "explanation": "<brief explanation>"}}
"""


def _extract_trace_context(spans: list[dict]) -> dict:
    """
    Extract evaluation context from a trace's spans.
    Returns user_request, reasoning_chain, evidence, final_response.
    """
    user_request = ""
    reasoning_steps = []
    evidence_parts = []
    final_response = ""

    # Sort by time
    spans.sort(key=lambda s: s["start_ts_ms"] or 0)

    for span in spans:
        kind = span["span_kind"]

        if kind == "ENTRY":
            # User request is in the prompt field or context
            user_request = span.get("prompt") or span.get("context") or ""

        elif kind == "REASON":
            model = span.get("model") or "unknown"
            prompt = span.get("prompt") or ""
            response = span.get("response") or ""
            reasoning_steps.append(f"[LLM call to {model}]: {prompt[:200]}")
            # Last REASON response is likely the final response
            if response:
                final_response = response

        elif kind == "RETRIEVE":
            collection = span.get("collection") or ""
            query = span.get("query_text") or ""
            rows = span.get("returned_rows") or "?"
            evidence_parts.append(f"[RAG: {collection}] query='{query[:100]}' returned {rows} results")

        elif kind == "TOOL":
            method = span.get("rpc_method") or span.get("url") or "unknown"
            response = span.get("response") or ""
            evidence_parts.append(f"[Tool: {method}] → {response[:300]}")

        elif kind == "AGENT":
            agent_name = span.get("agent_name") or "unknown"
            reasoning_steps.append(f"[Agent: {agent_name} activated]")

    return {
        "user_request": user_request or "(not captured)",
        "reasoning_chain": "\n".join(reasoning_steps) or "(no reasoning captured)",
        "evidence": "\n".join(evidence_parts) or "(no tools/retrieval used)",
        "final_response": final_response or "(no response captured)",
    }


def _call_eval_llm(prompt: str) -> dict:
    """Call LLM for evaluation. Returns parsed scores dict."""
    client = ollama.Client(host=OLLAMA_BASE_URL)
    try:
        response = client.chat(
            model=EVAL_MODEL,
            messages=[
                {"role": "system", "content": "You are a strict JSON-only evaluator. Respond with valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            options={"temperature": 0.1},  # Low temp for consistent scoring
        )
        content = response["message"]["content"]

        # Try to parse JSON from the response
        # Handle cases where LLM wraps JSON in markdown code blocks
        if "```json" in content:
            content = content.split("```json")[1].split("```")[0]
        elif "```" in content:
            content = content.split("```")[1].split("```")[0]

        scores = json.loads(content.strip())

        # Validate and clamp scores
        for key in ("completeness", "coherence", "hallucination", "groundedness", "relevance"):
            if key not in scores:
                scores[key] = 3  # default mid-score
            scores[key] = max(1, min(5, int(scores[key])))

        # Compute overall if not provided
        if "overall" not in scores:
            dims = [scores["completeness"], scores["coherence"], scores["hallucination"],
                    scores["groundedness"], scores["relevance"]]
            scores["overall"] = round(sum(dims) / len(dims), 2)
        else:
            scores["overall"] = round(float(scores["overall"]), 2)

        if "explanation" not in scores:
            scores["explanation"] = ""

        return scores

    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse LLM eval response as JSON: {e}")
        return {
            "completeness": 3, "coherence": 3, "hallucination": 3,
            "groundedness": 3, "relevance": 3, "overall": 3.0,
            "explanation": f"eval_parse_error: {str(e)[:100]}",
        }
    except Exception as e:
        logger.error(f"LLM eval call failed: {e}")
        return {
            "completeness": 0, "coherence": 0, "hallucination": 0,
            "groundedness": 0, "relevance": 0, "overall": 0.0,
            "explanation": f"eval_error: {str(e)[:100]}",
        }


def evaluate_traces(input_path: str, output_path: str) -> int:
    """
    Read agent_steps, group by trace, build eval context,
    call LLM-as-judge, store quality scores.
    """
    if not DeltaTable.is_deltatable(input_path):
        logger.error(f"Input delta table not found at {input_path}")
        return 0

    dt = DeltaTable(input_path)
    table = dt.to_pyarrow_table()
    logger.info(f"Read {table.num_rows} agent_steps rows")

    # Group spans by trace_id
    traces = defaultdict(list)
    for i in range(table.num_rows):
        trace_id = table.column("trace_id")[i].as_py()
        traces[trace_id].append({
            "span_kind": table.column("span_kind")[i].as_py(),
            "sub_span_kind": table.column("sub_span_kind")[i].as_py(),
            "start_ts_ms": table.column("start_ts_ms")[i].as_py(),
            "end_ts_ms": table.column("end_ts_ms")[i].as_py(),
            "session_id": table.column("session_id")[i].as_py(),
            "model": table.column("model")[i].as_py(),
            "prompt": table.column("prompt")[i].as_py(),
            "response": table.column("response")[i].as_py(),
            "context": table.column("context")[i].as_py(),
            "agent_name": table.column("agent_name")[i].as_py(),
            "collection": table.column("collection")[i].as_py(),
            "query_text": table.column("query_text")[i].as_py(),
            "returned_rows": table.column("returned_rows")[i].as_py(),
            "rpc_method": table.column("rpc_method")[i].as_py(),
            "url": table.column("url")[i].as_py(),
        })

    logger.info(f"Evaluating quality for {len(traces)} traces...")
    import time

    rows = []
    for idx, (trace_id, spans) in enumerate(traces.items()):
        logger.info(f"  [{idx+1}/{len(traces)}] Evaluating trace {trace_id[:16]}...")

        # Extract context
        ctx = _extract_trace_context(spans)

        # Build eval prompt
        prompt = EVAL_PROMPT_TEMPLATE.format(**ctx)

        # Call LLM judge
        scores = _call_eval_llm(prompt)

        # Timing from spans
        start_ts = min(s["start_ts_ms"] for s in spans if s["start_ts_ms"])
        end_ts = max(s["end_ts_ms"] for s in spans if s["end_ts_ms"])
        session_id = spans[0]["session_id"] or ""

        rows.append({
            "trace_id": trace_id,
            "session_id": session_id,
            "completeness": float(scores["completeness"]),
            "coherence": float(scores["coherence"]),
            "hallucination": float(scores["hallucination"]),
            "groundedness": float(scores["groundedness"]),
            "relevance": float(scores["relevance"]),
            "overall": float(scores["overall"]),
            "explanation": scores.get("explanation", ""),
            "eval_model": EVAL_MODEL,
            "eval_timestamp_ms": int(time.time() * 1000),
            "start_ts_ms": start_ts,
            "end_ts_ms": end_ts,
        })

    if not rows:
        logger.warning("No traces to evaluate.")
        return 0

    out_table = pa.Table.from_pylist(rows, schema=QUALITY_SCHEMA)
    write_deltalake(output_path, out_table, mode="overwrite")
    logger.info(f"Wrote {len(rows)} quality scores to {output_path}")
    return len(rows)


def print_summary(output_path: str) -> None:
    """Print quality scores summary."""
    if not DeltaTable.is_deltatable(output_path):
        return

    dt = DeltaTable(output_path)
    table = dt.to_pyarrow_table()
    n = table.num_rows

    print(f"\n{'='*70}")
    print(f" quality_scores: {n} traces evaluated")
    print(f"{'='*70}")

    if n == 0:
        return

    dims = ["completeness", "coherence", "hallucination", "groundedness", "relevance", "overall"]
    print(f"\n  {'dimension':<16} {'avg':>6} {'min':>6} {'max':>6}")
    print(f"  {'-'*16} {'-'*6} {'-'*6} {'-'*6}")
    for dim in dims:
        vals = [table.column(dim)[i].as_py() for i in range(n)]
        avg_val = sum(vals) / len(vals)
        print(f"  {dim:<16} {avg_val:>6.2f} {min(vals):>6.1f} {max(vals):>6.1f}")

    print(f"\n  Eval model: {table.column('eval_model')[0].as_py()}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    written = evaluate_traces(INPUT_DELTA_PATH, OUTPUT_DELTA_PATH)
    if written > 0:
        print_summary(OUTPUT_DELTA_PATH)
