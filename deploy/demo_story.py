#!/usr/bin/env python3
"""
Demo story driver for the Trajectory Analytics pipeline.

Drives a deliberate calm -> stress A/B against the running chat server so the
three thesis questions land with real spread, then prints a narrated verdict
pulled straight from the analytics endpoints:

  Q1  How do agent execution paths change over time?
  Q2  Does a quality drop co-occur with trajectory change AND GPU contention?
  Q3  Which trajectory mutations lead to incorrect outcomes?

The story it manufactures:
  * BASELINE phase — light single-user traffic. One dominant path, high quality,
    idle GPU. This becomes the earliest window = the calm reference point.
  * STRESS phase — many concurrent users saturate the single GPU. Inference
    slows, retries/timeouts fork new paths (trajectory drift), responses degrade
    (quality drop), GPU contention climbs. Later windows flip to
    gpu_induced_degradation and the harmful mutations surface in Q3.

Usage:
  # Full A/B run (drives traffic, then narrates):
  python3 deploy/demo_story.py --url http://localhost:8000 \
      --baseline-convos 6 --stress-users 8 --stress-duration 180

  # Just re-narrate the current analytics state (no traffic) — use live on stage:
  python3 deploy/demo_story.py --check

  # Calibrate the GPU pressure threshold from observed contention:
  python3 deploy/demo_story.py --calibrate
"""
import argparse
import asyncio
import os
import random
import sys
import time

import httpx

try:
    # Reuse the realistic multi-turn conversations from the load test.
    from load_test import CONVERSATIONS
except ImportError:  # pragma: no cover - allow running from repo root
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from load_test import CONVERSATIONS


# ── ANSI helpers ──────────────────────────────────────────────────────────────
def _c(code: str, s: str) -> str:
    return f"\033[{code}m{s}\033[0m"


def bold(s: str) -> str:
    return _c("1", s)


def dim(s: str) -> str:
    return _c("2", s)


def green(s: str) -> str:
    return _c("32", s)


def yellow(s: str) -> str:
    return _c("33", s)


def red(s: str) -> str:
    return _c("31", s)


def cyan(s: str) -> str:
    return _c("36", s)


def hr(title: str = "") -> None:
    line = "=" * 78
    if title:
        print(f"\n{cyan(line)}\n{bold(title)}\n{cyan(line)}")
    else:
        print(cyan(line))


# ── Traffic generators ────────────────────────────────────────────────────────
async def _run_session(client: httpx.AsyncClient, url: str, convo: list[str], tag: str) -> None:
    session_id = None
    for i, message in enumerate(convo):
        payload = {"message": message}
        if session_id:
            payload["session_id"] = session_id
        start = time.time()
        try:
            resp = await client.post(f"{url}/api/chat", json=payload, timeout=240.0)
            dur = time.time() - start
            if resp.status_code == 200:
                data = resp.json()
                session_id = data.get("session_id", session_id)
                print(dim(f"  [{tag} t{i+1}] {resp.status_code} {data.get('status','?')} {dur:.1f}s"))
            else:
                print(yellow(f"  [{tag} t{i+1}] HTTP {resp.status_code} {dur:.1f}s"))
        except Exception as e:  # noqa: BLE001
            print(red(f"  [{tag} t{i+1}] ERROR {e}"))
        await asyncio.sleep(random.uniform(0.8, 2.5))


async def run_baseline(url: str, n_convos: int) -> None:
    """Light, fully sequential traffic = the calm reference window."""
    hr("PHASE A — BASELINE (calm, single user)")
    print(dim(f"  {n_convos} conversations, sequential, idle GPU\n"))
    async with httpx.AsyncClient() as client:
        for k in range(n_convos):
            convo = CONVERSATIONS[k % len(CONVERSATIONS)]
            await _run_session(client, url, convo, tag=f"base-{k+1:02d}")


async def run_stress(url: str, users: int, duration: int) -> None:
    """Heavy concurrency = saturate the single GPU, force drift + degradation."""
    hr("PHASE B — STRESS (concurrent load, GPU saturation)")
    print(dim(f"  {users} concurrent users, {duration}s, hammering one GPU\n"))
    end = time.time() + duration
    wave = 0
    async with httpx.AsyncClient() as client:
        while time.time() < end:
            wave += 1
            tasks = []
            for u in range(users):
                convo = random.choice(CONVERSATIONS)
                tasks.append(_run_session(client, url, convo, tag=f"w{wave}-u{u+1}"))
            await asyncio.gather(*tasks, return_exceptions=True)
            if time.time() < end:
                await asyncio.sleep(random.uniform(1.0, 3.0))


# ── Analytics readback / narration ────────────────────────────────────────────
def _get(url: str, path: str) -> dict | None:
    try:
        r = httpx.get(f"{url}{path}", timeout=30.0)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:  # noqa: BLE001
        return None


def narrate_q1(url: str, window_size: str) -> None:
    hr("Q1 — HOW DO PATHS CHANGE OVER TIME?")
    data = _get(url, f"/analytics/correlation/windows?window_size={window_size}")
    if not data or not data.get("windows"):
        print(yellow("  No windows yet — let the stream trigger (~30-60s) and retry --check."))
        return
    wins = data["windows"]
    print(dim(f"  window_size={window_size}, {len(wins)} windows (chronological)\n"))
    print(bold(f"  {'window':>8} {'traces':>6} {'uniq':>4} {'dom_share':>9} {'entropy':>7} {'drift_JSD':>9}"))
    t0 = wins[0]["window_start_ms"]
    for w in wins:
        rel = int((w["window_start_ms"] - t0) / 1000)
        drift = w["trajectory_drift_jsd"]
        mark = "  <- baseline" if w.get("is_baseline") else (red("  <- DRIFT") if drift >= 0.15 else "")
        print(
            f"  {rel:>6}s {w['trace_count']:>6} {w['unique_signatures']:>4} "
            f"{w['dominant_share']:>9.2f} {w['trajectory_entropy']:>7.2f} {drift:>9.3f}{mark}"
        )
    first, last = wins[0], wins[-1]
    print()
    print(
        f"  {bold('Story:')} dominant-path share "
        f"{first['dominant_share']:.0%} -> {last['dominant_share']:.0%}, "
        f"entropy {first['trajectory_entropy']:.2f} -> {last['trajectory_entropy']:.2f}, "
        f"JSD drift peaks at {max(w['trajectory_drift_jsd'] for w in wins):.3f}."
    )
    print(dim("  Rising entropy + JSD = the agent stopped taking one clean path and fragmented."))


def narrate_q2(url: str, window_size: str) -> None:
    hr("Q2 — DOES QUALITY DROP CO-OCCUR WITH TRAJECTORY CHANGE *AND* GPU CONTENTION?")
    data = _get(url, f"/analytics/correlation/windows?window_size={window_size}")
    if not data or not data.get("windows"):
        print(yellow("  No windows yet."))
        return
    wins = data["windows"]
    print(bold(
        f"  {'window':>8} {'q_avg':>6} {'q_delta':>7} {'gpu':>6} "
        f"{'corr':>6} {'qDrop':>5} {'drift':>5} {'gpu!':>4}  verdict"
    ))
    t0 = wins[0]["window_start_ms"]
    for w in wins:
        rel = int((w["window_start_ms"] - t0) / 1000)
        flag = w["correlation_flag"]
        fmt = red if flag == "gpu_induced_degradation" else (
            yellow if flag not in ("normal",) else green)
        print(
            f"  {rel:>6}s {w['quality_overall_avg']:>6.2f} {w['quality_delta']:>+7.2f} "
            f"{w['gpu_contention_avg']:>6.3f} {w['contention_quality_corr']:>+6.2f} "
            f"{w['quality_drop']:>5} {w['trajectory_drift']:>5} {w['gpu_pressure']:>4}  {fmt(flag)}"
        )
    alerts = _get(url, "/analytics/correlation/alerts") or {}
    by_flag = alerts.get("by_flag", {})
    gpu_hits = by_flag.get("gpu_induced_degradation", 0)
    print()
    if gpu_hits:
        print(green(
            f"  {bold('Story:')} {gpu_hits} window(s) where ALL THREE fired together — "
            "quality_drop AND trajectory_drift AND gpu_pressure = gpu_induced_degradation."))
        print(dim("  That triple co-occurrence is the thesis: infra pressure mutated the path and tanked quality."))
    else:
        worst = min(wins, key=lambda w: w["contention_quality_corr"])
        print(yellow(
            "  No full gpu_induced_degradation window yet. Strongest signal: "
            f"corr(gpu,quality)={worst['contention_quality_corr']:+.2f} "
            f"at gpu={worst['gpu_contention_avg']:.3f}."))
        print(dim("  If gpu_pressure never fires, lower GPU_PRESSURE_THRESHOLD (see --calibrate) and rerun stream_windows."))


def narrate_q3(url: str, bad_threshold: float) -> None:
    hr("Q3 — WHICH TRAJECTORY MUTATIONS LEAD TO INCORRECT OUTCOMES?")
    data = _get(url, f"/analytics/mutations?bad_threshold={bad_threshold}")
    if not data or not data.get("mutations"):
        print(yellow("  No correlated traces yet."))
        return
    print(dim(
        f"  {data['total_traces']} traces | global_bad_rate={data['global_bad_rate']:.2f} "
        f"| dominant={data['dominant_signature']} | harmful paths={data['harmful_count']}\n"))
    print(bold(
        f"  {'count':>5} {'bad':>5} {'lift':>5} {'q':>5} {'dStep':>6} {'dLLM':>5} {'retry':>5}  signature"))
    for m in data["mutations"][:10]:
        tag = green(" DOM") if m["is_dominant"] else (red("HARM") if m["lift"] > 1.0 else "    ")
        sig = m["signature"]
        sig = sig if len(sig) <= 34 else sig[:31] + "..."
        print(
            f"  {m['count']:>5} {m['bad_rate']:>5.2f} {m['lift']:>5.2f} {m['avg_quality']:>5.2f} "
            f"{m['step_delta_vs_dominant']:>+6.1f} {m['llm_delta_vs_dominant']:>+5.1f} "
            f"{m['avg_retry_count']:>5.1f}  {tag} {cyan(sig)}"
        )
    harmful = [m for m in data["mutations"] if m["lift"] > 1.0 and not m["is_dominant"]]
    if harmful:
        w = harmful[0]
        print()
        print(green(
            f"  {bold('Story:')} the worst mutation has {w['bad_rate']:.0%} bad outcomes "
            f"({w['lift']:.1f}x the baseline)."))
        why = []
        if w["step_delta_vs_dominant"] < 0:
            why.append(f"{abs(w['step_delta_vs_dominant']):.0f} fewer steps (truncated reasoning)")
        if w["llm_delta_vs_dominant"] < 0:
            why.append(f"{abs(w['llm_delta_vs_dominant']):.0f} fewer LLM calls")
        if w["avg_retry_count"] > 0.5:
            why.append(f"{w['avg_retry_count']:.1f} avg retries (fallback churn)")
        if why:
            print(dim("  Why it fails: " + ", ".join(why) + "."))


def calibrate(url: str) -> None:
    hr("CALIBRATION — GPU contention spread")
    g = _get(url, "/analytics/gpu")
    if not g:
        print(yellow("  No gpu_metrics yet — start the collector on the GPU box first."))
        return
    s = g["summary"]
    print(f"  samples={s['total_samples']}  avg={s['avg_contention']:.3f}  "
          f"max={s['max_contention']:.3f}  high(>0.7)={s['high_contention_pct']}%")
    avg, mx = s["avg_contention"], s["max_contention"]
    if mx <= avg + 0.01:
        print(yellow("  Contention barely moves — drive the stress phase while watching this."))
        return
    suggested = round(avg + 0.6 * (mx - avg), 2)
    print()
    print(green(f"  Suggested GPU_PRESSURE_THRESHOLD={suggested} "
                f"(between idle≈{avg:.2f} and peak≈{mx:.2f})."))
    print(dim("  Set it, then restart stream_windows so Q2's gpu_pressure light fires under load."))


def narrate_all(url: str, window_size: str, bad_threshold: float) -> None:
    narrate_q1(url, window_size)
    narrate_q2(url, window_size)
    narrate_q3(url, bad_threshold)
    hr()
    print(bold("  Dashboard:") + f" {url}/static/analytics.html")
    print(dim("  Re-pull these verdicts live anytime with:  python3 deploy/demo_story.py --check\n"))


# ── Main ──────────────────────────────────────────────────────────────────────
async def _amain(args) -> None:
    # Health check
    health = _get(args.url, "/ingest/status")
    if health is None:
        print(red(f"Chat server not reachable at {args.url}. Start the pipeline first."))
        return

    await run_baseline(args.url, args.baseline_convos)
    print(dim(f"\n  Settling {args.gap}s so the baseline window closes cleanly...\n"))
    await asyncio.sleep(args.gap)
    await run_stress(args.url, args.stress_users, args.stress_duration)

    wait = args.settle
    print(dim(f"\n  Waiting {wait}s for streams to recompute windows...\n"))
    await asyncio.sleep(wait)
    narrate_all(args.url, args.window_size, args.bad_threshold)


def main() -> None:
    # Line-buffer stdout so progress is visible live when piped/teed on stage.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:  # noqa: BLE001
        pass
    p = argparse.ArgumentParser(description="Trajectory analytics demo story driver")
    p.add_argument("--url", default="http://localhost:8000")
    p.add_argument("--check", action="store_true", help="Only narrate current state (no traffic)")
    p.add_argument("--calibrate", action="store_true", help="Report GPU contention spread + threshold suggestion")
    p.add_argument("--baseline-convos", type=int, default=6)
    p.add_argument("--stress-users", type=int, default=8)
    p.add_argument("--stress-duration", type=int, default=180)
    p.add_argument("--gap", type=int, default=20, help="Seconds between baseline and stress")
    p.add_argument("--settle", type=int, default=75, help="Seconds to wait for windows after stress")
    p.add_argument("--window-size", default="5min", choices=["5min", "30min", "1h"])
    p.add_argument("--bad-threshold", type=float, default=3.0)
    args = p.parse_args()

    if args.calibrate:
        calibrate(args.url)
        return
    if args.check:
        narrate_all(args.url, args.window_size, args.bad_threshold)
        return
    asyncio.run(_amain(args))


if __name__ == "__main__":
    main()
