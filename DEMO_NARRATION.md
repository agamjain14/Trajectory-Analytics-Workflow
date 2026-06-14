# 🎬 Demo Recording Script — Agent Trajectory Analytics

A full screen-by-screen video script. Follow it top to bottom and the demo records itself.
Total runtime ≈ **6–7 minutes**. Each scene tells you **🎥 SHOW** (what to put on screen),
**🎤 SAY** (read aloud, word-for-word), and **👁️ READ THE CHART** (how to interpret every line,
bar, and dot so you sound like you know the data cold).

> **Golden rule:** point your cursor at the exact tile you're talking about. The viewer follows
> your mouse. Never talk about a chart that isn't on screen.

---

## ✅ Pre-flight (do this before you hit record)

1. Pipeline + SSH tunnel running, GPU box live. Quick check (run as a **single line**):
   `curl -s http://localhost:8000/ingest/status` → expect `"gpu_source":"real"`.
2. Open the dashboard: <http://localhost:8000/static/analytics.html> and click **↻ Refresh**.
3. Have **one terminal** visible for the "it's real" proof shot at the end.
4. Recording: `Cmd + Shift + 5` → *Record Selected Portion* → drag over the browser window.
5. Zoom the browser to ~110–125% so tiles are legible on video.
6. Take a breath. Smile. Roll.

---

# 🎬 SCENE 1 — The Hook & The Problem (0:00–0:45)

**🎥 SHOW:** Your face cam or a title slide, OR just the top of the dashboard before scrolling.

**🎤 SAY:**
> "Hi — in the next six minutes I'm going to show you something most AI teams are flying blind on.
>
> AI agents are **non-deterministic**. You send the same request twice and the agent can take a
> completely different path, and the answer quality can quietly fall apart. When that happens,
> everyone asks the same shallow question: *'was the model just dumb today?'*
>
> That misses the real cause. The question I actually care about is:
> **did the infrastructure underneath the agent — the GPU — change how the agent behaved?**
>
> So I built a system that watches an AI agent and joins three things nobody usually looks at
> together: the agent's **execution path**, its **answer quality**, and the **GPU pressure** at
> that exact moment. Let me show you what it caught."

**💡 Why this opener works:** you state the problem, the wrong question everyone asks, and your
sharper question — all in 30 seconds. The audience now knows exactly what to watch for.

---

# 🎬 SCENE 2 — What This Actually Is (0:45–1:15)

**🎥 SHOW:** Top of the dashboard — the title bar "Agent Trajectory Analytics — Assurance Dashboard".

**🎤 SAY:**
> "Here's the setup. The agent is a **travel planner** — you ask it to plan a trip, and an
> orchestrator delegates to sub-agents that do research, find flights, find hotels, and write
> the itinerary. Every one of those steps is instrumented with **OpenTelemetry**.
>
> That telemetry streams through **Apache Spark and Delta Lake**, where I extract the agent's
> path, score the answer quality with a second model acting as a **judge**, and pull in **real
> GPU metrics** — then join it all into the view you're looking at.
>
> And one detail that matters: this whole analytics stack runs **locally on my Mac**. The only
> things on the remote GPU — a real RTX 3060 — are the model doing inference and the judge.
> **Every GPU number you'll see is measured off that physical card. Nothing is simulated.**"

**💡 Why it matters:** establishes credibility (real hardware) and scope (real streaming stack)
before you show a single number.

---

# 🎬 SCENE 3 — The Experiment I Ran (1:15–1:40)

**🎥 SHOW:** Still on the top of the dashboard. Optionally hover the **↻ Refresh** button.

**🎤 SAY:**
> "To make the effect visible, I ran a controlled **A/B experiment**. First a **calm phase**:
> one user, idle GPU — that's my clean baseline. Then a **stress phase**: I slammed that single
> GPU with up to twelve concurrent users until it choked. Response time went from about
> **eighteen seconds to over two minutes**, and some requests timed out and fell back to
> shortcut paths. Then I let the analytics recompute. Everything from here is the result."

---

# 🎬 SCENE 4 — The KPI Strip (1:40–2:10)

**🎥 SHOW:** The four cards at the very top: **Total Traces · Avg Quality · GPU Contention · Alerts**.
Also point at the colored **status badge** (HEALTHY / WARNING / DEGRADED) near the title.

**👁️ READ THE CHART:**
- **Total Traces** = how many complete agent runs I've captured; the subtitle shows the number of
  **distinct path templates**. More templates than expected = the agent is taking many different routes.
- **Avg Quality** = mean judge score, scale **0 to 5**.
- **GPU Contention** = mean contention index, **0 = idle, 1 = saturated**.
- **Alerts** = how many time windows the correlation engine flagged as anomalous.
- **Status badge** auto-colors: green under 0.4 contention, yellow 0.4–0.7, red above 0.7.

**🎤 SAY:**
> "Top-line health first. I've collected **around 57 agent runs** across **13 distinct execution
> paths** — that path count is already a smell; a healthy agent should mostly reuse one route.
> Average quality is **2.8 out of 5**, dragged down by the stress phase. Average GPU contention
> **0.40**, and the engine raised **five correlation alerts**. Those alerts are the heart of this
> demo — hold that thought."

---

# 🎬 SCENE 5 — AI Insights (Proof the GPU Brain Is Live) (2:10–2:35)

**🎥 SHOW:** The **🧠 AI Insights** box, including the small **"Generated by llama3.2"** label
underneath.

**👁️ READ THE CHART:** This paragraph is **not hard-coded** — it's written live by the model on
the remote GPU, reading the same data you see. The timestamp label is your proof the GPU path is alive.

**🎤 SAY:**
> "Before the charts — see this summary? It's written **live by the model running on the remote
> GPU**, reading the exact same data. That 'Generated by llama3.2' stamp is your proof the GPU is
> in the loop. And notice it already spotted the GPU-versus-quality link on its own and is
> recommending fixes. So there's a real LLM on real hardware doing analysis here, not just static
> charts."

---

# 🎬 SCENE 6 — Quality Scores by Trace + GPU Contention Over Time (2:35–3:20)

**🎥 SHOW:** Scroll to the **first chart row** — two charts side by side:
left = **Quality Scores by Trace**, right = **GPU Contention Over Time**. Point at them in turn.

**👁️ READ THE LEFT CHART — Quality Scores by Trace:**
- X-axis = each trace in time order (left = earlier calm runs, right = later stress runs).
- Y-axis = judge quality 0–5.
- **What to look for:** bars/points start **high and steady** on the left, then **sag and scatter**
  toward the right as stress hits. That downward slope *is* the quality degradation.

**👁️ READ THE RIGHT CHART — GPU Contention Over Time:**
- X-axis = time. Y-axis = contention index 0–1.
- **What to look for:** a **flat, low line** at the start (calm, ~0.19) that **climbs and spikes**
  as I pile on users (up toward 0.75). This is the **real RTX 3060's** load curve.

**🎤 SAY:**
> "Now put these two charts side by side — and this is the whole story in one glance. On the left,
> **quality by trace** over time: high and steady early, then it sags and scatters. On the right,
> **GPU contention** over the same period: flat and calm at the start, then climbing and spiking
> as I add users. Quality falls **exactly as** the GPU heats up. That's the correlation I'm going
> to prove rigorously in a second — but you can already see it with your eyes."

---

# 🎬 SCENE 7 — Q1: Trajectory Template Distribution (3:20–4:00)
### *How do agent paths change over time?*

**🎥 SHOW:** The **left chart of the second row — Trajectory Template Distribution.**

**👁️ READ THE CHART:**
- Each slice/bar = one distinct path the agent took; its size = how often that path was used.
- **Calm baseline:** one big dominant slice — the agent mostly reused a single reliable route.
- **Under stress:** that one slice **fragments** into many small ones — the agent is improvising
  new, shorter paths.
- The underlying drift metric (Jensen-Shannon divergence vs. the calm window) climbs from
  **0.0 → nearly 1.0**, where 0 = "identical to baseline" and 1 = "completely different behavior."

**🎤 SAY:**
> "Question one: **do the agent's paths drift over time?** In the calm phase, runs funneled into
> **one dominant path** — predictable, low variety. Under GPU stress, that single path **shattered
> into many**. I quantify this with a drift score — Jensen-Shannon divergence — comparing each
> time window to the calm baseline. It climbs from **zero to nearly one**. Zero means 'same as
> baseline,' one means 'totally different behavior.' So I'm not hand-waving that the agent got
> flaky — I can show **exactly when** and **exactly how much** it diverged."

---

# 🎬 SCENE 8 — Q2 Visual: Quality vs GPU Contention Scatter (4:00–4:30)
### *The correlation, as a picture*

**🎥 SHOW:** The **right chart of the second row — Quality vs GPU Contention (per trace)** scatter.

**👁️ READ THE CHART:**
- Each **dot = one agent run**. X-axis = GPU contention (left low, right high).
  Y-axis = quality (bottom bad, top good).
- **What to look for:** dots form a **downward cloud from top-left to bottom-right** — low
  contention pairs with high quality; high contention pairs with low quality.
- Top-left = healthy calm runs. Bottom-right = degraded stress runs.

**🎤 SAY:**
> "Same relationship, now as a picture. Every dot is one run. Contention rises along the X-axis,
> and quality on the Y-axis trends **down** — a clear top-left-to-bottom-right slope. The dots in
> the top-left are my calm, healthy runs; the ones falling into the bottom-right are the stressed,
> degraded ones. The correlation is real and visible — not a coincidence I cherry-picked."

---

# 🎬 SCENE 9 — Q2 ⭐ THE CENTERPIECE: Correlation Alerts (4:30–5:30)
### *Does a quality drop happen at the same time as path change AND GPU contention?*

**🎥 SHOW:** The **Correlation Alerts** tile (left card of the third row). Slowly scroll through the
alert cards. Each card shows a **verdict label** and a detail line with `q_avg`, `drift_jsd`,
and `gpu_contention`.

**👁️ READ THE CHART:** For every time window the engine checks **three independent signals** —
*did quality drop?*, *did the path drift?*, *was the GPU under pressure?* — and stamps a verdict:
- `gpu_induced_degradation` → **all three fired** = the infrastructure caused it.
- `app_layer_degradation` → quality dropped but **GPU was idle** = a prompt/model problem, not infra.

**🎤 SAY — part 1 (the thesis fires):**
> "This is the centerpiece — the question that makes this whole system worth building. For every
> time window, the engine asks three independent questions: did **quality drop**, did the **path
> drift**, and was the **GPU under pressure**? When **all three trip together**, it stamps the
> window **`gpu_induced_degradation`** — meaning the infrastructure mutated the path and crushed
> quality at the same moment. You can see several of these, with quality falling more than a full
> point while drift spikes. That's my thesis, proven on real GPU telemetry."

**🎤 SAY — part 2 (point at the `app_layer_degradation` card — the knockout punch):**
> "But now look at **this** one — `app_layer_degradation`. Quality crashed even harder here, almost
> **two full points** — but the **GPU was idle**, contention only 0.21. So the engine **refuses to
> blame the hardware**. It says: this one is a prompt or model issue, not infrastructure. **That
> distinction is the entire point.** A dumb monitor would page the infra team for both of these
> drops. Mine tells you **which** failures are actually infra-caused — so your team fixes the right
> thing instead of chasing ghosts at 3 a.m."

**💡 Why it matters:** anyone can flag "quality dropped." The rare, valuable thing is correctly
**separating infra-caused failures from app-caused ones.** This is the slide that wins the demo.

---

# 🎬 SCENE 10 — Infrastructure Topology (5:30–5:45)

**🎥 SHOW:** The **Infrastructure Topology** tile (right card of the third row) — a node card with
your real GPU (`node-1` → `gpu-0`).

**👁️ READ THE CHART:** This is the **physical machine** the metrics come from — one node, one GPU.
It's the literal "infrastructure" half of "infrastructure-aware."

**🎤 SAY:**
> "Quick grounding shot: this topology tile is the **actual physical node and GPU** every metric is
> streaming from. There's a real machine behind all these numbers."

---

# 🎬 SCENE 11 — Trajectory Templates Table (5:45–6:05)

**🎥 SHOW:** The **Trajectory Templates** table. Columns: **Signature · Count · Share · Steps ·
LLM Calls · Tools · Distribution.**

**👁️ READ THE CHART:**
- Each row = one distinct path signature.
- **Steps / LLM Calls / Tools** = how much work that path did.
- **The healthy dominant path** has the highest count and the **most steps** (it does full research +
  verification). Watch this column — it sets up the Q3 punchline.

**🎤 SAY:**
> "Here's the catalog of every path the agent took. The key columns are **Steps** and **LLM Calls** —
> how much reasoning each path did. The dominant healthy path at the top does the **most** work:
> full research, then verification. Remember that — because the failures did the opposite."

---

# 🎬 SCENE 12 — Q3: Harmful Trajectory Mutations (6:05–6:45)
### *Which path mutations actually lead to wrong answers?*

**🎥 SHOW:** Scroll to the bottom table — **Harmful Trajectory Mutations (Q3)**. Columns:
**Signature · Count · Bad rate · Lift · Avg quality · Avg steps (Δ dom) · Avg LLM (Δ dom) ·
Avg retries · Avg GPU.**

**👁️ READ THE CHART:**
- **Bad rate** = fraction of runs on this path that produced a bad answer (quality below threshold).
- **Lift** = how many times worse than the overall baseline failure rate (1.0 = average; 3.4 = 3.4× worse).
- **Avg steps (Δ dom)** / **Avg LLM (Δ dom)** = how many **fewer** steps and model calls this path took
  versus the healthy dominant path. **Negative numbers = the agent skipped work.**
- **What to look for:** the top rows have **100% bad rate, high lift, and strongly negative Δ steps /
  Δ LLM** — proof the agent truncated its own reasoning.

**🎤 SAY:**
> "Last question — and this is the actionable one. Not every path mutation is harmful, so the engine
> isolates the ones that are. These top variants have a **100% bad-outcome rate — about 3.4 times the
> baseline failure rate.** And it tells me **why**: look at the delta columns — each took roughly
> **six fewer reasoning steps and one fewer model call** than the healthy path. Under GPU pressure
> the agent **truncated its own reasoning** — it skipped the research and verification steps and
> shipped a wrong answer. The healthy path scores about **3.5 out of 5**; these score near **zero**.
> That's not just an alarm — that's a **root cause I can hand to engineering and fix.**"

---

# 🎬 SCENE 13 — The "It's Real, Not Simulated" Proof (6:45–7:00)

**🎥 SHOW:** Cut to your terminal. Run each as a **single line** (no trailing `;`):

```bash
# The app declares its metrics source is REAL:
curl -s http://localhost:8000/ingest/status
#   -> {"gpu_source":"real", ...}

# The physical GPU itself (on the box):
ssh -i ~/.ssh/innovation -p 15367 root@91.150.160.38 nvidia-smi
#   -> shows the real NVIDIA GeForce RTX 3060
```

**🎤 SAY:**
> "And to be crystal clear this isn't faked — the app reports its metric source as **real**, and
> here's `nvidia-smi` on the actual box showing the physical **RTX 3060**. Real hardware, real
> telemetry, end to end."

---

# 🎬 SCENE 14 — Close (7:00–7:20)

**🎥 SHOW:** Scroll back up to the **Correlation Alerts** tile (your strongest visual) and rest there.

**🎤 SAY:**
> "So — one pipeline, three answers. I can see **how** agent paths drift over time, **prove** when a
> quality drop is the infrastructure's fault versus the app's fault, and **pinpoint** the exact path
> mutations that cause wrong answers — all correlated against a real GPU.
>
> That's the jump from *'the agent feels unreliable'* to *'this truncated path, under this measured
> GPU contention, is exactly why it failed — and here's the fix.'* Thanks for watching."

**🛑 Stop recording.**

---

## 🗺️ Quick scroll map (tape this next to your screen)

| Scene | Scroll position / tile | Question |
|---|---|---|
| 1–3 | Top of page (title) | Context + problem |
| 4 | KPI strip (4 cards) | Scale |
| 5 | 🧠 AI Insights box | GPU brain is live |
| 6 | Row 1: Quality by Trace + GPU Over Time | Visual correlation |
| 7 | Row 2 left: Trajectory Distribution | **Q1** drift |
| 8 | Row 2 right: Quality vs Contention scatter | **Q2** visual |
| 9 ⭐ | Row 3 left: **Correlation Alerts** | **Q2** verdict (centerpiece) |
| 10 | Row 3 right: Infrastructure Topology | Real machine |
| 11 | Trajectory Templates table | Healthy path = most steps |
| 12 | Harmful Trajectory Mutations table | **Q3** root cause |
| 13 | Terminal | Proof it's real |
| 14 | Back to Correlation Alerts | Close |

---

## 🏷️ Verdict-label cheat sheet (so you never fumble a term)

| Label on the dashboard | Plain English |
|---|---|
| `gpu_induced_degradation` | Quality dropped **because** the GPU was under pressure (all 3 signals fired). |
| `app_layer_degradation` | Quality dropped but the **GPU was fine** — it's a prompt/model issue, not infra. |
| `trajectory_drift_no_quality_impact` | Path changed, but quality held — drift without harm. |
| `quality_drop_stable_trajectory` | Quality dropped but the path didn't change — likely content/input issue. |
| `normal` | Healthy window — nothing wrong. |

---

## 📸 If you'd rather ship stills than a video — 5 screenshots

| # | Tile | Proves |
|---|---|---|
| 1 | Quality by Trace + GPU Over Time (side by side) | the visual correlation |
| 2 | Trajectory Template Distribution | Q1 — paths fragment over time |
| 3 ⭐ | Correlation Alerts (show **both** verdict types) | Q2 — infra-caused vs app-caused |
| 4 | Harmful Trajectory Mutations table | Q3 — which paths fail, and why |
| 5 | Terminal `gpu_source":"real"` | metrics are real, not simulated |

Screenshot region: `Cmd + Shift + 4`, drag the tile (saves to Desktop).

---

## 🔁 Want a fresh spike live on camera (~3 min)

```bash
python3 deploy/demo_story.py --baseline-convos 4 --stress-users 8 --stress-duration 120
```

Drives calm→stress traffic and re-prints the Q1/Q2/Q3 verdicts in the terminal — great B-roll
while you narrate. Then click **↻ Refresh** on the dashboard to show the new windows appear.
