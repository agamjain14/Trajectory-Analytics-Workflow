"""
FastAPI Chat Server for Travel Agent AI.
Intelligent multi-turn conversation: parses user intent via LLM, asks clarifying
questions for missing info, only executes trip planning when all details are gathered.
Fully instrumented end-to-end.
"""

import asyncio
import json
import os
import time
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from src.telemetry import init_telemetry, shutdown_telemetry, get_tracer, set_current_session_id
from src.metrics import AppMetrics
from src.rag import RAGRetriever
from src.tools import BuiltinTools, HTTPTool, MCPToolClient
from src.llm_client import LLMClient
from src.agents import (
    OrchestratorAgent,
    ResearchAgent,
    FlightAgent,
    HotelAgent,
    ItineraryAgent,
)
from src.session_store import (
    create_session,
    end_session,
    resume_session,
    list_sessions,
    add_message,
    get_session_history,
    get_session,
    get_session_context,
    set_session_context,
)


# --- Global app state ---
orchestrator: OrchestratorAgent = None
llm_client: LLMClient = None
tracer_instance = None

# Required fields for trip planning — only destination is truly mandatory
REQUIRED_FIELDS = ["destination"]

# Smart defaults applied when user doesn't provide specifics
SMART_DEFAULTS = {
    "origin": "nearest major airport",
    "departure_date": "2026-07-15",
    "return_date": "2026-07-22",
    "passengers": 1,
    "interests": "sightseeing, local cuisine, culture",
    "budget": "mid-range",
}

EXTRACT_INTENT_PROMPT = """You are a travel info extractor. Parse the user's message into a JSON object.

EXAMPLES:
User: "bombay to goa, 2 pax, luxury trip, week long, mid july"
→ {{"intent": "plan_trip", "destination": "Goa", "origin": "Mumbai", "passengers": 2, "budget": "luxury", "departure_date": "2026-07-15", "return_date": "2026-07-22"}}

User: "plan my vacation to rome"
→ {{"intent": "plan_trip", "destination": "Rome"}}

User: "adventure"
→ {{"intent": "plan_trip", "interests": "adventure"}}

User: "just suggest something"
→ {{"intent": "proceed"}}

User: "make it luxury and add beaches"
→ {{"intent": "modify_plan", "budget": "luxury", "interests": "beaches"}}

User: "change destination to Paris"
→ {{"intent": "modify_plan", "destination": "Paris"}}

User: "give me detailed itinerary from India, family of 3, mid-budget, 10 days, mid september"
→ {{"intent": "modify_plan", "origin": "India", "passengers": 3, "budget": "mid-range", "departure_date": "2026-09-15", "return_date": "2026-09-25"}}

User: "from Mumbai, 2 people, luxury"
→ {{"intent": "modify_plan", "origin": "Mumbai", "passengers": 2, "budget": "luxury"}}

User: "no" or "whatever" or "you decide"
→ {{"intent": "proceed"}}

User: "that's a good trip" or "thanks" or "cool" or "nice" or "looks great" or "awesome"
→ {{"intent": "conversational"}}

User: "tell me more about the hotels" or "what about food?" or "any tips?"
→ {{"intent": "conversational"}}

User: "ok" or "sure" or "sounds good"
→ {{"intent": "conversational"}}

FIELDS TO EXTRACT (only if user stated them):
- "intent": "plan_trip" | "modify_plan" | "proceed" | "conversational" | "unclear"
  - "plan_trip" = user is providing NEW trip details or asking to plan a trip
  - "modify_plan" = user wants to CHANGE something specific about an existing plan (new destination, different budget, etc.)
  - "proceed" = user wants you to stop asking and just do it
  - "conversational" = user is reacting, thanking, asking follow-up questions, or chatting (NOT requesting a new plan or modification)
  - "unclear" = can't determine
- "destination": city/country name
- "origin": departure city
- "departure_date": YYYY-MM-DD (interpret "mid july" as 2026-07-15, "early aug" as 2026-08-01, etc.)
- "return_date": YYYY-MM-DD (calculate from duration if given, e.g. "week long" = +7 days)
- "passengers": number
- "interests": comma-separated activities
- "budget": budget/mid-range/luxury

RULES:
- "X to Y" means origin=X, destination=Y
- "from X" means origin=X (departure city), do NOT treat it as destination
- "to Y" means destination=Y
- If previously collected already has a destination, do NOT overwrite it unless the user explicitly says "change destination to Z" or "I want to go to Z instead"
- "2 pax" or "2 people" or "couple" → passengers=2, "family of N" → passengers=N
- "week long" → return_date = departure_date + 7 days, "10 days" → return_date = departure_date + 10 days
- "mid september" → departure_date = 2026-09-15
- Do NOT treat typos/curse words as destinations (e.g. "godamm" is frustration, not a place)
- If user is just reacting to a plan (positive/negative feedback without specific changes) → conversational
- If previously collected has a destination and user is adding more details (origin, budget, dates, etc.), intent should be "modify_plan", NOT "plan_trip"
- Return ONLY valid JSON, no explanation

User message: "{message}"
Previously collected: {context}

JSON:"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize AI components on startup, cleanup on shutdown."""
    global orchestrator, llm_client, tracer_instance

    tracer_obj, meter, logger = init_telemetry()
    app_metrics = AppMetrics(meter)
    rag = RAGRetriever(app_metrics)
    tools = BuiltinTools(app_metrics)
    http_tool = HTTPTool(app_metrics)
    mcp_client = MCPToolClient(app_metrics)
    llm_client = LLMClient(app_metrics)

    research_agent = ResearchAgent(llm_client, rag, app_metrics)
    flight_agent = FlightAgent(llm_client, tools, mcp_client, app_metrics)
    hotel_agent = HotelAgent(llm_client, tools, mcp_client, app_metrics)
    itinerary_agent = ItineraryAgent(llm_client, tools, http_tool, mcp_client, app_metrics)

    orchestrator = OrchestratorAgent(
        llm=llm_client,
        research_agent=research_agent,
        flight_agent=flight_agent,
        hotel_agent=hotel_agent,
        itinerary_agent=itinerary_agent,
        app_metrics=app_metrics,
    )
    tracer_instance = get_tracer()
    logger.info("chat_server.ready")

    # Start background tasks: synthetic metrics fallback + correlation.
    # In METRICS_MODE=real, Spark owns the metrics pipeline and correlation, so
    # these no-Spark loops stay off to avoid dual-writing the Delta tables.
    # The /ingest/* endpoints remain active in all modes.
    if os.getenv("METRICS_MODE", "synthetic").lower() != "real":
        from src.live_metrics import synthetic_metrics_loop, correlation_loop
        asyncio.create_task(synthetic_metrics_loop())
        asyncio.create_task(correlation_loop())
        logger.info("chat_server.background_tasks_started")
    else:
        logger.info("chat_server.background_tasks_skipped metrics_mode=real")

    yield
    shutdown_telemetry()


app = FastAPI(title="Travel Agent AI Chat", version="1.0.0", lifespan=lifespan)

from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount analytics API endpoints into the same app
from src.analytics_api import app as analytics_app
app.mount("/analytics-api", analytics_app)

# Also expose analytics routes at top level for backwards compat
from src.analytics_api import (
    get_trajectories, get_trajectory_detail, get_quality_scores,
    get_quality_detail, get_gpu_metrics, get_network_metrics,
    get_topology, get_correlated_traces, get_analytics_windows,
    get_correlation_alerts, get_microstructure, get_routing, get_llm_summary,
)
app.get("/analytics/trajectories")(get_trajectories)
app.get("/analytics/trajectories/{trace_id}")(get_trajectory_detail)
app.get("/analytics/quality")(get_quality_scores)
app.get("/analytics/quality/{trace_id}")(get_quality_detail)
app.get("/analytics/gpu")(get_gpu_metrics)
app.get("/analytics/network")(get_network_metrics)
app.get("/analytics/topology")(get_topology)
app.get("/analytics/correlation/traces")(get_correlated_traces)
app.get("/analytics/correlation/windows")(get_analytics_windows)
app.get("/analytics/correlation/alerts")(get_correlation_alerts)
app.get("/analytics/microstructure")(get_microstructure)
app.get("/analytics/routing")(get_routing)
app.get("/analytics/summary")(get_llm_summary)

# Mount live metrics ingest endpoints (Vast.ai collectors push here)
from src.live_metrics import router as live_metrics_router
app.include_router(live_metrics_router)

app.mount("/static", StaticFiles(directory="static"), name="static")


# --- Models ---

class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str


class NewSessionRequest(BaseModel):
    title: Optional[str] = None


# --- Intent Parsing ---

# Use the smarter model for intent parsing (JSON extraction needs accuracy)
INTENT_MODEL = os.getenv("INTENT_MODEL", "llama3.2")


def extract_info_from_message(message: str, existing_context: dict) -> dict:
    """Use LLM to extract travel parameters and intent from user message."""
    prompt = EXTRACT_INTENT_PROMPT.format(
        message=message,
        context=json.dumps(existing_context) if existing_context else "{}",
    )
    messages = [
        {"role": "system", "content": "You extract structured travel info from user messages. Return only valid JSON."},
        {"role": "user", "content": prompt},
    ]

    response = llm_client.chat(messages, agent_name="intent_parser", temperature=0.1, model_override=INTENT_MODEL)
    content = response["content"].strip()

    # Parse JSON from response
    try:
        # Handle cases where LLM wraps in markdown code block
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        return json.loads(content)
    except json.JSONDecodeError:
        import re
        match = re.search(r'\{[^}]+\}', content)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
            return {"intent": "plan_trip"}


def get_missing_fields(context: dict) -> list[str]:
    """Only destination is truly required."""
    return [f for f in REQUIRED_FIELDS if f not in context or not context[f]]


def apply_smart_defaults(context: dict) -> dict:
    """Fill in missing optional fields with smart defaults."""
    for field, default in SMART_DEFAULTS.items():
        if field not in context or not context[field]:
            context[field] = default
    return context


def generate_conversational_response(message: str, context: dict, history_summary: str) -> str:
    """Generate a natural conversational response (no trip execution)."""
    destination = context.get("destination", "your destination")
    prompt = f"""You are a travel assistant. The user already has a trip plan to {destination}.
They just said: "{message}"

Context of their trip: {json.dumps(context)}

Respond naturally and helpfully. If they're giving positive feedback, acknowledge it warmly.
If they ask a follow-up question, answer based on what you know.
If they want to know more, offer suggestions.
Keep it brief (2-3 sentences max). Remind them they can ask to modify the plan if they want changes."""

    messages = [
        {"role": "system", "content": "You are a friendly travel assistant. Be concise and helpful."},
        {"role": "user", "content": prompt},
    ]

    response = llm_client.chat(messages, agent_name="conversational", temperature=0.7)
    return response["content"]


# --- API Endpoints ---

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")


@app.get("/topology")
async def serve_topology():
    return FileResponse("static/topology.html")


@app.get("/api/sessions")
async def api_list_sessions():
    sessions = list_sessions(limit=50)
    return {"sessions": sessions}


@app.post("/api/sessions")
async def api_create_session(req: NewSessionRequest):
    session = create_session(title=req.title)
    set_current_session_id(session["session_id"])
    return session


@app.get("/api/sessions/{session_id}")
async def api_get_session(session_id: str):
    set_current_session_id(session_id)
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@app.get("/api/sessions/{session_id}/history")
async def api_get_history(session_id: str):
    set_current_session_id(session_id)
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    history = get_session_history(session_id)
    return {"session_id": session_id, "messages": history}


@app.post("/api/sessions/{session_id}/end")
async def api_end_session(session_id: str):
    set_current_session_id(session_id)
    session = get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    end_session(session_id)
    return {"status": "ended", "session_id": session_id}


@app.post("/api/sessions/{session_id}/resume")
async def api_resume_session(session_id: str):
    set_current_session_id(session_id)
    session = resume_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@app.post("/api/chat")
async def api_chat(req: ChatRequest):
    """
    Intelligent chat endpoint.
    Flow:
      1. Parse user message to extract travel info (LLM call - traced)
      2. Merge with existing session context
      3. If info is complete -> execute trip planning (full agent pipeline - traced)
      4. If info is missing -> ask clarifying questions (LLM call - traced)
    """
    global orchestrator, llm_client, tracer_instance

    with tracer_instance.start_as_current_span("POST /api/chat") as root_span:
        request_start = time.time()
        # OTel HTTP Server Semantic Convention
        root_span.set_attribute("http.request.method", "POST")
        root_span.set_attribute("http.route", "/api/chat")
        root_span.set_attribute("orchestration.input.user_message", req.message)

        # --- Resolve session ---
        if req.session_id:
            session = get_session(req.session_id)
            if not session:
                title = req.message[:50]
                session = create_session(title=title)
        else:
            title = req.message[:50]
            session = create_session(title=title)

        session_id = session["session_id"]

        # Propagate session_id to ALL subsequent spans via context
        set_current_session_id(session_id)

        root_span.set_attribute("session.id", session_id)
        turn = session.get("total_turns", 0) + 1
        root_span.set_attribute("session.turn.number", turn)

        # --- Save user message ---
        add_message(session_id, turn, "user", req.message)

        # --- Get existing context ---
        ctx = get_session_context(session_id)
        current_params = ctx["params"]
        state = ctx["state"]

        # --- Extract info from this message (intent classification) ---
        extracted = extract_info_from_message(req.message, current_params)
        intent = extracted.pop("intent", "plan_trip")
        extracted.pop("modification", None)
        root_span.set_attribute("orchestration.output.intent", intent)
        root_span.set_attribute("orchestration.output.extracted", json.dumps(extracted))

        # --- Merge extracted travel fields into context ---
        if intent == "modify_plan" and current_params.get("destination"):
            extracted_dest = extracted.get("destination", "")
            if extracted_dest and extracted_dest.lower() == current_params["destination"].lower():
                extracted.pop("destination", None)
        current_params.update({k: v for k, v in extracted.items() if v})

        # --- Handle conversational intent (no re-execution) ---
        if intent == "conversational" or (intent == "unclear" and state == "completed"):
            assistant_msg = generate_conversational_response(req.message, current_params, "")
            add_message(session_id, turn, "assistant", assistant_msg, metadata={"type": "conversational"})

            request_duration = (time.time() - request_start) * 1000
            root_span.set_attribute("orchestration.action", "conversational")
            root_span.set_attribute("orchestration.output.response", assistant_msg)
            root_span.set_attribute("http.response.status_code", 200)
            root_span.set_status(StatusCode.OK)

            return {
                "session_id": session_id,
                "turn": turn,
                "role": "assistant",
                "content": assistant_msg,
                "status": "completed",
                "total_duration_ms": round(request_duration, 2),
            }

        # --- Simple decision: have destination → execute. No destination → ask once. ---
        has_destination = bool(current_params.get("destination"))

        # If user says "proceed" with no destination, pick one for them
        if intent == "proceed" and not has_destination:
            current_params["destination"] = "Bali, Indonesia"
            has_destination = True

        if not has_destination:
            assistant_msg = "Where would you like to go? Just name the destination (e.g. 'Goa', 'Paris', 'Tokyo')."
            set_session_context(session_id, current_params, state="gathering")
            add_message(session_id, turn, "assistant", assistant_msg, metadata={"type": "ask_destination"})

            request_duration = (time.time() - request_start) * 1000
            root_span.set_attribute("orchestration.action", "ask_destination")
            root_span.set_attribute("orchestration.output.response", assistant_msg)
            root_span.set_attribute("http.response.status_code", 200)
            root_span.set_status(StatusCode.OK)

            return {
                "session_id": session_id,
                "turn": turn,
                "role": "assistant",
                "content": assistant_msg,
                "status": "gathering",
                "collected": current_params,
                "missing": ["destination"],
                "total_duration_ms": round(request_duration, 2),
            }

        # --- Apply smart defaults for anything not provided ---
        current_params = apply_smart_defaults(current_params)

        # --- Execute trip planning (PLAN span wraps only the agent pipeline) ---
        with tracer_instance.start_as_current_span("chat.execute_plan") as exec_span:
            exec_span.set_attribute("orchestration.type", "plan_execution")
            exec_span.set_attribute("session.id", session_id)

            set_session_context(session_id, current_params, state="executing")

            interests = current_params.get("interests", "sightseeing")
            if isinstance(interests, str):
                interests_list = [i.strip() for i in interests.split(",")]
            else:
                interests_list = interests

            passengers = current_params.get("passengers", 1)
            if isinstance(passengers, str):
                passengers = int(passengers) if passengers.isdigit() else 1

            # Run the full orchestrator pipeline (instrumented internally)
            result = orchestrator.plan_trip(
                destination=current_params["destination"],
                origin=current_params["origin"],
                departure_date=current_params["departure_date"],
                return_date=current_params["return_date"],
                passengers=passengers,
                interests=interests_list,
                budget=current_params["budget"],
            )

            # Save result
            set_session_context(session_id, current_params, state="completed")

            if result["status"] == "success":
                assistant_msg = (
                    f"## Trip to {current_params['destination']} planned!\n\n"
                    f"**Duration:** {result['duration_days']} days | "
                    f"**Workflow:** {result['workflow_duration_ms']}ms\n\n"
                    f"### Research\n{result['research']}\n\n"
                    f"### Flights\n{result['flights']}\n\n"
                    f"### Hotels\n{result['hotels']}\n\n"
                    f"### Itinerary\n{result['itinerary']}\n\n"
                    f"---\n*Want to change anything? Just tell me (e.g. \"make it luxury\" or \"add beach activities\")*"
                )
                add_message(session_id, turn, "assistant", assistant_msg, metadata={
                    "type": "plan_result",
                    "status": "success",
                    "workflow_duration_ms": result["workflow_duration_ms"],
                })
                exec_span.set_status(StatusCode.OK)
            else:
                assistant_msg = (
                    f"Trip planning failed: {result.get('error')}\n\n"
                    "Make sure Ollama is running (`ollama serve`) and model is pulled (`ollama pull llama3.2`)."
                )
                add_message(session_id, turn, "assistant", assistant_msg, metadata={
                    "type": "plan_result", "status": "error",
                })
                exec_span.set_status(StatusCode.ERROR, result.get("error", "unknown"))

        request_duration = (time.time() - request_start) * 1000
        root_span.set_attribute("orchestration.action", "execute_plan")
        root_span.set_attribute("orchestration.output.response", assistant_msg)
        root_span.set_attribute("http.response.status_code", 200 if result["status"] == "success" else 500)

        if result["status"] == "success":
            root_span.set_status(StatusCode.OK)
            return {
                "session_id": session_id,
                "turn": turn,
                "role": "assistant",
                "content": assistant_msg,
                "status": "success",
                "workflow_duration_ms": result["workflow_duration_ms"],
                "total_duration_ms": round(request_duration, 2),
            }
        else:
            root_span.set_status(StatusCode.ERROR)
            return {
                "session_id": session_id,
                "turn": turn,
                "role": "assistant",
                "content": assistant_msg,
                "status": "error",
                "total_duration_ms": round(request_duration, 2),
            }
