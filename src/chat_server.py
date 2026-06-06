"""
FastAPI Chat Server for Travel Agent AI.
Intelligent multi-turn conversation: parses user intent via LLM, asks clarifying
questions for missing info, only executes trip planning when all details are gathered.
Fully instrumented end-to-end.
"""

import json
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
    llm_client = LLMClient(app_metrics)

    research_agent = ResearchAgent(llm_client, rag, app_metrics)
    flight_agent = FlightAgent(llm_client, tools, app_metrics)
    hotel_agent = HotelAgent(llm_client, tools, app_metrics)
    itinerary_agent = ItineraryAgent(llm_client, tools, app_metrics)

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
    yield
    shutdown_telemetry()


app = FastAPI(title="Travel Agent AI Chat", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


# --- Models ---

class ChatRequest(BaseModel):
    session_id: Optional[str] = None
    message: str


class NewSessionRequest(BaseModel):
    title: Optional[str] = None


# --- Intent Parsing ---

def extract_info_from_message(message: str, existing_context: dict) -> dict:
    """Use LLM to extract travel parameters and intent from user message."""
    with tracer_instance.start_as_current_span("intent.extract") as span:
        span.set_attribute("intent.user_message", message)
        span.set_attribute("intent.existing_context", json.dumps(existing_context))

        prompt = EXTRACT_INTENT_PROMPT.format(
            message=message,
            context=json.dumps(existing_context) if existing_context else "{}",
        )
        messages = [
            {"role": "system", "content": "You extract structured travel info from user messages. Return only valid JSON."},
            {"role": "user", "content": prompt},
        ]

        response = llm_client.chat(messages, agent_name="intent_parser", temperature=0.1)
        content = response["content"].strip()

        # Parse JSON from response
        try:
            # Handle cases where LLM wraps in markdown code block
            if content.startswith("```"):
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:]
            extracted = json.loads(content)
            span.set_attribute("intent.extracted_fields", list(extracted.keys()))
            span.set_attribute("intent.detected", extracted.get("intent", "unclear"))
            span.set_status(StatusCode.OK)
            return extracted
        except json.JSONDecodeError:
            span.set_status(StatusCode.ERROR, "json_parse_failed")
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
    with tracer_instance.start_as_current_span("chat.conversational") as span:
        span.set_attribute("chat.type", "conversational")

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
        span.set_status(StatusCode.OK)
        return response["content"]


# --- API Endpoints ---

@app.get("/")
async def serve_ui():
    return FileResponse("static/index.html")


@app.get("/api/sessions")
async def api_list_sessions():
    with tracer_instance.start_as_current_span("api.list_sessions") as span:
        sessions = list_sessions(limit=50)
        span.set_attribute("sessions.count", len(sessions))
        return {"sessions": sessions}


@app.post("/api/sessions")
async def api_create_session(req: NewSessionRequest):
    with tracer_instance.start_as_current_span("api.create_session") as span:
        session = create_session(title=req.title)
        set_current_session_id(session["session_id"])
        span.set_attribute("session.id", session["session_id"])
        span.set_status(StatusCode.OK)
        return session


@app.get("/api/sessions/{session_id}")
async def api_get_session(session_id: str):
    set_current_session_id(session_id)
    with tracer_instance.start_as_current_span("api.get_session") as span:
        span.set_attribute("session.id", session_id)
        session = get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        return session


@app.get("/api/sessions/{session_id}/history")
async def api_get_history(session_id: str):
    set_current_session_id(session_id)
    with tracer_instance.start_as_current_span("api.get_history") as span:
        span.set_attribute("session.id", session_id)
        session = get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        history = get_session_history(session_id)
        span.set_attribute("history.count", len(history))
        return {"session_id": session_id, "messages": history}


@app.post("/api/sessions/{session_id}/end")
async def api_end_session(session_id: str):
    set_current_session_id(session_id)
    with tracer_instance.start_as_current_span("api.end_session") as span:
        span.set_attribute("session.id", session_id)
        session = get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        end_session(session_id)
        span.set_status(StatusCode.OK)
        return {"status": "ended", "session_id": session_id}


@app.post("/api/sessions/{session_id}/resume")
async def api_resume_session(session_id: str):
    set_current_session_id(session_id)
    with tracer_instance.start_as_current_span("api.resume_session") as span:
        span.set_attribute("session.id", session_id)
        session = resume_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        span.set_status(StatusCode.OK)
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

    with tracer_instance.start_as_current_span("api.chat") as root_span:
        request_start = time.time()
        root_span.set_attribute("chat.user_message", req.message)

        # --- Resolve session ---
        with tracer_instance.start_as_current_span("session.resolve") as sess_span:
            if req.session_id:
                session = get_session(req.session_id)
                if not session:
                    # Session doesn't exist (e.g. DB was reset) — create a new one
                    title = req.message[:50]
                    session = create_session(title=title)
                    sess_span.set_attribute("session.action", "recreated")
                else:
                    sess_span.set_attribute("session.action", "existing")
            else:
                title = req.message[:50]
                session = create_session(title=title)
                sess_span.set_attribute("session.action", "auto_created")

            session_id = session["session_id"]
            sess_span.set_attribute("session.id", session_id)
            sess_span.set_status(StatusCode.OK)

        # Propagate session_id to ALL subsequent spans via context
        set_current_session_id(session_id)

        root_span.set_attribute("session.id", session_id)
        turn = session.get("total_turns", 0) + 1
        root_span.set_attribute("turn.number", turn)

        # --- Save user message ---
        with tracer_instance.start_as_current_span("session.save_user_message") as span:
            span.set_attribute("session.id", session_id)
            add_message(session_id, turn, "user", req.message)
            span.set_status(StatusCode.OK)

        # --- Get existing context ---
        with tracer_instance.start_as_current_span("session.load_context") as span:
            ctx = get_session_context(session_id)
            current_params = ctx["params"]
            state = ctx["state"]
            span.set_attribute("context.state", state)
            span.set_attribute("context.known_fields", list(current_params.keys()))
            span.set_status(StatusCode.OK)

        # --- Extract info from this message ---
        extracted = extract_info_from_message(req.message, current_params)
        intent = extracted.pop("intent", "plan_trip")
        extracted.pop("modification", None)
        root_span.set_attribute("intent.detected", intent)
        root_span.set_attribute("intent.extracted", json.dumps(extracted))

        # --- Merge extracted travel fields into context ---
        with tracer_instance.start_as_current_span("session.merge_context") as span:
            # For modify_plan: don't overwrite destination unless explicitly changed
            if intent == "modify_plan" and current_params.get("destination"):
                extracted_dest = extracted.get("destination", "")
                # Only overwrite destination if it's genuinely different and intentional
                if extracted_dest and extracted_dest.lower() == current_params["destination"].lower():
                    extracted.pop("destination", None)
            current_params.update({k: v for k, v in extracted.items() if v})
            span.set_attribute("context.merged_fields", list(current_params.keys()))
            span.set_status(StatusCode.OK)

        # --- Handle conversational intent (no re-execution) ---
        if intent == "conversational" or (intent == "unclear" and state == "completed"):
            assistant_msg = generate_conversational_response(req.message, current_params, "")
            add_message(session_id, turn, "assistant", assistant_msg, metadata={"type": "conversational"})

            request_duration = (time.time() - request_start) * 1000
            root_span.set_attribute("chat.action", "conversational")
            root_span.set_attribute("chat.assistant_response", assistant_msg)
            root_span.set_attribute("chat.total_duration_ms", round(request_duration, 2))
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
            # This is the ONLY question we ask — hardcoded, no LLM call, no randomness
            assistant_msg = "Where would you like to go? Just name the destination (e.g. 'Goa', 'Paris', 'Tokyo')."
            set_session_context(session_id, current_params, state="gathering")
            add_message(session_id, turn, "assistant", assistant_msg, metadata={"type": "ask_destination"})

            request_duration = (time.time() - request_start) * 1000
            root_span.set_attribute("chat.action", "ask_destination")
            root_span.set_attribute("chat.assistant_response", assistant_msg)
            root_span.set_attribute("chat.total_duration_ms", round(request_duration, 2))
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

        # --- Execute trip planning ---
        with tracer_instance.start_as_current_span("chat.execute_plan") as exec_span:
            exec_span.set_attribute("session.id", session_id)
            exec_span.set_attribute("plan.destination", current_params["destination"])
            exec_span.set_attribute("plan.origin", current_params["origin"])

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
            with tracer_instance.start_as_current_span(f"session.turn.{turn}") as turn_span:
                turn_span.set_attribute("session.id", session_id)
                turn_span.set_attribute("turn.number", turn)
                turn_span.set_attribute("turn.destination", current_params["destination"])

                result = orchestrator.plan_trip(
                    destination=current_params["destination"],
                    origin=current_params["origin"],
                    departure_date=current_params["departure_date"],
                    return_date=current_params["return_date"],
                    passengers=passengers,
                    interests=interests_list,
                    budget=current_params["budget"],
                )

                if result["status"] == "success":
                    turn_span.set_status(StatusCode.OK)
                else:
                    turn_span.set_status(StatusCode.ERROR, result.get("error", "unknown"))

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
        root_span.set_attribute("chat.action", "execute_plan")
        root_span.set_attribute("chat.assistant_response", assistant_msg)
        root_span.set_attribute("chat.total_duration_ms", round(request_duration, 2))

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
