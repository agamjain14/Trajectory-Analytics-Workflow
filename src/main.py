"""
Travel Agent AI - Chat Interface with Session Persistence.
Provides a ChatGPT-style conversational interface with persistent sessions
that can be resumed. All interactions are traced via OpenTelemetry -> Pulsar -> Delta Table.
"""

import sys
import time
import json
import uuid
from typing import Optional

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from src.telemetry import init_telemetry, shutdown_telemetry, get_tracer
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
)


# --- App Initialization ---

def create_app():
    """Initialize all components and return the orchestrator agent."""
    tracer, meter, logger = init_telemetry()
    logger.info("app.starting")

    app_metrics = AppMetrics(meter)
    rag = RAGRetriever(app_metrics)
    tools = BuiltinTools(app_metrics)
    http_tool = HTTPTool(app_metrics)
    mcp_client = MCPToolClient(app_metrics)
    llm = LLMClient(app_metrics)

    research_agent = ResearchAgent(llm, rag, app_metrics)
    flight_agent = FlightAgent(llm, tools, app_metrics)
    hotel_agent = HotelAgent(llm, tools, app_metrics)
    itinerary_agent = ItineraryAgent(llm, tools, app_metrics)

    orchestrator = OrchestratorAgent(
        llm=llm,
        research_agent=research_agent,
        flight_agent=flight_agent,
        hotel_agent=hotel_agent,
        itinerary_agent=itinerary_agent,
        app_metrics=app_metrics,
    )

    logger.info("app.ready")
    return orchestrator, logger, llm


# --- Chat Session Logic ---

def show_banner():
    print("\n" + "=" * 60)
    print("   TRAVEL AGENT AI - Chat Interface")
    print("=" * 60)
    print("  Commands:")
    print("    /new           - Start a new session")
    print("    /sessions      - List previous sessions")
    print("    /resume <id>   - Resume a previous session")
    print("    /history       - Show current session history")
    print("    /end           - End current session")
    print("    /quit          - End session and exit")
    print("=" * 60)


def show_sessions():
    sessions = list_sessions(limit=15)
    if not sessions:
        print("\n  No previous sessions found.\n")
        return
    print(f"\n  {'#':<4} {'Status':<8} {'Turns':<6} {'Title':<30} {'Session ID'}")
    print(f"  {'-'*4} {'-'*8} {'-'*6} {'-'*30} {'-'*36}")
    for i, s in enumerate(sessions, 1):
        status = s["status"]
        turns = s["total_turns"]
        title = s["title"][:28]
        sid = s["session_id"][:8] + "..."
        print(f"  {i:<4} {status:<8} {turns:<6} {title:<30} {s['session_id']}")
    print()


def show_history(session_id: str):
    history = get_session_history(session_id)
    if not history:
        print("\n  No messages in this session yet.\n")
        return
    print(f"\n  Session history ({len(history)} messages):")
    print(f"  {'-' * 55}")
    for msg in history:
        role = msg["role"].upper()
        content = msg["content"]
        turn = msg["turn_number"]
        # Truncate long assistant messages for display
        if len(content) > 200:
            content = content[:200] + "..."
        print(f"  [Turn {turn}] {role}: {content}")
    print()


def run_session_turn(orchestrator, session_id: str, turn: int, tracer) -> dict | None:
    """Run one turn of the travel planning conversation."""
    print()
    user_input = input("  You: ").strip()

    if not user_input:
        return None
    if user_input.startswith("/"):
        return {"command": user_input}

    # Parse the user input as a destination query
    destination = user_input

    with tracer.start_as_current_span(f"session.turn.{turn}") as turn_span:
        turn_span.set_attribute("orchestration.type", "session_turn")
        turn_span.set_attribute("session.id", session_id)
        turn_span.set_attribute("session.turn.number", turn)
        turn_span.set_attribute("agent.parameter.destination", destination)

        # Ask follow-up details
        print(f"\n  Planning trip to: {destination}")
        origin = input("  Departing from (default: New York): ").strip() or "New York"
        dep_date = input("  Departure date (default: 2026-07-15): ").strip() or "2026-07-15"
        ret_date = input("  Return date (default: 2026-07-22): ").strip() or "2026-07-22"
        passengers_str = input("  Passengers (default: 1): ").strip() or "1"
        interests_str = input("  Interests (comma-separated, default: sightseeing): ").strip() or "sightseeing"
        budget = input("  Budget (budget/mid-range/luxury, default: mid-range): ").strip() or "mid-range"

        passengers = int(passengers_str) if passengers_str.isdigit() else 1
        interests = [i.strip() for i in interests_str.split(",")]

        turn_span.set_attribute("agent.parameter.origin", origin)
        turn_span.set_attribute("agent.parameter.departure_date", dep_date)
        turn_span.set_attribute("agent.parameter.return_date", ret_date)
        turn_span.set_attribute("agent.parameter.passengers", passengers)

        # Save user message
        user_msg = f"Plan trip to {destination} from {origin}, {dep_date} to {ret_date}, {passengers} pax, interests: {interests_str}, budget: {budget}"
        add_message(session_id, turn, "user", user_msg, metadata={
            "destination": destination, "origin": origin,
            "departure_date": dep_date, "return_date": ret_date,
            "passengers": passengers, "interests": interests, "budget": budget,
        })

        print(f"\n  Thinking...")
        print(f"  (Orchestrator -> Research -> Flights -> Hotels -> Itinerary)\n")

        result = orchestrator.plan_trip(
            destination=destination,
            origin=origin,
            departure_date=dep_date,
            return_date=ret_date,
            passengers=passengers,
            interests=interests,
            budget=budget,
        )

        if result["status"] == "success":
            turn_span.set_status(StatusCode.OK)

            # Build assistant response
            assistant_msg = (
                f"Trip to {destination} planned!\n\n"
                f"Duration: {result['duration_days']} days | Workflow: {result['workflow_duration_ms']}ms\n\n"
                f"--- Research ---\n{result['research'][:500]}\n\n"
                f"--- Flights ---\n{result['flights'][:500]}\n\n"
                f"--- Hotels ---\n{result['hotels'][:500]}\n\n"
                f"--- Itinerary ---\n{result['itinerary'][:800]}"
            )

            # Save assistant message
            add_message(session_id, turn, "assistant", assistant_msg, metadata={
                "status": "success", "workflow_duration_ms": result["workflow_duration_ms"],
            })

            # Display
            print(f"  {'=' * 55}")
            print(f"  Assistant: Trip to {destination.upper()} planned!")
            print(f"  {'=' * 55}")
            print(f"  Duration: {result['duration_days']} days")
            print(f"  Workflow time: {result['workflow_duration_ms']}ms")
            print(f"\n  {'-' * 20} Research {'-' * 20}")
            print(f"  {result['research'][:400]}")
            print(f"\n  {'-' * 20} Flights {'-' * 20}")
            print(f"  {result['flights'][:400]}")
            print(f"\n  {'-' * 20} Hotels {'-' * 20}")
            print(f"  {result['hotels'][:400]}")
            print(f"\n  {'-' * 20} Itinerary {'-' * 20}")
            print(f"  {result['itinerary'][:600]}")
        else:
            turn_span.set_status(StatusCode.ERROR, result.get("error", "unknown"))
            error_msg = f"Trip planning failed: {result.get('error')}"
            add_message(session_id, turn, "assistant", error_msg, metadata={"status": "error"})
            print(f"\n  Assistant: [ERROR] {error_msg}")
            print("  Make sure Ollama is running: `ollama serve`")
            print("  Make sure model is pulled: `ollama pull llama3.2`")

    return {"turn": turn}


def run_chat():
    """Main chat loop with session management."""
    orchestrator, logger, llm = create_app()
    tracer = get_tracer()

    show_banner()

    # State
    current_session = None
    session_span = None
    turn = 0

    try:
        while True:
            # If no active session, prompt to start or resume
            if current_session is None:
                print("\n  No active session. Start a new one or resume an existing one.")
                print("  Type /new, /sessions, /resume <id>, or /quit\n")
                cmd = input("  > ").strip()

                if cmd == "/quit":
                    break
                elif cmd == "/new":
                    current_session = create_session()
                    turn = 0
                    session_span = tracer.start_span("session")
                    session_span.set_attribute("session.id", current_session["session_id"])
                    session_span.set_attribute("session.type", "interactive")
                    session_span.set_attribute("app.name", "travel-agent-ai")
                    print(f"\n  New session started: {current_session['session_id']}")
                    print(f"  Title: {current_session['title']}")
                    print(f"\n  Type your destination to plan a trip. Use /end to end session.\n")
                elif cmd == "/sessions":
                    show_sessions()
                elif cmd.startswith("/resume"):
                    parts = cmd.split(maxsplit=1)
                    if len(parts) < 2:
                        print("  Usage: /resume <session_id>")
                        continue
                    sid = parts[1].strip()
                    resumed = resume_session(sid)
                    if resumed:
                        current_session = resumed
                        turn = resumed.get("total_turns", 0)
                        session_span = tracer.start_span("session")
                        session_span.set_attribute("session.id", current_session["session_id"])
                        session_span.set_attribute("session.type", "resumed")
                        session_span.set_attribute("session.resumed_at_turn", turn)
                        session_span.set_attribute("app.name", "travel-agent-ai")
                        print(f"\n  Resumed session: {sid}")
                        print(f"  Title: {current_session['title']} | Turns so far: {turn}")
                        # Show recent history for context
                        history = get_session_history(sid)
                        if history:
                            print(f"\n  Last messages:")
                            for msg in history[-4:]:
                                role = msg["role"].upper()
                                content = msg["content"][:150]
                                print(f"    [{msg['turn_number']}] {role}: {content}...")
                        print(f"\n  Continue the conversation. Use /end to end session.\n")
                    else:
                        print(f"  Session '{sid}' not found.")
                else:
                    if cmd and not cmd.startswith("/"):
                        print("  No active session. Use /new to start one.")
                continue

            # Active session — run chat turn
            with trace.use_span(session_span, end_on_exit=False):
                result = run_session_turn(orchestrator, current_session["session_id"], turn + 1, tracer)

            if result is None:
                continue

            if "command" in result:
                cmd = result["command"]
                if cmd == "/end":
                    session_span.set_attribute("session.turns", turn)
                    session_span.set_status(StatusCode.OK)
                    session_span.end()
                    end_session(current_session["session_id"])
                    print(f"\n  Session ended. ({turn} turns)")
                    current_session = None
                    session_span = None
                elif cmd == "/history":
                    show_history(current_session["session_id"])
                elif cmd == "/sessions":
                    show_sessions()
                elif cmd == "/quit":
                    session_span.set_attribute("session.turns", turn)
                    session_span.set_status(StatusCode.OK)
                    session_span.end()
                    end_session(current_session["session_id"])
                    print(f"\n  Session ended. ({turn} turns)")
                    break
                elif cmd == "/new":
                    # End current and start new
                    session_span.set_attribute("session.turns", turn)
                    session_span.set_status(StatusCode.OK)
                    session_span.end()
                    end_session(current_session["session_id"])
                    current_session = create_session()
                    turn = 0
                    session_span = tracer.start_span("session")
                    session_span.set_attribute("session.id", current_session["session_id"])
                    session_span.set_attribute("session.type", "interactive")
                    session_span.set_attribute("app.name", "travel-agent-ai")
                    print(f"\n  Previous session ended. New session: {current_session['session_id']}\n")
                else:
                    print(f"  Unknown command: {cmd}")
            else:
                turn = result["turn"]

    except (KeyboardInterrupt, EOFError):
        print("\n\n  Interrupted.")
        if current_session and session_span:
            session_span.set_attribute("session.turns", turn)
            session_span.set_status(StatusCode.OK)
            session_span.end()
            end_session(current_session["session_id"])

    # Flush all telemetry
    shutdown_telemetry()
    print("\n  All telemetry flushed. Goodbye.")


if __name__ == "__main__":
    run_chat()
