"""
Main entrypoint for the Travel Agent AI Application.
Demonstrates the full workflow: RAG retrieval, tool calls, agent-to-agent orchestration,
LLM reasoning via Ollama, retry logic, and comprehensive OpenTelemetry instrumentation.
"""

import sys
import time
import json
from typing import Optional

from src.telemetry import init_telemetry
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


def create_app():
    """Initialize all components and return the orchestrator agent."""
    # Step 1: Initialize telemetry (traces, metrics, logs)
    tracer, meter, logger = init_telemetry()
    logger.info("app.starting")

    # Step 2: Initialize metrics
    app_metrics = AppMetrics(meter)

    # Step 3: Initialize RAG retriever
    rag = RAGRetriever(app_metrics)

    # Step 4: Initialize tools
    tools = BuiltinTools(app_metrics)
    http_tool = HTTPTool(app_metrics)
    mcp_client = MCPToolClient(app_metrics)

    # Step 5: Initialize LLM client (Ollama)
    llm = LLMClient(app_metrics)

    # Step 6: Initialize specialist agents
    research_agent = ResearchAgent(llm, rag, app_metrics)
    flight_agent = FlightAgent(llm, tools, app_metrics)
    hotel_agent = HotelAgent(llm, tools, app_metrics)
    itinerary_agent = ItineraryAgent(llm, tools, app_metrics)

    # Step 7: Initialize orchestrator
    orchestrator = OrchestratorAgent(
        llm=llm,
        research_agent=research_agent,
        flight_agent=flight_agent,
        hotel_agent=hotel_agent,
        itinerary_agent=itinerary_agent,
        app_metrics=app_metrics,
    )

    logger.info("app.ready")
    return orchestrator, logger, http_tool, mcp_client


def demo_mcp_tool_call(mcp_client: MCPToolClient, logger):
    """Demonstrate an MCP tool server call (will fail gracefully if server not running)."""
    logger.info("demo.mcp_tool_call", description="Attempting MCP server connection")
    tools_list = mcp_client.list_tools()
    if tools_list.get("tools"):
        logger.info("demo.mcp_tools_available", tools=len(tools_list["tools"]))
        result = mcp_client.invoke_tool("get_weather", {"city": "Paris", "date": "2026-07-15"})
        logger.info("demo.mcp_result", result=result)
    else:
        logger.warning("demo.mcp_server_unavailable", message="MCP server not running, using builtin tools")


def demo_http_tool_call(http_tool: HTTPTool, logger):
    """Demonstrate an HTTP tool call to a public API."""
    logger.info("demo.http_tool_call", description="Calling public API")
    result = http_tool.call("https://httpbin.org/json", method="GET")
    if not result.get("error"):
        logger.info("demo.http_success", response_keys=list(result.get("data", {}).keys())[:3])
    else:
        logger.warning("demo.http_failed", result=result)


def run_travel_planning():
    """Run the interactive multi-turn travel planning application."""
    orchestrator, logger, http_tool, mcp_client = create_app()

    print("\n" + "=" * 70)
    print("  🌍 TRAVEL AGENT AI - Trajectory Analytics Demo")
    print("=" * 70)
    print("\nThis demo showcases:")
    print("  • Multi-agent orchestration (Orchestrator → Research/Flight/Hotel/Itinerary)")
    print("  • RAG retrieval from travel knowledge base")
    print("  • Tool calls (HTTP APIs + MCP protocol)")
    print("  • LLM reasoning via Ollama (local model)")
    print("  • Retry logic with exponential backoff")
    print("  • Full OpenTelemetry instrumentation (traces, metrics, logs)")
    print("=" * 70)

    # Initial demos on startup
    print("\n📡 [Init] HTTP Tool Call Demo...")
    demo_http_tool_call(http_tool, logger)

    print("\n🔌 [Init] MCP Tool Server Call Demo...")
    demo_mcp_tool_call(mcp_client, logger)

    # Interactive loop
    print("\n" + "=" * 70)
    print("  🗺️  INTERACTIVE TRAVEL PLANNER")
    print("  Type a destination to plan a trip, or 'quit' to exit.")
    print("  Each request generates traces, metrics, and logs.")
    print("=" * 70)

    turn = 0
    while True:
        print()
        destination = input("🌍 Where would you like to travel? → ").strip()
        if destination.lower() in ("quit", "exit", "q"):
            print("\n👋 Goodbye! Check your telemetry backends for the traces.")
            break
        if not destination:
            continue

        turn += 1
        print(f"\n{'─' * 50}")
        print(f"  Turn {turn}: Planning trip to {destination}")
        print(f"{'─' * 50}")

        # Gather optional parameters
        origin = input("  ✈️  Departing from (default: New York): ").strip() or "New York"
        dep_date = input("  📅 Departure date (default: 2026-07-15): ").strip() or "2026-07-15"
        ret_date = input("  📅 Return date (default: 2026-07-22): ").strip() or "2026-07-22"
        passengers_str = input("  👥 Passengers (default: 1): ").strip() or "1"
        interests_str = input("  ❤️  Interests (comma-separated, default: sightseeing): ").strip() or "sightseeing"
        budget = input("  💰 Budget (budget/mid-range/luxury, default: mid-range): ").strip() or "mid-range"

        passengers = int(passengers_str) if passengers_str.isdigit() else 1
        interests = [i.strip() for i in interests_str.split(",")]

        print(f"\n⏳ Planning your trip to {destination}...")
        print(f"   (Orchestrator → Research → Flights → Hotels → Itinerary)\n")

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
            print(f"\n{'═' * 70}")
            print(f"  ✅ TRIP TO {destination.upper()} - COMPLETE (Turn {turn})")
            print(f"{'═' * 70}")
            print(f"\n📍 Destination: {result['destination']}")
            print(f"📅 Duration: {result['duration_days']} days")
            print(f"⏱️  Workflow time: {result['workflow_duration_ms']}ms")
            print(f"\n{'─' * 40} Research {'─' * 40}")
            print(result["research"][:600])
            print(f"\n{'─' * 40} Flights {'─' * 40}")
            print(result["flights"][:600])
            print(f"\n{'─' * 40} Hotels {'─' * 40}")
            print(result["hotels"][:600])
            print(f"\n{'─' * 40} Itinerary {'─' * 40}")
            print(result["itinerary"][:1200])
        else:
            print(f"\n❌ Trip planning failed: {result.get('error')}")
            print("   Make sure Ollama is running: `ollama serve`")
            print("   Make sure model is pulled: `ollama pull llama3.2`")

        print(f"\n📊 Telemetry emitted for turn {turn}:")
        print(f"   • Traces → localhost:16686 (Jaeger UI)")
        print(f"   • Metrics → localhost:9090 (Prometheus)")
        print(f"   • Logs → console + OTLP collector")


if __name__ == "__main__":
    run_travel_planning()
