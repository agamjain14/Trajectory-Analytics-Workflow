"""
Travel Agent System - Multi-agent orchestration.
Each agent is a specialist that handles a specific aspect of travel planning.
Agent-to-agent communication is fully traced with OpenTelemetry.
"""

import time
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field

from opentelemetry import trace
from opentelemetry.trace import StatusCode

from src.telemetry import get_tracer, get_logger
from src.metrics import AppMetrics
from src.llm_client import LLMClient
from src.rag import RAGRetriever
from src.tools import BuiltinTools, HTTPTool, MCPToolClient


@dataclass
class AgentMessage:
    """Message passed between agents."""
    from_agent: str
    to_agent: str
    content: str
    context: Dict[str, Any] = field(default_factory=dict)
    tool_results: List[Dict[str, Any]] = field(default_factory=list)


class BaseAgent:
    """Base class for all travel agents with common tracing."""

    def __init__(self, name: str, llm: LLMClient, app_metrics: AppMetrics):
        self.name = name
        self.llm = llm
        self.metrics = app_metrics

    def _create_span_name(self, operation: str) -> str:
        return f"agent.{self.name}.{operation}"


class ResearchAgent(BaseAgent):
    """
    Researches destinations using RAG retrieval.
    Provides destination info, travel tips, and cultural context.
    """

    def __init__(self, llm: LLMClient, rag: RAGRetriever, app_metrics: AppMetrics):
        super().__init__("research_agent", llm, app_metrics)
        self.rag = rag

    def research_destination(self, destination: str, interests: List[str] = None) -> AgentMessage:
        """Research a destination using RAG + LLM reasoning."""
        with get_tracer().start_as_current_span(self._create_span_name("research_destination")) as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("agent.destination", destination)
            span.set_attribute("agent.interests", str(interests or []))

            # Step 1: RAG retrieval
            query = f"{destination} travel guide tips"
            if interests:
                query += " " + " ".join(interests)

            docs = self.rag.retrieve(query, n_results=3)
            context_text = "\n".join([doc["text"] for doc in docs])

            # Step 2: LLM reasoning over retrieved context
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a travel research specialist. Use the provided context to give "
                        "detailed, helpful information about the destination. Include practical tips, "
                        "best times to visit, budget estimates, and must-see attractions."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Research destination: {destination}\n"
                        f"Traveler interests: {interests or 'general'}\n\n"
                        f"Context from knowledge base:\n{context_text}\n\n"
                        f"Provide a comprehensive research summary for this destination."
                    ),
                },
            ]

            response = self.llm.chat(messages, agent_name=self.name)

            span.set_attribute("agent.rag_docs_used", len(docs))
            span.set_status(StatusCode.OK)

            get_logger().info(
                "agent.research_complete",
                agent=self.name,
                destination=destination,
                docs_retrieved=len(docs),
            )

            return AgentMessage(
                from_agent=self.name,
                to_agent="orchestrator",
                content=response["content"],
                context={"destination": destination, "sources": [d["id"] for d in docs]},
            )


class FlightAgent(BaseAgent):
    """
    Searches for flights using tool calls.
    Handles flight comparisons and booking recommendations.
    """

    def __init__(self, llm: LLMClient, tools: BuiltinTools, app_metrics: AppMetrics):
        super().__init__("flight_agent", llm, app_metrics)
        self.tools = tools

    def search_flights(
        self, origin: str, destination: str, date: str, passengers: int = 1
    ) -> AgentMessage:
        """Search flights and provide recommendations."""
        with get_tracer().start_as_current_span(self._create_span_name("search_flights")) as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("flight.origin", origin)
            span.set_attribute("flight.destination", destination)
            span.set_attribute("flight.date", date)

            # Step 1: Call flight search tool
            flight_results = self.tools.search_flights(origin, destination, date, passengers)

            # Step 2: LLM analyzes results and provides recommendation
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a flight booking specialist. Analyze the flight options and "
                        "provide a clear recommendation considering price, duration, and convenience. "
                        "Present options in a traveler-friendly format."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Find flights from {origin} to {destination} on {date} for {passengers} passenger(s).\n\n"
                        f"Available flights:\n{json.dumps(flight_results['flights'], indent=2)}\n\n"
                        f"Analyze these options and recommend the best choice."
                    ),
                },
            ]

            response = self.llm.chat(messages, agent_name=self.name)
            span.set_status(StatusCode.OK)

            get_logger().info(
                "agent.flights_searched",
                agent=self.name,
                origin=origin,
                destination=destination,
                options=len(flight_results["flights"]),
            )

            return AgentMessage(
                from_agent=self.name,
                to_agent="orchestrator",
                content=response["content"],
                context={"origin": origin, "destination": destination, "date": date},
                tool_results=[flight_results],
            )


class HotelAgent(BaseAgent):
    """
    Searches for hotels and accommodations.
    Considers budget, preferences, and location.
    """

    def __init__(self, llm: LLMClient, tools: BuiltinTools, app_metrics: AppMetrics):
        super().__init__("hotel_agent", llm, app_metrics)
        self.tools = tools

    def search_hotels(
        self, city: str, checkin: str, checkout: str, guests: int = 1, budget: str = "mid-range"
    ) -> AgentMessage:
        """Search hotels and provide recommendations."""
        with get_tracer().start_as_current_span(self._create_span_name("search_hotels")) as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("hotel.city", city)
            span.set_attribute("hotel.checkin", checkin)
            span.set_attribute("hotel.checkout", checkout)
            span.set_attribute("hotel.budget", budget)

            # Step 1: Call hotel search tool
            hotel_results = self.tools.search_hotels(city, checkin, checkout, guests)

            # Step 2: Get weather for the stay
            weather = self.tools.get_weather(city, checkin)

            # Step 3: LLM recommendation
            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are a hotel and accommodation specialist. Recommend the best option "
                        "based on the traveler's budget, amenity preferences, and location needs. "
                        "Consider weather conditions for relevant suggestions."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Find hotels in {city} from {checkin} to {checkout} for {guests} guest(s).\n"
                        f"Budget preference: {budget}\n\n"
                        f"Available hotels:\n{json.dumps(hotel_results['hotels'], indent=2)}\n\n"
                        f"Weather forecast:\n{json.dumps(weather, indent=2)}\n\n"
                        f"Recommend the best option and explain why."
                    ),
                },
            ]

            response = self.llm.chat(messages, agent_name=self.name)
            span.set_status(StatusCode.OK)

            get_logger().info(
                "agent.hotels_searched",
                agent=self.name,
                city=city,
                options=len(hotel_results["hotels"]),
            )

            return AgentMessage(
                from_agent=self.name,
                to_agent="orchestrator",
                content=response["content"],
                context={"city": city, "checkin": checkin, "checkout": checkout},
                tool_results=[hotel_results, weather],
            )


class ItineraryAgent(BaseAgent):
    """
    Creates day-by-day travel itineraries.
    Synthesizes research, flights, hotels, and activities into a cohesive plan.
    """

    def __init__(self, llm: LLMClient, tools: BuiltinTools, app_metrics: AppMetrics):
        super().__init__("itinerary_agent", llm, app_metrics)
        self.tools = tools

    def create_itinerary(
        self,
        destination: str,
        duration_days: int,
        research: AgentMessage,
        flights: AgentMessage,
        hotels: AgentMessage,
        interests: List[str] = None,
    ) -> AgentMessage:
        """Create a complete itinerary from all gathered information."""
        with get_tracer().start_as_current_span(self._create_span_name("create_itinerary")) as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("itinerary.destination", destination)
            span.set_attribute("itinerary.duration_days", duration_days)

            # Get currency info for budget
            currency_info = self.tools.currency_convert(1000, "USD", "EUR")

            messages = [
                {
                    "role": "system",
                    "content": (
                        "You are an expert travel itinerary planner. Create a detailed day-by-day "
                        "itinerary that combines all the research, flight, and hotel information. "
                        "Include specific activities, time estimates, and practical tips for each day. "
                        "Make it exciting and well-organized."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Create a {duration_days}-day itinerary for {destination}.\n"
                        f"Traveler interests: {interests or 'general sightseeing'}\n\n"
                        f"--- Destination Research ---\n{research.content}\n\n"
                        f"--- Flight Info ---\n{flights.content}\n\n"
                        f"--- Hotel Info ---\n{hotels.content}\n\n"
                        f"--- Currency ---\n$1000 USD = €{currency_info['converted']} EUR\n\n"
                        f"Create a complete day-by-day itinerary with morning, afternoon, and evening activities."
                    ),
                },
            ]

            response = self.llm.chat(messages, agent_name=self.name)
            span.set_status(StatusCode.OK)

            get_logger().info(
                "agent.itinerary_created",
                agent=self.name,
                destination=destination,
                days=duration_days,
            )

            return AgentMessage(
                from_agent=self.name,
                to_agent="orchestrator",
                content=response["content"],
                context={
                    "destination": destination,
                    "duration_days": duration_days,
                    "interests": interests,
                },
            )


class OrchestratorAgent(BaseAgent):
    """
    Main orchestrator that coordinates all specialist agents.
    Routes user requests, manages agent-to-agent communication, and synthesizes results.
    """

    def __init__(
        self,
        llm: LLMClient,
        research_agent: ResearchAgent,
        flight_agent: FlightAgent,
        hotel_agent: HotelAgent,
        itinerary_agent: ItineraryAgent,
        app_metrics: AppMetrics,
    ):
        super().__init__("orchestrator", llm, app_metrics)
        self.research_agent = research_agent
        self.flight_agent = flight_agent
        self.hotel_agent = hotel_agent
        self.itinerary_agent = itinerary_agent

    def plan_trip(
        self,
        destination: str,
        origin: str = "New York",
        departure_date: str = "2026-07-15",
        return_date: str = "2026-07-22",
        passengers: int = 1,
        interests: List[str] = None,
        budget: str = "mid-range",
    ) -> Dict[str, Any]:
        """
        Full trip planning workflow: research -> flights -> hotels -> itinerary.
        This is the main entry point that orchestrates all agents.
        """
        with get_tracer().start_as_current_span("agent.orchestrator.plan_trip") as span:
            span.set_attribute("agent.name", self.name)
            span.set_attribute("trip.destination", destination)
            span.set_attribute("trip.origin", origin)
            span.set_attribute("trip.departure_date", departure_date)
            span.set_attribute("trip.return_date", return_date)
            span.set_attribute("trip.passengers", passengers)

            self.metrics.agent_active.add(1, {"agent": self.name})
            workflow_start = time.time()

            try:
                # Step 1: Research destination
                get_logger().info("orchestrator.step", step="research", destination=destination)
                self.metrics.record_agent_handoff(self.name, "research_agent")
                research_result = self.research_agent.research_destination(
                    destination, interests
                )

                # Step 2: Search flights
                get_logger().info("orchestrator.step", step="flights", origin=origin, destination=destination)
                self.metrics.record_agent_handoff(self.name, "flight_agent")
                flight_result = self.flight_agent.search_flights(
                    origin, destination, departure_date, passengers
                )

                # Step 3: Search hotels
                get_logger().info("orchestrator.step", step="hotels", city=destination)
                self.metrics.record_agent_handoff(self.name, "hotel_agent")
                hotel_result = self.hotel_agent.search_hotels(
                    destination, departure_date, return_date, passengers, budget
                )

                # Step 4: Create itinerary
                # Calculate duration
                from datetime import datetime
                dep = datetime.strptime(departure_date, "%Y-%m-%d")
                ret = datetime.strptime(return_date, "%Y-%m-%d")
                duration_days = (ret - dep).days

                get_logger().info("orchestrator.step", step="itinerary", days=duration_days)
                self.metrics.record_agent_handoff(self.name, "itinerary_agent")
                itinerary_result = self.itinerary_agent.create_itinerary(
                    destination=destination,
                    duration_days=duration_days,
                    research=research_result,
                    flights=flight_result,
                    hotels=hotel_result,
                    interests=interests,
                )

                # Step 5: Final synthesis
                workflow_duration = time.time() - workflow_start
                self.metrics.workflow_duration.record(
                    workflow_duration, {"workflow": "plan_trip"}
                )
                self.metrics.workflow_requests_total.add(
                    1, {"workflow": "plan_trip", "status": "success"}
                )

                span.set_attribute("workflow.duration_ms", workflow_duration * 1000)
                span.set_attribute("workflow.agents_used", 4)
                span.set_status(StatusCode.OK)

                get_logger().info(
                    "orchestrator.trip_planned",
                    destination=destination,
                    duration_ms=round(workflow_duration * 1000, 2),
                    agents_used=4,
                )

                return {
                    "status": "success",
                    "destination": destination,
                    "duration_days": duration_days,
                    "research": research_result.content,
                    "flights": flight_result.content,
                    "hotels": hotel_result.content,
                    "itinerary": itinerary_result.content,
                    "workflow_duration_ms": round(workflow_duration * 1000, 2),
                }

            except Exception as e:
                workflow_duration = time.time() - workflow_start
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                self.metrics.workflow_requests_total.add(
                    1, {"workflow": "plan_trip", "status": "error"}
                )
                get_logger().error(
                    "orchestrator.trip_planning_failed",
                    destination=destination,
                    error=str(e),
                    duration_ms=round(workflow_duration * 1000, 2),
                )
                return {"status": "error", "error": str(e)}

            finally:
                self.metrics.agent_active.add(-1, {"agent": self.name})
