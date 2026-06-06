"""
Tool execution module.
Implements HTTP tool calls and MCP (Model Context Protocol) tool server interactions.
All calls are instrumented with OpenTelemetry traces and metrics.
"""

import time
import json
from typing import Any, Dict, Optional

import requests
import httpx
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from src.telemetry import get_tracer, get_logger
from src.metrics import AppMetrics


class HTTPTool:
    """Executes HTTP-based tool calls with tracing."""

    def __init__(self, app_metrics: AppMetrics):
        self.metrics = app_metrics
        self.session = requests.Session()

    def call(
        self, url: str, method: str = "GET", payload: Optional[Dict] = None, headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Execute an HTTP tool call with full span instrumentation."""
        with get_tracer().start_as_current_span(f"HTTP {method.upper()}") as span:
            # OTel HTTP Client Semantic Convention attributes
            span.set_attribute("http.request.method", method.upper())
            span.set_attribute("url.full", url)
            # Extract server address from URL
            from urllib.parse import urlparse
            parsed = urlparse(url)
            span.set_attribute("server.address", parsed.hostname or "")
            span.set_attribute("server.port", parsed.port or (443 if parsed.scheme == "https" else 80))

            start_time = time.time()
            try:
                if method.upper() == "GET":
                    response = self.session.get(url, headers=headers, timeout=30)
                elif method.upper() == "POST":
                    response = self.session.post(
                        url, json=payload, headers=headers, timeout=30
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                duration = time.time() - start_time
                span.set_attribute("http.response.status_code", response.status_code)
                span.set_attribute("http.response.body.size", len(response.content))

                success = response.status_code < 400
                self.metrics.record_tool_call(
                    duration=duration, tool_name=f"http_{method.lower()}", success=success
                )

                if not success:
                    span.set_status(StatusCode.ERROR, f"HTTP {response.status_code}")
                    get_logger().warning(
                        "tool.http_error",
                        url=url,
                        status_code=response.status_code,
                    )
                    return {"error": True, "status_code": response.status_code, "body": response.text}

                span.set_status(StatusCode.OK)
                get_logger().info(
                    "tool.http_success",
                    url=url,
                    status_code=response.status_code,
                    duration_ms=round(duration * 1000, 2),
                )

                try:
                    return {"error": False, "data": response.json()}
                except json.JSONDecodeError:
                    return {"error": False, "data": response.text}

            except requests.exceptions.RequestException as e:
                duration = time.time() - start_time
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                self.metrics.record_tool_call(
                    duration=duration, tool_name=f"http_{method.lower()}", success=False
                )
                get_logger().error("tool.http_exception", url=url, error=str(e))
                return {"error": True, "exception": str(e)}


class MCPToolClient:
    """
    MCP (Model Context Protocol) tool client.
    Connects to a local MCP tool server and executes tool invocations.
    """

    def __init__(self, app_metrics: AppMetrics, server_url: Optional[str] = None):
        self.metrics = app_metrics
        self.server_url = server_url or "http://localhost:8001"
        self.client = httpx.Client(timeout=30.0)

    def list_tools(self) -> Dict[str, Any]:
        """List available tools from the MCP server."""
        with get_tracer().start_as_current_span("travel-tools/list_tools") as span:
            # OTel RPC Semantic Convention attributes
            span.set_attribute("rpc.system", "mcp")
            span.set_attribute("rpc.service", "travel-tools")
            span.set_attribute("rpc.method", "list_tools")
            from urllib.parse import urlparse
            parsed = urlparse(self.server_url)
            span.set_attribute("server.address", parsed.hostname or "localhost")
            span.set_attribute("server.port", parsed.port or 8001)

            try:
                response = self.client.get(f"{self.server_url}/tools")
                span.set_attribute("rpc.response.tools_count", len(response.json().get("tools", [])))
                return response.json()
            except Exception as e:
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                get_logger().warning("tool.mcp_list_failed", error=str(e))
                return {"tools": []}

    def invoke_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke a tool on the MCP server with tracing."""
        with get_tracer().start_as_current_span(f"travel-tools/{tool_name}") as span:
            # OTel RPC Semantic Convention attributes
            span.set_attribute("rpc.system", "mcp")
            span.set_attribute("rpc.service", "travel-tools")
            span.set_attribute("rpc.method", tool_name)
            from urllib.parse import urlparse
            parsed = urlparse(self.server_url)
            span.set_attribute("server.address", parsed.hostname or "localhost")
            span.set_attribute("server.port", parsed.port or 8001)

            start_time = time.time()
            try:
                response = self.client.post(
                    f"{self.server_url}/tools/{tool_name}/invoke",
                    json={"arguments": arguments},
                )
                duration = time.time() - start_time

                success = response.status_code < 400
                self.metrics.record_tool_call(
                    duration=duration, tool_name=f"mcp_{tool_name}", success=success
                )

                if success:
                    span.set_status(StatusCode.OK)
                    get_logger().info(
                        "tool.mcp_invoke_success",
                        tool=tool_name,
                        duration_ms=round(duration * 1000, 2),
                    )
                    return {"error": False, "result": response.json()}
                else:
                    span.set_status(StatusCode.ERROR, f"MCP error {response.status_code}")
                    return {"error": True, "status_code": response.status_code}

            except Exception as e:
                duration = time.time() - start_time
                span.set_status(StatusCode.ERROR, str(e))
                span.record_exception(e)
                self.metrics.record_tool_call(
                    duration=duration, tool_name=f"mcp_{tool_name}", success=False
                )
                get_logger().error("tool.mcp_invoke_failed", tool=tool_name, error=str(e))
                return {"error": True, "exception": str(e)}


# --- Built-in travel tools (simulated MCP tools) ---

class BuiltinTools:
    """Local travel tool implementations (simulates MCP tool server responses)."""

    def __init__(self, app_metrics: AppMetrics):
        self.metrics = app_metrics

    def search_flights(self, origin: str, destination: str, date: str, passengers: int = 1) -> Dict[str, Any]:
        """Simulated flight search tool."""
        with get_tracer().start_as_current_span("builtin/search_flights") as span:
            # OTel RPC Semantic Convention attributes (local tool as RPC)
            span.set_attribute("rpc.system", "tool")
            span.set_attribute("rpc.service", "builtin")
            span.set_attribute("rpc.method", "search_flights")
            span.set_attribute("tool.parameter.origin", origin)
            span.set_attribute("tool.parameter.destination", destination)
            span.set_attribute("tool.parameter.date", date)
            span.set_attribute("tool.parameter.passengers", passengers)

            start_time = time.time()
            # Simulated flight results
            result = {
                "origin": origin,
                "destination": destination,
                "date": date,
                "passengers": passengers,
                "flights": [
                    {
                        "airline": "SkyWay Airlines",
                        "flight_number": "SW-142",
                        "departure": "08:30",
                        "arrival": "14:45",
                        "duration": "6h 15m",
                        "price": 450.00,
                        "stops": 0,
                        "class": "economy",
                    },
                    {
                        "airline": "Global Air",
                        "flight_number": "GA-891",
                        "departure": "11:00",
                        "arrival": "18:30",
                        "duration": "7h 30m",
                        "price": 380.00,
                        "stops": 1,
                        "class": "economy",
                    },
                    {
                        "airline": "SkyWay Airlines",
                        "flight_number": "SW-200",
                        "departure": "16:00",
                        "arrival": "22:15",
                        "duration": "6h 15m",
                        "price": 520.00,
                        "stops": 0,
                        "class": "premium_economy",
                    },
                ],
            }
            duration = time.time() - start_time

            self.metrics.record_tool_call(duration=duration, tool_name="search_flights", success=True)
            span.set_status(StatusCode.OK)
            get_logger().info("tool.search_flights", origin=origin, destination=destination, results=len(result["flights"]))
            return result

    def search_hotels(self, city: str, checkin: str, checkout: str, guests: int = 1) -> Dict[str, Any]:
        """Simulated hotel search tool."""
        with get_tracer().start_as_current_span("builtin/search_hotels") as span:
            span.set_attribute("rpc.system", "tool")
            span.set_attribute("rpc.service", "builtin")
            span.set_attribute("rpc.method", "search_hotels")
            span.set_attribute("tool.parameter.city", city)
            span.set_attribute("tool.parameter.checkin", checkin)
            span.set_attribute("tool.parameter.checkout", checkout)
            span.set_attribute("tool.parameter.guests", guests)

            start_time = time.time()
            result = {
                "city": city,
                "checkin": checkin,
                "checkout": checkout,
                "guests": guests,
                "hotels": [
                    {
                        "name": "Grand Plaza Hotel",
                        "rating": 4.5,
                        "price_per_night": 180.00,
                        "amenities": ["wifi", "pool", "breakfast", "gym"],
                        "location": "city center",
                        "cancellation": "free until 24h before",
                    },
                    {
                        "name": "Cozy Boutique Inn",
                        "rating": 4.8,
                        "price_per_night": 220.00,
                        "amenities": ["wifi", "breakfast", "rooftop bar"],
                        "location": "old town",
                        "cancellation": "free until 48h before",
                    },
                    {
                        "name": "Budget Traveler Hostel",
                        "rating": 4.2,
                        "price_per_night": 45.00,
                        "amenities": ["wifi", "shared kitchen", "lounge"],
                        "location": "near train station",
                        "cancellation": "non-refundable",
                    },
                ],
            }
            duration = time.time() - start_time

            self.metrics.record_tool_call(duration=duration, tool_name="search_hotels", success=True)
            span.set_status(StatusCode.OK)
            get_logger().info("tool.search_hotels", city=city, results=len(result["hotels"]))
            return result

    def get_weather(self, city: str, date: str) -> Dict[str, Any]:
        """Simulated weather forecast tool."""
        with get_tracer().start_as_current_span("builtin/get_weather") as span:
            span.set_attribute("rpc.system", "tool")
            span.set_attribute("rpc.service", "builtin")
            span.set_attribute("rpc.method", "get_weather")
            span.set_attribute("tool.parameter.city", city)
            span.set_attribute("tool.parameter.date", date)

            start_time = time.time()
            result = {
                "city": city,
                "date": date,
                "temperature_high": 26,
                "temperature_low": 18,
                "condition": "partly cloudy",
                "humidity": 55,
                "rain_chance": 20,
                "wind_speed": "12 km/h",
                "recommendation": "Pack light layers and sunscreen",
            }
            duration = time.time() - start_time

            self.metrics.record_tool_call(duration=duration, tool_name="get_weather", success=True)
            span.set_status(StatusCode.OK)
            get_logger().info("tool.get_weather", city=city)
            return result

    def get_visa_info(self, nationality: str, destination: str) -> Dict[str, Any]:
        """Simulated visa requirement lookup tool."""
        with get_tracer().start_as_current_span("builtin/get_visa_info") as span:
            span.set_attribute("rpc.system", "tool")
            span.set_attribute("rpc.service", "builtin")
            span.set_attribute("rpc.method", "get_visa_info")
            span.set_attribute("tool.parameter.nationality", nationality)
            span.set_attribute("tool.parameter.destination", destination)

            start_time = time.time()
            result = {
                "nationality": nationality,
                "destination": destination,
                "visa_required": True,
                "visa_type": "tourist",
                "processing_time": "5-10 business days",
                "validity": "90 days",
                "documents_required": ["passport", "photo", "bank statement", "return ticket"],
            }
            duration = time.time() - start_time

            self.metrics.record_tool_call(duration=duration, tool_name="get_visa_info", success=True)
            span.set_status(StatusCode.OK)
            get_logger().info("tool.get_visa_info", nationality=nationality, destination=destination)
            return result

    def currency_convert(self, amount: float, from_currency: str, to_currency: str) -> Dict[str, Any]:
        """Simulated currency conversion tool."""
        with get_tracer().start_as_current_span("builtin/currency_convert") as span:
            span.set_attribute("rpc.system", "tool")
            span.set_attribute("rpc.service", "builtin")
            span.set_attribute("rpc.method", "currency_convert")
            span.set_attribute("tool.parameter.amount", amount)
            span.set_attribute("tool.parameter.from_currency", from_currency)
            span.set_attribute("tool.parameter.to_currency", to_currency)

            start_time = time.time()
            # Simulated rates
            rates = {"USD_EUR": 0.92, "USD_JPY": 149.5, "USD_GBP": 0.79, "USD_INR": 83.1, "EUR_USD": 1.09}
            key = f"{from_currency}_{to_currency}"
            rate = rates.get(key, 1.0)
            result = {
                "from": from_currency,
                "to": to_currency,
                "amount": amount,
                "converted": round(amount * rate, 2),
                "rate": rate,
            }
            duration = time.time() - start_time

            self.metrics.record_tool_call(duration=duration, tool_name="currency_convert", success=True)
            span.set_status(StatusCode.OK)
            return result
