"""
MCP Tool Server for Travel Agent.
A minimal FastAPI server that exposes travel tools via Model Context Protocol.
This simulates an external MCP server that agents can call.
"""

import time
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Travel MCP Tool Server", version="0.1.0")


# --- Models ---

class ToolInvokeRequest(BaseModel):
    arguments: Dict[str, Any]


# --- Tool Registry ---

TOOLS = {
    "search_flights": {
        "name": "search_flights",
        "description": "Search for available flights between two cities",
        "parameters": {
            "origin": {"type": "string", "description": "Departure city/airport code"},
            "destination": {"type": "string", "description": "Arrival city/airport code"},
            "date": {"type": "string", "description": "Travel date (YYYY-MM-DD)"},
            "passengers": {"type": "integer", "description": "Number of passengers", "default": 1},
        },
    },
    "search_hotels": {
        "name": "search_hotels",
        "description": "Search for hotels in a city",
        "parameters": {
            "city": {"type": "string", "description": "City name"},
            "checkin": {"type": "string", "description": "Check-in date (YYYY-MM-DD)"},
            "checkout": {"type": "string", "description": "Check-out date (YYYY-MM-DD)"},
            "guests": {"type": "integer", "description": "Number of guests", "default": 1},
        },
    },
    "get_weather": {
        "name": "get_weather",
        "description": "Get weather forecast for a city",
        "parameters": {
            "city": {"type": "string", "description": "City name"},
            "date": {"type": "string", "description": "Date for forecast (YYYY-MM-DD)"},
        },
    },
    "get_visa_info": {
        "name": "get_visa_info",
        "description": "Check visa requirements for a nationality traveling to a destination",
        "parameters": {
            "nationality": {"type": "string", "description": "Traveler's nationality"},
            "destination": {"type": "string", "description": "Destination country"},
        },
    },
    "currency_convert": {
        "name": "currency_convert",
        "description": "Convert currency amounts",
        "parameters": {
            "amount": {"type": "number", "description": "Amount to convert"},
            "from_currency": {"type": "string", "description": "Source currency code"},
            "to_currency": {"type": "string", "description": "Target currency code"},
        },
    },
}


# --- Tool Implementations ---

def _search_flights(args: Dict) -> Dict:
    return {
        "flights": [
            {"airline": "SkyWay Airlines", "flight_number": "SW-142", "departure": "08:30", "arrival": "14:45", "price": 450.00, "stops": 0},
            {"airline": "Global Air", "flight_number": "GA-891", "departure": "11:00", "arrival": "18:30", "price": 380.00, "stops": 1},
            {"airline": "SkyWay Airlines", "flight_number": "SW-200", "departure": "16:00", "arrival": "22:15", "price": 520.00, "stops": 0},
        ]
    }


def _search_hotels(args: Dict) -> Dict:
    return {
        "hotels": [
            {"name": "Grand Plaza Hotel", "rating": 4.5, "price_per_night": 180.00, "amenities": ["wifi", "pool", "breakfast"]},
            {"name": "Cozy Boutique Inn", "rating": 4.8, "price_per_night": 220.00, "amenities": ["wifi", "breakfast", "rooftop bar"]},
            {"name": "Budget Traveler Hostel", "rating": 4.2, "price_per_night": 45.00, "amenities": ["wifi", "shared kitchen"]},
        ]
    }


def _get_weather(args: Dict) -> Dict:
    return {
        "city": args.get("city"),
        "temperature_high": 26,
        "temperature_low": 18,
        "condition": "partly cloudy",
        "rain_chance": 20,
    }


def _get_visa_info(args: Dict) -> Dict:
    return {
        "visa_required": True,
        "visa_type": "tourist",
        "processing_time": "5-10 business days",
        "validity": "90 days",
    }


def _currency_convert(args: Dict) -> Dict:
    rates = {"USD_EUR": 0.92, "USD_JPY": 149.5, "USD_GBP": 0.79, "EUR_USD": 1.09}
    key = f"{args['from_currency']}_{args['to_currency']}"
    rate = rates.get(key, 1.0)
    return {"converted": round(args["amount"] * rate, 2), "rate": rate}


TOOL_HANDLERS = {
    "search_flights": _search_flights,
    "search_hotels": _search_hotels,
    "get_weather": _get_weather,
    "get_visa_info": _get_visa_info,
    "currency_convert": _currency_convert,
}


# --- API Endpoints ---

@app.get("/tools")
def list_tools():
    """List all available tools (MCP tools/list)."""
    return {"tools": list(TOOLS.values())}


@app.post("/tools/{tool_name}/invoke")
def invoke_tool(tool_name: str, request: ToolInvokeRequest):
    """Invoke a tool by name (MCP tools/call)."""
    if tool_name not in TOOL_HANDLERS:
        raise HTTPException(status_code=404, detail=f"Tool '{tool_name}' not found")

    handler = TOOL_HANDLERS[tool_name]
    result = handler(request.arguments)
    return {"result": result, "tool": tool_name, "timestamp": time.time()}


@app.get("/health")
def health():
    return {"status": "healthy", "tools_available": len(TOOLS)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
