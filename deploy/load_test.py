#!/usr/bin/env python3
"""
Load test for the Travel Agent AI Chat Server.
Simulates realistic multi-turn travel planning conversations to stress
the full agent pipeline (intent parsing, orchestration, sub-agents).

Usage:
    python3 deploy/load_test.py --url http://localhost:8000 --users 5 --duration 180
"""
import argparse
import asyncio
import time
import random
import httpx

# Realistic multi-turn travel planning conversations
CONVERSATIONS = [
    # Quick single-destination trip
    [
        "I want to plan a trip to Tokyo",
        "2 passengers, luxury budget",
        "We love sushi and anime, departing mid August from San Francisco",
    ],
    # Vague start that needs clarification
    [
        "Help me plan a vacation",
        "Bali",
        "From Mumbai, 2 weeks, mid-range budget, family of 4",
        "We're interested in beaches and temples",
    ],
    # Rapid-fire complete request
    [
        "Mumbai to Goa, 2 pax, luxury trip, week long, mid July",
    ],
    # Modification flow
    [
        "Plan a trip to Paris from London",
        "Actually change destination to Rome",
        "Make it luxury and add food tours",
    ],
    # Adventure trip
    [
        "I want an adventure trip to New Zealand",
        "Solo traveler, 10 days, leaving early September",
        "Bungee jumping, hiking, and skydiving",
    ],
    # Family vacation
    [
        "Family vacation to Orlando for the kids",
        "Family of 5, from Chicago, budget-friendly",
        "Departing December 20, returning January 2",
    ],
    # Honeymoon planning
    [
        "Plan a honeymoon trip",
        "Maldives",
        "From Delhi, 2 passengers, luxury, 5 days, late October",
        "Romantic dinners, snorkeling, spa",
    ],
    # European multi-city (complex)
    [
        "I want to explore Europe",
        "Barcelona",
        "From New York, solo, mid-range, 2 weeks in June",
        "Art, nightlife, and architecture",
    ],
    # Minimal input — let agent pick
    [
        "Surprise me with a trip suggestion",
        "just suggest something",
    ],
    # Business + leisure
    [
        "Trip to Singapore, flying from Sydney",
        "3 days business then 4 days leisure, mid-range",
        "I like street food and modern architecture",
    ],
    # Southeast Asia backpacking
    [
        "Backpacking trip to Thailand",
        "Solo, budget, 3 weeks from Bangkok, leaving November",
        "Temples, street food, diving in the islands",
    ],
    # Winter getaway
    [
        "I need a warm winter escape",
        "Cancun",
        "From Toronto, couple, all-inclusive luxury, 7 days, January",
    ],
]


async def simulate_user_session(
    client: httpx.AsyncClient, url: str, user_id: int, conversation: list[str]
) -> dict:
    """Simulate a single user's multi-turn travel planning session."""
    session_id = None
    results = []
    session_start = time.time()

    for turn_idx, message in enumerate(conversation):
        start = time.time()
        payload = {"message": message}
        if session_id:
            payload["session_id"] = session_id

        try:
            resp = await client.post(f"{url}/api/chat", json=payload, timeout=180.0)
            duration = time.time() - start
            status = resp.status_code

            if status == 200:
                data = resp.json()
                session_id = data.get("session_id", session_id)
                resp_status = data.get("status", "unknown")
                content_preview = data.get("content", "")[:80]
                print(
                    f"  [user-{user_id:02d} turn-{turn_idx+1}] "
                    f"{status} ({resp_status}) {duration:.1f}s — {content_preview}..."
                )
            else:
                print(f"  [user-{user_id:02d} turn-{turn_idx+1}] {status} in {duration:.1f}s")

            results.append({"turn": turn_idx + 1, "status": status, "duration": duration})
        except Exception as e:
            duration = time.time() - start
            print(f"  [user-{user_id:02d} turn-{turn_idx+1}] ERROR: {e} after {duration:.1f}s")
            results.append({"turn": turn_idx + 1, "status": 0, "duration": duration, "error": str(e)})

        # Simulate human think time between turns
        await asyncio.sleep(random.uniform(1.0, 4.0))

    total_session_time = time.time() - session_start
    return {
        "user_id": user_id,
        "turns": len(conversation),
        "results": results,
        "session_duration": total_session_time,
        "session_id": session_id,
    }


async def run_load_test(url: str, concurrent_users: int, duration_sec: int):
    """Run concurrent user sessions for the specified duration."""
    print(f"Travel Agent Load Test")
    print(f"  Target:     {url}")
    print(f"  Users:      {concurrent_users} concurrent")
    print(f"  Duration:   {duration_sec}s")
    print("=" * 70)

    all_sessions = []
    end_time = time.time() + duration_sec
    user_counter = 0

    async with httpx.AsyncClient() as client:
        while time.time() < end_time:
            # Launch a wave of concurrent user sessions
            tasks = []
            for _ in range(concurrent_users):
                user_counter += 1
                conversation = random.choice(CONVERSATIONS)
                print(f"\n[user-{user_counter:02d}] Starting session: \"{conversation[0][:50]}...\"")
                tasks.append(
                    simulate_user_session(client, url, user_counter, conversation)
                )

            wave_results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in wave_results:
                if isinstance(r, dict):
                    all_sessions.append(r)
                else:
                    print(f"  Session failed: {r}")

            # Pause between waves to simulate staggered arrivals
            remaining = end_time - time.time()
            if remaining > 0:
                await asyncio.sleep(min(random.uniform(3.0, 8.0), remaining))

    # --- Summary ---
    print("\n" + "=" * 70)
    print("LOAD TEST SUMMARY")
    print("=" * 70)

    total_sessions = len(all_sessions)
    total_turns = sum(s["turns"] for s in all_sessions)
    all_results = [r for s in all_sessions for r in s["results"]]
    total_requests = len(all_results)
    successful = sum(1 for r in all_results if r.get("status") == 200)
    failed = total_requests - successful
    durations = [r["duration"] for r in all_results if r.get("status") == 200]

    print(f"Sessions completed:  {total_sessions}")
    print(f"Total turns/requests:{total_turns}")
    print(f"Successful:          {successful}/{total_requests} ({100*successful/total_requests:.1f}%)" if total_requests else "")
    print(f"Failed:              {failed}")

    if durations:
        avg_dur = sum(durations) / len(durations)
        p50 = sorted(durations)[len(durations) // 2]
        p95 = sorted(durations)[int(len(durations) * 0.95)]
        p99 = sorted(durations)[int(len(durations) * 0.99)]
        print(f"Avg latency:         {avg_dur:.2f}s")
        print(f"P50 latency:         {p50:.2f}s")
        print(f"P95 latency:         {p95:.2f}s")
        print(f"P99 latency:         {p99:.2f}s")

    session_durations = [s["session_duration"] for s in all_sessions]
    if session_durations:
        print(f"Avg session time:    {sum(session_durations)/len(session_durations):.1f}s")

    print(f"Total test runtime:  {duration_sec}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Travel Agent AI load test")
    parser.add_argument("--url", default="http://localhost:8000", help="Chat server URL")
    parser.add_argument("--users", type=int, default=5, help="Concurrent user sessions per wave")
    parser.add_argument("--duration", type=int, default=180, help="Test duration in seconds")
    args = parser.parse_args()

    asyncio.run(run_load_test(args.url, args.users, args.duration))
