"""
Session Store for the Travel Agent chat interface.
Persists session metadata and conversation history to a local SQLite database
so sessions can be listed, resumed, and analyzed.
"""

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

SESSION_DB_PATH = os.getenv("SESSION_DB_PATH", "./data/sessions.db")


def _get_conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SESSION_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(SESSION_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id TEXT PRIMARY KEY,
            title TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            total_turns INTEGER DEFAULT 0,
            metadata TEXT DEFAULT '{}'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            turn_number INTEGER NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            metadata TEXT DEFAULT '{}',
            FOREIGN KEY (session_id) REFERENCES sessions(session_id)
        )
    """)
    conn.commit()
    return conn


def create_session(title: str | None = None) -> dict:
    """Create a new session and return its metadata."""
    conn = _get_conn()
    session_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    title = title or f"Session {now[:10]}"
    conn.execute(
        "INSERT INTO sessions (session_id, title, created_at, updated_at, status) VALUES (?, ?, ?, ?, ?)",
        (session_id, title, now, now, "active"),
    )
    conn.commit()
    conn.close()
    return {"session_id": session_id, "title": title, "created_at": now, "status": "active", "total_turns": 0}


def end_session(session_id: str) -> None:
    """Mark a session as ended."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE sessions SET status = 'ended', updated_at = ? WHERE session_id = ?",
        (now, session_id),
    )
    conn.commit()
    conn.close()


def resume_session(session_id: str) -> dict | None:
    """Resume a session — set status back to active, return session info."""
    conn = _get_conn()
    row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
    if not row:
        conn.close()
        return None
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE sessions SET status = 'active', updated_at = ? WHERE session_id = ?",
        (now, session_id),
    )
    conn.commit()
    session = dict(row)
    session["status"] = "active"
    conn.close()
    return session


def list_sessions(limit: int = 20) -> list[dict]:
    """List recent sessions, most recent first."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT session_id, title, created_at, updated_at, status, total_turns FROM sessions ORDER BY updated_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_message(session_id: str, turn_number: int, role: str, content: str, metadata: dict | None = None) -> None:
    """Add a message (user or assistant) to session history."""
    conn = _get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO messages (session_id, turn_number, role, content, timestamp, metadata) VALUES (?, ?, ?, ?, ?, ?)",
        (session_id, turn_number, role, content, now, json.dumps(metadata or {})),
    )
    conn.execute(
        "UPDATE sessions SET total_turns = ?, updated_at = ? WHERE session_id = ?",
        (turn_number, now, session_id),
    )
    conn.commit()
    conn.close()


def get_session_history(session_id: str) -> list[dict]:
    """Get all messages for a session in order."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT turn_number, role, content, timestamp, metadata FROM messages WHERE session_id = ? ORDER BY id ASC",
        (session_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_session(session_id: str) -> dict | None:
    """Get session metadata."""
    conn = _get_conn()
    row = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
    conn.close()
    return dict(row) if row else None
