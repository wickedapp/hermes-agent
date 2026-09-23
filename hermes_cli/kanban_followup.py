"""Durable, artifact-driven follow-up for long-running Kanban work.

This module deliberately has no scheduler of its own.  The gateway's existing
Kanban notifier watcher calls :func:`evaluate_tick`, then drains the durable
outbox.  Consequently a restart cannot forget either a follow-up or a send
that still needs evidence.
"""

from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from hermes_cli.kanban_db import write_txn


STATUS_PLATFORM = "telegram"
STATUS_CHAT_ID = "-5277676345"
SLA_SECONDS = {"15m": 15 * 60, "30m": 30 * 60, "60m": 60 * 60, "90m": 90 * 60}
_ARTIFACT_KEYS = {
    "head", "head_sha", "commit_sha", "pr_url", "artifact", "artifacts",
    "verdict", "canary", "deployment", "evidence_url", "evidence_path",
}
_NOISY_EVENT_KINDS = {"heartbeat", "log", "comment", "claimed", "spawned"}
_TERMINAL_STATES = {"done", "archived"}
_STATE_EVENT_KINDS = {
    "created", "completed", "blocked", "unblocked", "archived", "restored",
}


@dataclass(frozen=True)
class OutboxItem:
    id: int
    control_id: str
    native_task_id: Optional[str]
    route_kind: str
    platform: str
    chat_id: str
    thread_id: str
    milestone: str
    artifact_fingerprint: str
    payload: dict[str, Any]
    lease_token: str

    @property
    def idempotency_key(self) -> str:
        raw = (
            f"{self.control_id}\0{self.route_kind}\0{self.milestone}\0"
            f"{self.artifact_fingerprint}"
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def register_link(
    conn: sqlite3.Connection,
    control_id: str,
    native_task_id: Optional[str],
    origin_platform: Optional[str] = None,
    origin_chat_id: Optional[str] = None,
    origin_thread_id: Optional[str] = None,
    created_at: Optional[int] = None,
) -> None:
    """Persist a generic control-id to native-task link and its Boss route."""
    now = int(time.time()) if created_at is None else int(created_at)
    with write_txn(conn):
        existing = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id=?",
            (str(control_id),),
        ).fetchone()
        if (
            existing is not None
            and existing["native_task_id"] not in (None, native_task_id)
            and native_task_id is not None
        ):
            raise ValueError(
                f"control id {control_id!r} is already linked to "
                f"{existing['native_task_id']!r}"
            )
        if existing is not None:
            for label, stored, requested in (
                ("platform", existing["origin_platform"], origin_platform),
                ("chat", existing["origin_chat_id"], origin_chat_id),
                ("thread", existing["origin_thread_id"] or "", origin_thread_id or ""),
            ):
                if stored not in (None, "") and requested not in (None, "", stored):
                    raise ValueError(
                        f"control id {control_id!r} already has a different Boss "
                        f"{label}; route changes require an explicit operator repair"
                    )
        conn.execute(
            """
            INSERT INTO kanban_followup_links (
                control_id, native_task_id, origin_platform, origin_chat_id,
                origin_thread_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(control_id) DO UPDATE SET
                native_task_id=COALESCE(native_task_id, excluded.native_task_id),
                origin_platform=COALESCE(origin_platform, excluded.origin_platform),
                origin_chat_id=COALESCE(origin_chat_id, excluded.origin_chat_id),
                origin_thread_id=CASE
                    WHEN excluded.origin_thread_id != '' THEN excluded.origin_thread_id
                    ELSE origin_thread_id
                END,
                updated_at=excluded.updated_at
            """,
            (
                str(control_id), native_task_id, origin_platform, origin_chat_id,
                origin_thread_id or "", now, now,
            ),
        )


def record_artifact(
    conn: sqlite3.Connection,
    native_task_id: str,
    artifact: dict[str, Any],
    *,
    created_at: Optional[int] = None,
) -> int:
    """Append a durable artifact event consumed by the follow-up evaluator."""
    clean = {key: value for key, value in artifact.items() if key in _ARTIFACT_KEYS}
    if not clean:
        raise ValueError("artifact must contain at least one recognized evidence key")
    cur = conn.execute("SELECT 1 FROM tasks WHERE id = ?", (native_task_id,)).fetchone()
    if cur is None:
        raise ValueError(f"unknown native task: {native_task_id}")
    now = int(time.time()) if created_at is None else int(created_at)
    with write_txn(conn):
        inserted = conn.execute(
            "INSERT INTO task_events (task_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
            (native_task_id, "followup_artifact", json.dumps(clean, sort_keys=True), now),
        )
    return int(inserted.lastrowid)


def _json_payload(raw: Any) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _snapshot(conn: sqlite3.Connection, native_task_id: Optional[str]) -> dict[str, Any]:
    """Return only durable state/evidence; never include PID, logs, or config."""
    if not native_task_id:
        return {"native_task_id": None, "state": "unlinked", "failure_count": 0}
    task = conn.execute(
        "SELECT id, status, consecutive_failures, current_run_id, claim_lock, "
        "worker_pid, started_at, last_heartbeat_at, created_at, last_failure_error, "
        "claim_expires "
        "FROM tasks WHERE id = ?",
        (native_task_id,),
    ).fetchone()
    if task is None:
        return {"native_task_id": native_task_id, "state": "missing", "failure_count": 0}

    artifacts: dict[str, Any] = {}
    verdict = None
    changed_at = int(task["created_at"] or 0)
    events = conn.execute(
        "SELECT kind, payload, created_at FROM task_events WHERE task_id = ? ORDER BY id ASC",
        (native_task_id,),
    ).fetchall()
    for event in events:
        if event["kind"] in _NOISY_EVENT_KINDS:
            continue
        payload = _json_payload(event["payload"])
        if event["kind"] == "followup_artifact" or event["kind"] in _STATE_EVENT_KINDS:
            changed_at = max(changed_at, int(event["created_at"] or 0))
        for key in _ARTIFACT_KEYS:
            if key in payload and payload[key] not in (None, "", []):
                artifacts[key] = payload[key]
        if payload.get("verdict") not in (None, ""):
            verdict = payload["verdict"]

    owner_issue = _owner_issue(conn, task)
    return {
        "native_task_id": native_task_id,
        "state": task["status"],
        "failure_count": int(task["consecutive_failures"] or 0),
        "failure_evidence": bool(str(task["last_failure_error"] or "").strip()),
        "artifacts": artifacts,
        "verdict": verdict,
        "owner_issue": owner_issue,
        "changed_at": changed_at,
    }


def _owner_issue(conn: sqlite3.Connection, task: sqlite3.Row) -> Optional[str]:
    if task["status"] != "running":
        return None
    run_id = task["current_run_id"]
    if run_id is None:
        return "missing_generation"
    run = conn.execute(
        "SELECT id, status, claim_lock, worker_pid, started_at FROM task_runs "
        "WHERE id = ? AND task_id = ?",
        (run_id, task["id"]),
    ).fetchone()
    if run is None:
        return "missing_generation"
    if run["status"] != "running":
        return "stale_generation"
    if run["claim_lock"] != task["claim_lock"] or run["worker_pid"] != task["worker_pid"]:
        return "mismatched_owner"
    lock = str(task["claim_lock"] or "")
    local_prefix = f"{socket.gethostname()}:"
    if ":" in lock and not lock.startswith(local_prefix):
        if task["claim_expires"] and int(task["claim_expires"]) < int(time.time()):
            return "delegation_owner_loss"
        return None
    pid = task["worker_pid"]
    if not pid:
        # Claim and process spawn are separate fenced writes. Give the
        # dispatcher a short window to publish the worker PID so the watcher
        # cannot reclaim a legitimate just-claimed generation mid-spawn.
        if int(time.time()) - int(run["started_at"] or 0) < 60:
            return None
        return "missing_owner"
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return "dead_owner"
    except (PermissionError, OSError):
        pass
    try:
        import psutil  # type: ignore
        # A live numeric PID is not proof that it is still our worker. If the
        # OS reused it after the recorded run began, recovery must stop rather
        # than signal an unrelated process.
        if psutil.Process(int(pid)).create_time() > float(run["started_at"] or 0) + 2:
            return "reused_owner_pid"
    except ImportError:
        pass
    except (psutil.NoSuchProcess, psutil.AccessDenied, ValueError, OSError):
        pass
    return None


def artifact_fingerprint(snapshot: dict[str, Any]) -> str:
    """Hash the stable task/artifact tuple; operational noise is excluded."""
    stable = {
        "native_task_id": snapshot.get("native_task_id"),
        "head": (snapshot.get("artifacts") or {}).get("head")
            or (snapshot.get("artifacts") or {}).get("head_sha")
            or (snapshot.get("artifacts") or {}).get("commit_sha"),
        "artifact": snapshot.get("artifacts") or {},
        "verdict": snapshot.get("verdict"),
        "state": snapshot.get("state"),
        "failure_count": int(snapshot.get("failure_count") or 0),
    }
    raw = json.dumps(stable, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _milestones(snapshot: dict[str, Any], age: int, unchanged_age: int) -> Iterable[str]:
    state = snapshot["state"]
    artifacts = snapshot.get("artifacts") or {}
    if state not in {"unlinked", "missing"}:
        yield "linked"
    if state in _TERMINAL_STATES:
        yield "terminal"
        return
    if state == "blocked":
        yield "blocker"
    if age >= SLA_SECONDS["15m"] and state in {"unlinked", "missing"}:
        yield "15m"
    has_material_artifact = bool(artifacts or snapshot.get("verdict"))
    grounded_failure = bool(snapshot.get("failure_evidence"))
    if age >= SLA_SECONDS["30m"] and not (has_material_artifact or grounded_failure):
        yield "30m"
    verified = bool(
        artifacts.get("commit_sha") or artifacts.get("head_sha") or artifacts.get("head")
    ) and bool(snapshot.get("verdict") or artifacts.get("verdict") or artifacts.get("canary"))
    if age >= SLA_SECONDS["60m"] and not (verified or grounded_failure):
        yield "60m"
    # A stale diff is not progress forever. Every new durable artifact/state
    # creates a new observation generation; 90 minutes without another one is
    # a stall regardless of how much evidence the previous generation held.
    if unchanged_age >= SLA_SECONDS["90m"]:
        yield "90m"
    if artifacts.get("head") or artifacts.get("head_sha") or artifacts.get("commit_sha") or artifacts.get("pr_url"):
        yield "new_head"
    if snapshot.get("verdict") or artifacts.get("verdict"):
        yield "verdict"
    if artifacts.get("canary") or artifacts.get("deployment"):
        yield "deployment"
    if snapshot.get("owner_issue"):
        yield "blocker"


def _routes(link: sqlite3.Row, milestone: str) -> Iterable[tuple[str, str, str, str]]:
    yield "status", STATUS_PLATFORM, STATUS_CHAT_ID, ""
    boss_milestones = {"linked", "new_head", "verdict", "deployment", "90m", "blocker", "terminal"}
    if milestone in boss_milestones and link["origin_platform"] and link["origin_chat_id"]:
        yield (
            "boss", str(link["origin_platform"]), str(link["origin_chat_id"]),
            str(link["origin_thread_id"] or ""),
        )


def evaluate_tick(conn: sqlite3.Connection, now: Optional[int] = None) -> int:
    """Materialize all currently due alerts into the outbox atomically."""
    timestamp = int(time.time()) if now is None else int(now)
    links = conn.execute("SELECT * FROM kanban_followup_links ORDER BY created_at").fetchall()
    inserted = 0
    safe_recoveries: list[tuple[str, str]] = []
    with write_txn(conn):
        for link in links:
            snapshot = _snapshot(conn, link["native_task_id"])
            fingerprint = artifact_fingerprint(snapshot)
            age = max(0, timestamp - int(link["created_at"]))
            # Clamp producer timestamps to evaluator time. This tolerates
            # clock skew between a worker and gateway and keeps replay tests
            # deterministic without allowing a future timestamp to postpone
            # an SLA indefinitely.
            first_seen = min(
                timestamp,
                int(snapshot.get("changed_at") or link["created_at"] or timestamp),
            )
            conn.execute(
                """
                INSERT INTO kanban_followup_observations (
                    control_id, artifact_fingerprint, first_seen_at, last_seen_at
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(control_id, artifact_fingerprint) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at
                """,
                (link["control_id"], fingerprint, first_seen, timestamp),
            )
            observed = conn.execute(
                "SELECT first_seen_at FROM kanban_followup_observations "
                "WHERE control_id=? AND artifact_fingerprint=?",
                (link["control_id"], fingerprint),
            ).fetchone()
            unchanged_age = max(0, timestamp - int(observed["first_seen_at"]))
            if snapshot.get("owner_issue") in {"dead_owner", "missing_owner"}:
                safe_recoveries.append((str(link["native_task_id"]), snapshot["owner_issue"]))
            for milestone in _milestones(snapshot, age, unchanged_age):
                # Link acceptance has one immutable semantic generation. All
                # other milestones, including terminal delivery, bind to the
                # exact durable artifact/state/verdict fingerprint. A reopened
                # task or later HEAD is therefore a distinct generation.
                milestone_fingerprint = fingerprint
                if milestone == "linked":
                    milestone_fingerprint = hashlib.sha256(
                        f"linked\0{link['native_task_id']}".encode("utf-8")
                    ).hexdigest()
                payload = {
                    "control_id": link["control_id"],
                    "milestone": milestone,
                    "age_seconds": age,
                    "fingerprint": fingerprint,
                    "snapshot": snapshot,
                }
                for route_kind, platform, chat_id, thread_id in _routes(link, milestone):
                    route_fingerprint = hashlib.sha256(
                        f"{route_kind}\0{platform}\0{chat_id}\0{thread_id}".encode("utf-8")
                    ).hexdigest()
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO kanban_followup_outbox (
                            control_id, native_task_id, route_kind, route_fingerprint,
                            platform, chat_id,
                            thread_id, milestone, artifact_fingerprint, payload,
                            next_attempt_at, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            link["control_id"], link["native_task_id"], route_kind,
                            route_fingerprint,
                            platform, chat_id, thread_id, milestone, milestone_fingerprint,
                            json.dumps(payload, sort_keys=True, default=str), timestamp, timestamp,
                        ),
                    )
                    inserted += int(cur.rowcount or 0)
    # Use the board's existing claim-lock CAS and run-closing recovery path.
    # This happens only after the blocker alert is durable, and only for
    # locally provable worker absence. Mismatch, PID reuse, remote ownership,
    # and stale generations remain explicit blockers and never auto-take over.
    if safe_recoveries:
        from hermes_cli import kanban_db as _kb

        def _already_absent(_pid: int, _signal: int) -> None:
            raise ProcessLookupError

        for task_id, reason in safe_recoveries:
            _kb.reclaim_task(
                conn, task_id, reason=f"followup fenced recovery: {reason}",
                # Absence was established above. Do not signal the numeric PID
                # again after the transaction boundary: the OS could reuse it
                # for an unrelated process in that tiny interval.
                signal_fn=_already_absent,
            )
    return inserted


def claim_pending(
    conn: sqlite3.Connection,
    now: Optional[int] = None,
    *,
    limit: int = 50,
    lease_seconds: int = 60,
) -> list[OutboxItem]:
    """Fence and return pending deliveries; expired claims are reclaimable."""
    timestamp = int(time.time()) if now is None else int(now)
    token = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    with write_txn(conn):
        rows = conn.execute(
            """
            SELECT id FROM kanban_followup_outbox
             WHERE status = 'pending' AND next_attempt_at <= ?
               AND (lease_token IS NULL OR lease_expires_at <= ?)
             ORDER BY id LIMIT ?
            """,
            (timestamp, timestamp, max(1, int(limit))),
        ).fetchall()
        ids = [int(row["id"]) for row in rows]
        if not ids:
            return []
        marks = ",".join("?" for _ in ids)
        conn.execute(
            f"UPDATE kanban_followup_outbox SET lease_token = ?, lease_expires_at = ? "
            f"WHERE id IN ({marks}) AND status = 'pending' "
            "AND (lease_token IS NULL OR lease_expires_at <= ?)",
            (token, timestamp + max(1, int(lease_seconds)), *ids, timestamp),
        )
        claimed = conn.execute(
            f"SELECT * FROM kanban_followup_outbox WHERE id IN ({marks}) AND lease_token = ?",
            (*ids, token),
        ).fetchall()
    return [
        OutboxItem(
            id=int(row["id"]), control_id=row["control_id"],
            native_task_id=row["native_task_id"], route_kind=row["route_kind"],
            platform=row["platform"], chat_id=row["chat_id"], thread_id=row["thread_id"],
            milestone=row["milestone"], artifact_fingerprint=row["artifact_fingerprint"],
            payload=_json_payload(row["payload"]), lease_token=token,
        )
        for row in claimed
    ]


def mark_delivered(
    conn: sqlite3.Connection,
    outbox_id: int,
    lease_token: str,
    evidence: str,
    *,
    delivered_at: Optional[int] = None,
) -> bool:
    """Complete a fenced send only when provider evidence/message-id exists."""
    if not str(evidence or "").strip():
        raise ValueError("delivery evidence or message id is required")
    timestamp = int(time.time()) if delivered_at is None else int(delivered_at)
    with write_txn(conn):
        cur = conn.execute(
            "UPDATE kanban_followup_outbox SET status='delivered', delivered_at=?, "
            "delivery_evidence=?, lease_token=NULL, lease_expires_at=NULL "
            "WHERE id=? AND status='pending' AND lease_token=?",
            (timestamp, str(evidence), int(outbox_id), lease_token),
        )
    return cur.rowcount == 1


def mark_failed(
    conn: sqlite3.Connection,
    outbox_id: int,
    lease_token: str,
    error: str,
    *,
    now: Optional[int] = None,
) -> bool:
    """Release a fenced send for retry with bounded exponential backoff."""
    timestamp = int(time.time()) if now is None else int(now)
    with write_txn(conn):
        row = conn.execute(
            "SELECT attempts FROM kanban_followup_outbox WHERE id=? AND status='pending' "
            "AND lease_token=?",
            (int(outbox_id), lease_token),
        ).fetchone()
        if row is None:
            return False
        attempts = int(row["attempts"] or 0) + 1
        delay = min(3600, 5 * (2 ** min(attempts - 1, 9)))
        cur = conn.execute(
            "UPDATE kanban_followup_outbox SET attempts=?, next_attempt_at=?, last_error=?, "
            "lease_token=NULL, lease_expires_at=NULL WHERE id=? AND lease_token=?",
            (attempts, timestamp + delay, str(error)[:1000], int(outbox_id), lease_token),
        )
    return cur.rowcount == 1
