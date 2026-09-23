"""Failure-replay tests for durable Kanban follow-up delivery."""

from __future__ import annotations

import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import psutil

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_followup as followup
from tools import kanban_tools


STATUS_ROUTE = {
    "platform": "telegram", "chat_id": "status-chat", "thread_id": "",
}


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
    evaluate_tick = followup.evaluate_tick

    def configured_tick(conn, now=None, **kwargs):
        kwargs.setdefault("status_route", STATUS_ROUTE)
        return evaluate_tick(conn, now=now, **kwargs)

    monkeypatch.setattr(followup, "evaluate_tick", configured_tick)
    return home


def _linked_task(conn, *, created_at=1_000, boss=True):
    task_id = kb.create_task(conn, title="long-running follow-up")
    followup.register_link(
        conn,
        "control-1",
        task_id,
        origin_platform="telegram" if boss else None,
        origin_chat_id="boss-chat" if boss else None,
        origin_thread_id="boss-thread" if boss else None,
        created_at=created_at,
    )
    conn.execute("UPDATE tasks SET created_at=? WHERE id=?", (created_at, task_id))
    conn.commit()
    # Most tests isolate a later milestone. Materialize and discard the
    # one-shot intake acknowledgement while retaining its durable dedupe row.
    followup.evaluate_tick(conn, now=created_at)
    for item in followup.claim_pending(conn, now=created_at):
        followup.mark_delivered(conn, item.id, item.lease_token, f"setup-{item.id}")
    return task_id


def _outbox_rows(conn):
    return conn.execute(
        "SELECT * FROM kanban_followup_outbox WHERE milestone != 'linked' ORDER BY id"
    ).fetchall()


def _publish_test_owner(conn, task_id, run_id, pid, process_started_at):
    conn.execute(
        "UPDATE tasks SET worker_pid=?, worker_process_started_at=? WHERE id=?",
        (pid, process_started_at, task_id),
    )
    conn.execute(
        "UPDATE task_runs SET worker_pid=?, worker_process_started_at=? WHERE id=?",
        (pid, process_started_at, run_id),
    )
    conn.commit()


def test_native_intake_atomically_persists_control_link(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn,
            title="software delivery",
            workspace_kind="worktree",
            workspace_path="/tmp/repo",
            followup_control_id="delegation-42",
            followup_origin_platform="telegram",
            followup_origin_chat_id="boss-chat",
        )
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id=?",
            ("delegation-42",),
        ).fetchone()

        assert link["native_task_id"] == task_id
        assert link["origin_chat_id"] == "boss-chat"

    with kb.connect() as conn:
        assert followup.evaluate_tick(conn, now=int(link["created_at"])) == 2
        routes = conn.execute(
            "SELECT route_kind FROM kanban_followup_outbox WHERE milestone='linked'"
        ).fetchall()
        assert {row["route_kind"] for row in routes} == {"status", "boss"}


def test_concurrent_control_intake_returns_winning_link(kanban_home):
    barrier = threading.Barrier(2)

    def create():
        with kb.connect() as conn:
            barrier.wait()
            return kb.create_task(
                conn,
                title="same delivery",
                workspace_kind="worktree",
                workspace_path="/tmp/repo",
                followup_control_id="concurrent-control",
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        task_ids = list(pool.map(lambda _: create(), range(2)))

    assert task_ids[0] == task_ids[1]
    with kb.connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE id=?", (task_ids[0],)
        ).fetchone()[0] == 1


def test_concurrent_control_intake_rejects_conflicting_boss_route(kanban_home):
    barrier = threading.Barrier(2)

    def create(chat_id):
        with kb.connect() as conn:
            barrier.wait()
            return kb.create_task(
                conn, title="same delivery", workspace_kind="worktree",
                workspace_path="/tmp/repo", followup_control_id="route-race",
                followup_origin_platform="telegram",
                followup_origin_chat_id=chat_id,
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(create, chat) for chat in ("boss-a", "boss-b")]
        outcomes = []
        for future in futures:
            try:
                outcomes.append(future.result())
            except ValueError as exc:
                outcomes.append(str(exc))

    assert sum(isinstance(value, str) and value.startswith("t_") for value in outcomes) == 1
    assert sum("different Boss chat" in value for value in outcomes) == 1


def test_model_callable_route_arguments_cannot_redirect_boss(kanban_home, monkeypatch):
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "telegram")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "spoofed-env-chat")
    assert "followup_origin_chat_id" not in kanban_tools.KANBAN_CREATE_SCHEMA["parameters"]["properties"]
    result = json.loads(kanban_tools._handle_create({
        "title": "untrusted route", "assignee": "worker",
        "followup_control_id": "route-control",
        "followup_origin_platform": "telegram",
        "followup_origin_chat_id": "attacker-chat",
    }))
    with kb.connect() as conn:
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id='route-control'"
        ).fetchone()
    assert result["task_id"] == link["native_task_id"]
    assert link["origin_platform"] is None
    assert link["origin_chat_id"] is None


def test_authenticated_origin_replays_same_route_idempotently(kanban_home):
    from gateway.session_context import clear_session_vars, set_session_vars

    tokens = set_session_vars(
        platform="telegram", chat_id="boss-chat", thread_id="thread-1",
    )
    try:
        args = {
            "title": "trusted route", "assignee": "worker",
            "followup_control_id": "trusted-control",
        }
        first = json.loads(kanban_tools._handle_create(args))
        replay = json.loads(kanban_tools._handle_create(args))
    finally:
        clear_session_vars(tokens)
    with kb.connect() as conn:
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id='trusted-control'"
        ).fetchone()
    assert first["task_id"] == replay["task_id"]
    assert (link["origin_platform"], link["origin_chat_id"], link["origin_thread_id"]) == (
        "telegram", "boss-chat", "thread-1",
    )


def test_status_route_requires_explicit_configuration(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="quiet install")
        followup.register_link(conn, "quiet-control", task_id)
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id='quiet-control'"
        ).fetchone()
    assert list(followup._routes(link, "linked", None)) == []
    assert list(followup._routes(link, "linked", STATUS_ROUTE)) == [
        ("status", "telegram", "status-chat", ""),
    ]


def test_worktree_intake_always_gets_native_followup_identity(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn, title="software delivery", workspace_kind="worktree",
            workspace_path="/tmp/repo",
        )
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE native_task_id=?", (task_id,)
        ).fetchone()

    assert link["control_id"] == f"native:{task_id}"


def test_control_id_retry_keeps_native_task_and_boss_thread(kanban_home):
    with kb.connect() as conn:
        first = kb.create_task(
            conn, title="first", workspace_kind="worktree",
            followup_control_id="intake-1", followup_origin_platform="telegram",
            followup_origin_chat_id="boss", followup_origin_thread_id="thread-1",
        )
        retried = kb.create_task(
            conn, title="retry", workspace_kind="worktree",
            followup_control_id="intake-1", followup_origin_platform="telegram",
            followup_origin_chat_id="boss",
        )
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id='intake-1'"
        ).fetchone()

    assert retried == first
    assert link["native_task_id"] == first
    assert link["origin_thread_id"] == "thread-1"


def test_control_id_retry_cannot_redirect_boss_route(kanban_home):
    with kb.connect() as conn:
        kb.create_task(
            conn, title="first", workspace_kind="worktree",
            followup_control_id="intake-1", followup_origin_platform="telegram",
            followup_origin_chat_id="boss-a", followup_origin_thread_id="thread-a",
        )
        with pytest.raises(ValueError, match="different Boss chat"):
            kb.create_task(
                conn, title="redirect", workspace_kind="worktree",
                followup_control_id="intake-1", followup_origin_platform="telegram",
                followup_origin_chat_id="boss-b", followup_origin_thread_id="thread-a",
            )
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE control_id='intake-1'"
        ).fetchone()

    assert link["origin_chat_id"] == "boss-a"


def test_stale_control_link_fails_closed_without_second_writer(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn, title="first", workspace_kind="worktree",
            followup_control_id="intake-1",
        )
        conn.execute("DELETE FROM tasks WHERE id=?", (task_id,))
        conn.commit()
        with pytest.raises(ValueError, match="references missing native task"):
            kb.create_task(
                conn, title="retry", workspace_kind="worktree",
                followup_control_id="intake-1",
            )
        task_count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]

    assert task_count == 0


def test_idempotent_native_retry_cannot_redirect_boss_thread(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(
            conn, title="first", workspace_kind="worktree",
            idempotency_key="delivery-1", followup_origin_platform="telegram",
            followup_origin_chat_id="boss-a", followup_origin_thread_id="thread-a",
        )
        with pytest.raises(ValueError, match="different Boss thread"):
            kb.create_task(
                conn, title="retry", workspace_kind="worktree",
                idempotency_key="delivery-1", followup_origin_platform="telegram",
                followup_origin_chat_id="boss-a", followup_origin_thread_id="thread-b",
            )
        link = conn.execute(
            "SELECT * FROM kanban_followup_links WHERE native_task_id=?", (task_id,)
        ).fetchone()

    assert link["origin_thread_id"] == "thread-a"


def test_register_link_replay_cannot_erase_native_task(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="native")
        followup.register_link(conn, "control-1", task_id)
        followup.register_link(conn, "control-1", None)
        link = conn.execute(
            "SELECT native_task_id FROM kanban_followup_links WHERE control_id='control-1'"
        ).fetchone()

    assert link["native_task_id"] == task_id


def test_register_link_can_fill_initially_unlinked_control(kanban_home):
    with kb.connect() as conn:
        task_id = kb.create_task(conn, title="native")
        followup.register_link(conn, "control-1", None)
        followup.register_link(conn, "control-1", task_id)
        link = conn.execute(
            "SELECT native_task_id FROM kanban_followup_links WHERE control_id='control-1'"
        ).fetchone()

    assert link["native_task_id"] == task_id


def test_pending_delivery_survives_restart_and_expired_lease(kanban_home):
    with kb.connect() as conn:
        _linked_task(conn, boss=False)
        assert followup.evaluate_tick(conn, now=7_000) == 3
        first_claim = followup.claim_pending(conn, now=7_000, lease_seconds=10)
        assert len(first_claim) == 3

    with kb.connect() as restarted:
        assert followup.claim_pending(restarted, now=7_009) == []
        replayed = followup.claim_pending(restarted, now=7_011)

    assert {item.id for item in replayed} == {item.id for item in first_claim}
    assert replayed[0].lease_token != first_claim[0].lease_token
    assert {item.idempotency_key for item in replayed} == {
        item.idempotency_key for item in first_claim
    }


def test_duplicate_ticks_do_not_duplicate_due_followups(kanban_home):
    with kb.connect() as conn:
        _linked_task(conn)
        first = followup.evaluate_tick(conn, now=7_000)
        second = followup.evaluate_tick(conn, now=7_000)

        assert first == 4
        assert second == 0
        assert len(_outbox_rows(conn)) == 4


def test_dead_runner_emits_blocker_without_relying_on_heartbeat(kanban_home, monkeypatch):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        claimed = kb.claim_task(conn, task_id, claimer="worker-generation")
        assert claimed is not None
        run_id = kb.get_task(conn, task_id).current_run_id
        _publish_test_owner(conn, task_id, run_id, 424242, 100.0)

        monkeypatch.setattr(
            psutil, "Process",
            lambda _pid: (_ for _ in ()).throw(psutil.NoSuchProcess(424242)),
        )
        followup.evaluate_tick(conn, now=1_001)
        rows = _outbox_rows(conn)
        recovered = kb.get_task(conn, task_id)

    assert [row["milestone"] for row in rows] == ["blocker"]
    assert followup._json_payload(rows[0]["payload"])["snapshot"]["owner_issue"] == "dead_owner"
    assert recovered.status == "ready"
    assert recovered.current_run_id is None


def test_mismatched_run_owner_is_fenced_before_pid_probe(kanban_home, monkeypatch):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.claim_task(conn, task_id, claimer="task-owner")
        task = kb.get_task(conn, task_id)
        conn.execute("UPDATE tasks SET worker_pid=1234 WHERE id=?", (task_id,))
        conn.execute(
            "UPDATE task_runs SET worker_pid=5678 WHERE id=?",
            (task.current_run_id,),
        )
        conn.commit()
        probes = []
        monkeypatch.setattr(os, "kill", lambda *args: probes.append(args))

        followup.evaluate_tick(conn, now=1_001)
        payload = followup._json_payload(_outbox_rows(conn)[0]["payload"])

    assert payload["snapshot"]["owner_issue"] == "mismatched_owner"
    assert probes == []


def test_reused_owner_pid_alerts_without_takeover(kanban_home, monkeypatch):
    import psutil

    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.claim_task(conn, task_id, claimer="legacy-local-owner")
        task = kb.get_task(conn, task_id)
        _publish_test_owner(conn, task_id, task.current_run_id, 2468, 100.0)
        class ReusedProcess:
            def __init__(self, pid):
                assert pid == 2468

            def create_time(self):
                return 100.001

        monkeypatch.setattr(psutil, "Process", ReusedProcess)
        followup.evaluate_tick(conn, now=1_001)
        payload = followup._json_payload(_outbox_rows(conn)[0]["payload"])
        task_after = kb.get_task(conn, task_id)

    assert payload["snapshot"]["owner_issue"] == "reused_owner_pid"
    assert task_after.status == "running"
    assert task_after.current_run_id == task.current_run_id


def test_fenced_recovery_cannot_reset_replacement_owner(kanban_home, monkeypatch):
    """A replacement published after observation survives stale recovery."""
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.claim_task(conn, task_id, claimer="old-owner")
        old = kb.get_task(conn, task_id)
        _publish_test_owner(conn, task_id, old.current_run_id, 424242, 100.0)
        monkeypatch.setattr(
            psutil, "Process",
            lambda _pid: (_ for _ in ()).throw(psutil.NoSuchProcess(424242)),
        )
        real_reclaim = kb.reclaim_task

        def race_reclaim(db, tid, **kwargs):
            replacement_run = db.execute(
                "INSERT INTO task_runs (task_id,status,claim_lock,worker_pid,started_at) "
                "VALUES (?, 'running', 'replacement', 777, 2000)",
                (tid,),
            ).lastrowid
            db.execute(
                "UPDATE tasks SET current_run_id=?, claim_lock='replacement', "
                "worker_pid=777 WHERE id=?",
                (replacement_run, tid),
            )
            db.commit()
            return real_reclaim(db, tid, **kwargs)

        monkeypatch.setattr(kb, "reclaim_task", race_reclaim)
        followup.evaluate_tick(conn, now=1_001)
        after = kb.get_task(conn, task_id)

    assert after.status == "running"
    assert after.claim_lock == "replacement"
    assert after.worker_pid == 777


def test_delayed_spawn_publish_cannot_overwrite_replacement_generation(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        old = kb.claim_task(conn, task_id, claimer="old-owner")
        assert old is not None
        with kb.write_txn(conn):
            conn.execute(
                "UPDATE task_runs SET status='reclaimed', outcome='reclaimed', "
                "ended_at=2000 WHERE id=?",
                (old.current_run_id,),
            )
            replacement = conn.execute(
                "INSERT INTO task_runs (task_id,status,claim_lock,started_at) "
                "VALUES (?, 'running', 'replacement', 2001)",
                (task_id,),
            ).lastrowid
            conn.execute(
                "UPDATE tasks SET current_run_id=?, claim_lock='replacement', "
                "worker_pid=NULL, worker_process_started_at=NULL WHERE id=?",
                (replacement, task_id),
            )

        assert not kb._set_worker_pid(
            conn, task_id, 424242,
            expected_run_id=old.current_run_id,
            expected_claim_lock="old-owner",
        )
        after = kb.get_task(conn, task_id)

    assert after.current_run_id == replacement
    assert after.claim_lock == "replacement"
    assert after.worker_pid is None


@pytest.mark.parametrize("pid_error", [None, PermissionError(), OSError("opaque")])
def test_matching_live_owner_pid_probe_is_not_a_blocker(kanban_home, monkeypatch, pid_error):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.claim_task(conn, task_id, claimer="same-owner")
        task = kb.get_task(conn, task_id)
        _publish_test_owner(conn, task_id, task.current_run_id, 2468, 100.0)
        probes = []

        def probe(pid, signal):
            probes.append((pid, signal))
            if pid_error is not None:
                raise pid_error

        monkeypatch.setattr(os, "kill", probe)

        class MatchingProcess:
            def __init__(self, pid):
                assert pid == 2468

            def create_time(self):
                return 100.0

            def is_running(self):
                return True

        monkeypatch.setattr(psutil, "Process", MatchingProcess)
        assert followup.evaluate_tick(conn, now=1_001) == 0

    assert probes == []


def test_unchanged_artifact_suppresses_repeated_milestone(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        followup.record_artifact(conn, task_id, {"head": "abc"}, created_at=1_100)
        assert followup.evaluate_tick(conn, now=5_000) == 2
        followup.record_artifact(conn, task_id, {"head": "abc"}, created_at=5_001)
        assert followup.evaluate_tick(conn, now=5_002) == 0

        assert len(_outbox_rows(conn)) == 2


def test_new_head_rearms_same_milestone(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        followup.record_artifact(conn, task_id, {"head": "abc"}, created_at=1_100)
        assert followup.evaluate_tick(conn, now=5_000) == 2
        followup.record_artifact(conn, task_id, {"head": "def"}, created_at=5_001)
        assert followup.evaluate_tick(conn, now=5_002) == 2

        rows = _outbox_rows(conn)
        assert [row["milestone"] for row in rows] == [
            "60m", "new_head", "60m", "new_head",
        ]
        assert len({row["artifact_fingerprint"] for row in rows}) == 2


def test_delivery_failure_releases_lease_and_retries_after_backoff(kanban_home):
    with kb.connect() as conn:
        _linked_task(conn, boss=False)
        followup.evaluate_tick(conn, now=3_000)
        item = followup.claim_pending(conn, now=3_000, limit=1)[0]
        assert followup.mark_failed(conn, item.id, item.lease_token, "network down", now=3_000)
        assert followup.claim_pending(conn, now=3_004, limit=1) == []
        retry = followup.claim_pending(conn, now=3_005, limit=1)[0]
        row = conn.execute(
            "SELECT attempts, last_error FROM kanban_followup_outbox WHERE id=?",
            (item.id,),
        ).fetchone()

    assert retry.id == item.id
    assert row["attempts"] == 1
    assert row["last_error"] == "network down"


def test_status_and_boss_routes_dedupe_independently(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn)
        kb.complete_task(conn, task_id, result="shipped")
        assert followup.evaluate_tick(conn, now=1_001) == 2
        assert followup.evaluate_tick(conn, now=1_002) == 0
        rows = _outbox_rows(conn)

    assert {row["route_kind"] for row in rows} == {"status", "boss"}
    assert len({row["route_fingerprint"] for row in rows}) == 2


@pytest.mark.parametrize(
    ("state", "expected_outcome"),
    [("done", "DONE"), ("archived", "DONE")],
)
def test_terminal_outcome_is_unique_per_route(kanban_home, state, expected_outcome):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        conn.execute("UPDATE tasks SET status=? WHERE id=?", (state, task_id))
        conn.commit()
        assert followup.evaluate_tick(conn, now=1_001) == 1
        assert followup.evaluate_tick(conn, now=9_999) == 0
        rows = _outbox_rows(conn)
        snapshot_state = followup._json_payload(rows[0]["payload"])["snapshot"]["state"]

    outcome = "DONE" if snapshot_state in {"done", "archived"} else "NOT DONE"
    assert outcome == expected_outcome
    assert [row["milestone"] for row in rows] == ["terminal"]


def test_terminal_new_head_creates_new_exact_fingerprint(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.complete_task(conn, task_id, result="done")
        followup.evaluate_tick(conn, now=1_001)
        followup.record_artifact(conn, task_id, {"head": "later-head"}, created_at=1_002)
        followup.evaluate_tick(conn, now=1_003)
        terminal_count = conn.execute(
            "SELECT COUNT(*) FROM kanban_followup_outbox WHERE milestone='terminal'"
        ).fetchone()[0]

    assert terminal_count == 2


def test_blocked_then_done_has_one_terminal_delivery(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.block_task(conn, task_id, reason="needs credential")
        followup.evaluate_tick(conn, now=1_001)
        assert conn.execute(
            "SELECT COUNT(*) FROM kanban_followup_outbox WHERE milestone='terminal'"
        ).fetchone()[0] == 0
        assert kb.unblock_task(conn, task_id)
        assert kb.complete_task(conn, task_id, result="fixed")
        followup.evaluate_tick(conn, now=1_002)
        rows = conn.execute(
            "SELECT * FROM kanban_followup_outbox WHERE milestone='terminal'"
        ).fetchall()

    assert len(rows) == 1


def test_stale_head_reaches_90m_stall_and_rearms_on_new_head(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        followup.record_artifact(conn, task_id, {"head": "abc"}, created_at=1_100)
        followup.evaluate_tick(conn, now=1_100)
        assert followup.evaluate_tick(conn, now=6_501) == 2
        assert followup.evaluate_tick(conn, now=6_502) == 0
        followup.record_artifact(conn, task_id, {"head": "def"}, created_at=6_503)
        followup.evaluate_tick(conn, now=6_503)
        assert followup.evaluate_tick(conn, now=11_904) == 1
        stalls = conn.execute(
            "SELECT * FROM kanban_followup_outbox WHERE milestone='90m'"
        ).fetchall()

    assert len(stalls) == 2


@pytest.mark.parametrize(
    "kind", ["crashed", "timed_out", "gave_up", "reclaimed", "stale"]
)
def test_fresh_failure_transition_resets_old_task_stall_clock(kanban_home, kind):
    with kb.connect() as conn:
        task_id = _linked_task(conn, created_at=1_000, boss=False)
        followup.evaluate_tick(conn, now=6_401)
        with kb.write_txn(conn):
            kb._append_event(conn, task_id, kind, {"evidence": "test"})
        now = conn.execute(
            "SELECT created_at FROM task_events WHERE task_id=? AND kind=? "
            "ORDER BY id DESC LIMIT 1",
            (task_id, kind),
        ).fetchone()["created_at"]
        followup.evaluate_tick(conn, now=now)
        latest = followup._snapshot(conn, task_id)
        fingerprint = followup.artifact_fingerprint(latest)
        stalls = conn.execute(
            "SELECT COUNT(*) FROM kanban_followup_outbox "
            "WHERE milestone='90m' AND artifact_fingerprint=?",
            (fingerprint,),
        ).fetchone()[0]

    assert stalls == 0


def test_old_queued_task_claim_resets_stall_clock(kanban_home):
    """An old queue age must not become an immediate running-task stall."""
    with kb.connect() as conn:
        task_id = _linked_task(conn, created_at=1_000, boss=False)
        conn.execute(
            "UPDATE tasks SET status_changed_at=? WHERE id=?", (1_000, task_id)
        )
        conn.commit()
        before_claim = conn.execute(
            "SELECT status_changed_at FROM tasks WHERE id=?", (task_id,)
        ).fetchone()["status_changed_at"]

        claimed = kb.claim_task(conn, task_id, claimer="fresh-generation")
        assert claimed is not None
        after_claim = conn.execute(
            "SELECT status_changed_at FROM tasks WHERE id=?", (task_id,)
        ).fetchone()["status_changed_at"]
        assert after_claim > before_claim

        followup.evaluate_tick(conn, now=after_claim)
        fingerprint = followup.artifact_fingerprint(followup._snapshot(conn, task_id))
        stalls = conn.execute(
            "SELECT COUNT(*) FROM kanban_followup_outbox "
            "WHERE milestone='90m' AND artifact_fingerprint=?",
            (fingerprint,),
        ).fetchone()[0]

    assert stalls == 0


@pytest.mark.parametrize(
    "artifact",
    [
        {"evidence_path": "/tmp/report.json"},
        {"evidence_url": "https://example.invalid/evidence"},
        {"canary": "passed"},
        {"deployment": "deploy-1"},
        {"verdict": "FAIL"},
    ],
)
def test_any_durable_artifact_satisfies_30m_sla(kanban_home, artifact):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        followup.record_artifact(conn, task_id, artifact, created_at=1_100)
        followup.evaluate_tick(conn, now=2_801)
        breaches = conn.execute(
            "SELECT * FROM kanban_followup_outbox WHERE milestone='30m'"
        ).fetchall()

    assert breaches == []


def test_completion_metadata_feeds_exact_head_verdict_artifact(kanban_home):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        assert kb.complete_task(
            conn, task_id, result="ready", metadata={
                "head_sha": "abc123", "verdict": "PASS",
                "evidence_path": "/tmp/review.txt",
            },
        )
        followup.evaluate_tick(conn, now=1_001)
        terminal = conn.execute(
            "SELECT payload FROM kanban_followup_outbox WHERE milestone='terminal'"
        ).fetchone()
        snapshot = followup._json_payload(terminal["payload"])["snapshot"]

    assert snapshot["artifacts"]["head_sha"] == "abc123"
    assert snapshot["verdict"] == "PASS"
    assert snapshot["artifacts"]["evidence_path"] == "/tmp/review.txt"


def test_isolated_dry_run_canary_requires_positive_message_id_evidence(kanban_home):
    """No adapter is contacted: emulate one successful canary receipt locally."""
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        followup.record_artifact(
            conn,
            task_id,
            {"head": "canary-head", "canary": "passed"},
            created_at=1_100,
        )
        followup.evaluate_tick(conn, now=5_000)
        canary = followup.claim_pending(conn, now=5_000, limit=1)[0]

        with pytest.raises(ValueError, match="message id"):
            followup.mark_delivered(conn, canary.id, canary.lease_token, "")
        assert followup.mark_delivered(
            conn, canary.id, canary.lease_token, "dry-run-message-id-1"
        )
        row = conn.execute(
            "SELECT status, delivery_evidence FROM kanban_followup_outbox WHERE id=?",
            (canary.id,),
        ).fetchone()

    assert dict(row) == {
        "status": "delivered",
        "delivery_evidence": "dry-run-message-id-1",
    }


def test_dispatched_crash_boundary_is_not_retried(kanban_home):
    with kb.connect() as conn:
        _linked_task(conn, boss=False)
        followup.evaluate_tick(conn, now=3_000)
        item = followup.claim_pending(conn, now=3_000, limit=1)[0]
        assert followup.mark_dispatched(
            conn, item.id, item.lease_token, f"adapter-call:{item.idempotency_key}"
        )
        # Simulate process death after platform acceptance and before receipt.
    with kb.connect() as restarted:
        assert followup.claim_pending(restarted, now=99_999) == []
        row = restarted.execute(
            "SELECT status, delivery_evidence FROM kanban_followup_outbox WHERE id=?",
            (item.id,),
        ).fetchone()
    assert row["status"] == "ambiguous"
    assert item.idempotency_key in row["delivery_evidence"]
