"""Failure-replay tests for durable Kanban follow-up delivery."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb
from hermes_cli import kanban_followup as followup


@pytest.fixture
def kanban_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db()
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
        conn.execute("UPDATE tasks SET worker_pid=424242 WHERE id=?", (task_id,))
        conn.execute("UPDATE task_runs SET worker_pid=424242 WHERE id=?", (run_id,))
        conn.commit()

        def missing_pid(pid, signal):
            assert (pid, signal) == (424242, 0)
            raise ProcessLookupError

        monkeypatch.setattr(os, "kill", missing_pid)
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
        conn.execute("UPDATE tasks SET worker_pid=2468 WHERE id=?", (task_id,))
        conn.execute(
            "UPDATE task_runs SET worker_pid=2468 WHERE id=?",
            (task.current_run_id,),
        )
        run_started = conn.execute(
            "SELECT started_at FROM task_runs WHERE id=?", (task.current_run_id,)
        ).fetchone()["started_at"]
        conn.commit()
        monkeypatch.setattr(os, "kill", lambda *_: None)

        class ReusedProcess:
            def __init__(self, pid):
                assert pid == 2468

            def create_time(self):
                return run_started + 30

        monkeypatch.setattr(psutil, "Process", ReusedProcess)
        followup.evaluate_tick(conn, now=1_001)
        payload = followup._json_payload(_outbox_rows(conn)[0]["payload"])
        task_after = kb.get_task(conn, task_id)

    assert payload["snapshot"]["owner_issue"] == "reused_owner_pid"
    assert task_after.status == "running"
    assert task_after.current_run_id == task.current_run_id


@pytest.mark.parametrize("pid_error", [None, PermissionError(), OSError("opaque")])
def test_matching_live_owner_pid_probe_is_not_a_blocker(kanban_home, monkeypatch, pid_error):
    with kb.connect() as conn:
        task_id = _linked_task(conn, boss=False)
        kb.claim_task(conn, task_id, claimer="same-owner")
        task = kb.get_task(conn, task_id)
        conn.execute("UPDATE tasks SET worker_pid=2468 WHERE id=?", (task_id,))
        conn.execute(
            "UPDATE task_runs SET worker_pid=2468 WHERE id=?",
            (task.current_run_id,),
        )
        conn.commit()
        probes = []

        def probe(pid, signal):
            probes.append((pid, signal))
            if pid_error is not None:
                raise pid_error

        monkeypatch.setattr(os, "kill", probe)
        assert followup.evaluate_tick(conn, now=1_001) == 0

    assert probes == [(2468, 0)]


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
