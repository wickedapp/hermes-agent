from __future__ import annotations

import sqlite3
import threading
import argparse
import importlib.util
import json
import sys
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


@pytest.fixture
def boards(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    kb.init_db(board="default")
    kb.create_board("other")
    with kb.connect(board="default") as conn:
        task_id = kb.create_task(conn, title="linked task", created_by="tester")
    return task_id


def test_entity_link_migration_preserves_existing_data_and_integrity(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(kb.SCHEMA_SQL)
    conn.execute(
        "INSERT INTO tasks (id,title,status,created_at,workspace_kind) VALUES (?,?,?,?,?)",
        ("t_legacy", "legacy", "todo", 1, "scratch"),
    )
    conn.commit()
    conn.close()

    kb.init_db(db_path)
    with kb.connect(db_path) as migrated:
        assert migrated.execute("SELECT title FROM tasks WHERE id='t_legacy'").fetchone()[0] == "legacy"
        assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
        assert migrated.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert migrated.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='task_entity_links'"
        ).fetchone()


def test_attach_replay_conflict_validation_and_cascade(boards):
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="contact",
            external_id="00123", relation_role="primary", created_by="tester",
            idempotency_key="attach-1",
        )
        assert link.external_id == "00123"
        assert kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="contact",
            external_id="00123", relation_role="primary", created_by="tester",
            idempotency_key="attach-1",
        ).link_id == link.link_id
        with pytest.raises(kb.EntityLinkIdempotencyConflict):
            kb.attach_entity_link(
                conn, board_slug="default", task_id=boards, entity_type="lead",
                external_id="00123", relation_role="primary", created_by="tester",
                idempotency_key="attach-1",
            )
        with pytest.raises(kb.EntityLinkIdempotencyConflict):
            kb.attach_entity_link(
                conn, board_slug="default", task_id=boards, entity_type="contact",
                external_id="00123", relation_role="primary", created_by="other-actor",
                idempotency_key="attach-1",
            )
        for bad in ("0", "-1", "+1", " 1", "1 ", "1.0", "abc"):
            with pytest.raises(ValueError):
                kb.attach_entity_link(
                    conn, board_slug="default", task_id=boards, entity_type="contact",
                    external_id=bad, relation_role="context", created_by="tester",
                    idempotency_key=f"bad-{bad}",
                )
        conn.execute("DELETE FROM tasks WHERE id=?", (boards,))
        assert kb.list_entity_links(conn, board_slug="default", task_id=boards) == []


def test_concurrent_equivalent_attach_has_one_canonical_row(boards):
    barrier = threading.Barrier(6)
    ids: list[str] = []
    errors: list[BaseException] = []

    def worker():
        try:
            with kb.connect(board="default") as conn:
                barrier.wait(timeout=5)
                ids.append(kb.attach_entity_link(
                    conn, board_slug="default", task_id=boards,
                    entity_type="ticket", external_id="77", relation_role="context",
                    created_by="tester", idempotency_key="same-attach",
                ).link_id)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert errors == []
    assert len(set(ids)) == 1
    with kb.connect(board="default") as conn:
        assert len(kb.list_entity_links(conn, board_slug="default", task_id=boards)) == 1


def test_detach_is_local_idempotent_and_board_isolated(boards):
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="invoice",
            external_id="9", relation_role="result", created_by="tester",
            idempotency_key="attach-invoice",
        )
        with pytest.raises(ValueError):
            kb.list_entity_links(conn, board_slug="other", task_id=boards)
        with pytest.raises(ValueError):
            kb.get_entity_link(
                conn, board_slug="other", task_id=boards, link_id=link.link_id,
            )
        with pytest.raises(ValueError):
            kb.detach_entity_link(
                conn, board_slug="other", task_id=boards, link_id=link.link_id,
                actor="tester", idempotency_key="wrong-board-detach",
            )
        assert kb.detach_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
            actor="tester", idempotency_key="detach-1",
        ) is True
        with pytest.raises(kb.EntityLinkIdempotencyConflict):
            kb.detach_entity_link(
                conn, board_slug="default", task_id=boards, link_id=link.link_id,
                actor="other-actor", idempotency_key="detach-1",
            )
        assert kb.detach_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
            actor="tester", idempotency_key="detach-1",
        ) is True
        with pytest.raises(kb.EntityLinkIdempotencyConflict):
            kb.detach_entity_link(
                conn, board_slug="default", task_id=boards, link_id="el_other",
                actor="tester", idempotency_key="detach-1",
            )


def test_resolver_contract_is_bounded_and_never_changes_task_status(boards):
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="customer",
            external_id="42", relation_role="primary", created_by="tester",
            idempotency_key="attach-customer",
        )
        before = kb.get_task(conn, boards).status
        with pytest.raises(kb.EntityLinkResolutionError) as unavailable:
            kb.resolve_entity_link(
                conn, board_slug="default", task_id=boards, link_id=link.link_id,
            )
        assert unavailable.value.code == "capability_unavailable"
        assert kb.get_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
        ).verification_state == "unverified"

        def fake_resolver(entity_link, refresh):
            assert entity_link.link_id == link.link_id
            assert refresh is True
            return {
                "verification_state": "verified", "last_seen_version": "v1",
                "display_snapshot": {"label": "Acme", "status": "active"},
            }

        updated = kb.resolve_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
            refresh=True, resolver=fake_resolver,
        )
        assert updated.verification_state == "verified"
        assert updated.display_snapshot == {"label": "Acme", "status": "active"}
        assert kb.get_task(conn, boards).status == before
        with pytest.raises(kb.EntityLinkResolutionError) as mismatch:
            kb.resolve_entity_link(
                conn, board_slug="default", task_id=boards, link_id=link.link_id,
                resolver=lambda _link, _refresh: {
                    "verification_state": "verified",
                    "display_snapshot": {"token": "secret"},
                },
            )
        assert mismatch.value.code == "remote_contract_mismatch"


@pytest.mark.parametrize("sensitive", [
    "Authorization: Bearer abc.def", "confirmation token=123", "person@example.com",
    "+1 (202) 555-0199", "12 Main Street", "ticket body: private details",
    "document content: secret", "HTTP/1.1 200 raw response",
    "1600 Pennsylvania Ave NW, Washington DC 20500",
    "12 Main Rd, Springfield", "Alice, 221B Baker St, London",
    "secret-token-abcdef123456", "credential_nonce-abcdef123456",
    "Unit 4B, Example Building", "SW1A 1AA",
    "PO Box 123, Washington DC", "P.O. Box 456, London",
    "Post Office Box 789, Boston", "sk-live-abc123", "sk-test-abc123",
    "pk-live-abc123", "pk-test-abc123", "a" * 32,
    "0123456789abcdef0123456789abcdef",
    "AKIAIOSFODNN7EXAMPLE", "ASIAIOSFODNN7EXAMPLE",
    "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signature123",
    "aB3dE5fG7hJ9kL2mN4pQ6rS8tU0vW2xY_Z4cV6bN8",
])
def test_resolver_rejects_sensitive_snapshot_without_persisting_or_logging(
    boards, sensitive, caplog,
):
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="contact",
            external_id="91", relation_role="context", created_by="tester",
            idempotency_key="sensitive-seed",
        )
        with pytest.raises(kb.EntityLinkResolutionError) as exc:
            kb.resolve_entity_link(
                conn, board_slug="default", task_id=boards, link_id=link.link_id,
                resolver=lambda _link, _refresh: {
                    "verification_state": "verified",
                    "display_snapshot": {"label": sensitive},
                },
            )
        assert exc.value.code == "remote_contract_mismatch"
        stored = kb.get_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
        )
        assert stored.display_snapshot is None
        assert stored.verification_state == "unverified"
        event_dump = json.dumps([dict(row) for row in conn.execute(
            "SELECT kind,payload FROM task_events WHERE task_id=?", (boards,),
        )])
        assert sensitive not in event_dump
    assert sensitive not in caplog.text


def test_snapshot_string_length_boundary_accepts_31_and_rejects_32(boards):
    with kb.connect(board="default") as conn:
        accepted = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="lead",
            external_id="92", relation_role="context", created_by="tester",
            idempotency_key="length-boundary",
        )
        updated = kb.resolve_entity_link(
            conn, board_slug="default", task_id=boards, link_id=accepted.link_id,
            resolver=lambda _link, _refresh: {
                "verification_state": "verified",
                "display_snapshot": {"label": "x" * 31},
            },
        )
        assert updated.display_snapshot == {"label": "x" * 31}
        before_events = len(kb.list_events(conn, boards))
        with pytest.raises(kb.EntityLinkResolutionError):
            kb.resolve_entity_link(
                conn, board_slug="default", task_id=boards, link_id=accepted.link_id,
                resolver=lambda _link, _refresh: {
                    "verification_state": "verified",
                    "display_snapshot": {"label": "x" * 32},
                },
            )
        assert kb.get_entity_link(
            conn, board_slug="default", task_id=boards, link_id=accepted.link_id,
        ).display_snapshot == {"label": "x" * 31}
        assert len(kb.list_events(conn, boards)) == before_events


def _seed_same_task_on_other(task_id: str) -> None:
    with kb.connect(board="other") as conn:
        conn.execute(
            "INSERT INTO tasks (id,title,status,created_at,workspace_kind) VALUES (?,?,?,?,?)",
            (task_id, "other task", "todo", 1, "scratch"),
        )


def test_cli_entity_link_routes_stay_on_explicit_board(boards, capsys):
    from hermes_cli.kanban import kanban_command

    _seed_same_task_on_other(boards)
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="lead",
            external_id="55", relation_role="context", created_by="tester",
            idempotency_key="cli-seed",
        )
    base = dict(kanban_action="entity-links", task_id=boards, json=True)
    assert kanban_command(argparse.Namespace(
        **base, board="other", entity_links_action="list",
    )) == 0
    assert json.loads(capsys.readouterr().out) == []
    assert kanban_command(argparse.Namespace(
        **base, board="other", entity_links_action="get", link_id=link.link_id,
    )) == 1
    assert "not found" in capsys.readouterr().err
    assert kanban_command(argparse.Namespace(
        **base, board="other", entity_links_action="detach", link_id=link.link_id,
        actor="tester", idempotency_key="cli-detach-other",
    )) == 0
    capsys.readouterr()
    with kb.connect(board="default") as conn:
        assert kb.get_entity_link(
            conn, board_slug="default", task_id=boards, link_id=link.link_id,
        ) is not None
    assert kanban_command(argparse.Namespace(
        **base, board="default", entity_links_action="resolve",
        link_id=link.link_id, refresh=False,
    )) == 1
    assert "capability_unavailable" in capsys.readouterr().err


def test_structured_tools_preserve_worker_pin_for_entity_link_routes(boards, monkeypatch):
    from tools import kanban_tools as tools

    _seed_same_task_on_other(boards)
    with kb.connect(board="default") as conn:
        link = kb.attach_entity_link(
            conn, board_slug="default", task_id=boards, entity_type="ticket",
            external_id="88", relation_role="primary", created_by="tester",
            idempotency_key="tool-seed",
        )
    monkeypatch.setenv("HERMES_KANBAN_TASK", boards)
    monkeypatch.setenv("HERMES_KANBAN_BOARD", "default")
    for handler, extra in (
        (tools._handle_list_entity_links, {}),
        (tools._handle_get_entity_link, {"link_id": link.link_id}),
        (tools._handle_detach_entity_link, {
            "link_id": link.link_id, "idempotency_key": "tool-detach-other",
        }),
    ):
        result = json.loads(handler({"board": "other", "task_id": boards, **extra}))
        assert "pinned to board default" in result["error"]
    result = json.loads(tools._handle_get_entity_link({
        "board": "default", "task_id": boards, "link_id": link.link_id,
    }))
    assert result["ok"] is True
    unavailable = json.loads(tools._handle_resolve_entity_link({
        "board": "default", "task_id": boards, "link_id": link.link_id,
    }))
    assert "capability_unavailable" in unavailable["error"]
    assert unavailable["code"] == "capability_unavailable"


def test_dashboard_entity_link_routes_are_board_isolated(boards):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    _seed_same_task_on_other(boards)
    plugin_file = Path(__file__).resolve().parents[2] / "plugins/kanban/dashboard/plugin_api.py"
    spec = importlib.util.spec_from_file_location("entity_link_dashboard_test", plugin_file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    app = FastAPI()
    app.include_router(module.router, prefix="/api/plugins/kanban")
    client = TestClient(app)
    attached = client.post(
        f"/api/plugins/kanban/tasks/{boards}/entity-links?board=default",
        json={"entity_type": "invoice", "external_id": "101",
              "relation_role": "result", "idempotency_key": "api-attach"},
    )
    assert attached.status_code == 200
    link_id = attached.json()["link"]["link_id"]
    listed = client.get(f"/api/plugins/kanban/tasks/{boards}/entity-links?board=other")
    assert listed.status_code == 200 and listed.json()["links"] == []
    assert client.get(
        f"/api/plugins/kanban/tasks/{boards}/entity-links/{link_id}?board=other"
    ).status_code == 404
    detached = client.post(
        f"/api/plugins/kanban/tasks/{boards}/entity-links/{link_id}/detach?board=other",
        json={"idempotency_key": "api-detach-other"},
    )
    assert detached.status_code == 200
    assert client.get(
        f"/api/plugins/kanban/tasks/{boards}/entity-links/{link_id}?board=default"
    ).status_code == 200
    unavailable = client.post(
        f"/api/plugins/kanban/tasks/{boards}/entity-links/{link_id}/resolve?board=default",
        json={"refresh": False},
    )
    assert unavailable.status_code == 501
    assert unavailable.json()["detail"]["code"] == "capability_unavailable"
