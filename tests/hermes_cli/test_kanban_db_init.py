from __future__ import annotations

import threading
import sqlite3
from pathlib import Path

import pytest

from hermes_cli import kanban_db as kb


def test_connect_initialization_is_thread_safe(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)

    db_path = kb.kanban_db_path(board="default")
    kb._INITIALIZED_PATHS.discard(str(db_path.resolve()))

    errors: list[BaseException] = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        try:
            barrier.wait(timeout=5)
            conn = kb.connect(board="default")
            conn.close()
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert errors == []
    with kb.connect(board="default") as conn:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(tasks)")}
    assert "max_retries" in cols
    with kb.connect(board="default") as conn:
        assert conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='task_entity_links'"
        ).fetchone()


def test_schema_and_additive_migrations_roll_back_together(tmp_path, monkeypatch):
    db_path = tmp_path / "rollback.db"
    original = kb._migrate_add_optional_columns

    def fail_after_migration(conn):
        original(conn)
        raise RuntimeError("forced migration failure")

    monkeypatch.setattr(kb, "_migrate_add_optional_columns", fail_after_migration)
    with pytest.raises(RuntimeError, match="forced migration failure"):
        kb.connect(db_path)

    raw = sqlite3.connect(db_path)
    try:
        assert raw.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='tasks'"
        ).fetchone() is None
        assert raw.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    finally:
        raw.close()
