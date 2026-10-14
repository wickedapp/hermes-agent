from __future__ import annotations

import sqlite3
from pathlib import Path

from hermes_cli import kanban_db as kb


def test_entity_link_migration_preserves_every_task_and_integrity(tmp_path):
    """RED on the pre-feature schema; green only after the additive migration."""
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    schema_fixture = (
        Path(__file__).parents[1]
        / "fixtures"
        / "kanban"
        / "pre_entity_links_schema.sql"
    )
    conn.executescript(schema_fixture.read_text(encoding="utf-8"))
    legacy_tasks = [
        ("t_legacy_1", "legacy one", "todo", 1, "scratch"),
        ("t_legacy_2", "legacy two", "done", 2, "scratch"),
    ]
    conn.executemany(
        "INSERT INTO tasks (id,title,status,created_at,workspace_kind) VALUES (?,?,?,?,?)",
        legacy_tasks,
    )
    conn.commit()
    conn.close()

    kb.init_db(db_path)
    kb.init_db(db_path)
    with kb.connect(db_path) as migrated:
        assert [tuple(row) for row in migrated.execute(
            "SELECT id,title,status,created_at,workspace_kind FROM tasks ORDER BY id"
        )] == legacy_tasks
        assert migrated.execute("PRAGMA foreign_key_check").fetchall() == []
        assert migrated.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert migrated.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='task_entity_links'"
        ).fetchone()
