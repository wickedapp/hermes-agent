-- Pre-entity-link Kanban schema snapshot from origin/main at a7cd254c2.
-- Keep this fixture independent from hermes_cli.kanban_db.SCHEMA_SQL so the
-- migration test exercises an actual old database rather than the new schema.
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    body TEXT,
    assignee TEXT,
    status TEXT NOT NULL,
    priority INTEGER DEFAULT 0,
    created_by TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    workspace_kind TEXT NOT NULL DEFAULT 'scratch',
    workspace_path TEXT,
    branch_name TEXT,
    claim_lock TEXT,
    claim_expires INTEGER,
    tenant TEXT,
    result TEXT,
    idempotency_key TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    worker_pid INTEGER,
    last_failure_error TEXT,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    current_run_id INTEGER,
    workflow_template_id TEXT,
    current_step_key TEXT,
    skills TEXT,
    model_override TEXT,
    max_retries INTEGER,
    session_id TEXT
);

CREATE TABLE IF NOT EXISTS task_links (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);

CREATE TABLE IF NOT EXISTS task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    author TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    run_id INTEGER,
    kind TEXT NOT NULL,
    payload TEXT,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS task_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    profile TEXT,
    step_key TEXT,
    status TEXT NOT NULL,
    claim_lock TEXT,
    claim_expires INTEGER,
    worker_pid INTEGER,
    max_runtime_seconds INTEGER,
    last_heartbeat_at INTEGER,
    started_at INTEGER NOT NULL,
    ended_at INTEGER,
    outcome TEXT,
    summary TEXT,
    metadata TEXT,
    error TEXT
);

CREATE TABLE IF NOT EXISTS kanban_notify_subs (
    task_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    thread_id TEXT NOT NULL DEFAULT '',
    user_id TEXT,
    notifier_profile TEXT,
    created_at INTEGER NOT NULL,
    last_event_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (task_id, platform, chat_id, thread_id)
);

CREATE INDEX IF NOT EXISTS idx_tasks_assignee_status ON tasks(assignee, status);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_links_child ON task_links(child_id);
CREATE INDEX IF NOT EXISTS idx_links_parent ON task_links(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_task ON task_comments(task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events(task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_runs_task ON task_runs(task_id, started_at);
CREATE INDEX IF NOT EXISTS idx_runs_status ON task_runs(status);
CREATE INDEX IF NOT EXISTS idx_notify_task ON kanban_notify_subs(task_id);
