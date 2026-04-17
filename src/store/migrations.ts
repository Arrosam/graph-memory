/**
 * graph-memory — DB migrations
 *
 * SRP: schema-only module. Each migration creates/alters tables for a single
 * feature area. `migrate()` is the only public entry point; connection
 * management lives in db.ts.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";

export function migrate(db: DatabaseSyncInstance): void {
  db.exec(`CREATE TABLE IF NOT EXISTS _migrations (v INTEGER PRIMARY KEY, at INTEGER NOT NULL)`);
  const cur = (db.prepare("SELECT MAX(v) as v FROM _migrations").get() as any)?.v ?? 0;
  const steps = [m1_core, m2_messages, m3_signals, m4_fts5, m5_vectors, m6_communities, m7_node_sessions];
  for (let i = cur; i < steps.length; i++) {
    steps[i](db);
    db.prepare("INSERT INTO _migrations (v,at) VALUES (?,?)").run(i + 1, Date.now());
  }
}

// ─── 核心表：节点 + 边 ──────────────────────────────────────

function m1_core(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_nodes (
      id              TEXT PRIMARY KEY,
      type            TEXT NOT NULL CHECK(type IN ('TASK','SKILL','EVENT')),
      name            TEXT NOT NULL,
      description     TEXT NOT NULL DEFAULT '',
      content         TEXT NOT NULL,
      status          TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','deprecated')),
      validated_count INTEGER NOT NULL DEFAULT 1,
      source_sessions TEXT NOT NULL DEFAULT '[]',
      community_id    TEXT,
      pagerank        REAL NOT NULL DEFAULT 0,
      created_at      INTEGER NOT NULL,
      updated_at      INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_gm_nodes_name ON gm_nodes(name);
    CREATE INDEX IF NOT EXISTS ix_gm_nodes_type_status ON gm_nodes(type, status);
    CREATE INDEX IF NOT EXISTS ix_gm_nodes_community ON gm_nodes(community_id);

    CREATE TABLE IF NOT EXISTS gm_edges (
      id          TEXT PRIMARY KEY,
      from_id     TEXT NOT NULL REFERENCES gm_nodes(id),
      to_id       TEXT NOT NULL REFERENCES gm_nodes(id),
      type        TEXT NOT NULL CHECK(type IN ('USED_SKILL','SOLVED_BY','REQUIRES','PATCHES','CONFLICTS_WITH')),
      instruction TEXT NOT NULL,
      condition   TEXT,
      session_id  TEXT NOT NULL,
      created_at  INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ix_gm_edges_from ON gm_edges(from_id);
    CREATE INDEX IF NOT EXISTS ix_gm_edges_to   ON gm_edges(to_id);
  `);
}

// ─── 消息存储 ────────────────────────────────────────────────

function m2_messages(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_messages (
      id          TEXT PRIMARY KEY,
      session_id  TEXT NOT NULL,
      turn_index  INTEGER NOT NULL,
      role        TEXT NOT NULL,
      content     TEXT NOT NULL,
      extracted   INTEGER NOT NULL DEFAULT 0,
      created_at  INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ix_gm_msg_session ON gm_messages(session_id, turn_index);
  `);
}

// ─── 信号存储 ────────────────────────────────────────────────

function m3_signals(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_signals (
      id          TEXT PRIMARY KEY,
      session_id  TEXT NOT NULL,
      turn_index  INTEGER NOT NULL,
      type        TEXT NOT NULL,
      data        TEXT NOT NULL DEFAULT '{}',
      processed   INTEGER NOT NULL DEFAULT 0,
      created_at  INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS ix_gm_sig_session ON gm_signals(session_id, processed);
  `);
}

// ─── FTS5 全文索引 ───────────────────────────────────────────

function m4_fts5(db: DatabaseSyncInstance): void {
  try {
    db.exec(`
      CREATE VIRTUAL TABLE IF NOT EXISTS gm_nodes_fts USING fts5(
        name,
        description,
        content,
        content=gm_nodes,
        content_rowid=rowid
      );
    `);
    db.exec(`
      CREATE TRIGGER IF NOT EXISTS gm_nodes_ai AFTER INSERT ON gm_nodes BEGIN
        INSERT INTO gm_nodes_fts(rowid, name, description, content)
        VALUES (NEW.rowid, NEW.name, NEW.description, NEW.content);
      END;
      CREATE TRIGGER IF NOT EXISTS gm_nodes_ad AFTER DELETE ON gm_nodes BEGIN
        INSERT INTO gm_nodes_fts(gm_nodes_fts, rowid, name, description, content)
        VALUES ('delete', OLD.rowid, OLD.name, OLD.description, OLD.content);
      END;
      CREATE TRIGGER IF NOT EXISTS gm_nodes_au AFTER UPDATE ON gm_nodes BEGIN
        INSERT INTO gm_nodes_fts(gm_nodes_fts, rowid, name, description, content)
        VALUES ('delete', OLD.rowid, OLD.name, OLD.description, OLD.content);
        INSERT INTO gm_nodes_fts(rowid, name, description, content)
        VALUES (NEW.rowid, NEW.name, NEW.description, NEW.content);
      END;
    `);
  } catch {
    // FTS5 不可用时静默降级到 LIKE 搜索
  }
}

// ─── 向量存储 ────────────────────────────────────────────────

function m5_vectors(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_vectors (
      node_id      TEXT PRIMARY KEY REFERENCES gm_nodes(id),
      content_hash TEXT NOT NULL,
      embedding    BLOB NOT NULL
    );
  `);
}

// ─── 社区描述存储 ────────────────────────────────────────────

function m6_communities(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_communities (
      id          TEXT PRIMARY KEY,
      summary     TEXT NOT NULL,
      node_count  INTEGER NOT NULL DEFAULT 0,
      embedding   BLOB,
      created_at  INTEGER NOT NULL,
      updated_at  INTEGER NOT NULL
    );
  `);
}

// ─── Node↔Session 索引表 ────────────────────────────────────
//
// 之前 getBySession 要扫 gm_nodes 并 json_each(source_sessions)——没索引，
// 节点数一大就线性变慢。这张表把关系正规化，给 session_id 加索引，查询
// 变成 index lookup。source_sessions JSON 字段保留作为权威存储，这张表
// 由 upsertNode / mergeNodes 同步维护 + 迁移时首次回填。

function m7_node_sessions(db: DatabaseSyncInstance): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS gm_node_sessions (
      node_id    TEXT NOT NULL REFERENCES gm_nodes(id) ON DELETE CASCADE,
      session_id TEXT NOT NULL,
      PRIMARY KEY (node_id, session_id)
    );
    CREATE INDEX IF NOT EXISTS ix_gm_node_sessions_session ON gm_node_sessions(session_id);
  `);

  // Backfill from the existing JSON column.
  const rows = db.prepare("SELECT id, source_sessions FROM gm_nodes").all() as Array<{
    id: string; source_sessions: string;
  }>;
  const insert = db.prepare(
    "INSERT OR IGNORE INTO gm_node_sessions (node_id, session_id) VALUES (?, ?)",
  );
  db.exec("BEGIN");
  try {
    for (const r of rows) {
      let sessions: string[] = [];
      try { sessions = JSON.parse(r.source_sessions ?? "[]"); } catch { /* skip bad JSON */ }
      for (const sid of sessions) {
        if (typeof sid === "string" && sid) insert.run(r.id, sid);
      }
    }
    db.exec("COMMIT");
  } catch (err) {
    db.exec("ROLLBACK");
    throw err;
  }
}
