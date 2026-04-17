/**
 * graph-memory — Node + Edge CRUD
 *
 * SRP: persist/read nodes and edges, plus session-scoped node lookup. Search
 * paths (FTS, graph walks, vector search) live in search.ts.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import type { GmNode, GmEdge, NodeType, EdgeType } from "../types.ts";
import { uid, toNode, toEdge, normalizeName } from "./common.ts";

// ─── 节点读取 ────────────────────────────────────────────────

export function findByName(db: DatabaseSyncInstance, name: string): GmNode | null {
  const r = db.prepare("SELECT * FROM gm_nodes WHERE name = ?").get(normalizeName(name)) as any;
  return r ? toNode(r) : null;
}

export function findById(db: DatabaseSyncInstance, id: string): GmNode | null {
  const r = db.prepare("SELECT * FROM gm_nodes WHERE id = ?").get(id) as any;
  return r ? toNode(r) : null;
}

export function allActiveNodes(db: DatabaseSyncInstance): GmNode[] {
  return (db.prepare("SELECT * FROM gm_nodes WHERE status='active'").all() as any[]).map(toNode);
}

export function allEdges(db: DatabaseSyncInstance): GmEdge[] {
  return (db.prepare("SELECT * FROM gm_edges").all() as any[]).map(toEdge);
}

// ─── 节点写入 ────────────────────────────────────────────────

export function upsertNode(
  db: DatabaseSyncInstance,
  c: { type: NodeType; name: string; description: string; content: string },
  sessionId: string,
): { node: GmNode; isNew: boolean } {
  const name = normalizeName(c.name);
  const ex = findByName(db, name);

  if (ex) {
    const sessions = JSON.stringify(Array.from(new Set([...ex.sourceSessions, sessionId])));
    const content = c.content.length > ex.content.length ? c.content : ex.content;
    const desc = c.description.length > ex.description.length ? c.description : ex.description;
    const count = ex.validatedCount + 1;
    db.prepare(`UPDATE gm_nodes SET content=?, description=?, validated_count=?,
      source_sessions=?, updated_at=? WHERE id=?`)
      .run(content, desc, count, sessions, Date.now(), ex.id);
    db.prepare("INSERT OR IGNORE INTO gm_node_sessions (node_id, session_id) VALUES (?, ?)")
      .run(ex.id, sessionId);
    return { node: { ...ex, content, description: desc, validatedCount: count }, isNew: false };
  }

  const id = uid("n");
  db.prepare(`INSERT INTO gm_nodes
    (id, type, name, description, content, status, validated_count, source_sessions, created_at, updated_at)
    VALUES (?,?,?,?,?,'active',1,?,?,?)`)
    .run(id, c.type, name, c.description, c.content, JSON.stringify([sessionId]), Date.now(), Date.now());
  db.prepare("INSERT OR IGNORE INTO gm_node_sessions (node_id, session_id) VALUES (?, ?)")
    .run(id, sessionId);
  return { node: findByName(db, name)!, isNew: true };
}

export function deprecate(db: DatabaseSyncInstance, nodeId: string): void {
  db.prepare("UPDATE gm_nodes SET status='deprecated', updated_at=? WHERE id=?")
    .run(Date.now(), nodeId);
}

/** 合并两个节点：keepId 保留，mergeId 标记 deprecated，边迁移 */
export function mergeNodes(db: DatabaseSyncInstance, keepId: string, mergeId: string): void {
  const keep = findById(db, keepId);
  const merge = findById(db, mergeId);
  if (!keep || !merge) return;

  // 合并 validatedCount + sourceSessions
  const sessions = JSON.stringify(
    Array.from(new Set([...keep.sourceSessions, ...merge.sourceSessions]))
  );
  const count = keep.validatedCount + merge.validatedCount;
  const content = keep.content.length >= merge.content.length ? keep.content : merge.content;
  const desc = keep.description.length >= merge.description.length ? keep.description : merge.description;

  db.prepare(`UPDATE gm_nodes SET content=?, description=?, validated_count=?,
    source_sessions=?, updated_at=? WHERE id=?`)
    .run(content, desc, count, sessions, Date.now(), keepId);

  // 同步 gm_node_sessions：把 merge 的所有关联移到 keep，再删 merge 的。
  db.prepare("INSERT OR IGNORE INTO gm_node_sessions (node_id, session_id) SELECT ?, session_id FROM gm_node_sessions WHERE node_id=?")
    .run(keepId, mergeId);
  db.prepare("DELETE FROM gm_node_sessions WHERE node_id=?").run(mergeId);

  // 迁移边：mergeId 的边指向 keepId
  db.prepare("UPDATE gm_edges SET from_id=? WHERE from_id=?").run(keepId, mergeId);
  db.prepare("UPDATE gm_edges SET to_id=? WHERE to_id=?").run(keepId, mergeId);

  // 删除自环（合并后可能出现 keepId → keepId）
  db.prepare("DELETE FROM gm_edges WHERE from_id = to_id").run();

  // 删除重复边（同 from+to+type 只保留一条）
  db.prepare(`
    DELETE FROM gm_edges WHERE id NOT IN (
      SELECT MIN(id) FROM gm_edges GROUP BY from_id, to_id, type
    )
  `).run();

  deprecate(db, mergeId);
}

/** 批量更新 PageRank 分数 */
export function updatePageranks(db: DatabaseSyncInstance, scores: Map<string, number>): void {
  const stmt = db.prepare("UPDATE gm_nodes SET pagerank=? WHERE id=?");
  db.exec("BEGIN");
  try {
    for (const [id, score] of scores) {
      stmt.run(score, id);
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

/** 批量更新社区 ID */
export function updateCommunities(db: DatabaseSyncInstance, labels: Map<string, string>): void {
  const stmt = db.prepare("UPDATE gm_nodes SET community_id=? WHERE id=?");
  db.exec("BEGIN");
  try {
    for (const [id, cid] of labels) {
      stmt.run(cid, id);
    }
    db.exec("COMMIT");
  } catch (e) {
    db.exec("ROLLBACK");
    throw e;
  }
}

// ─── 边 CRUD ─────────────────────────────────────────────────

export function upsertEdge(
  db: DatabaseSyncInstance,
  e: { fromId: string; toId: string; type: EdgeType; instruction: string; condition?: string; sessionId: string },
): void {
  const ex = db.prepare("SELECT id FROM gm_edges WHERE from_id=? AND to_id=? AND type=?")
    .get(e.fromId, e.toId, e.type) as any;
  if (ex) {
    db.prepare("UPDATE gm_edges SET instruction=? WHERE id=?")
      .run(e.instruction, ex.id);
    return;
  }
  db.prepare(`INSERT INTO gm_edges (id, from_id, to_id, type, instruction, condition, session_id, created_at)
    VALUES (?,?,?,?,?,?,?,?)`)
    .run(uid("e"), e.fromId, e.toId, e.type, e.instruction, e.condition ?? null, e.sessionId, Date.now());
}

export function edgesFrom(db: DatabaseSyncInstance, id: string): GmEdge[] {
  return (db.prepare("SELECT * FROM gm_edges WHERE from_id=?").all(id) as any[]).map(toEdge);
}

export function edgesTo(db: DatabaseSyncInstance, id: string): GmEdge[] {
  return (db.prepare("SELECT * FROM gm_edges WHERE to_id=?").all(id) as any[]).map(toEdge);
}

// ─── 按 session 查询 ────────────────────────────────────────

export function getBySession(db: DatabaseSyncInstance, sessionId: string): GmNode[] {
  // Uses the gm_node_sessions index (session_id → node_id) instead of scanning
  // gm_nodes + json_each. O(matches) instead of O(nodes * avg_sessions_per_node).
  return (db.prepare(`
    SELECT n.* FROM gm_nodes n
    JOIN gm_node_sessions s ON s.node_id = n.id
    WHERE s.session_id = ? AND n.status = 'active'
  `).all(sessionId) as any[]).map(toNode);
}
