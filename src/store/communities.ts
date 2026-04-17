/**
 * graph-memory — Community summary CRUD
 *
 * SRP: persist LLM-generated community descriptions and their embeddings.
 * Detection and summarization logic lives in src/graph/community.ts;
 * vector-based community search lives in search.ts.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";

export interface CommunitySummary {
  id: string;
  summary: string;
  nodeCount: number;
  createdAt: number;
  updatedAt: number;
}

export function upsertCommunitySummary(
  db: DatabaseSyncInstance, id: string, summary: string, nodeCount: number, embedding?: number[],
): void {
  const now = Date.now();
  const blob = embedding ? new Uint8Array(new Float32Array(embedding).buffer) : null;
  const ex = db.prepare("SELECT id FROM gm_communities WHERE id=?").get(id) as any;
  if (ex) {
    if (blob) {
      db.prepare("UPDATE gm_communities SET summary=?, node_count=?, embedding=?, updated_at=? WHERE id=?")
        .run(summary, nodeCount, blob, now, id);
    } else {
      db.prepare("UPDATE gm_communities SET summary=?, node_count=?, updated_at=? WHERE id=?")
        .run(summary, nodeCount, now, id);
    }
  } else {
    db.prepare("INSERT INTO gm_communities (id, summary, node_count, embedding, created_at, updated_at) VALUES (?,?,?,?,?,?)")
      .run(id, summary, nodeCount, blob, now, now);
  }
}

export function getCommunitySummary(db: DatabaseSyncInstance, id: string): CommunitySummary | null {
  const r = db.prepare("SELECT * FROM gm_communities WHERE id=?").get(id) as any;
  if (!r) return null;
  return { id: r.id, summary: r.summary, nodeCount: r.node_count, createdAt: r.created_at, updatedAt: r.updated_at };
}

export function getAllCommunitySummaries(db: DatabaseSyncInstance): CommunitySummary[] {
  return (db.prepare("SELECT * FROM gm_communities ORDER BY node_count DESC").all() as any[])
    .map(r => ({ id: r.id, summary: r.summary, nodeCount: r.node_count, createdAt: r.created_at, updatedAt: r.updated_at }));
}

/** 清除已不存在的社区描述 */
export function pruneCommunitySummaries(db: DatabaseSyncInstance): number {
  const result = db.prepare(`
    DELETE FROM gm_communities WHERE id NOT IN (
      SELECT DISTINCT community_id FROM gm_nodes WHERE community_id IS NOT NULL AND status='active'
    )
  `).run();
  return result.changes;
}
