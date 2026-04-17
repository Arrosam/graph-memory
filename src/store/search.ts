/**
 * graph-memory — Read paths: full-text, graph walk, vector search
 *
 * SRP: all read-side query functions that return nodes/edges for recall.
 * Writes (CRUD) live in nodes.ts / vectors.ts / communities.ts.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import type { GmNode, GmEdge } from "../types.ts";
import { toNode, toEdge } from "./common.ts";

// ─── FTS5 能力探测 ───────────────────────────────────────────

const _fts5Cache = new WeakMap<DatabaseSyncInstance, boolean>();

function fts5Available(db: DatabaseSyncInstance): boolean {
  const cached = _fts5Cache.get(db);
  if (cached !== undefined) return cached;
  let result: boolean;
  try {
    db.prepare("SELECT * FROM gm_nodes_fts LIMIT 0").all();
    result = true;
  } catch {
    result = false;
  }
  _fts5Cache.set(db, result);
  return result;
}

// ─── 关键词搜索 ──────────────────────────────────────────────

export function searchNodes(db: DatabaseSyncInstance, query: string, limit = 6): GmNode[] {
  const terms = query.trim().split(/\s+/).filter(Boolean).slice(0, 8);
  if (!terms.length) return topNodes(db, limit);

  if (fts5Available(db)) {
    try {
      const ftsQuery = terms.map(t => `"${t.replace(/"/g, "")}"`).join(" OR ");
      const rows = db.prepare(`
        SELECT n.*, rank FROM gm_nodes_fts fts
        JOIN gm_nodes n ON n.rowid = fts.rowid
        WHERE gm_nodes_fts MATCH ? AND n.status = 'active'
        ORDER BY rank LIMIT ?
      `).all(ftsQuery, limit) as any[];
      if (rows.length > 0) return rows.map(toNode);
    } catch { /* FTS 查询失败，降级 */ }
  }

  const where = terms.map(() => "(name LIKE ? OR description LIKE ? OR content LIKE ?)").join(" OR ");
  const likes = terms.flatMap(t => [`%${t}%`, `%${t}%`, `%${t}%`]);
  return (db.prepare(`
    SELECT * FROM gm_nodes WHERE status='active' AND (${where})
    ORDER BY pagerank DESC, validated_count DESC, updated_at DESC LIMIT ?
  `).all(...likes, limit) as any[]).map(toNode);
}

/** 热门节点：综合 pagerank + validatedCount 排序 */
export function topNodes(db: DatabaseSyncInstance, limit = 6): GmNode[] {
  return (db.prepare(`
    SELECT * FROM gm_nodes WHERE status='active'
    ORDER BY pagerank DESC, validated_count DESC, updated_at DESC LIMIT ?
  `).all(limit) as any[]).map(toNode);
}

// ─── 递归 CTE 图遍历 ────────────────────────────────────────

export function graphWalk(
  db: DatabaseSyncInstance,
  seedIds: string[],
  maxDepth: number,
): { nodes: GmNode[]; edges: GmEdge[] } {
  if (!seedIds.length) return { nodes: [], edges: [] };

  const placeholders = seedIds.map(() => "?").join(",");

  const walkRows = db.prepare(`
    WITH RECURSIVE walk(node_id, depth) AS (
      SELECT id, 0 FROM gm_nodes WHERE id IN (${placeholders}) AND status='active'
      UNION
      SELECT
        CASE WHEN e.from_id = w.node_id THEN e.to_id ELSE e.from_id END,
        w.depth + 1
      FROM walk w
      JOIN gm_edges e ON (e.from_id = w.node_id OR e.to_id = w.node_id)
      WHERE w.depth < ?
    )
    SELECT DISTINCT node_id FROM walk
  `).all(...seedIds, maxDepth) as any[];

  const nodeIds = walkRows.map((r: any) => r.node_id);
  if (!nodeIds.length) return { nodes: [], edges: [] };

  const np = nodeIds.map(() => "?").join(",");
  const nodes = (db.prepare(`
    SELECT * FROM gm_nodes WHERE id IN (${np}) AND status='active'
  `).all(...nodeIds) as any[]).map(toNode);

  const edges = (db.prepare(`
    SELECT * FROM gm_edges WHERE from_id IN (${np}) AND to_id IN (${np})
  `).all(...nodeIds, ...nodeIds) as any[]).map(toEdge);

  return { nodes, edges };
}

// ─── 向量搜索 ────────────────────────────────────────────────

export type ScoredNode = { node: GmNode; score: number };

export function vectorSearchWithScore(db: DatabaseSyncInstance, queryVec: number[], limit: number, minScore = 0.35): ScoredNode[] {
  const rows = db.prepare(`
    SELECT v.node_id, v.embedding, n.*
    FROM gm_vectors v JOIN gm_nodes n ON n.id = v.node_id
    WHERE n.status = 'active'
  `).all() as any[];

  if (!rows.length) return [];

  const q = new Float32Array(queryVec);
  const qNorm = Math.sqrt(q.reduce((s, x) => s + x * x, 0));
  if (qNorm === 0) return [];

  return rows
    .map(row => {
      const raw = row.embedding as Uint8Array;
      const v = new Float32Array(raw.buffer, raw.byteOffset, raw.byteLength / 4);
      let dot = 0, vNorm = 0;
      const len = Math.min(v.length, q.length);
      for (let i = 0; i < len; i++) {
        dot += v[i] * q[i];
        vNorm += v[i] * v[i];
      }
      return { score: dot / (Math.sqrt(vNorm) * qNorm + 1e-9), node: toNode(row) };
    })
    .filter(s => s.score > minScore)
    .sort((a, b) => b.score - a.score)
    .slice(0, limit);
}

/** 兼容旧接口 */
export function vectorSearch(db: DatabaseSyncInstance, queryVec: number[], limit: number, minScore = 0.35): GmNode[] {
  return vectorSearchWithScore(db, queryVec, limit, minScore).map(s => s.node);
}

// ─── 社区相关查询 ────────────────────────────────────────────

export type ScoredCommunity = { id: string; summary: string; score: number; nodeCount: number };

/**
 * 社区代表节点：每个社区取最近更新的 topN 个节点
 * 用于泛化召回 —— 用户问"做了哪些工作"时按领域返回概览
 */
export function communityRepresentatives(db: DatabaseSyncInstance, perCommunity = 2): GmNode[] {
  const rows = db.prepare(`
    SELECT * FROM gm_nodes
    WHERE status = 'active' AND community_id IS NOT NULL
    ORDER BY community_id, updated_at DESC
  `).all() as any[];

  const byCommunity = new Map<string, GmNode[]>();
  for (const r of rows) {
    const node = toNode(r);
    const cid = r.community_id as string;
    if (!byCommunity.has(cid)) byCommunity.set(cid, []);
    const list = byCommunity.get(cid)!;
    if (list.length < perCommunity) list.push(node);
  }

  // 社区按最新更新时间排序
  const communities = Array.from(byCommunity.entries())
    .sort((a, b) => {
      const aTime = Math.max(...a[1].map(n => n.updatedAt));
      const bTime = Math.max(...b[1].map(n => n.updatedAt));
      return bTime - aTime;
    });

  const result: GmNode[] = [];
  for (const [, nodes] of communities) {
    result.push(...nodes);
  }
  return result;
}

/**
 * 社区向量搜索：用 query 向量匹配社区 embedding，返回按相似度排序的社区
 */
export function communityVectorSearch(db: DatabaseSyncInstance, queryVec: number[], minScore = 0.15): ScoredCommunity[] {
  const rows = db.prepare(
    "SELECT id, summary, node_count, embedding FROM gm_communities WHERE embedding IS NOT NULL"
  ).all() as any[];

  if (!rows.length) return [];

  const q = new Float32Array(queryVec);
  const qNorm = Math.sqrt(q.reduce((s, x) => s + x * x, 0));
  if (qNorm === 0) return [];

  return rows
    .map(r => {
      const raw = r.embedding as Uint8Array;
      const v = new Float32Array(raw.buffer, raw.byteOffset, raw.byteLength / 4);
      let dot = 0, vNorm = 0;
      const len = Math.min(v.length, q.length);
      for (let i = 0; i < len; i++) {
        dot += v[i] * q[i];
        vNorm += v[i] * v[i];
      }
      return {
        id: r.id as string,
        summary: r.summary as string,
        score: dot / (Math.sqrt(vNorm) * qNorm + 1e-9),
        nodeCount: r.node_count as number,
      };
    })
    .filter(s => s.score > minScore)
    .sort((a, b) => b.score - a.score);
}

/**
 * 按社区 ID 列表获取成员节点（按时间倒序）
 */
export function nodesByCommunityIds(db: DatabaseSyncInstance, communityIds: string[], perCommunity = 3): GmNode[] {
  if (!communityIds.length) return [];
  const placeholders = communityIds.map(() => "?").join(",");
  const rows = db.prepare(`
    SELECT * FROM gm_nodes
    WHERE community_id IN (${placeholders}) AND status='active'
    ORDER BY community_id, updated_at DESC
  `).all(...communityIds) as any[];

  const byCommunity = new Map<string, GmNode[]>();
  for (const r of rows) {
    const node = toNode(r);
    const cid = r.community_id as string;
    if (!byCommunity.has(cid)) byCommunity.set(cid, []);
    const list = byCommunity.get(cid)!;
    if (list.length < perCommunity) list.push(node);
  }

  const result: GmNode[] = [];
  for (const cid of communityIds) {
    const members = byCommunity.get(cid);
    if (members) result.push(...members);
  }
  return result;
}
