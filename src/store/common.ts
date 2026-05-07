/**
 * graph-memory — Store-internal helpers
 *
 * SRP: row→type mappers, ID generation, name normalization, and aggregate
 * stats. Shared across the store/* modules.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import { randomUUID } from "crypto";
import type { GmNode, GmEdge } from "../types.ts";

export function uid(p: string): string {
  return `${p}-${randomUUID()}`;
}

export function toNode(r: any): GmNode {
  return {
    id: r.id, type: r.type, name: r.name,
    description: r.description ?? "", content: r.content,
    status: r.status, validatedCount: r.validated_count,
    sourceSessions: JSON.parse(r.source_sessions ?? "[]"),
    communityId: r.community_id ?? null,
    pagerank: r.pagerank ?? 0,
    createdAt: r.created_at, updatedAt: r.updated_at,
  };
}

export function toEdge(r: any): GmEdge {
  return {
    id: r.id, fromId: r.from_id, toId: r.to_id, type: r.type,
    instruction: r.instruction, condition: r.condition ?? undefined,
    sessionId: r.session_id, createdAt: r.created_at,
  };
}

/** Split an array into fixed-size chunks. Useful for SQL `IN (...)` queries
 *  where the parameter list could exceed SQLITE_MAX_VARIABLE_NUMBER (32766
 *  on modern builds, 999 on older ones). */
export function chunked<T>(arr: T[], size = 500): T[][] {
  if (arr.length <= size) return [arr];
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/** 标准化 name：全小写，空格转连字符。
 *  保留所有 Unicode 字母 / 数字（Latin、CJK、Hiragana/Katakana、Hangul、
 *  CJK Extension A 等），避免日韩或扩展区中文被剥光导致节点名退化为空串。 */
export function normalizeName(name: string): string {
  return name.trim().toLowerCase()
    .replace(/[\s_]+/g, "-")
    .replace(/[^\p{L}\p{N}\-]/gu, "")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "");
}

/** 综合统计：节点数 / 边数 / 社区数，带按 type 分桶。 */
export function getStats(db: DatabaseSyncInstance): {
  totalNodes: number;
  byType: Record<string, number>;
  totalEdges: number;
  byEdgeType: Record<string, number>;
  communities: number;
} {
  const totalNodes = (db.prepare("SELECT COUNT(*) as c FROM gm_nodes WHERE status='active'").get() as any).c;
  const byType: Record<string, number> = {};
  for (const r of db.prepare("SELECT type, COUNT(*) as c FROM gm_nodes WHERE status='active' GROUP BY type").all() as any[]) {
    byType[r.type] = r.c;
  }
  const totalEdges = (db.prepare("SELECT COUNT(*) as c FROM gm_edges").get() as any).c;
  const byEdgeType: Record<string, number> = {};
  for (const r of db.prepare("SELECT type, COUNT(*) as c FROM gm_edges GROUP BY type").all() as any[]) {
    byEdgeType[r.type] = r.c;
  }
  const communities = (db.prepare(
    "SELECT COUNT(DISTINCT community_id) as c FROM gm_nodes WHERE status='active' AND community_id IS NOT NULL"
  ).get() as any).c;
  return { totalNodes, byType, totalEdges, byEdgeType, communities };
}
