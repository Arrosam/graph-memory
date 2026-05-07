/**
 * graph-memory — Vector storage CRUD
 *
 * SRP: write/read vector embeddings keyed by node_id. Vector-based search
 * lives in search.ts.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import { createHash } from "crypto";

/**
 * Decode a SQLite blob (Uint8Array / Buffer) into a Float32Array. Float32Array
 * requires its byteOffset to be a multiple of 4; SQLite blobs come back as
 * Node Buffers whose underlying offset is not guaranteed to be aligned. Copy
 * into a fresh ArrayBuffer to sidestep RangeError on misaligned reads.
 */
export function blobToFloat32(raw: Uint8Array): Float32Array {
  const len = raw.byteLength >>> 2;
  const out = new Float32Array(len);
  // Copy via Uint8Array view of the destination — fast and alignment-safe.
  new Uint8Array(out.buffer).set(raw.subarray(0, len * 4));
  return out;
}

export function saveVector(db: DatabaseSyncInstance, nodeId: string, content: string, vec: number[]): void {
  const hash = createHash("md5").update(content).digest("hex");
  const f32 = new Float32Array(vec);
  const blob = new Uint8Array(f32.buffer, f32.byteOffset, f32.byteLength);
  db.prepare(`INSERT INTO gm_vectors (node_id, content_hash, embedding) VALUES (?,?,?)
    ON CONFLICT(node_id) DO UPDATE SET content_hash=excluded.content_hash, embedding=excluded.embedding`)
    .run(nodeId, hash, blob);
}

export function getVectorHash(db: DatabaseSyncInstance, nodeId: string): string | null {
  return (db.prepare("SELECT content_hash FROM gm_vectors WHERE node_id=?").get(nodeId) as any)?.content_hash ?? null;
}

/** 获取所有向量（供去重/聚类用） */
export function getAllVectors(db: DatabaseSyncInstance): Array<{ nodeId: string; embedding: Float32Array }> {
  const rows = db.prepare(`
    SELECT v.node_id, v.embedding FROM gm_vectors v
    JOIN gm_nodes n ON n.id = v.node_id WHERE n.status = 'active'
  `).all() as any[];
  return rows.map(r => ({
    nodeId: r.node_id,
    embedding: blobToFloat32(r.embedding as Uint8Array),
  }));
}
