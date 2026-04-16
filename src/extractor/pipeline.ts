/**
 * graph-memory — Extraction Pipeline
 *
 * SRP: Unified extract → persist → embed flow.
 * Eliminates the 3x duplication that existed in runTurnExtract, compact, and session_end.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import type { Recaller } from "../recaller/recall.ts";
import type { Extractor } from "./extract.ts";
import {
  upsertNode,
  upsertEdge,
  findByName,
  getBySession,
  markExtracted,
} from "../store/store.ts";
import { invalidateGraphCache } from "../graph/pagerank.ts";

export interface PipelineResult {
  nodesExtracted: number;
  edgesExtracted: number;
  nodeDetails: string;
  edgeDetails: string;
}

/**
 * Extract knowledge from messages, persist nodes/edges to DB, sync embeddings,
 * and mark messages as extracted.
 *
 * @param db        - SQLite database instance
 * @param recaller  - Recaller instance (for embedding sync)
 * @param extractor - Extractor instance (for LLM extraction)
 * @param sessionId - Current session identifier
 * @param messages  - Raw messages to extract from (must have turn_index)
 */
export async function extractAndPersist(
  db: DatabaseSyncInstance,
  recaller: Recaller,
  extractor: Extractor,
  sessionId: string,
  messages: any[],
): Promise<PipelineResult> {
  if (!messages.length) {
    return { nodesExtracted: 0, edgesExtracted: 0, nodeDetails: "", edgeDetails: "" };
  }

  const existing = getBySession(db, sessionId).map((n) => n.name);
  const result = await extractor.extract({ messages, existingNames: existing });

  const nameToId = new Map<string, string>();
  for (const nc of result.nodes) {
    const { node } = upsertNode(
      db,
      { type: nc.type, name: nc.name, description: nc.description, content: nc.content },
      sessionId,
    );
    nameToId.set(node.name, node.id);
    recaller.syncEmbed(node).catch(() => {});
  }

  for (const ec of result.edges) {
    const fromId = nameToId.get(ec.from) ?? findByName(db, ec.from)?.id;
    const toId = nameToId.get(ec.to) ?? findByName(db, ec.to)?.id;
    if (fromId && toId) {
      upsertEdge(db, {
        fromId,
        toId,
        type: ec.type,
        instruction: ec.instruction,
        condition: ec.condition,
        sessionId,
      });
    }
  }

  const maxTurn = Math.max(...messages.map((m: any) => m.turn_index));
  markExtracted(db, sessionId, maxTurn);

  if (result.nodes.length || result.edges.length) {
    invalidateGraphCache(db);
  }

  return {
    nodesExtracted: result.nodes.length,
    edgesExtracted: result.edges.length,
    nodeDetails: result.nodes.map((n: any) => `${n.type}:${n.name}`).join(", "),
    edgeDetails: result.edges.map((e: any) => `${e.from}→[${e.type}]→${e.to}`).join(", "),
  };
}
