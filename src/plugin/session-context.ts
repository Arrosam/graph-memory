/**
 * graph-memory — SessionContext
 *
 * SRP: own every per-session state map and expose the helpers that read,
 * write, and invalidate them. The ContextEngine and event handlers share
 * this instance — keeping the state in one place is what makes SessionId/
 * sessionKey aliasing safe and cleanup leak-free.
 *
 * Also hosts the extract-pipeline glue (scheduleExtract, ingestMessage,
 * runTurnExtract) because those functions mutate the same maps.
 */

import { createHash } from "crypto";
import type { GmConfig } from "../types.ts";
import { saveMessage } from "../store/messages.ts";
import { drainExtractAndPersist } from "../extractor/pipeline.ts";
import type { Extractor } from "../extractor/extract.ts";
import type { SessionManager, Logger } from "../session/session-manager.ts";

export interface BudgetSplit {
  msgBudget: number | undefined;
  graphBudget: number;
  episodicBudget: number;
  target: number;
  pct: number;
}

export interface SessionContextDeps {
  cfg: GmConfig;
  logger: Logger;
  sessions: SessionManager;
  extractor: Extractor;
}

export class SessionContext {
  readonly msgSeq = new Map<string, number>();
  readonly recalled = new Map<string, { nodes: any[]; edges: any[] }>();
  readonly turnCounter = new Map<string, number>();
  /** per-session Promise chain shared by afterTurn + compact */
  readonly extractChain = new Map<string, Promise<unknown>>();
  /** recall is expensive; skip if same prompt already recalled */
  readonly recallPromptHash = new Map<string, string>();
  /** sessions where the host doesn't route engine.ingest — afterTurn persists */
  readonly afterTurnSaveMode = new Set<string>();

  constructor(private deps: SessionContextDeps) {}

  // ─── Pure helpers ──────────────────────────────────────────

  /** SHA-256 prefix — DJB2 would crowd on long prompts. */
  hashPrompt(s: string): string {
    return createHash("sha256").update(s).digest("hex").slice(0, 16);
  }

  /**
   * Split tokenBudget into msg / graph / episodic portions.
   * Target window = tokenBudget * compactWindowPercent.
   * Within the window: 70% / 20% / 10% (10% slack).
   * 0 / undefined everywhere when tokenBudget is unset.
   */
  splitBudget(tokenBudget: number | undefined): BudgetSplit {
    const pct = this.deps.cfg.compactWindowPercent ?? 0.75;
    const target = tokenBudget ? Math.floor(tokenBudget * pct) : 0;
    if (!target) {
      return { msgBudget: undefined, graphBudget: 0, episodicBudget: 0, target: 0, pct };
    }
    return {
      msgBudget: Math.floor(target * 0.70),
      graphBudget: Math.floor(target * 0.20),
      episodicBudget: Math.floor(target * 0.10),
      target,
      pct,
    };
  }

  /** Read a per-session map by sessionId with sessionKey fallback. */
  readSessionState<V>(m: Map<string, V>, sessionId?: string, sessionKey?: string): V | undefined {
    if (sessionId) {
      const v = m.get(sessionId);
      if (v !== undefined) return v;
    }
    if (sessionKey && sessionKey !== sessionId) return m.get(sessionKey);
    return undefined;
  }

  /** Last message is a tool result → we're mid tool-chain. */
  isToolLoopTail(msgs: unknown): boolean {
    if (!Array.isArray(msgs) || !msgs.length) return false;
    const last = (msgs as any[])[msgs.length - 1];
    const role = last?.role;
    return role === "tool" || role === "toolResult" || role === "tool_result";
  }

  /** Delete entries from every state map under every provided key. */
  clearSessionState(...keys: Array<string | undefined>): void {
    const set = new Set(keys.filter((k): k is string => !!k));
    for (const k of set) {
      this.extractChain.delete(k);
      this.msgSeq.delete(k);
      this.recalled.delete(k);
      this.recallPromptHash.delete(k);
      this.turnCounter.delete(k);
      this.afterTurnSaveMode.delete(k);
    }
  }

  // ─── Extract-pipeline glue ─────────────────────────────────

  /** Schedule a drain-extract serialized per session; returns totals. */
  scheduleExtract(
    sessionId: string, sessionKey?: string, agentId?: string,
  ): Promise<{ nodes: number; edges: number }> {
    const prev = this.extractChain.get(sessionId) ?? Promise.resolve({ nodes: 0, edges: 0 });
    const next = prev.then(async () => {
      try {
        const { db, recaller } = this.deps.sessions.getSessionResources(sessionId, sessionKey, agentId);
        const r = await drainExtractAndPersist(db, recaller, this.deps.extractor, sessionId);
        if (r.nodesExtracted || r.edgesExtracted) {
          this.deps.logger.info(
            `[graph-memory] extracted ${r.nodesExtracted} nodes [${r.nodeDetails}], ${r.edgesExtracted} edges [${r.edgeDetails}] (${r.batches} batch${r.batches === 1 ? "" : "es"})`,
          );
        }
        return { nodes: r.nodesExtracted, edges: r.edgesExtracted };
      } catch (err) {
        this.deps.logger.error(`[graph-memory] extract failed: ${err}`);
        return { nodes: 0, edges: 0 };
      }
    });
    this.extractChain.set(sessionId, next);
    return next;
  }

  /** Persist a single message and bump per-session turn counter. */
  ingestMessage(sessionId: string, message: any, sessionKey?: string, agentId?: string): void {
    const { db } = this.deps.sessions.getSessionResources(sessionId, sessionKey, agentId);
    let seq = this.msgSeq.get(sessionId);
    if (seq === undefined) {
      const row = db.prepare(
        "SELECT MAX(turn_index) as maxTurn FROM gm_messages WHERE session_id=?",
      ).get(sessionId) as any;
      seq = Number(row?.maxTurn) || 0;
    }
    seq += 1;
    this.msgSeq.set(sessionId, seq);
    saveMessage(db, sessionId, seq, message.role ?? "unknown", message);
  }

  /** Extract after a turn arrives. Routed through scheduleExtract for serialization. */
  async runTurnExtract(
    sessionId: string, newMessages: any[], sessionKey?: string, agentId?: string,
  ): Promise<void> {
    if (!newMessages.length) return;
    await this.scheduleExtract(sessionId, sessionKey, agentId);
  }

  // ─── Shutdown ──────────────────────────────────────────────

  /** Clear every map and dispose SessionManager. */
  disposeAll(): void {
    this.extractChain.clear();
    this.msgSeq.clear();
    this.recalled.clear();
    this.recallPromptHash.clear();
    this.turnCounter.clear();
    this.afterTurnSaveMode.clear();
    this.deps.sessions.dispose();
  }
}
