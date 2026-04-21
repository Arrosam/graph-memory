/**
 * graph-memory — Session Manager
 *
 * SRP: Manages session-agent bindings and per-agent DB/Recaller routing.
 * All resource caching, agent ID resolution, and session propagation live here.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import { getDb, resolveAgentDbPath } from "../store/db.ts";
import { Recaller } from "../recaller/recall.ts";
import type { GmConfig } from "../types.ts";
import type { EmbedFn } from "../engine/embed.ts";

export interface AgentResources {
  db: DatabaseSyncInstance;
  recaller: Recaller;
}

export interface Logger {
  info(msg: string): void;
  warn(msg: string): void;
  error(msg: string): void;
}

export class SessionManager {
  private sessionAgentMap = new Map<string, string>();
  /**
   * Subagent child keys mapped to their parent's agentId. Set by
   * propagateSession() when a subagent is spawned, and takes precedence over
   * any explicit agentId the child later presents. Ensures the child opens
   * the parent's DB so compaction / extraction share one graph.
   */
  private subagentOverride = new Map<string, string>();
  private agentCache = new Map<string, AgentResources>();
  private sharedEmbedFn: EmbedFn | null = null;

  constructor(
    private cfg: GmConfig,
    private logger: Logger,
  ) {}

  /** Set the shared embedding function and propagate to all cached Recallers. */
  setEmbedFn(fn: EmbedFn): void {
    this.sharedEmbedFn = fn;
    for (const res of this.agentCache.values()) {
      res.recaller.setEmbedFn(fn);
    }
  }

  /**
   * Peek whether an agentId can be resolved for a session without opening
   * any DB. Mirrors getSessionResources' resolution order: explicit agent,
   * cfg fallback, previously-bound session map, or sessionKey pattern.
   */
  canResolveAgent(sessionId?: string, sessionKey?: string, agentId?: string): boolean {
    if (sessionKey && this.subagentOverride.has(sessionKey)) return true;
    if (sessionId && this.subagentOverride.has(sessionId)) return true;
    if (agentId?.trim()) return true;
    if (this.cfg.agentId?.trim()) return true;
    if (sessionKey && this.sessionAgentMap.has(sessionKey)) return true;
    if (sessionId && this.sessionAgentMap.has(sessionId)) return true;
    if (sessionKey && /(?:^|:)agent:([^:]+)/.test(sessionKey)) return true;
    return false;
  }

  /** Bind a session to an agent identity from context. */
  bindSession(ctx: any): void {
    const aid = ctx?.agentId?.trim();
    if (!aid) return;
    // Subagent override is authoritative — don't let the child's own agentId
    // clobber the parent binding propagated via prepareSubagentSpawn.
    if (ctx.sessionId && !this.subagentOverride.has(ctx.sessionId)) {
      this.sessionAgentMap.set(ctx.sessionId, aid);
    }
    if (
      ctx.sessionKey
      && ctx.sessionKey !== ctx.sessionId
      && !this.subagentOverride.has(ctx.sessionKey)
    ) {
      this.sessionAgentMap.set(ctx.sessionKey, aid);
    }
  }

  /** Check whether a session has been seen before. */
  hasSession(sessionId: string): boolean {
    return this.sessionAgentMap.has(sessionId);
  }

  /**
   * Get DB + Recaller for a given agentId (lazy-creates on first use).
   *
   * Strict mode: agentId (or cfg.agentId fallback) is required. Callers
   * without a resolvable agentId get an error — the plugin refuses to open
   * the unscoped "shared" DB so cross-agent data never mixes.
   */
  getAgentResources(agentId?: string): AgentResources {
    const aid = agentId?.trim() || this.cfg.agentId?.trim() || "";
    if (!aid) {
      throw new Error("[graph-memory] no agentId resolvable; refusing to open shared DB");
    }
    let cached = this.agentCache.get(aid);
    if (cached) return cached;

    const path = resolveAgentDbPath(this.cfg.dbPath, aid);
    const agentDb = getDb(path);
    const agentRecaller = new Recaller(agentDb, this.cfg);
    if (this.sharedEmbedFn) agentRecaller.setEmbedFn(this.sharedEmbedFn);

    cached = { db: agentDb, recaller: agentRecaller };
    this.agentCache.set(aid, cached);
    this.logger.info(`[graph-memory] initialized DB: ${path} (agent=${aid})`);
    return cached;
  }

  /** Get DB + Recaller for a session, resolving the agentId from multiple sources. */
  getSessionResources(sessionId?: string, sessionKey?: string, agentId?: string): AgentResources {
    const resolved = this.resolveAgentId(sessionId, sessionKey, agentId);
    if (resolved && sessionId && !this.sessionAgentMap.has(sessionId)) {
      this.sessionAgentMap.set(sessionId, resolved);
      if (sessionKey && sessionKey !== sessionId) {
        this.sessionAgentMap.set(sessionKey, resolved);
      }
    }
    return this.getAgentResources(resolved);
  }

  /**
   * Propagate agent binding from parent session to child session. Also
   * records the child as a subagent so future explicit-agentId arguments
   * can't redirect it to a different DB.
   */
  propagateSession(parentKey: string, childKey: string): void {
    const parentAgent = this.sessionAgentMap.get(parentKey);
    if (!parentAgent) return;
    this.sessionAgentMap.set(childKey, parentAgent);
    this.subagentOverride.set(childKey, parentAgent);
  }

  /** Clean up session-agent mappings for a given session. */
  cleanupSession(sessionId?: string, sessionKey?: string): void {
    if (sessionId) {
      this.sessionAgentMap.delete(sessionId);
      this.subagentOverride.delete(sessionId);
    }
    if (sessionKey) {
      this.sessionAgentMap.delete(sessionKey);
      this.subagentOverride.delete(sessionKey);
    }
  }

  /** Release all resources. */
  dispose(): void {
    this.sessionAgentMap.clear();
    this.subagentOverride.clear();
    this.agentCache.clear();
  }

  /**
   * Resolve agentId. Subagent override wins over explicit agentId so that a
   * spawned child always lands on its parent's DB, even if the host passes
   * the child a distinct agentId.
   */
  private resolveAgentId(
    sessionId?: string,
    sessionKey?: string,
    explicitAgentId?: string,
  ): string | undefined {
    const override =
      (sessionKey ? this.subagentOverride.get(sessionKey) : undefined) ||
      (sessionId ? this.subagentOverride.get(sessionId) : undefined);
    if (override) return override;
    return (
      explicitAgentId?.trim() ||
      (sessionKey ? this.sessionAgentMap.get(sessionKey) : undefined) ||
      (sessionId ? this.sessionAgentMap.get(sessionId) : undefined) ||
      this.parseAgentIdFromKey(sessionKey) ||
      undefined
    );
  }

  /** Extract agentId from OpenClaw sessionKey patterns like "agent:<id>:…" */
  private parseAgentIdFromKey(sessionKey?: string): string | undefined {
    if (!sessionKey) return undefined;
    const m = sessionKey.match(/(?:^|:)agent:([^:]+)/);
    return m?.[1]?.trim() || undefined;
  }
}
