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
    if (ctx.sessionId) this.sessionAgentMap.set(ctx.sessionId, aid);
    if (ctx.sessionKey && ctx.sessionKey !== ctx.sessionId) {
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
  getSessionResources(sessionId: string, sessionKey?: string, agentId?: string): AgentResources {
    const resolved = this.resolveAgentId(sessionId, sessionKey, agentId);
    if (resolved && !this.sessionAgentMap.has(sessionId)) {
      this.sessionAgentMap.set(sessionId, resolved);
      if (sessionKey && sessionKey !== sessionId) {
        this.sessionAgentMap.set(sessionKey, resolved);
      }
    }
    return this.getAgentResources(resolved);
  }

  /** Propagate agent binding from parent session to child session. */
  propagateSession(parentKey: string, childKey: string): void {
    const parentAgent = this.sessionAgentMap.get(parentKey);
    if (parentAgent) this.sessionAgentMap.set(childKey, parentAgent);
  }

  /** Clean up session-agent mappings for a given session. */
  cleanupSession(sessionId?: string, sessionKey?: string): void {
    if (sessionId) this.sessionAgentMap.delete(sessionId);
    if (sessionKey) this.sessionAgentMap.delete(sessionKey);
  }

  /** Release all resources. */
  dispose(): void {
    this.sessionAgentMap.clear();
    this.agentCache.clear();
  }

  /** Resolve agentId: explicit → sessionAgentMap → sessionKey pattern → undefined */
  private resolveAgentId(
    sessionId?: string,
    sessionKey?: string,
    explicitAgentId?: string,
  ): string | undefined {
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
