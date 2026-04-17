/**
 * graph-memory — OpenClaw event handlers
 *
 * SRP: wire session_start / before_prompt_build / session_end. These are
 * host-level events that don't fit the ContextEngine interface. Per-session
 * state is read/written through the shared SessionContext so handlers stay
 * consistent with engine methods.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import type { GmConfig } from "../types.ts";
import type { SessionManager } from "../session/session-manager.ts";
import type { Extractor } from "../extractor/extract.ts";
import type { CompleteFn } from "../engine/llm.ts";
import type { SessionContext } from "./session-context.ts";
import { getBySession, upsertNode, upsertEdge, findByName, deprecate } from "../store/nodes.ts";
import { cleanPrompt } from "../message/normalize.ts";
import { runMaintenance } from "../graph/maintenance.ts";

export interface EventHandlersDeps {
  cfg: GmConfig;
  api: OpenClawPluginApi;
  sessions: SessionManager;
  extractor: Extractor;
  llm: CompleteFn;
  state: SessionContext;
}

export function registerEventHandlers(deps: EventHandlersDeps): void {
  registerSessionStart(deps);
  registerBeforePromptBuild(deps);
  registerSessionEnd(deps);
}

// ─── session_start ──────────────────────────────────────────

function registerSessionStart({ api, sessions }: EventHandlersDeps): void {
  api.on("session_start", async (_event: any, ctx: any) => {
    api.logger.info(
      `[graph-memory] session_start ctx keys=[${ctx ? Object.keys(ctx).join(",") : "null"}] agentId=${ctx?.agentId ?? "∅"} sessionId=${(ctx?.sessionId ?? "∅").slice(0, 8)} sessionKey=${(ctx?.sessionKey ?? "∅").slice(0, 20)}`,
    );
    sessions.bindSession(ctx);
    // Warm the per-agent DB so the first before_prompt_build / ingest
    // doesn't pay the open+migrate cost on the critical path.
    if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
      return;
    }
    try {
      sessions.getAgentResources(ctx?.agentId);
    } catch (err) {
      api.logger.warn(`[graph-memory] session_start DB warm-up failed: ${err}`);
    }
  });
}

// ─── before_prompt_build: recall ────────────────────────────

function registerBeforePromptBuild({ api, sessions, state }: EventHandlersDeps): void {
  api.on("before_prompt_build", async (event: any, ctx: any) => {
    try {
      if (!sessions.hasSession(ctx?.sessionId)) {
        api.logger.info(
          `[graph-memory] before_prompt_build ctx keys=[${ctx ? Object.keys(ctx).join(",") : "null"}] agentId=${ctx?.agentId ?? "∅"} sessionId=${(ctx?.sessionId ?? "∅").slice(0, 8)}`,
        );
      }
      sessions.bindSession(ctx);
      if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) return;

      const rawPrompt = typeof event?.prompt === "string" ? event.prompt : "";
      const prompt = cleanPrompt(rawPrompt);
      if (!prompt) return;
      if (prompt.includes("/new or /reset") || prompt.includes("new session was started")) return;

      // Tool-loop short-circuit: if the host supplied messages and the last
      // one is a tool result, we're mid tool-call chain — user prompt hasn't
      // changed, don't re-embed. We also seed the hash so assemble()'s own
      // guard skips too (previous turn had already populated `recalled`).
      const sid = ctx?.sessionId;
      const sk = ctx?.sessionKey;
      const h = state.hashPrompt(prompt);
      if (state.isToolLoopTail(event?.messages)) {
        if (sid) state.recallPromptHash.set(sid, h);
        return;
      }

      // Hash guard: dedupe across turns. Check both sessionId and sessionKey
      // (subagents may have state populated under sessionKey only).
      if (state.readSessionState(state.recallPromptHash, sid, sk) === h) {
        return; // already recalled this exact prompt for this session
      }

      api.logger.info(`[graph-memory] recall query: "${prompt.slice(0, 80)}"`);

      const { recaller } = sessions.getAgentResources(ctx?.agentId);
      const res = await recaller.recall(prompt);
      if (sid) state.recallPromptHash.set(sid, h);
      if (res.nodes.length && sid) state.recalled.set(sid, res);
      if (res.nodes.length) {
        api.logger.info(
          `[graph-memory] recalled ${res.nodes.length} nodes, ${res.edges.length} edges`,
        );
      }
    } catch (err) {
      api.logger.warn(`[graph-memory] recall failed: ${err}`);
    }
  });
}

// ─── session_end: finalize + maintenance ────────────────────

function registerSessionEnd(deps: EventHandlersDeps): void {
  const { api, sessions, extractor, llm, cfg, state } = deps;

  api.on("session_end", async (event: any, ctx: any) => {
    sessions.bindSession(ctx);
    const sid =
      ctx?.sessionKey ?? ctx?.sessionId ?? event?.sessionKey ?? event?.sessionId;
    if (!sid) return;

    // No agentId ever bound to this session → nothing to finalize. Clean
    // up in-memory state and skip the finalize/maintenance work that would
    // otherwise try to open a DB we never created.
    if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
      state.clearSessionState(ctx?.sessionId, ctx?.sessionKey, event?.sessionId, event?.sessionKey);
      sessions.cleanupSession(ctx?.sessionId, ctx?.sessionKey);
      return;
    }

    try {
      const { db, recaller } = sessions.getAgentResources(ctx?.agentId);
      const nodes = getBySession(db, sid);
      if (nodes.length) {
        const summary = (
          db.prepare(
            "SELECT name, type, validated_count, pagerank FROM gm_nodes WHERE status='active' ORDER BY pagerank DESC LIMIT 20",
          ).all() as any[]
        )
          .map((n) => `${n.type}:${n.name}(v${n.validated_count},pr${n.pagerank.toFixed(3)})`)
          .join(", ");

        const fin = await extractor.finalize({ sessionNodes: nodes, graphSummary: summary });

        for (const nc of fin.promotedSkills) {
          if (nc.name && nc.content) {
            upsertNode(db, {
              type: "SKILL", name: nc.name,
              description: nc.description ?? "", content: nc.content,
            }, sid);
          }
        }
        for (const ec of fin.newEdges) {
          const fromId = findByName(db, ec.from)?.id;
          const toId = findByName(db, ec.to)?.id;
          if (fromId && toId) {
            upsertEdge(db, {
              fromId, toId, type: ec.type,
              instruction: ec.instruction, sessionId: sid,
            });
          }
        }
        for (const id of fin.invalidations) deprecate(db, id);
      }

      const embedFn = recaller.getEmbedFn() ?? undefined;
      const result = await runMaintenance(db, cfg, llm, embedFn);
      api.logger.info(
        `[graph-memory] maintenance: ${result.durationMs}ms, ` +
        `dedup=${result.dedup.merged}, ` +
        `communities=${result.community.count}, ` +
        `summaries=${result.communitySummaries}, ` +
        `top_pr=${result.pagerank.topK.slice(0, 3).map((n: any) => `${n.name}(${n.score.toFixed(3)})`).join(",")}`,
      );
    } catch (err) {
      api.logger.error(`[graph-memory] session_end error: ${err}`);
    } finally {
      // Clean up under every known key. Writers may have used sessionId
      // (ingest/assemble/afterTurn) while pre-populated state from subagent
      // spawn used sessionKey — we need to hit both to avoid leaks.
      state.clearSessionState(ctx?.sessionId, ctx?.sessionKey, event?.sessionId, event?.sessionKey);
      sessions.cleanupSession(ctx?.sessionId, ctx?.sessionKey);
    }
  });
}
