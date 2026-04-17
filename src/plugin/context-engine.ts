/**
 * graph-memory — ContextEngine factory
 *
 * SRP: implement the OpenClaw ContextEngine interface (bootstrap / ingest /
 * assemble / compact / afterTurn / prepareSubagentSpawn / onSubagentEnded /
 * dispose). All per-session state lives in the injected SessionContext.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import type { GmConfig } from "../types.ts";
import type { SessionManager } from "../session/session-manager.ts";
import type { Extractor } from "../extractor/extract.ts";
import type { CompleteFn } from "../engine/llm.ts";
import type { SessionContext } from "./session-context.ts";
import { getBySession, edgesFrom, edgesTo } from "../store/nodes.ts";
import { assembleContext } from "../format/assemble.ts";
import { sanitizeToolUseResultPairing } from "../format/transcript-repair.ts";
import { invalidateGraphCache, computeGlobalPageRank } from "../graph/pagerank.ts";
import { detectCommunities } from "../graph/community.ts";
import { cleanPrompt, normalizeMessageContent } from "../message/normalize.ts";
import { sliceLastTurn } from "../message/slice.ts";

export interface ContextEngineDeps {
  cfg: GmConfig;
  api: OpenClawPluginApi;
  sessions: SessionManager;
  extractor: Extractor;
  llm: CompleteFn;
  state: SessionContext;
}

export function createContextEngine(deps: ContextEngineDeps) {
  const { cfg, api, sessions, llm, state } = deps;

  return {
    info: {
      id: "graph-memory",
      name: "Graph Memory",
      ownsCompaction: true,
    },

    async bootstrap({ sessionId, sessionKey, agentId }: {
      sessionId: string; sessionKey?: string; agentId?: string; [k: string]: any;
    }) {
      if (agentId) {
        const aid = agentId.trim();
        if (aid && sessionId) sessions.bindSession({ agentId: aid, sessionId, sessionKey });
      }
      if (!sessions.canResolveAgent(sessionId, sessionKey, agentId)) {
        return { bootstrapped: false };
      }
      // Eagerly open the agent DB here too — bootstrap runs before any
      // context-engine call, so this is the right moment to pay the cost.
      try {
        sessions.getAgentResources(agentId);
      } catch (err) {
        api.logger.warn(`[graph-memory] bootstrap DB warm-up failed: ${err}`);
      }
      return { bootstrapped: true };
    },

    async ingest({
      sessionId, sessionKey, message, isHeartbeat, agentId, ...rest
    }: {
      sessionId: string; sessionKey?: string; message: any; isHeartbeat?: boolean; agentId?: string; [k: string]: any;
    }) {
      if (isHeartbeat) return { ingested: false };
      if (!sessions.canResolveAgent(sessionId, sessionKey, agentId)) {
        return { ingested: false };
      }
      if (!sessions.hasSession(sessionId)) {
        const extraKeys = Object.keys(rest).join(",");
        api.logger.info(
          `[graph-memory] ingest first-seen sid=${sessionId.slice(0, 8)} agentId=${agentId ?? "∅"} sessionKey=${(sessionKey ?? "∅").slice(0, 30)} extraKeys=[${extraKeys}]`,
        );
      }
      try {
        state.ingestMessage(sessionId, message, sessionKey, agentId);
      } catch (err) {
        api.logger.warn(`[graph-memory] ingest failed: ${err}`);
        return { ingested: false };
      }
      return { ingested: true };
    },

    async assemble({
      sessionId, sessionKey, messages, tokenBudget, prompt, agentId,
    }: {
      sessionId: string; sessionKey?: string; messages: any[]; tokenBudget?: number; prompt?: string; agentId?: string; [k: string]: any;
    }) {
      // No agent resolvable → pass messages through untouched; no graph context.
      if (!sessions.canResolveAgent(sessionId, sessionKey, agentId)) {
        const passthrough = sliceLastTurn(messages, undefined);
        return {
          messages: normalizeMessageContent(passthrough.messages),
          estimatedTokens: passthrough.tokens,
        };
      }
      const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
      const activeNodes = getBySession(db, sessionId);
      const activeEdges = activeNodes.flatMap((n) => [
        ...edgesFrom(db, n.id),
        ...edgesTo(db, n.id),
      ]);

      let rec = state.readSessionState(state.recalled, sessionId, sessionKey) ?? { nodes: [], edges: [] };
      if (prompt) {
        const cleaned = cleanPrompt(prompt);
        if (cleaned) {
          const h = state.hashPrompt(cleaned);
          const cachedHash = state.readSessionState(state.recallPromptHash, sessionId, sessionKey);
          // Skip recall if:
          //  (a) hash already matches (before_prompt_build already did it), or
          //  (b) the host is in a tool-loop — the user prompt hasn't changed
          //      and re-embedding mid-chain blocks the agent.
          const inToolLoop = state.isToolLoopTail(messages);
          if (cachedHash !== h && !inToolLoop) {
            try {
              const freshRec = await recaller.recall(cleaned);
              if (freshRec.nodes.length) {
                rec = freshRec;
                state.recalled.set(sessionId, freshRec);
              }
              state.recallPromptHash.set(sessionId, h);
            } catch (err) {
              api.logger.warn(`[graph-memory] assemble recall failed: ${err}`);
            }
          }
        }
      }

      const totalGmNodes = activeNodes.length + rec.nodes.length;
      const { msgBudget, graphBudget, episodicBudget, target, pct } = state.splitBudget(tokenBudget);

      if (totalGmNodes === 0) {
        const trimmed = sliceLastTurn(messages, msgBudget);
        return { messages: normalizeMessageContent(trimmed.messages), estimatedTokens: trimmed.tokens };
      }

      const lastTurn = sliceLastTurn(messages, msgBudget);
      const repaired = sanitizeToolUseResultPairing(lastTurn.messages);

      const { xml, systemPrompt, tokens: gmTokens, episodicXml, episodicTokens } = assembleContext(db, {
        tokenBudget: graphBudget,
        episodicTokenBudget: episodicBudget,
        activeNodes,
        activeEdges,
        recalledNodes: rec.nodes,
        recalledEdges: rec.edges,
      });

      if (lastTurn.dropped > 0 || episodicTokens > 0) {
        api.logger.info(
          `[graph-memory] assemble: ${lastTurn.messages.length} msgs (~${lastTurn.tokens} tok), ` +
          `dropped ${lastTurn.dropped} older msgs` +
          (target ? ` (target ${target}=${Math.round(pct * 100)}% of ${tokenBudget}; msg ${msgBudget}, graph ${graphBudget}, episodic ${episodicBudget})` : "") +
          `, graph ~${gmTokens} tok` +
          (episodicTokens > 0 ? `, episodic ~${episodicTokens} tok` : ""),
        );
      }

      let systemPromptAddition: string | undefined;
      const parts = [systemPrompt, xml, episodicXml].filter(Boolean);
      if (parts.length) {
        systemPromptAddition = parts.join("\n\n");
      }

      // gmTokens already covers systemPrompt + xml + episodic (assembleContext
      // computes it from the joined string). Just sum with message tokens.
      const totalTok = lastTurn.tokens + gmTokens;

      return {
        messages: normalizeMessageContent(repaired),
        estimatedTokens: totalTok,
        ...(systemPromptAddition ? { systemPromptAddition } : {}),
      };
    },

    async compact({
      sessionId, sessionKey, tokenBudget, currentTokenCount, agentId,
    }: {
      sessionId: string; sessionKey?: string; sessionFile: string; tokenBudget?: number;
      force?: boolean; currentTokenCount?: number; agentId?: string; [k: string]: any;
    }) {
      const tokensBefore = currentTokenCount ?? 0;
      if (!sessions.canResolveAgent(sessionId, sessionKey, agentId)) {
        return { ok: false, compacted: false, reason: "no agentId" };
      }
      try {
        // Share the per-session extract chain with afterTurn — no double-LLM race.
        const counts = await state.scheduleExtract(sessionId, sessionKey, agentId);

        // Actual message trimming happens in assemble(). Project what assemble
        // WILL produce by running assembleContext against current graph state
        // (pure, no side effects) and combining with the message budget cap.
        const { msgBudget, graphBudget, episodicBudget } = state.splitBudget(tokenBudget);

        const { db } = sessions.getSessionResources(sessionId, sessionKey, agentId);
        const activeNodes = getBySession(db, sessionId);
        const activeEdges = activeNodes.flatMap((n) => [
          ...edgesFrom(db, n.id),
          ...edgesTo(db, n.id),
        ]);
        const rec = state.readSessionState(state.recalled, sessionId, sessionKey) ?? { nodes: [], edges: [] };
        const { tokens: gmTokens } = assembleContext(db, {
          tokenBudget: graphBudget,
          episodicTokenBudget: episodicBudget,
          activeNodes,
          activeEdges,
          recalledNodes: rec.nodes,
          recalledEdges: rec.edges,
        });

        // Project the after-size. If we don't know tokensBefore (host didn't
        // pass currentTokenCount), report the cap as the best we can offer.
        // Otherwise: the real message portion is capped to min(msgBudget, currentMsgs)
        // but we only know the total, so we use min(tokensBefore, msgBudget)
        // as an upper bound for the trimmed messages.
        let tokensAfter: number;
        if (tokensBefore > 0 && msgBudget && msgBudget > 0) {
          tokensAfter = Math.min(tokensBefore, msgBudget + gmTokens);
        } else if (msgBudget && msgBudget > 0) {
          tokensAfter = msgBudget + gmTokens;
        } else {
          tokensAfter = tokensBefore; // no budget given → can't project
        }

        // Only claim compacted:true if we actually shrunk (or extracted fresh
        // nodes that will let future recalls replace raw messages).
        const didShrink = tokensBefore > 0 && tokensAfter < tokensBefore;
        const didExtract = counts.nodes > 0 || counts.edges > 0;

        api.logger.info(
          `[graph-memory] compact: extracted ${counts.nodes} nodes, ${counts.edges} edges; ` +
          `tokensBefore=${tokensBefore} tokensAfter~${tokensAfter} ` +
          `(msgBudget ${msgBudget ?? "∅"}, graph+episodic ${gmTokens})`,
        );

        return {
          ok: true,
          compacted: didShrink || didExtract,
          result: {
            summary: `extracted ${counts.nodes} nodes, ${counts.edges} edges`,
            tokensBefore,
            tokensAfter,
          },
        };
      } catch (err) {
        api.logger.error(`[graph-memory] compact failed: ${err}`);
        return { ok: false, compacted: false, reason: String(err) };
      }
    },

    async afterTurn({
      sessionId, sessionKey, messages, prePromptMessageCount, isHeartbeat, agentId,
    }: {
      sessionId: string; sessionKey?: string; sessionFile: string; messages: any[];
      prePromptMessageCount: number; autoCompactionSummary?: string; isHeartbeat?: boolean;
      tokenBudget?: number; agentId?: string; [k: string]: any;
    }) {
      if (isHeartbeat) return;
      if (!sessions.canResolveAgent(sessionId, sessionKey, agentId)) return;

      const newMessages = messages.slice(prePromptMessageCount ?? 0);
      const totalMsgs = state.msgSeq.get(sessionId) ?? 0;
      api.logger.info(
        `[graph-memory] afterTurn sid=${sessionId.slice(0, 8)} newMsgs=${newMessages.length} totalMsgs=${totalMsgs}`,
      );

      // Detect hosts that don't route per-message engine.ingest. When the
      // first afterTurn arrives with newMsgs>0 but msgSeq is still 0, the
      // plugin's ingest was never called — extraction would starve because
      // gm_messages is empty. Flip into afterTurn-persistence mode for this
      // session so subsequent turns always persist before extracting.
      if (
        !state.afterTurnSaveMode.has(sessionId)
        && totalMsgs === 0
        && newMessages.length > 0
      ) {
        state.afterTurnSaveMode.add(sessionId);
        api.logger.info(
          `[graph-memory] afterTurn taking over message persistence for sid=${sessionId.slice(0, 8)} (host didn't route ingest)`,
        );
      }

      if (state.afterTurnSaveMode.has(sessionId)) {
        for (const m of newMessages) {
          try {
            state.ingestMessage(sessionId, m, sessionKey, agentId);
          } catch (err) {
            api.logger.warn(`[graph-memory] afterTurn ingestMessage failed: ${err}`);
          }
        }
      }

      state.runTurnExtract(sessionId, newMessages, sessionKey, agentId).catch((err) => {
        api.logger.error(`[graph-memory] turn extract failed: ${err}`);
      });

      const turns = (state.turnCounter.get(sessionId) ?? 0) + 1;
      state.turnCounter.set(sessionId, turns);
      const maintainInterval = cfg.compactTurnCount ?? 7;

      if (turns % maintainInterval === 0) {
        try {
          const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
          invalidateGraphCache(db);
          const pr = computeGlobalPageRank(db, cfg);
          const comm = detectCommunities(db);
          api.logger.info(
            `[graph-memory] periodic maintenance (turn ${turns}): ` +
            `pagerank top=${pr.topK.slice(0, 3).map(n => n.name).join(",")}, ` +
            `communities=${comm.count}`,
          );

          if (comm.communities.size > 0) {
            (async () => {
              try {
                const { summarizeCommunities } = await import("../graph/community.ts");
                const embedFn = recaller.getEmbedFn() ?? undefined;
                const summaries = await summarizeCommunities(db, comm.communities, llm, embedFn);
                api.logger.info(
                  `[graph-memory] community summaries refreshed: ${summaries} summaries`,
                );
              } catch (e) {
                api.logger.error(`[graph-memory] community summary failed: ${e}`);
              }
            })();
          }
        } catch (err) {
          api.logger.error(`[graph-memory] periodic maintenance failed: ${err}`);
        }
      }
    },

    async prepareSubagentSpawn({
      parentSessionKey, childSessionKey,
    }: {
      parentSessionKey: string; childSessionKey: string;
    }) {
      const rec = state.recalled.get(parentSessionKey);
      if (rec) state.recalled.set(childSessionKey, rec);
      // Propagate the hash too so the child's first before_prompt_build
      // skips re-embedding if the prompt is identical to parent's.
      const parentHash = state.recallPromptHash.get(parentSessionKey);
      if (parentHash) state.recallPromptHash.set(childSessionKey, parentHash);
      sessions.propagateSession(parentSessionKey, childSessionKey);
      return {
        rollback: () => {
          state.clearSessionState(childSessionKey);
          sessions.cleanupSession(childSessionKey);
        },
      };
    },

    async onSubagentEnded({ childSessionKey }: { childSessionKey: string }) {
      state.clearSessionState(childSessionKey);
      sessions.cleanupSession(childSessionKey);
    },

    async dispose() {
      state.disposeAll();
    },
  };
}
