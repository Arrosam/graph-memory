/**
 * graph-memory — Knowledge Graph Memory plugin for OpenClaw
 *
 * By: adoresever
 * Email: Wywelljob@gmail.com
 *
 * v1.1.0：
 *   - 去掉 signals 机制，每轮直接提取
 *   - content 模板改为纯文本（无 markdown）
 *   - 提取规则放宽：讨论、分析、对比也会提取
 *
 * Architecture (SOLID):
 *   - SessionManager  — session-agent routing & per-agent DB management   (SRP)
 *   - extractAndPersist — unified extract→persist pipeline                (SRP, DRY)
 *   - message-processor — prompt cleaning, normalization, slicing         (SRP)
 *   - register-tools   — tool definitions, decoupled from plugin wiring  (SRP, OCP)
 *   - Recaller.getEmbedFn() — replaces (recaller as any).embed           (DIP)
 *   - This file         — thin orchestration layer only
 */
import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { createCompleteFn } from "./src/engine/llm.ts";
import { createEmbedFn } from "./src/engine/embed.ts";
import { Extractor } from "./src/extractor/extract.ts";
import { drainExtractAndPersist } from "./src/extractor/pipeline.ts";
import { assembleContext } from "./src/format/assemble.ts";
import { sanitizeToolUseResultPairing } from "./src/format/transcript-repair.ts";
import { runMaintenance } from "./src/graph/maintenance.ts";
import { invalidateGraphCache, computeGlobalPageRank } from "./src/graph/pagerank.ts";
import { detectCommunities } from "./src/graph/community.ts";
import {
  saveMessage,
  upsertNode, upsertEdge, findByName,
  getBySession, edgesFrom, edgesTo,
  deprecate,
} from "./src/store/store.ts";
import { DEFAULT_CONFIG, type GmConfig } from "./src/types.ts";
import { SessionManager } from "./src/session/session-manager.ts";
import { registerTools } from "./src/tools/register-tools.ts";
import {
  readProviderModel,
  cleanPrompt,
  normalizeMessageContent,
  sliceLastTurn,
} from "./src/message/message-processor.ts";

// ─── Plugin Object ───────────────────────────────────────────

const graphMemoryPlugin = {
  id: "graph-memory",
  name: "Graph Memory",
  description:
    "知识图谱记忆引擎：从对话提取三元组，FTS5+图遍历+PageRank 跨对话召回，社区聚类+向量去重自动维护",

  register(api: OpenClawPluginApi) {
    // ── Config ────────────────────────────────────────────
    const raw =
      api.pluginConfig && typeof api.pluginConfig === "object"
        ? (api.pluginConfig as any)
        : {};
    const cfg: GmConfig = { ...DEFAULT_CONFIG, ...raw };
    const { provider, model } = readProviderModel(api.config);

    // ── Core dependencies ─────────────────────────────────
    const llm = createCompleteFn(provider, model, cfg.llm);
    const extractor = new Extractor(cfg, llm);
    const sessions = new SessionManager(cfg, api.logger);

    // ── Initialize embedding (async, non-blocking) ────────
    createEmbedFn(cfg, (m) => api.logger.info(m))
      .then((fn) => {
        if (fn) {
          sessions.setEmbedFn(fn);
          api.logger.info("[graph-memory] vector search ready");
        } else {
          api.logger.info(
            "[graph-memory] FTS5 search mode（向量需 apiKey：可写 embedding 或复用 llm；OpenAI 兼容 baseURL 才能调 /embeddings）",
          );
        }
      })
      .catch(() => {
        api.logger.info("[graph-memory] FTS5 search mode");
      });

    // ── Session runtime state ─────────────────────────────
    const msgSeq = new Map<string, number>();
    const recalled = new Map<string, { nodes: any[]; edges: any[] }>();
    const turnCounter = new Map<string, number>();
    // extractChain: per-session Promise chain shared by afterTurn + compact
    // so the two extract paths can never race on the same unextracted rows.
    const extractChain = new Map<string, Promise<unknown>>();
    // Recall is expensive (embed + FTS); skip if the same prompt already recalled.
    const recallPromptHash = new Map<string, string>();

    function hashPrompt(s: string): string {
      let h = 5381;
      for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
      return String(h);
    }

    /**
     * Split tokenBudget into msg / graph / episodic portions.
     *
     * Target window = tokenBudget * compactWindowPercent.
     * Within the window: 70% msgs / 20% graph / 10% episodic (10% slack left for
     * host overhead, system prompts host adds, etc.).
     *
     * All values are 0 / undefined when tokenBudget is unset — callers treat
     * that as "unbounded" to preserve legacy behavior for hosts that don't
     * supply a budget.
     */
    function splitBudget(
      tokenBudget: number | undefined,
      cfg: GmConfig,
    ): {
      msgBudget: number | undefined;
      graphBudget: number;
      episodicBudget: number;
      target: number;
      pct: number;
    } {
      const pct = cfg.compactWindowPercent ?? 0.75;
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

    /** Schedule a drain-extract serialized per session; returns totals. */
    function scheduleExtract(
      sessionId: string, sessionKey?: string, agentId?: string,
    ): Promise<{ nodes: number; edges: number }> {
      const prev = extractChain.get(sessionId) ?? Promise.resolve({ nodes: 0, edges: 0 });
      const next = prev.then(async () => {
        try {
          const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
          const r = await drainExtractAndPersist(db, recaller, extractor, sessionId);
          if (r.nodesExtracted || r.edgesExtracted) {
            api.logger.info(
              `[graph-memory] extracted ${r.nodesExtracted} nodes [${r.nodeDetails}], ${r.edgesExtracted} edges [${r.edgeDetails}] (${r.batches} batch${r.batches === 1 ? "" : "es"})`,
            );
          }
          return { nodes: r.nodesExtracted, edges: r.edgesExtracted };
        } catch (err) {
          api.logger.error(`[graph-memory] extract failed: ${err}`);
          return { nodes: 0, edges: 0 };
        }
      });
      extractChain.set(sessionId, next);
      return next;
    }

    // ── Helpers ───────────────────────────────────────────

    function ingestMessage(sessionId: string, message: any, sessionKey?: string, agentId?: string): void {
      const { db } = sessions.getSessionResources(sessionId, sessionKey, agentId);
      let seq = msgSeq.get(sessionId);
      if (seq === undefined) {
        const row = db.prepare(
          "SELECT MAX(turn_index) as maxTurn FROM gm_messages WHERE session_id=?",
        ).get(sessionId) as any;
        seq = Number(row?.maxTurn) || 0;
      }
      seq += 1;
      msgSeq.set(sessionId, seq);
      saveMessage(db, sessionId, seq, message.role ?? "unknown", message);
    }

    async function runTurnExtract(
      sessionId: string, newMessages: any[], sessionKey?: string, agentId?: string,
    ): Promise<void> {
      if (!newMessages.length) return;
      // Route through scheduleExtract so compact + afterTurn share the chain.
      await scheduleExtract(sessionId, sessionKey, agentId);
    }

    // ── session_start ─────────────────────────────────────

    api.on("session_start", async (_event: any, ctx: any) => {
      api.logger.info(
        `[graph-memory] session_start ctx keys=[${ctx ? Object.keys(ctx).join(",") : "null"}] agentId=${ctx?.agentId ?? "∅"} sessionId=${(ctx?.sessionId ?? "∅").slice(0, 8)} sessionKey=${(ctx?.sessionKey ?? "∅").slice(0, 20)}`,
      );
      sessions.bindSession(ctx);
    });

    // ── before_prompt_build: recall ───────────────────────

    api.on("before_prompt_build", async (event: any, ctx: any) => {
      try {
        if (!sessions.hasSession(ctx?.sessionId)) {
          api.logger.info(
            `[graph-memory] before_prompt_build ctx keys=[${ctx ? Object.keys(ctx).join(",") : "null"}] agentId=${ctx?.agentId ?? "∅"} sessionId=${(ctx?.sessionId ?? "∅").slice(0, 8)}`,
          );
        }
        sessions.bindSession(ctx);

        const rawPrompt = typeof event?.prompt === "string" ? event.prompt : "";
        const prompt = cleanPrompt(rawPrompt);
        if (!prompt) return;
        if (prompt.includes("/new or /reset") || prompt.includes("new session was started")) return;

        api.logger.info(`[graph-memory] recall query: "${prompt.slice(0, 80)}"`);

        const { recaller } = sessions.getAgentResources(ctx?.agentId);
        const res = await recaller.recall(prompt);
        if (res.nodes.length) {
          if (ctx?.sessionId) recalled.set(ctx.sessionId, res);
          if (ctx?.sessionKey && ctx.sessionKey !== ctx?.sessionId) {
            recalled.set(ctx.sessionKey, res);
          }
          api.logger.info(
            `[graph-memory] recalled ${res.nodes.length} nodes, ${res.edges.length} edges`,
          );
        }
      } catch (err) {
        api.logger.warn(`[graph-memory] recall failed: ${err}`);
      }
    });

    // ── ContextEngine ─────────────────────────────────────

    const engine = {
      info: {
        id: "graph-memory",
        name: "Graph Memory",
        ownsCompaction: true,
      },

      async bootstrap({ sessionId, sessionKey, agentId }: { sessionId: string; sessionKey?: string; agentId?: string; [k: string]: any }) {
        if (agentId) {
          const aid = agentId.trim();
          if (aid && sessionId) sessions.bindSession({ agentId: aid, sessionId, sessionKey });
        }
        return { bootstrapped: true };
      },

      async ingest({
        sessionId, sessionKey, message, isHeartbeat, agentId, ...rest
      }: {
        sessionId: string; sessionKey?: string; message: any; isHeartbeat?: boolean; agentId?: string; [k: string]: any;
      }) {
        if (isHeartbeat) return { ingested: false };
        if (!sessions.hasSession(sessionId)) {
          const extraKeys = Object.keys(rest).join(",");
          api.logger.info(
            `[graph-memory] ingest first-seen sid=${sessionId.slice(0, 8)} agentId=${agentId ?? "∅"} sessionKey=${(sessionKey ?? "∅").slice(0, 30)} extraKeys=[${extraKeys}]`,
          );
        }
        ingestMessage(sessionId, message, sessionKey, agentId);
        return { ingested: true };
      },

      async assemble({
        sessionId, sessionKey, messages, tokenBudget, prompt, agentId,
      }: {
        sessionId: string; sessionKey?: string; messages: any[]; tokenBudget?: number; prompt?: string; agentId?: string; [k: string]: any;
      }) {
        const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
        const activeNodes = getBySession(db, sessionId);
        const activeEdges = activeNodes.flatMap((n) => [
          ...edgesFrom(db, n.id),
          ...edgesTo(db, n.id),
        ]);

        let rec = recalled.get(sessionId) ?? { nodes: [], edges: [] };
        if (prompt) {
          const cleaned = cleanPrompt(prompt);
          if (cleaned) {
            // before_prompt_build already ran for this prompt; skip re-embedding
            // unless the prompt actually changed.
            const h = hashPrompt(cleaned);
            if (recallPromptHash.get(sessionId) !== h) {
              try {
                const freshRec = await recaller.recall(cleaned);
                if (freshRec.nodes.length) {
                  rec = freshRec;
                  recalled.set(sessionId, freshRec);
                }
                recallPromptHash.set(sessionId, h);
              } catch (err) {
                api.logger.warn(`[graph-memory] assemble recall failed: ${err}`);
              }
            }
          }
        }

        const totalGmNodes = activeNodes.length + rec.nodes.length;
        const { msgBudget, graphBudget, episodicBudget, target, pct } = splitBudget(tokenBudget, cfg);

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
        try {
          // Share the per-session extract chain with afterTurn — no double-LLM race.
          const counts = await scheduleExtract(sessionId, sessionKey, agentId);

          // Actual message trimming happens in assemble(). Project what assemble
          // WILL produce by running assembleContext against current graph state
          // (pure, no side effects) and combining with the message budget cap.
          const { msgBudget, graphBudget, episodicBudget } = splitBudget(tokenBudget, cfg);

          const { db } = sessions.getSessionResources(sessionId, sessionKey, agentId);
          const activeNodes = getBySession(db, sessionId);
          const activeEdges = activeNodes.flatMap((n) => [
            ...edgesFrom(db, n.id),
            ...edgesTo(db, n.id),
          ]);
          const rec = recalled.get(sessionId) ?? { nodes: [], edges: [] };
          const { tokens: gmTokens } = assembleContext(db, {
            tokenBudget: graphBudget,
            episodicTokenBudget: episodicBudget,
            activeNodes,
            activeEdges,
            recalledNodes: rec.nodes,
            recalledEdges: rec.edges,
          });

          // Upper bound: messages capped to msgBudget + graph + episodic.
          // Never claim an after-size larger than before.
          const msgCap = msgBudget && msgBudget > 0 ? msgBudget : tokensBefore;
          const projected = Math.min(tokensBefore || Number.MAX_SAFE_INTEGER, msgCap) + gmTokens;
          const tokensAfter = tokensBefore > 0 ? Math.min(tokensBefore, projected) : projected;

          // Only claim compacted:true if we actually shrunk (or extracted fresh nodes
          // that will let future recalls replace raw messages).
          const didShrink = tokensBefore > 0 && tokensAfter < tokensBefore;
          const didExtract = counts.nodes > 0 || counts.edges > 0;

          api.logger.info(
            `[graph-memory] compact: extracted ${counts.nodes} nodes, ${counts.edges} edges; ` +
            `tokensBefore=${tokensBefore} tokensAfter~${tokensAfter} ` +
            `(msgCap ${msgCap}, graph+episodic ${gmTokens})`,
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

        const newMessages = messages.slice(prePromptMessageCount ?? 0);
        const totalMsgs = msgSeq.get(sessionId) ?? 0;
        api.logger.info(
          `[graph-memory] afterTurn sid=${sessionId.slice(0, 8)} newMsgs=${newMessages.length} totalMsgs=${totalMsgs}`,
        );

        runTurnExtract(sessionId, newMessages, sessionKey, agentId).catch((err) => {
          api.logger.error(`[graph-memory] turn extract failed: ${err}`);
        });

        const turns = (turnCounter.get(sessionId) ?? 0) + 1;
        turnCounter.set(sessionId, turns);
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
                  const { summarizeCommunities } = await import("./src/graph/community.ts");
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
        const rec = recalled.get(parentSessionKey);
        if (rec) recalled.set(childSessionKey, rec);
        sessions.propagateSession(parentSessionKey, childSessionKey);
        return {
          rollback: () => {
            recalled.delete(childSessionKey);
            sessions.cleanupSession(childSessionKey);
          },
        };
      },

      async onSubagentEnded({ childSessionKey }: { childSessionKey: string }) {
        recalled.delete(childSessionKey);
        recallPromptHash.delete(childSessionKey);
        msgSeq.delete(childSessionKey);
        sessions.cleanupSession(childSessionKey);
      },

      async dispose() {
        extractChain.clear();
        msgSeq.clear();
        recalled.clear();
        recallPromptHash.clear();
        turnCounter.clear();
        sessions.dispose();
      },
    };

    api.registerContextEngine("graph-memory", () => engine);

    // ── session_end: finalize + maintenance ────────────────

    api.on("session_end", async (event: any, ctx: any) => {
      sessions.bindSession(ctx);
      const sid =
        ctx?.sessionKey ?? ctx?.sessionId ?? event?.sessionKey ?? event?.sessionId;
      if (!sid) return;

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
        extractChain.delete(sid);
        msgSeq.delete(sid);
        recalled.delete(sid);
        recallPromptHash.delete(sid);
        turnCounter.delete(sid);
        sessions.cleanupSession(ctx?.sessionId, ctx?.sessionKey);
      }
    });

    // ── Register agent tools ──────────────────────────────

    registerTools(api, sessions, cfg, llm);

    api.logger.info(
      `[graph-memory] ready | dbBase=${cfg.dbPath} (per-agent: <base>-{agentId}.db)` +
      ` | provider=${provider} | model=${cfg.llm?.model ?? model}`,
    );
  },
};

export default graphMemoryPlugin;
