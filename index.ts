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
import { extractAndPersist } from "./src/extractor/pipeline.ts";
import { assembleContext } from "./src/format/assemble.ts";
import { sanitizeToolUseResultPairing } from "./src/format/transcript-repair.ts";
import { runMaintenance } from "./src/graph/maintenance.ts";
import { invalidateGraphCache, computeGlobalPageRank } from "./src/graph/pagerank.ts";
import { detectCommunities } from "./src/graph/community.ts";
import {
  saveMessage, getUnextracted,
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
<<<<<<< Updated upstream
    const turnCounter = new Map<string, number>();
    const extractChain = new Map<string, Promise<void>>();
=======
    const turnCounter = new Map<string, number>(); // 社区维护计数器
    // assemble 每轮都会被调用 → 同一 prompt 命中即复用，避免重复 embed/FTS
    const recallPromptHash = new Map<string, string>();

    /** 简单 string hash（djb2），避免在 prompt 字符串大时占内存 */
    function hashPrompt(s: string): string {
      let h = 5381;
      for (let i = 0; i < s.length; i++) h = ((h << 5) + h + s.charCodeAt(i)) | 0;
      return String(h);
    }

    // ── 提取串行化（同 session Promise chain，不同 session 并行）────
    // 类型用 unknown 以兼容返回提取计数的 scheduleExtract
    const extractChain = new Map<string, Promise<unknown>>();

    /**
     * 从 gm_messages 批量提取未处理消息到图谱。
     * 分批驱干净（每批 50，最多 MAX_BATCHES 轮）防止单次调用遗漏大轮。
     */
    async function drainExtract(sessionId: string, sessionKey?: string, agentId?: string): Promise<{ nodes: number; edges: number }> {
      const { db: sdb, recaller: sRecaller } = getSessionResources(sessionId, sessionKey, agentId);
      let totalNodes = 0, totalEdges = 0;
      const MAX_BATCHES = 5;

      for (let b = 0; b < MAX_BATCHES; b++) {
        const msgs = getUnextracted(sdb, sessionId, 50);
        if (!msgs.length) break;

        const existing = getBySession(sdb, sessionId).map((n) => n.name);
        const result = await extractor.extract({ messages: msgs, existingNames: existing });

        const nameToId = new Map<string, string>();
        for (const nc of result.nodes) {
          const { node } = upsertNode(sdb, {
            type: nc.type, name: nc.name,
            description: nc.description, content: nc.content,
          }, sessionId);
          nameToId.set(node.name, node.id);
          sRecaller.syncEmbed(node).catch(() => {});
        }
        for (const ec of result.edges) {
          const fromId = nameToId.get(ec.from) ?? findByName(sdb, ec.from)?.id;
          const toId = nameToId.get(ec.to) ?? findByName(sdb, ec.to)?.id;
          if (fromId && toId) {
            upsertEdge(sdb, {
              fromId, toId, type: ec.type,
              instruction: ec.instruction, condition: ec.condition, sessionId,
            });
          }
        }
        const maxTurn = Math.max(...msgs.map((m: any) => m.turn_index));
        markExtracted(sdb, sessionId, maxTurn);

        totalNodes += result.nodes.length;
        totalEdges += result.edges.length;

        if (result.nodes.length || result.edges.length) {
          invalidateGraphCache(sdb);
          const nodeDetails = result.nodes.map((n: any) => `${n.type}:${n.name}`).join(", ");
          const edgeDetails = result.edges.map((e: any) => `${e.from}→[${e.type}]→${e.to}`).join(", ");
          api.logger.info(
            `[graph-memory] extracted ${result.nodes.length} nodes [${nodeDetails}], ${result.edges.length} edges [${edgeDetails}] (batch ${b + 1})`,
          );
        }
        // 本批次未满 50 条 → 已清空，退出
        if (msgs.length < 50) break;
      }
      return { nodes: totalNodes, edges: totalEdges };
    }

    /**
     * 把 drainExtract 挂到 session Promise chain 上，afterTurn + compact 共享，
     * 避免两条路径并发命中同一批未提取消息 → 重复 LLM 调用 / 竞态写入。
     */
    function scheduleExtract(sessionId: string, sessionKey?: string, agentId?: string): Promise<{ nodes: number; edges: number }> {
      const prev = extractChain.get(sessionId) ?? Promise.resolve({ nodes: 0, edges: 0 });
      const next = prev.then(
        () => drainExtract(sessionId, sessionKey, agentId).catch((err) => {
          api.logger.error(`[graph-memory] extract failed: ${err}`);
          return { nodes: 0, edges: 0 };
        }),
      );
      extractChain.set(sessionId, next);
      return next;
    }
>>>>>>> Stashed changes

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
<<<<<<< Updated upstream

      const prev = extractChain.get(sessionId) ?? Promise.resolve();
      const next = prev.then(async () => {
        try {
          const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
          const msgs = getUnextracted(db, sessionId, 50);
          if (!msgs.length) return;

          const result = await extractAndPersist(db, recaller, extractor, sessionId, msgs);

          if (result.nodesExtracted || result.edgesExtracted) {
            api.logger.info(
              `[graph-memory] extracted ${result.nodesExtracted} nodes [${result.nodeDetails}], ${result.edgesExtracted} edges [${result.edgeDetails}]`,
            );
          }
        } catch (err) {
          api.logger.error(`[graph-memory] turn extract failed: ${err}`);
        }
      });
      extractChain.set(sessionId, next);
      return next;
=======
      // 全部走 scheduleExtract → 与 compact 同链，防并发竞态
      await scheduleExtract(sessionId, sessionKey, agentId);
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
=======
        // OpenClaw 2026.03.28: use the prompt for a fresh, accurate recall
        // at assembly time instead of relying solely on the pre-cached result
        // from before_agent_start. Cache by prompt hash so 同一 prompt 重新装配
        // 不会触发二次 embed/FTS（before_prompt_build + assemble 都会进入这里）。
>>>>>>> Stashed changes
        let rec = recalled.get(sessionId) ?? { nodes: [], edges: [] };
        if (prompt) {
          const cleaned = cleanPrompt(prompt);
          if (cleaned) {
<<<<<<< Updated upstream
            try {
              const freshRec = await recaller.recall(cleaned);
              if (freshRec.nodes.length) {
                rec = freshRec;
                recalled.set(sessionId, freshRec);
              }
            } catch (err) {
              api.logger.warn(`[graph-memory] assemble recall failed: ${err}`);
=======
            const h = hashPrompt(cleaned);
            if (recallPromptHash.get(sessionId) !== h) {
              try {
                const freshRec = await sRecaller.recall(cleaned);
                if (freshRec.nodes.length) {
                  rec = freshRec;
                  recalled.set(sessionId, freshRec);
                }
                recallPromptHash.set(sessionId, h);
              } catch (err) {
                api.logger.warn(`[graph-memory] assemble recall failed: ${err}`);
                // fall through to cached rec
              }
>>>>>>> Stashed changes
            }
          }
        }

        const totalGmNodes = activeNodes.length + rec.nodes.length;
        const pct = cfg.compactWindowPercent ?? 0.75;
        const maxTok = tokenBudget ? Math.floor(tokenBudget * pct) : undefined;

        if (totalGmNodes === 0) {
          const trimmed = sliceLastTurn(messages, maxTok);
          return { messages: normalizeMessageContent(trimmed.messages), estimatedTokens: trimmed.tokens };
        }

        const lastTurn = sliceLastTurn(messages, maxTok);
        const repaired = sanitizeToolUseResultPairing(lastTurn.messages);

        const { xml, systemPrompt, tokens: gmTokens, episodicXml, episodicTokens } = assembleContext(db, {
          tokenBudget: 0,
          activeNodes,
          activeEdges,
          recalledNodes: rec.nodes,
          recalledEdges: rec.edges,
        });

        if (lastTurn.dropped > 0 || episodicTokens > 0) {
          api.logger.info(
            `[graph-memory] assemble: ${lastTurn.messages.length} msgs (~${lastTurn.tokens} tok), ` +
            `dropped ${lastTurn.dropped} older msgs` +
            (maxTok ? ` (budget ${maxTok} tok, ${Math.round(pct * 100)}% of ${tokenBudget})` : "") +
            `, graph ~${gmTokens} tok` +
            (episodicTokens > 0 ? `, episodic ~${episodicTokens} tok` : ""),
          );
        }

        let systemPromptAddition: string | undefined;
        const parts = [systemPrompt, xml, episodicXml].filter(Boolean);
        if (parts.length) {
          systemPromptAddition = parts.join("\n\n");
        }

        // estimatedTokens 要覆盖：消息 + 图谱 XML + 溯源片段 + systemPrompt 前言
        const sysTok = systemPromptAddition ? Math.ceil(systemPromptAddition.length / 3) : 0;
        // gmTokens 已含 xml（由 assembleContext 返回），但 episodicTokens 单独返回。
        // 取 sysTok 为严格上界，避免重复累加。
        const totalTok = Math.max(gmTokens + episodicTokens, sysTok) + lastTurn.tokens;

        return {
          messages: normalizeMessageContent(repaired),
          estimatedTokens: totalTok,
          ...(systemPromptAddition ? { systemPromptAddition } : {}),
        };
      },

      async compact({
<<<<<<< Updated upstream
        sessionId, sessionKey, currentTokenCount, agentId,
=======
        sessionId,
        sessionKey,
        tokenBudget,
        currentTokenCount,
        agentId,
        ...rest
>>>>>>> Stashed changes
      }: {
        sessionId: string; sessionKey?: string; sessionFile: string; tokenBudget?: number;
        force?: boolean; currentTokenCount?: number; agentId?: string; [k: string]: any;
      }) {
<<<<<<< Updated upstream
        const { db, recaller } = sessions.getSessionResources(sessionId, sessionKey, agentId);
        const msgs = getUnextracted(db, sessionId, 50);
        if (!msgs.length) {
          return { ok: true, compacted: true, result: { summary: "no unextracted messages", tokensBefore: currentTokenCount ?? 0 } };
        }

        try {
          const result = await extractAndPersist(db, recaller, extractor, sessionId, msgs);
          api.logger.info(
            `[graph-memory] compact: extracted ${result.nodesExtracted} nodes, ${result.edgesExtracted} edges (assemble handles context trimming)`,
=======
        const tokensBefore = currentTokenCount ?? 0;
        try {
          // 提取走 scheduleExtract → 与 afterTurn 共享 Promise chain
          // 防止同一批未提取消息被双路径并发处理
          const counts = await scheduleExtract(sessionId, sessionKey, agentId);

          // 估算 tokensAfter：实际裁剪发生在下一次 assemble，
          // 这里按 assemble 的行为预测——保留 compactWindowPercent × budget。
          // 若调用方没给 budget，则退化为 currentTokenCount（没有信号能说裁了多少）。
          const pct = cfg.compactWindowPercent ?? 0.75;
          const tokensAfter = tokenBudget
            ? Math.min(tokensBefore || Number.MAX_SAFE_INTEGER, Math.floor(tokenBudget * pct))
            : tokensBefore;

          api.logger.info(
            `[graph-memory] compact: extracted ${counts.nodes} nodes, ${counts.edges} edges; ` +
            `tokensBefore=${tokensBefore} tokensAfter~${tokensAfter} (assemble will apply trim)`,
>>>>>>> Stashed changes
          );
          return {
            ok: true, compacted: true,
            result: {
<<<<<<< Updated upstream
              summary: `extracted ${result.nodesExtracted} nodes, ${result.edgesExtracted} edges`,
              tokensBefore: currentTokenCount ?? 0,
=======
              summary: `extracted ${counts.nodes} nodes, ${counts.edges} edges`,
              tokensBefore,
              tokensAfter,
>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
=======
// ─── 取最近 N 轮用户交互（保留多步任务上下文） ──────────────

function estimateMsgTokens(msg: any): number {
  const text = typeof msg.content === "string"
    ? msg.content
    : JSON.stringify(msg.content ?? "");
  return Math.ceil(text.length / 3);
}

const MAX_KEEP_TURNS = 50;  // 上限（防极端情况下扫描整个历史）；实际裁剪由 token 预算驱动
const LAST_TURN_BUDGET_PCT = 0.6; // 最新一轮最多占总预算的 60%，剩余给历史轮

/**
 * 提取 assistant 消息中的纯文本内容，去掉 tool_use/thinking 等 schema
 */
function extractAssistantText(msg: any): string {
  if (typeof msg.content === "string") return msg.content;
  if (!Array.isArray(msg.content)) return "";
  return msg.content
    .filter((b: any) => b && typeof b === "object" && b.type === "text" && typeof b.text === "string")
    .map((b: any) => b.text)
    .join("\n")
    .trim();
}

/**
 * 提取 user 消息的纯文本内容
 * 去掉 OpenClaw 包装的 metadata（Sender JSON block、命令前缀、时间戳等）
 */
function extractUserText(msg: any): string {
  let raw: string;
  if (typeof msg.content === "string") {
    raw = msg.content;
  } else if (!Array.isArray(msg.content)) {
    raw = String(msg.content ?? "");
  } else {
    raw = msg.content
      .filter((b: any) => b && typeof b === "object" && b.type === "text" && typeof b.text === "string")
      .map((b: any) => b.text)
      .join("\n")
      .trim();
  }

  // 去掉 OpenClaw metadata: "Sender (untrusted metadata):\n```json\n{...}\n```\n实际内容"
  // 策略：找最后一个 ``` 闭合后的内容，如果没有 ``` 就用 cleanPrompt 兜底
  const fenceEnd = raw.lastIndexOf("```");
  if (fenceEnd >= 0 && raw.includes("Sender")) {
    raw = raw.slice(fenceEnd + 3).trim();
  }

  // 兜底：去掉命令前缀、时间戳标记等
  raw = raw.replace(/^\/\w+\s+/, "").trim();
  raw = raw.replace(/^\[[\w\s\-:]+\]\s*/, "").trim();

  return raw;
}

/**
 * 按 token 预算动态保留最近 N 轮对话。
 *
 * - 最新 1 轮：完整保留（含 tool_result，截断超长的）
 * - 更早的轮：只保留 user + assistant 纯文本（去掉 tool schema / thinking）
 * - 从最早轮开始逐轮裁剪，直到 token 预算内或只剩 1 轮
 *
 * @param maxTokens 0 或 undefined 表示不限制，使用 MAX_KEEP_TURNS 兜底
 */
function sliceLastTurn(
  messages: any[],
  maxTokens?: number,
): { messages: any[]; tokens: number; dropped: number } {
  if (!messages.length) {
    return { messages: [], tokens: 0, dropped: 0 };
  }

  // ── 识别所有 user 轮的起始位置（倒序）────────────────
  const userIndices: number[] = [];
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === "user") {
      userIndices.push(i);
      if (userIndices.length >= MAX_KEEP_TURNS) break;
    }
  }
  if (!userIndices.length) {
    return { messages: [], tokens: 0, dropped: messages.length };
  }

  // userIndices 是倒序的：[最新user, ..., 最早user]
  const lastTurnUserIdx = userIndices[0];

  // ── 最新 1 轮：截断超长 tool_result；若整体仍超预算，按 tool 块降级 ──
  const TOOL_MAX = 6000;
  let lastTurnMsgs: any[] = messages.slice(lastTurnUserIdx).map((msg: any) => {
    if (msg.role !== "tool" && msg.role !== "toolResult") return msg;
    if (typeof msg.content !== "string") return msg;
    if (msg.content.length <= TOOL_MAX) return msg;
    const head = Math.floor(TOOL_MAX * 0.6);
    const tail = Math.floor(TOOL_MAX * 0.3);
    return { ...msg, content: msg.content.slice(0, head) + `\n...[truncated ${msg.content.length - head - tail} chars]...\n` + msg.content.slice(-tail) };
  });

  let lastTurnTokens = 0;
  for (const msg of lastTurnMsgs) lastTurnTokens += estimateMsgTokens(msg);

  // 若最新一轮仍爆表，按"最旧 tool/toolResult 优先"逐步截断至 1 字
  // （保留结构以免 tool_use/tool_result 配对破裂，只把 content 清空/简化）
  if (maxTokens && maxTokens > 0) {
    const lastTurnCap = Math.floor(maxTokens * LAST_TURN_BUDGET_PCT);
    if (lastTurnTokens > lastTurnCap) {
      const STUB = "[tool output elided for context budget]";
      // 从头开始（最旧非 user 的 tool 块）逐一剥离，保留最末若干块
      for (let i = 0; i < lastTurnMsgs.length - 1 && lastTurnTokens > lastTurnCap; i++) {
        const m = lastTurnMsgs[i];
        if (!m || m.role === "user") continue;
        if (m.role !== "tool" && m.role !== "toolResult") continue;
        if (typeof m.content !== "string") continue;
        const before = estimateMsgTokens(m);
        const after = { ...m, content: STUB };
        const afterTok = estimateMsgTokens(after);
        if (afterTok < before) {
          lastTurnMsgs[i] = after;
          lastTurnTokens = lastTurnTokens - before + afterTok;
        }
      }
    }
  }

  // ── 更早的轮：按轮分组，只保留 user+assistant 纯文本 ──
  // turns[0] = 最早轮, turns[last] = 倒数第 2 轮
  type TurnSlice = { msgs: any[]; tokens: number };
  const olderTurns: TurnSlice[] = [];

  for (let t = userIndices.length - 1; t >= 1; t--) {
    const startIdx = userIndices[t];
    const endIdx = userIndices[t - 1]; // next (newer) user turn start
    const turnMsgs: any[] = [];
    let turnTokens = 0;

    for (let i = startIdx; i < endIdx; i++) {
      const msg = messages[i];
      if (!msg) continue;
      if (msg.role === "user") {
        const text = extractUserText(msg);
        if (text) {
          const m = { role: "user", content: text };
          turnMsgs.push(m);
          turnTokens += estimateMsgTokens(m);
        }
      } else if (msg.role === "assistant") {
        const text = extractAssistantText(msg);
        if (text) {
          const m = { role: "assistant", content: text };
          turnMsgs.push(m);
          turnTokens += estimateMsgTokens(m);
        }
      }
      // tool / toolResult / thinking 等跳过
    }
    if (turnMsgs.length) olderTurns.push({ msgs: turnMsgs, tokens: turnTokens });
  }

  // ── 按 token 预算从最早轮开始裁剪 ─────────────────────
  let totalTokens = lastTurnTokens;
  for (const t of olderTurns) totalTokens += t.tokens;

  let droppedTurns = 0;
  if (maxTokens && maxTokens > 0) {
    // 从最早轮（olderTurns[0]）开始逐轮移除
    while (olderTurns.length > 0 && totalTokens > maxTokens) {
      const oldest = olderTurns.shift()!;
      totalTokens -= oldest.tokens;
      droppedTurns++;
    }
  }

  // ── 合并 ─────────────────────────────────────────────
  const keptMsgs = [
    ...olderTurns.flatMap((t) => t.msgs),
    ...lastTurnMsgs,
  ];
  const dropped = messages.length - keptMsgs.length;

  return { messages: keptMsgs, tokens: totalTokens, dropped };
}

>>>>>>> Stashed changes
export default graphMemoryPlugin;
