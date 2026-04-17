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
import { createHash } from "crypto";
import { readdirSync, existsSync } from "fs";
import { dirname, basename, extname } from "path";
import { resolvePath } from "./src/store/db.ts";
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
    // Deep-merge for nested objects (llm, embedding) so user overriding a single
    // field like { llm: { model: "x" } } doesn't wipe the other defaults.
    const cfg: GmConfig = mergeConfig(DEFAULT_CONFIG, raw);
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

    // ── Pre-warm DBs at register-time ──────────────────────
    // Rather than lazy-opening on the first session_start (which happens when
    // the user actually sends a message), scan the memory directory for
    // existing per-agent DB files and open them all upfront. This moves the
    // open+migrate cost to plugin init so conversation-start is always fast.
    try {
      preWarmAllDbs();
    } catch (err) {
      api.logger.warn(`[graph-memory] DB pre-warm failed: ${err}`);
    }

    function preWarmAllDbs(): void {
      const t0 = Date.now();
      const opened: string[] = [];

      // Only pre-open DBs tied to a concrete agentId. No shared fallback —
      // we don't create the unscoped base DB under any circumstance.
      if (cfg.agentId?.trim()) {
        try {
          sessions.getAgentResources(cfg.agentId);
          opened.push(cfg.agentId.trim());
        } catch (err) {
          api.logger.warn(`[graph-memory] pre-warm cfg.agentId failed: ${err}`);
        }
      }

      // Scan the directory for per-agent DBs written by previous runs.
      // Files matching "<stem>-<agentId><ext>" only — bare "<stem><ext>"
      // (the legacy shared DB) is intentionally skipped.
      const resolved = resolvePath(cfg.dbPath);
      const dir = dirname(resolved);
      const base = basename(resolved);
      const ext = extname(base);
      const stem = ext ? base.slice(0, -ext.length) : base;

      if (!existsSync(dir)) return;

      for (const f of readdirSync(dir)) {
        if (!f.startsWith(stem + "-")) continue;
        if (ext && !f.endsWith(ext)) continue;
        const agentId = f.slice(stem.length + 1, ext ? -ext.length : undefined);
        if (!agentId) continue;
        if (opened.includes(agentId)) continue;
        try {
          sessions.getAgentResources(agentId);
          opened.push(agentId);
        } catch (err) {
          api.logger.warn(`[graph-memory] pre-warm agent=${agentId} failed: ${err}`);
        }
      }

      api.logger.info(
        `[graph-memory] pre-warmed ${opened.length} agent DB(s) in ${Date.now() - t0}ms: ${opened.join(", ") || "(none)"}`,
      );
    }

    // ── Session runtime state ─────────────────────────────
    const msgSeq = new Map<string, number>();
    const recalled = new Map<string, { nodes: any[]; edges: any[] }>();
    const turnCounter = new Map<string, number>();
    // extractChain: per-session Promise chain shared by afterTurn + compact
    // so the two extract paths can never race on the same unextracted rows.
    const extractChain = new Map<string, Promise<unknown>>();
    // Recall is expensive (embed + FTS); skip if the same prompt already recalled.
    const recallPromptHash = new Map<string, string>();
    // Sessions where the host does NOT route per-message engine.ingest (only
    // afterTurn fires). The plugin takes over message persistence so extraction
    // isn't starved. Membership is a one-way flip per session, decided on the
    // first afterTurn that sees newMsgs>0 while msgSeq is still 0.
    const afterTurnSaveMode = new Set<string>();

    // Non-crypto-strong but collision-resistant enough for cache dedupe.
    // Use SHA-256 prefix — DJB2's 32-bit space gets crowded on long prompts.
    function hashPrompt(s: string): string {
      return createHash("sha256").update(s).digest("hex").slice(0, 16);
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
    function splitBudget(tokenBudget: number | undefined): {
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

    /** Delete entries from all session state maps under every provided key. */
    function clearSessionState(...keys: Array<string | undefined>): void {
      const set = new Set(keys.filter((k): k is string => !!k));
      for (const k of set) {
        extractChain.delete(k);
        msgSeq.delete(k);
        recalled.delete(k);
        recallPromptHash.delete(k);
        turnCounter.delete(k);
        afterTurnSaveMode.delete(k);
      }
    }

    /** Read a per-session map by sessionId with sessionKey fallback. */
    function readSessionState<V>(m: Map<string, V>, sessionId?: string, sessionKey?: string): V | undefined {
      if (sessionId) {
        const v = m.get(sessionId);
        if (v !== undefined) return v;
      }
      if (sessionKey && sessionKey !== sessionId) return m.get(sessionKey);
      return undefined;
    }

    /** Check if the last message in the array is a tool result (tool-loop). */
    function isToolLoopTail(msgs: unknown): boolean {
      if (!Array.isArray(msgs) || !msgs.length) return false;
      const last = (msgs as any[])[msgs.length - 1];
      const role = last?.role;
      return role === "tool" || role === "toolResult" || role === "tool_result";
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

    // ── before_prompt_build: recall ───────────────────────

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
        const h = hashPrompt(prompt);
        if (isToolLoopTail(event?.messages)) {
          if (sid) recallPromptHash.set(sid, h);
          return;
        }

        // Hash guard: dedupe across turns. Check both sessionId and sessionKey
        // (subagents may have state populated under sessionKey only).
        if (readSessionState(recallPromptHash, sid, sk) === h) {
          return; // already recalled this exact prompt for this session
        }

        api.logger.info(`[graph-memory] recall query: "${prompt.slice(0, 80)}"`);

        const { recaller } = sessions.getAgentResources(ctx?.agentId);
        const res = await recaller.recall(prompt);
        if (sid) recallPromptHash.set(sid, h);
        if (res.nodes.length && sid) recalled.set(sid, res);
        if (res.nodes.length) {
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
          ingestMessage(sessionId, message, sessionKey, agentId);
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

        let rec = readSessionState(recalled, sessionId, sessionKey) ?? { nodes: [], edges: [] };
        if (prompt) {
          const cleaned = cleanPrompt(prompt);
          if (cleaned) {
            const h = hashPrompt(cleaned);
            const cachedHash = readSessionState(recallPromptHash, sessionId, sessionKey);
            // Skip recall if:
            //  (a) hash already matches (before_prompt_build already did it), or
            //  (b) the host is in a tool-loop — the user prompt hasn't changed
            //      and re-embedding mid-chain blocks the agent.
            const inToolLoop = isToolLoopTail(messages);
            if (cachedHash !== h && !inToolLoop) {
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
        const { msgBudget, graphBudget, episodicBudget, target, pct } = splitBudget(tokenBudget);

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
          const counts = await scheduleExtract(sessionId, sessionKey, agentId);

          // Actual message trimming happens in assemble(). Project what assemble
          // WILL produce by running assembleContext against current graph state
          // (pure, no side effects) and combining with the message budget cap.
          const { msgBudget, graphBudget, episodicBudget } = splitBudget(tokenBudget);

          const { db } = sessions.getSessionResources(sessionId, sessionKey, agentId);
          const activeNodes = getBySession(db, sessionId);
          const activeEdges = activeNodes.flatMap((n) => [
            ...edgesFrom(db, n.id),
            ...edgesTo(db, n.id),
          ]);
          const rec = readSessionState(recalled, sessionId, sessionKey) ?? { nodes: [], edges: [] };
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
        const totalMsgs = msgSeq.get(sessionId) ?? 0;
        api.logger.info(
          `[graph-memory] afterTurn sid=${sessionId.slice(0, 8)} newMsgs=${newMessages.length} totalMsgs=${totalMsgs}`,
        );

        // Detect hosts that don't route per-message engine.ingest. When the
        // first afterTurn arrives with newMsgs>0 but msgSeq is still 0, the
        // plugin's ingest was never called — extraction would starve because
        // gm_messages is empty. Flip into afterTurn-persistence mode for this
        // session so subsequent turns always persist before extracting.
        if (
          !afterTurnSaveMode.has(sessionId)
          && totalMsgs === 0
          && newMessages.length > 0
        ) {
          afterTurnSaveMode.add(sessionId);
          api.logger.info(
            `[graph-memory] afterTurn taking over message persistence for sid=${sessionId.slice(0, 8)} (host didn't route ingest)`,
          );
        }

        if (afterTurnSaveMode.has(sessionId)) {
          for (const m of newMessages) {
            try {
              ingestMessage(sessionId, m, sessionKey, agentId);
            } catch (err) {
              api.logger.warn(`[graph-memory] afterTurn ingestMessage failed: ${err}`);
            }
          }
        }

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
        // Propagate the hash too so the child's first before_prompt_build
        // skips re-embedding if the prompt is identical to parent's.
        const parentHash = recallPromptHash.get(parentSessionKey);
        if (parentHash) recallPromptHash.set(childSessionKey, parentHash);
        sessions.propagateSession(parentSessionKey, childSessionKey);
        return {
          rollback: () => {
            clearSessionState(childSessionKey);
            sessions.cleanupSession(childSessionKey);
          },
        };
      },

      async onSubagentEnded({ childSessionKey }: { childSessionKey: string }) {
        clearSessionState(childSessionKey);
        sessions.cleanupSession(childSessionKey);
      },

      async dispose() {
        extractChain.clear();
        msgSeq.clear();
        recalled.clear();
        recallPromptHash.clear();
        turnCounter.clear();
        afterTurnSaveMode.clear();
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

      // No agentId ever bound to this session → nothing to finalize. Clean
      // up in-memory state and skip the finalize/maintenance work that would
      // otherwise try to open a DB we never created.
      if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
        clearSessionState(ctx?.sessionId, ctx?.sessionKey, event?.sessionId, event?.sessionKey);
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
        clearSessionState(ctx?.sessionId, ctx?.sessionKey, event?.sessionId, event?.sessionKey);
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

// ─── Config merge ────────────────────────────────────────────

/**
 * Merge user plugin config over defaults. Plain objects (llm, embedding) are
 * merged one level deep so a partial override like `{ llm: { model: "x" } }`
 * doesn't wipe other default fields in `llm`. Arrays and non-plain values are
 * replaced wholesale.
 */
function mergeConfig(defaults: GmConfig, user: Record<string, any>): GmConfig {
  const out: any = { ...defaults };
  for (const [k, v] of Object.entries(user)) {
    const d = (defaults as any)[k];
    if (
      v && typeof v === "object" && !Array.isArray(v) &&
      d && typeof d === "object" && !Array.isArray(d)
    ) {
      out[k] = { ...d, ...v };
    } else if (v !== undefined) {
      out[k] = v;
    }
  }
  return out as GmConfig;
}
