/**
 * graph-memory — Knowledge Graph Memory plugin for OpenClaw
 *
 * By: adoresever
 * Email: Wywelljob@gmail.com
 *
 * Architecture:
 *   - src/plugin/config.ts          — pluginConfig loader + deep merge
 *   - src/plugin/pre-warm.ts        — scan + open per-agent DBs at init
 *   - src/plugin/session-context.ts — shared per-session state + helpers
 *   - src/plugin/context-engine.ts  — bootstrap/ingest/assemble/compact/...
 *   - src/plugin/event-handlers.ts  — session_start / before_prompt_build / session_end
 *   - src/session/session-manager.ts — per-agent DB + Recaller cache
 *   - src/tools/register-tools.ts   — gm_search / gm_record / gm_stats / gm_maintain
 *   - This file                     — thin wiring only
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { createCompleteFn } from "./src/engine/llm.ts";
import { createEmbedFn } from "./src/engine/embed.ts";
import { Extractor } from "./src/extractor/extract.ts";
import { SessionManager } from "./src/session/session-manager.ts";
import { registerTools } from "./src/tools/register-tools.ts";
import { readProviderModel } from "./src/message/config.ts";
import { loadPluginConfig } from "./src/plugin/config.ts";
import { preWarmAllDbs } from "./src/plugin/pre-warm.ts";
import { SessionContext } from "./src/plugin/session-context.ts";
import { createContextEngine } from "./src/plugin/context-engine.ts";
import { registerEventHandlers } from "./src/plugin/event-handlers.ts";

const graphMemoryPlugin = {
  id: "graph-memory",
  name: "Graph Memory",
  description:
    "知识图谱记忆引擎：从对话提取三元组，FTS5+图遍历+PageRank 跨对话召回，社区聚类+向量去重自动维护",

  register(api: OpenClawPluginApi) {
    const cfg = loadPluginConfig(api);
    const { provider, model } = readProviderModel(api.config);

    const llm = createCompleteFn(provider, model, cfg.llm);
    const extractor = new Extractor(cfg, llm);
    const sessions = new SessionManager(cfg, api.logger);

    // Embedding init is non-blocking — first recall either gets the real
    // embed function or silently falls back to FTS5.
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

    // Open every existing per-agent DB up front so the first session_start
    // doesn't pay the open+migrate cost when the user sends their first message.
    try {
      preWarmAllDbs(cfg, sessions, api.logger);
    } catch (err) {
      api.logger.warn(`[graph-memory] DB pre-warm failed: ${err}`);
    }

    const state = new SessionContext({ cfg, logger: api.logger, sessions, extractor });
    const engine = createContextEngine({ cfg, api, sessions, extractor, llm, state });
    api.registerContextEngine("graph-memory", () => engine);
    registerEventHandlers({ cfg, api, sessions, extractor, llm, state });
    registerTools(api, sessions, cfg, llm);

    api.logger.info(
      `[graph-memory] ready | dbBase=${cfg.dbPath} (per-agent: <base>-{agentId}.db)` +
      ` | provider=${provider} | model=${cfg.llm?.model ?? model}`,
    );
  },
};

export default graphMemoryPlugin;
