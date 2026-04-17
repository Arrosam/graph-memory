/**
 * graph-memory — Tool Registration
 *
 * SRP: Defines and registers all agent-facing tools (gm_search, gm_record,
 * gm_stats, gm_maintain). Decoupled from plugin wiring.
 *
 * OCP: Adding a new tool means adding one more call here — no changes to
 * the core plugin registration logic.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { Type } from "@sinclair/typebox";
import type { SessionManager } from "../session/session-manager.ts";
import type { GmConfig } from "../types.ts";
import { normalizeNodeType } from "../types.ts";
import type { CompleteFn } from "../engine/llm.ts";
import { upsertNode, upsertEdge, findByName, getStats } from "../store/store.ts";
import { runMaintenance } from "../graph/maintenance.ts";

export function registerTools(
  api: OpenClawPluginApi,
  sessions: SessionManager,
  cfg: GmConfig,
  llm: CompleteFn,
): void {
  registerSearchTool(api, sessions);
  registerRecordTool(api, sessions);
  registerStatsTool(api, sessions);
  registerMaintainTool(api, sessions, cfg, llm);
}

function registerSearchTool(api: OpenClawPluginApi, sessions: SessionManager): void {
  api.registerTool(
    (ctx: any) => ({
      name: "gm_search",
      label: "Search Graph Memory",
      description: "搜索知识图谱中的相关经验、技能和解决方案。遇到可能之前解决过的问题时调用。",
      parameters: Type.Object({
        query: Type.String({ description: "搜索关键词或问题描述" }),
      }),
      async execute(_toolCallId: string, params: { query: string }) {
        const { query } = params;
        if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
          return {
            content: [{ type: "text", text: "graph-memory 未启用：当前上下文无 agentId。" }],
            details: { error: "no_agent_id", query },
          };
        }
        const { recaller } = sessions.getAgentResources(ctx?.agentId);
        const res = await recaller.recall(query);
        if (!res.nodes.length) {
          return {
            content: [{ type: "text", text: "图谱中未找到相关记录。" }],
            details: { count: 0, query },
          };
        }

        const lines = res.nodes.map(
          (n) =>
            `[${n.type}] ${n.name} (pr:${n.pagerank.toFixed(3)})\n${n.description}\n${n.content.slice(0, 400)}`,
        );
        const edgeLines = res.edges.map((e) => {
          const from = res.nodes.find((n) => n.id === e.fromId)?.name ?? e.fromId;
          const to = res.nodes.find((n) => n.id === e.toId)?.name ?? e.toId;
          return `  ${from} --[${e.type}]--> ${to}: ${e.instruction}`;
        });

        const text = [
          `找到 ${res.nodes.length} 个节点：\n`,
          ...lines,
          ...(edgeLines.length ? ["\n关系：", ...edgeLines] : []),
        ].join("\n\n");

        return {
          content: [{ type: "text", text }],
          details: { count: res.nodes.length, query },
        };
      },
    }),
    { name: "gm_search" },
  );
}

function registerRecordTool(api: OpenClawPluginApi, sessions: SessionManager): void {
  api.registerTool(
    (ctx: any) => ({
      name: "gm_record",
      label: "Record to Graph Memory",
      description: "手动记录经验到知识图谱。发现重要解法、踩坑经验或工作流程时调用。",
      parameters: Type.Object({
        name: Type.String({ description: "节点名称（全小写连字符）" }),
        type: Type.String({ description: "实体类型：TASK、SKILL 或 EVENT" }),
        description: Type.String({ description: "一句话说明" }),
        content: Type.String({ description: "纯文本格式的知识内容" }),
        relatedSkill: Type.Optional(
          Type.String({ description: "可选：关联的已有技能名（建立 SOLVED_BY 关系）" }),
        ),
      }),
      async execute(
        _toolCallId: string,
        p: { name: string; type: string; description: string; content: string; relatedSkill?: string },
      ) {
        const sid = ctx?.sessionKey ?? ctx?.sessionId ?? "manual";
        if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
          return {
            content: [{ type: "text", text: "graph-memory 未启用：当前上下文无 agentId。" }],
            details: { error: "no_agent_id" },
          };
        }
        const { db, recaller } = sessions.getAgentResources(ctx?.agentId);
        const nodeType = normalizeNodeType(p.type);
        if (!nodeType) {
          return {
            content: [
              { type: "text", text: `类型无效：${p.type}。只允许 TASK、SKILL、EVENT。` },
            ],
            details: { error: "invalid_type", type: p.type },
          };
        }
        const { node } = upsertNode(
          db,
          { type: nodeType, name: p.name, description: p.description, content: p.content },
          sid,
        );
        if (p.relatedSkill) {
          const rel = findByName(db, p.relatedSkill);
          if (rel) {
            upsertEdge(db, {
              fromId: node.id,
              toId: rel.id,
              type: "SOLVED_BY",
              instruction: `关联 ${p.relatedSkill}`,
              sessionId: sid,
            });
          }
        }
        recaller.syncEmbed(node).catch(() => {});
        return {
          content: [{ type: "text", text: `已记录：${node.name} (${node.type})` }],
          details: { name: node.name, type: node.type },
        };
      },
    }),
    { name: "gm_record" },
  );
}

function registerStatsTool(api: OpenClawPluginApi, sessions: SessionManager): void {
  api.registerTool(
    (ctx: any) => ({
      name: "gm_stats",
      label: "Graph Memory Stats",
      description: "查看知识图谱的统计信息：节点数、边数、社区数、PageRank Top 节点。",
      parameters: Type.Object({}),
      async execute(_toolCallId: string, _params: any) {
        if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
          return {
            content: [{ type: "text", text: "graph-memory 未启用：当前上下文无 agentId。" }],
            details: { error: "no_agent_id" },
          };
        }
        const { db } = sessions.getAgentResources(ctx?.agentId);
        const stats = getStats(db);
        const topPr = db
          .prepare(
            "SELECT name, type, pagerank FROM gm_nodes WHERE status='active' ORDER BY pagerank DESC LIMIT 5",
          )
          .all() as any[];

        const text = [
          `知识图谱统计`,
          `节点：${stats.totalNodes} 个 (${Object.entries(stats.byType).map(([t, c]) => `${t}: ${c}`).join(", ")})`,
          `边：${stats.totalEdges} 条 (${Object.entries(stats.byEdgeType).map(([t, c]) => `${t}: ${c}`).join(", ")})`,
          `社区：${stats.communities} 个`,
          `PageRank Top 5：`,
          ...topPr.map((n, i) => `  ${i + 1}. ${n.name} (${n.type}, pr=${n.pagerank.toFixed(4)})`),
        ].join("\n");
        return {
          content: [{ type: "text", text }],
          details: stats,
        };
      },
    }),
    { name: "gm_stats" },
  );
}

function registerMaintainTool(
  api: OpenClawPluginApi,
  sessions: SessionManager,
  cfg: GmConfig,
  llm: CompleteFn,
): void {
  api.registerTool(
    (ctx: any) => ({
      name: "gm_maintain",
      label: "Graph Memory Maintenance",
      description:
        "手动触发图维护：运行去重、PageRank 重算、社区检测。通常 session_end 时自动运行，这个工具用于手动触发。",
      parameters: Type.Object({}),
      async execute(_toolCallId: string, _params: any) {
        if (!sessions.canResolveAgent(ctx?.sessionId, ctx?.sessionKey, ctx?.agentId)) {
          return {
            content: [{ type: "text", text: "graph-memory 未启用：当前上下文无 agentId。" }],
            details: { error: "no_agent_id" },
          };
        }
        const { db, recaller } = sessions.getAgentResources(ctx?.agentId);
        const embedFn = recaller.getEmbedFn() ?? undefined;
        const result = await runMaintenance(db, cfg, llm, embedFn);
        const text = [
          `图维护完成（${result.durationMs}ms）`,
          `去重：发现 ${result.dedup.pairs.length} 对相似节点，合并 ${result.dedup.merged} 对`,
          ...(result.dedup.pairs.length > 0
            ? result.dedup.pairs
                .slice(0, 5)
                .map((p) => `  "${p.nameA}" ≈ "${p.nameB}" (${(p.similarity * 100).toFixed(1)}%)`)
            : []),
          `社区：${result.community.count} 个`,
          `PageRank Top 5：`,
          ...result.pagerank.topK
            .slice(0, 5)
            .map((n, i) => `  ${i + 1}. ${n.name} (${n.score.toFixed(4)})`),
        ].join("\n");
        return {
          content: [{ type: "text", text }],
          details: {
            durationMs: result.durationMs,
            dedupMerged: result.dedup.merged,
            communities: result.community.count,
          },
        };
      },
    }),
    { name: "gm_maintain" },
  );
}
