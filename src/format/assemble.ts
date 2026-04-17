/**
 * graph-memory
 *
 * By: adoresever
 * Email: Wywelljob@gmail.com
 */

import { type DatabaseSyncInstance } from "@photostructure/sqlite";
import type { GmNode, GmEdge } from "../types.ts";
import { getCommunitySummary, getEpisodicMessages } from "../store/store.ts";

const CHARS_PER_TOKEN = 3;

/**
 * 构建知识图谱的 system prompt 引导文字
 */
export function buildSystemPromptAddition(params: {
  selectedNodes: Array<{ type: string; src: "active" | "recalled" }>;
  edgeCount: number;
}): string {
  const { selectedNodes, edgeCount } = params;
  if (selectedNodes.length === 0) return "";

  const recalledCount = selectedNodes.filter(n => n.src === "recalled").length;
  const hasRecalled = recalledCount > 0;
  const skillCount = selectedNodes.filter(n => n.type === "SKILL").length;
  const eventCount = selectedNodes.filter(n => n.type === "EVENT").length;
  const taskCount = selectedNodes.filter(n => n.type === "TASK").length;
  const isRich = selectedNodes.length >= 4 || edgeCount >= 3;

  const sections: string[] = [];

  sections.push(
    "## Graph Memory — 知识图谱记忆",
    "",
    "Below `<knowledge_graph>` is your accumulated experience from past conversations.",
    "It contains structured knowledge — NOT raw conversation history.",
    "",
    `Current graph: ${skillCount} skills, ${eventCount} events, ${taskCount} tasks, ${edgeCount} relationships.`,
  );

  if (hasRecalled) {
    sections.push(
      "",
      `**${recalledCount} nodes recalled from OTHER conversations** — these are proven solutions that worked before.`,
      "Apply them directly when the current situation matches their trigger conditions.",
    );
  }

  sections.push(
    "",
    "## Recalled context for this query",
    "",
    "This is a context engine. The following was retrieved by semantic search for the current message:",
    "",
    "- **`<episodic_context>`** — Trimmed conversation traces from sessions that produced the knowledge nodes, ordered by time.",
    "- **`<knowledge_graph>`** — Relevant triples (TASK/SKILL/EVENT) and edges, grouped by community.",
    "- **Recent 5 turns** — Last turn in full, previous 4 turns as user+assistant text only.",
    "",
    "Read this context first. Use `gm_search` only if insufficient. Use `gm_record` to save new knowledge.",
  );

  if (isRich) {
    sections.push(
      "",
      "**Graph navigation:** Edges show how knowledge connects:",
      "- `SOLVED_BY`: an EVENT was fixed by a SKILL — apply the skill when you see similar errors",
      "- `USED_SKILL`: a TASK used a SKILL — reuse the same approach for similar tasks",
      "- `PATCHES`: a newer SKILL corrects an older one — prefer the newer version",
      "- `CONFLICTS_WITH`: two SKILLs are mutually exclusive — check conditions before choosing",
    );
  }

  return sections.join("\n");
}

/** 估算单个节点渲染为 XML 的 token 数 */
function estimateNodeXmlTokens(n: GmNode): number {
  // <tag name="..." desc="..." source="recalled" updated="YYYY-MM-DD">\ncontent\n  </tag>
  const chars =
    60 +
    n.name.length +
    (n.description?.length ?? 0) +
    (n.content?.length ?? 0);
  return Math.ceil(chars / CHARS_PER_TOKEN);
}

/**
 * 组装知识图谱为 XML context
 *
 * @param params.tokenBudget         图谱 XML 的 token 预算。>0 时按预算截断节点；0 = 不截断（遗留行为）
 * @param params.episodicTokenBudget episodic 上下文的 token 预算。>0 时按预算截断；0/缺省 = 按字符软上限
 */
export function assembleContext(
  db: DatabaseSyncInstance,
  params: {
    tokenBudget: number;
    episodicTokenBudget?: number;
    activeNodes: GmNode[];
    activeEdges: GmEdge[];
    recalledNodes: GmNode[];
    recalledEdges: GmEdge[];
  },
): { xml: string | null; systemPrompt: string; tokens: number; episodicXml: string; episodicTokens: number } {
  const map = new Map<string, GmNode & { src: "active" | "recalled" }>();
  for (const n of params.recalledNodes) map.set(n.id, { ...n, src: "recalled" });
  for (const n of params.activeNodes) map.set(n.id, { ...n, src: "active" });

  // 排序：本 session > SKILL优先 > validatedCount > 全局pagerank基线
  const TYPE_PRI: Record<string, number> = { SKILL: 3, TASK: 2, EVENT: 1 };
  const candidates = Array.from(map.values())
    .filter(n => n.status === "active")
    .sort((a, b) =>
      (a.src === b.src ? 0 : a.src === "active" ? -1 : 1) ||
      (TYPE_PRI[b.type] ?? 0) - (TYPE_PRI[a.type] ?? 0) ||
      b.validatedCount - a.validatedCount ||
      b.pagerank - a.pagerank
    );

  // 按 tokenBudget 截断（>0 时生效）；0 表示无限制（保留遗留测试语义）
  let selected: typeof candidates;
  if (params.tokenBudget > 0) {
    selected = [];
    // Reserve overhead for <knowledge_graph>, <community>, <edges> wrappers
    const WRAPPER_OVERHEAD = 40;
    let used = WRAPPER_OVERHEAD;
    for (const n of candidates) {
      const est = estimateNodeXmlTokens(n);
      if (used + est > params.tokenBudget && selected.length > 0) break;
      selected.push(n);
      used += est;
    }
  } else {
    selected = candidates;
  }

  if (!selected.length) return { xml: null, systemPrompt: "", tokens: 0, episodicXml: "", episodicTokens: 0 };

  const idToName = new Map<string, string>();
  for (const n of selected) idToName.set(n.id, n.name);

  const selectedIds = new Set(selected.map(n => n.id));
  const allEdges = [...params.activeEdges, ...params.recalledEdges];
  const seen = new Set<string>();
  const edges = allEdges.filter(e =>
    selectedIds.has(e.fromId) && selectedIds.has(e.toId) && !seen.has(e.id) && seen.add(e.id)
  );

  // 按社区分组节点
  const byCommunity = new Map<string, typeof selected>();
  const noCommunity: typeof selected = [];
  for (const n of selected) {
    if (n.communityId) {
      if (!byCommunity.has(n.communityId)) byCommunity.set(n.communityId, []);
      byCommunity.get(n.communityId)!.push(n);
    } else {
      noCommunity.push(n);
    }
  }

  // 生成节点 XML（按社区分组）
  const xmlParts: string[] = [];

  for (const [cid, members] of byCommunity) {
    const summary = getCommunitySummary(db, cid);
    const label = summary ? escapeXml(summary.summary) : cid;
    xmlParts.push(`  <community id="${cid}" desc="${label}">`);
    for (const n of members) {
      const tag = n.type.toLowerCase();
      const srcAttr = n.src === "recalled" ? ` source="recalled"` : "";
      const timeAttr = ` updated="${new Date(n.updatedAt).toISOString().slice(0, 10)}"`;
      xmlParts.push(`    <${tag} name="${n.name}" desc="${escapeXml(n.description)}"${srcAttr}${timeAttr}>\n${n.content.trim()}\n    </${tag}>`);
    }
    xmlParts.push(`  </community>`);
  }

  // 无社区的节点直接放顶层
  for (const n of noCommunity) {
    const tag = n.type.toLowerCase();
    const srcAttr = n.src === "recalled" ? ` source="recalled"` : "";
    const timeAttr = ` updated="${new Date(n.updatedAt).toISOString().slice(0, 10)}"`;
    xmlParts.push(`  <${tag} name="${n.name}" desc="${escapeXml(n.description)}"${srcAttr}${timeAttr}>\n${n.content.trim()}\n  </${tag}>`);
  }

  const nodesXml = xmlParts.join("\n");

  const edgesXml = edges.length
    ? `\n  <edges>\n${edges.map(e => {
        const fromName = idToName.get(e.fromId) ?? e.fromId;
        const toName = idToName.get(e.toId) ?? e.toId;
        const cond = e.condition ? ` when="${escapeXml(e.condition)}"` : "";
        return `    <e type="${e.type}" from="${fromName}" to="${toName}"${cond}>${escapeXml(e.instruction)}</e>`;
      }).join("\n")}\n  </edges>`
    : "";

  const xml = `<knowledge_graph>\n${nodesXml}${edgesXml}\n</knowledge_graph>`;

  const systemPrompt = buildSystemPromptAddition({
    selectedNodes: selected.map(n => ({ type: n.type, src: n.src })),
    edgeCount: edges.length,
  });

  // ── 溯源选拉：PPR top 3 节点 → 拉原始 user/assistant 对话 ──
  const topNodes = selected.slice(0, 3);
  const episodicParts: string[] = [];
  const episodicBudget = params.episodicTokenBudget ?? 0;
  let episodicUsed = 0;

  // 按预算分摊每节点字符上限；无预算时按原 500 字符软上限
  const perNodeCharCap = episodicBudget > 0
    ? Math.max(200, Math.floor((episodicBudget * CHARS_PER_TOKEN) / Math.max(1, topNodes.length)))
    : 500;

  for (const node of topNodes) {
    if (!node.sourceSessions?.length) continue;
    // 取最近的 2 个 session
    const recentSessions = node.sourceSessions.slice(-2);
    const msgs = getEpisodicMessages(db, recentSessions, node.updatedAt, perNodeCharCap);
    if (!msgs.length) continue;

    const lines = msgs.map(m =>
      `    [${m.role.toUpperCase()}] ${escapeXml(m.text.slice(0, 200))}`
    ).join("\n");
    const chunk = `  <trace node="${node.name}">\n${lines}\n  </trace>`;
    const chunkTok = Math.ceil(chunk.length / CHARS_PER_TOKEN);

    // 预算硬上限：超出就停（至少保留一条）
    if (episodicBudget > 0 && episodicUsed + chunkTok > episodicBudget && episodicParts.length > 0) break;
    episodicParts.push(chunk);
    episodicUsed += chunkTok;
  }

  const episodicXml = episodicParts.length
    ? `<episodic_context>\n${episodicParts.join("\n")}\n</episodic_context>`
    : "";

  const fullContent = systemPrompt + "\n\n" + xml + (episodicXml ? "\n\n" + episodicXml : "");
  return {
    xml,
    systemPrompt,
    tokens: Math.ceil(fullContent.length / CHARS_PER_TOKEN),
    episodicXml,
    episodicTokens: Math.ceil(episodicXml.length / CHARS_PER_TOKEN),
  };
}

function escapeXml(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}