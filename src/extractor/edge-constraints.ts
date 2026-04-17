/**
 * graph-memory — Edge type constraints and auto-correction
 *
 * SRP: enforce valid edge types and direction constraints. Acts as a pure
 * validator between the LLM's raw edge output and the persisted graph.
 */

// 合法边类型
export const VALID_EDGE_TYPES = new Set([
  "USED_SKILL", "SOLVED_BY", "REQUIRES", "PATCHES", "CONFLICTS_WITH",
]);

/** 边类型 → 合法的 from 节点类型 */
export const EDGE_FROM_CONSTRAINT: Record<string, Set<string>> = {
  USED_SKILL:     new Set(["TASK"]),
  SOLVED_BY:      new Set(["EVENT", "SKILL"]),
  REQUIRES:       new Set(["SKILL"]),
  PATCHES:        new Set(["SKILL"]),
  CONFLICTS_WITH: new Set(["SKILL"]),
};

/** 边类型 → 合法的 to 节点类型 */
export const EDGE_TO_CONSTRAINT: Record<string, Set<string>> = {
  USED_SKILL:     new Set(["SKILL"]),
  SOLVED_BY:      new Set(["SKILL"]),
  REQUIRES:       new Set(["SKILL"]),
  PATCHES:        new Set(["SKILL"]),
  CONFLICTS_WITH: new Set(["SKILL"]),
};

/**
 * 名称标准化（与 store.ts 的 normalizeName 实现等价，在此独立保留以避免
 * extractor 反向依赖 store）。
 */
export function normalizeName(name: string): string {
  return name.trim().toLowerCase()
    .replace(/[\s_]+/g, "-")
    .replace(/[^a-z0-9\u4e00-\u9fff\-]/g, "")
    .replace(/-{2,}/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * Auto-correct an LLM-produced edge: fix mis-labelled types when the node
 * pair uniquely determines the correct edge type, drop edges whose types
 * or directions violate constraints.
 *
 * Returns the corrected edge or `null` if the edge must be discarded.
 */
export function correctEdgeType(
  edge: { from: string; to: string; type: string; instruction: string; condition?: string },
  nameToType: Map<string, string>,
): typeof edge | null {
  const fromType = nameToType.get(normalizeName(edge.from));
  const toType = nameToType.get(normalizeName(edge.to));

  if (!fromType || !toType) return edge;

  let type = edge.type;

  if (fromType === "TASK" && toType === "SKILL" && type !== "USED_SKILL") {
    if (process.env.GM_DEBUG) {
      console.log(`  [DEBUG] edge corrected: ${edge.from} ->[${type}]-> ${edge.to} => USED_SKILL`);
    }
    type = "USED_SKILL";
  }

  if (fromType === "EVENT" && toType === "SKILL" && type !== "SOLVED_BY") {
    if (process.env.GM_DEBUG) {
      console.log(`  [DEBUG] edge corrected: ${edge.from} ->[${type}]-> ${edge.to} => SOLVED_BY`);
    }
    type = "SOLVED_BY";
  }

  if (!VALID_EDGE_TYPES.has(type)) {
    if (process.env.GM_DEBUG) {
      console.log(`  [DEBUG] edge dropped: invalid type "${type}"`);
    }
    return null;
  }

  const fromOk = EDGE_FROM_CONSTRAINT[type]?.has(fromType) ?? false;
  const toOk = EDGE_TO_CONSTRAINT[type]?.has(toType) ?? false;
  if (!fromOk || !toOk) {
    if (process.env.GM_DEBUG) {
      console.log(`  [DEBUG] edge dropped: ${fromType}->[${type}]->${toType} violates direction constraint`);
    }
    return null;
  }

  return { ...edge, type };
}
