/**
 * graph-memory — Pre-warm agent DBs at plugin init
 *
 * SRP: scan the memory directory for existing <stem>-<agentId><ext> files
 * and open them so the first session_start avoids paying open+migrate cost.
 * The bare shared DB (no agentId suffix) is intentionally skipped — strict
 * mode refuses to create it.
 */

import { readdirSync, existsSync } from "fs";
import { dirname, basename, extname } from "path";
import type { GmConfig } from "../types.ts";
import { resolvePath } from "../store/db.ts";
import type { SessionManager, Logger } from "../session/session-manager.ts";

export function preWarmAllDbs(
  cfg: GmConfig,
  sessions: SessionManager,
  logger: Logger,
): void {
  const t0 = Date.now();
  const opened: string[] = [];

  // Only pre-open DBs tied to a concrete agentId. No shared fallback —
  // we don't create the unscoped base DB under any circumstance.
  if (cfg.agentId?.trim()) {
    try {
      sessions.getAgentResources(cfg.agentId);
      opened.push(cfg.agentId.trim());
    } catch (err) {
      logger.warn(`[graph-memory] pre-warm cfg.agentId failed: ${err}`);
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

  if (!existsSync(dir)) {
    logger.info(
      `[graph-memory] pre-warmed ${opened.length} agent DB(s) in ${Date.now() - t0}ms: ${opened.join(", ") || "(none)"}`,
    );
    return;
  }

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
      logger.warn(`[graph-memory] pre-warm agent=${agentId} failed: ${err}`);
    }
  }

  logger.info(
    `[graph-memory] pre-warmed ${opened.length} agent DB(s) in ${Date.now() - t0}ms: ${opened.join(", ") || "(none)"}`,
  );
}
