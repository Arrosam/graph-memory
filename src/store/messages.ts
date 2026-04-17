/**
 * graph-memory — Message + signal storage
 *
 * SRP: persist per-turn messages and process-once signals, plus the
 * episodic-recall query used by assemble.
 */

import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import type { Signal } from "../types.ts";
import { uid } from "./common.ts";

// ─── 消息 CRUD ───────────────────────────────────────────────

export function saveMessage(
  db: DatabaseSyncInstance, sid: string, turn: number, role: string, content: unknown
): void {
  db.prepare(`INSERT OR IGNORE INTO gm_messages (id, session_id, turn_index, role, content, created_at)
    VALUES (?,?,?,?,?,?)`)
    .run(uid("m"), sid, turn, role, JSON.stringify(content), Date.now());
}

export function getMessages(db: DatabaseSyncInstance, sid: string, limit?: number): any[] {
  if (limit) {
    return db.prepare("SELECT * FROM gm_messages WHERE session_id=? ORDER BY turn_index DESC LIMIT ?")
      .all(sid, limit) as any[];
  }
  return db.prepare("SELECT * FROM gm_messages WHERE session_id=? ORDER BY turn_index")
    .all(sid) as any[];
}

export function getUnextracted(db: DatabaseSyncInstance, sid: string, limit: number): any[] {
  return db.prepare("SELECT * FROM gm_messages WHERE session_id=? AND extracted=0 ORDER BY turn_index LIMIT ?")
    .all(sid, limit) as any[];
}

export function markExtracted(db: DatabaseSyncInstance, sid: string, upToTurn: number): void {
  db.prepare("UPDATE gm_messages SET extracted=1 WHERE session_id=? AND turn_index<=?")
    .run(sid, upToTurn);
}

/**
 * 溯源选拉：按 session 拉取 user/assistant 核心对话（跳过 tool/toolResult）
 * 用于 assemble 时补充三元组的原始上下文
 *
 * @param nearTime  优先取时间最接近的消息（节点的 updatedAt）
 * @param maxChars  总字符上限
 */
export function getEpisodicMessages(
  db: DatabaseSyncInstance,
  sessionIds: string[],
  nearTime: number,
  maxChars: number = 1500,
): Array<{ sessionId: string; turnIndex: number; role: string; text: string; createdAt: number }> {
  if (!sessionIds.length) return [];

  const results: Array<{ sessionId: string; turnIndex: number; role: string; text: string; createdAt: number }> = [];
  let usedChars = 0;

  // 按 session 逐个拉，优先最近的 session
  for (const sid of sessionIds) {
    if (usedChars >= maxChars) break;

    // 只拉 user 和 assistant，按时间距离 nearTime 最近排序
    const rows = db.prepare(`
      SELECT turn_index, role, content, created_at FROM gm_messages
      WHERE session_id = ? AND role IN ('user', 'assistant')
      ORDER BY ABS(created_at - ?) ASC
      LIMIT 6
    `).all(sid, nearTime) as any[];

    for (const r of rows) {
      if (usedChars >= maxChars) break;
      let text = "";
      try {
        const parsed = JSON.parse(r.content);
        if (typeof parsed === "string") {
          text = parsed;
        } else if (typeof parsed?.content === "string") {
          text = parsed.content;
        } else if (Array.isArray(parsed)) {
          text = parsed
            .filter((b: any) => b.type === "text")
            .map((b: any) => b.text ?? "")
            .join("\n");
        } else {
          text = String(parsed).slice(0, 300);
        }
      } catch {
        text = String(r.content).slice(0, 300);
      }

      if (!text.trim()) continue;
      const truncated = text.slice(0, Math.min(text.length, maxChars - usedChars));
      results.push({
        sessionId: sid,
        turnIndex: r.turn_index,
        role: r.role,
        text: truncated,
        createdAt: r.created_at,
      });
      usedChars += truncated.length;
    }
  }

  return results;
}

// ─── 信号 CRUD ───────────────────────────────────────────────

export function saveSignal(db: DatabaseSyncInstance, sid: string, s: Signal): void {
  db.prepare(`INSERT INTO gm_signals (id, session_id, turn_index, type, data, created_at)
    VALUES (?,?,?,?,?,?)`)
    .run(uid("s"), sid, s.turnIndex, s.type, JSON.stringify(s.data), Date.now());
}

export function pendingSignals(db: DatabaseSyncInstance, sid: string): Signal[] {
  return (db.prepare("SELECT * FROM gm_signals WHERE session_id=? AND processed=0 ORDER BY turn_index")
    .all(sid) as any[])
    .map(r => ({ type: r.type, turnIndex: r.turn_index, data: JSON.parse(r.data) }));
}

export function markSignalsDone(db: DatabaseSyncInstance, sid: string): void {
  db.prepare("UPDATE gm_signals SET processed=1 WHERE session_id=?").run(sid);
}
