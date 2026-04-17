/**
 * graph-memory — DB connection management
 *
 * SRP: file-path resolution, SQLite handle lifecycle, and pragma setup.
 * Schema/migration logic lives in migrations.ts and is run by getDb()
 * on first open.
 */

import { DatabaseSync, type DatabaseSyncInstance } from "@photostructure/sqlite";
import { mkdirSync } from "fs";
import { homedir } from "os";
import { migrate } from "./migrations.ts";

const _dbMap = new Map<string, DatabaseSyncInstance>();

export function resolvePath(p: string): string {
  return p.replace(/^~/, homedir());
}

export function resolveAgentDbPath(dbPath: string, agentId?: string): string {
  const aid = agentId?.trim();
  if (!aid) return dbPath;

  // Sanitize: only alphanumeric, hyphens, underscores
  const safe = aid.replace(/[^a-zA-Z0-9_-]/g, "_");
  if (!safe) return dbPath;

  const lastSlash = Math.max(dbPath.lastIndexOf("/"), dbPath.lastIndexOf("\\"));
  const dotIdx = dbPath.lastIndexOf(".");

  if (dotIdx > lastSlash) {
    // Has extension: insert suffix before it
    return `${dbPath.slice(0, dotIdx)}-${safe}${dbPath.slice(dotIdx)}`;
  }
  // No extension: append suffix
  return `${dbPath}-${safe}`;
}

export function getDb(dbPath: string): DatabaseSyncInstance {
  const resolved = resolvePath(dbPath);
  const existing = _dbMap.get(resolved);
  if (existing) return existing;

  // 同时处理 Windows 和 Unix 路径分隔符
  const lastSeparator = Math.max(
    resolved.lastIndexOf("/"),
    resolved.lastIndexOf("\\")
  );

  if (lastSeparator > 0) {
    const dirPath = resolved.substring(0, lastSeparator);
    mkdirSync(dirPath, { recursive: true });
  }
  // lastSeparator <= 0 → 根目录 / 驱动器根 / 当前目录，不建目录

  const db = new DatabaseSync(resolved);
  try {
    db.exec("PRAGMA journal_mode = WAL");
    db.exec("PRAGMA foreign_keys = ON");
    migrate(db);
  } catch (err) {
    db.close();
    throw err;
  }
  _dbMap.set(resolved, db);
  return db;
}

/** 仅用于测试：关闭并重置单例（不传 path 则关闭全部） */
export function closeDb(dbPath?: string): void {
  if (dbPath) {
    const resolved = resolvePath(dbPath);
    const db = _dbMap.get(resolved);
    if (db) { db.close(); _dbMap.delete(resolved); }
  } else {
    for (const db of _dbMap.values()) db.close();
    _dbMap.clear();
  }
}
