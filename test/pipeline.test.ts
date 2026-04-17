/**
 * graph-memory — OpenClaw flow integration tests
 *
 * Simulates the host → plugin message flow:
 *   1. Host delivers messages (saveMessage — mimics ingest / afterTurn-save)
 *   2. Plugin drains unextracted messages → extractor → persist
 *   3. Subsequent drains only process new messages (incremental)
 *
 * Uses a fake LLM to count invocations; embedding is disabled (Recaller
 * without setEmbedFn falls into FTS-only mode, so syncEmbed is a no-op).
 */

import { describe, it, expect, beforeEach } from "vitest";
import type { DatabaseSyncInstance } from "@photostructure/sqlite";
import { createTestDb } from "./helpers.ts";
import { drainExtractAndPersist } from "../src/extractor/pipeline.ts";
import { Extractor } from "../src/extractor/extract.ts";
import { Recaller } from "../src/recaller/recall.ts";
import {
  saveMessage,
  allActiveNodes,
  getUnextracted,
  allEdges,
} from "../src/store/store.ts";
import { DEFAULT_CONFIG, type GmConfig } from "../src/types.ts";

// ─── Fakes ───────────────────────────────────────────────────

function makeLlm() {
  const responses: string[] = [];
  let callCount = 0;
  const fn = async (_system: string, _user: string): Promise<string> => {
    callCount++;
    return responses.shift() ?? '{"nodes":[],"edges":[]}';
  };
  return {
    fn,
    get callCount() { return callCount; },
    queue(response: string) { responses.push(response); },
  };
}

function extractionJson(nodes: Array<{ type: string; name: string; content?: string }>, edges: any[] = []) {
  return JSON.stringify({
    nodes: nodes.map(n => ({
      type: n.type,
      name: n.name,
      description: `desc of ${n.name}`,
      content: n.content ?? `content of ${n.name}`,
    })),
    edges,
  });
}

// ─── Suite ───────────────────────────────────────────────────

describe("OpenClaw flow: send → afterTurn → extract", () => {
  let db: DatabaseSyncInstance;
  const cfg: GmConfig = { ...DEFAULT_CONFIG, dbPath: ":memory:" };
  const sid = "session-1";

  beforeEach(() => {
    db = createTestDb();
  });

  it("单轮：messages 落盘 → drain 提取 1 次 → 节点入图 + 消息标记 extracted", async () => {
    saveMessage(db, sid, 1, "user", { role: "user", content: "部署 bilibili mcp" });
    saveMessage(db, sid, 2, "assistant", { role: "assistant", content: "用 docker 部署" });

    const llm = makeLlm();
    llm.queue(extractionJson([
      { type: "SKILL", name: "deploy-bilibili-mcp", content: "1. docker build\n2. docker push" },
    ]));

    const extractor = new Extractor(cfg, llm.fn);
    const recaller = new Recaller(db, cfg);

    const r = await drainExtractAndPersist(db, recaller, extractor, sid);

    expect(llm.callCount).toBe(1);
    expect(r.nodesExtracted).toBe(1);
    expect(r.batches).toBe(1);
    expect(getUnextracted(db, sid, 100)).toHaveLength(0);
    expect(allActiveNodes(db)).toHaveLength(1);
    expect(allActiveNodes(db)[0].name).toBe("deploy-bilibili-mcp");
  });

  it("多轮增量：第二轮新消息被提取；老消息不重复送 LLM", async () => {
    saveMessage(db, sid, 1, "user", { role: "user", content: "first" });
    saveMessage(db, sid, 2, "assistant", { role: "assistant", content: "ack" });

    const llm = makeLlm();
    llm.queue(extractionJson([{ type: "SKILL", name: "skill-one" }]));

    const extractor = new Extractor(cfg, llm.fn);
    const recaller = new Recaller(db, cfg);

    await drainExtractAndPersist(db, recaller, extractor, sid);
    expect(llm.callCount).toBe(1);
    expect(allActiveNodes(db)).toHaveLength(1);

    // 第二轮：host 追加新消息
    saveMessage(db, sid, 3, "user", { role: "user", content: "followup" });
    saveMessage(db, sid, 4, "assistant", { role: "assistant", content: "ack2" });

    llm.queue(extractionJson([{ type: "SKILL", name: "skill-two" }]));

    await drainExtractAndPersist(db, recaller, extractor, sid);
    expect(llm.callCount).toBe(2);
    expect(allActiveNodes(db)).toHaveLength(2);

    const names = allActiveNodes(db).map(n => n.name).sort();
    expect(names).toEqual(["skill-one", "skill-two"]);
  });

  it("空队列：没 unextracted 时 drain 是 no-op，LLM 不被调用", async () => {
    const llm = makeLlm();
    const extractor = new Extractor(cfg, llm.fn);
    const recaller = new Recaller(db, cfg);

    const r = await drainExtractAndPersist(db, recaller, extractor, sid);

    expect(llm.callCount).toBe(0);
    expect(r.nodesExtracted).toBe(0);
    expect(r.batches).toBe(0);
  });

  it("Batch 溢出：unextracted > batchSize 时 drain 循环多批", async () => {
    // 120 条消息，batchSize=50 → 50 + 50 + 20 = 3 批
    for (let i = 1; i <= 120; i++) {
      saveMessage(db, sid, i, i % 2 === 0 ? "assistant" : "user", {
        role: i % 2 === 0 ? "assistant" : "user",
        content: `msg ${i}`,
      });
    }

    const llm = makeLlm();
    // 每批返回一个节点
    llm.queue(extractionJson([{ type: "SKILL", name: "batch-0" }]));
    llm.queue(extractionJson([{ type: "SKILL", name: "batch-1" }]));
    llm.queue(extractionJson([{ type: "SKILL", name: "batch-2" }]));

    const extractor = new Extractor(cfg, llm.fn);
    const recaller = new Recaller(db, cfg);

    const r = await drainExtractAndPersist(db, recaller, extractor, sid, 50);

    expect(llm.callCount).toBe(3);
    expect(r.nodesExtracted).toBe(3);
    expect(getUnextracted(db, sid, 200)).toHaveLength(0);
    expect(allActiveNodes(db)).toHaveLength(3);
  });

  it("含边：drain 提取节点 + 边，边被持久化", async () => {
    saveMessage(db, sid, 1, "user", { role: "user", content: "importerror libgl1 用 conda" });
    saveMessage(db, sid, 2, "assistant", { role: "assistant", content: "装 libgl1" });

    const llm = makeLlm();
    llm.queue(JSON.stringify({
      nodes: [
        { type: "EVENT", name: "importerror-libgl1", description: "libgl1 缺失", content: "现象..." },
        { type: "SKILL", name: "install-libgl1", description: "conda 装 libgl1", content: "conda install..." },
      ],
      edges: [
        { from: "importerror-libgl1", to: "install-libgl1", type: "SOLVED_BY", instruction: "conda install libgl1" },
      ],
    }));

    const extractor = new Extractor(cfg, llm.fn);
    const recaller = new Recaller(db, cfg);

    const r = await drainExtractAndPersist(db, recaller, extractor, sid);

    expect(r.nodesExtracted).toBe(2);
    expect(r.edgesExtracted).toBe(1);
    expect(allActiveNodes(db)).toHaveLength(2);
    const edges = allEdges(db);
    expect(edges).toHaveLength(1);
    expect(edges[0].type).toBe("SOLVED_BY");
  });
});
