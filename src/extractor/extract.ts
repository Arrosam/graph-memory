/**
 * graph-memory — Extractor orchestration
 *
 * SRP: coordinate LLM calls for extract/finalize and parse their JSON output
 * into typed ExtractionResult / FinalizeResult. Prompts, edge constraints,
 * and tolerant-JSON helpers live in sibling modules.
 */

import type { GmConfig, ExtractionResult, FinalizeResult } from "../types.ts";
import { normalizeNodeType } from "../types.ts";
import type { CompleteFn } from "../engine/llm.ts";
import { EXTRACT_SYS, EXTRACT_USER, FINALIZE_SYS, FINALIZE_USER } from "./prompts.ts";
import {
  VALID_EDGE_TYPES,
  correctEdgeType,
  normalizeName,
} from "./edge-constraints.ts";
import { extractJson } from "./json-parser.ts";

export class Extractor {
  constructor(private _cfg: GmConfig, private llm: CompleteFn) {}

  async extract(params: {
    messages: any[];
    existingNames: string[];
  }): Promise<ExtractionResult> {
    const msgs = params.messages
      .map(m => `[${(m.role ?? "?").toUpperCase()} t=${m.turn_index ?? 0}]\n${
        String(typeof m.content === "string" ? m.content : JSON.stringify(m.content)).slice(0, 800)
      }`).join("\n\n---\n\n");

    const raw = await this.llm(
      EXTRACT_SYS,
      EXTRACT_USER(msgs, params.existingNames.join(", ")),
    );

    if (process.env.GM_DEBUG) {
      console.log("\n  [DEBUG] LLM raw response (first 2000 chars):");
      console.log("  " + raw.slice(0, 2000).replace(/\n/g, "\n  "));
    }

    return this.parseExtract(raw);
  }

  async finalize(params: { sessionNodes: any[]; graphSummary: string }): Promise<FinalizeResult> {
    const raw = await this.llm(FINALIZE_SYS, FINALIZE_USER(params.sessionNodes, params.graphSummary));
    return this.parseFinalize(raw, params.sessionNodes);
  }

  private parseExtract(raw: string): ExtractionResult {
    try {
      const json = extractJson(raw);
      const p = JSON.parse(json);

      const nodes = (p.nodes ?? []).filter((n: any) => {
        if (!n.name || !n.type || !n.content) return false;
        const nt = normalizeNodeType(String(n.type));
        if (!nt) {
          if (process.env.GM_DEBUG) console.log(`  [DEBUG] node dropped: invalid type "${n.type}"`);
          return false;
        }
        n.type = nt;
        if (!n.description) n.description = "";
        n.name = normalizeName(n.name);
        return true;
      });

      const nameToType = new Map<string, string>();
      for (const n of nodes) nameToType.set(n.name, n.type);

      const edges = (p.edges ?? [])
        .filter((e: any) => e.from && e.to && e.type && e.instruction)
        .map((e: any) => {
          e.from = normalizeName(e.from);
          e.to = normalizeName(e.to);
          return correctEdgeType(e, nameToType);
        })
        .filter((e: any) => e !== null);

      return { nodes, edges };
    } catch (err) {
      throw new Error(
        `[graph-memory] extraction parse failed: ${err}\nraw (first 200): ${raw.slice(0, 200)}`,
      );
    }
  }

  private parseFinalize(raw: string, sessionNodes?: any[]): FinalizeResult {
    try {
      const json = extractJson(raw);
      const p = JSON.parse(json);

      const nameToType = new Map<string, string>();
      if (sessionNodes) {
        for (const n of sessionNodes) {
          if (n.name && n.type) nameToType.set(normalizeName(n.name), n.type);
        }
      }
      const promotedSkills = (p.promotedSkills ?? []).filter((n: any) => n.name && n.content);
      for (const n of promotedSkills) {
        nameToType.set(normalizeName(n.name), n.type ?? "SKILL");
      }

      const newEdges = (p.newEdges ?? [])
        .filter((e: any) => e.from && e.to && e.type && VALID_EDGE_TYPES.has(e.type))
        .map((e: any) => {
          e.from = normalizeName(e.from);
          e.to = normalizeName(e.to);
          return correctEdgeType(e, nameToType);
        })
        .filter((e: any) => e !== null);

      return {
        promotedSkills,
        newEdges,
        invalidations: p.invalidations ?? [],
      };
    } catch { return { promotedSkills: [], newEdges: [], invalidations: [] }; }
  }
}
