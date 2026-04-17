/**
 * graph-memory — Plugin config loading
 *
 * SRP: read pluginConfig from the host API and merge it with DEFAULT_CONFIG.
 * Nested objects (llm, embedding) are merged one level deep so partial
 * user overrides don't wipe defaults.
 */

import type { OpenClawPluginApi } from "openclaw/plugin-sdk";
import { DEFAULT_CONFIG, type GmConfig } from "../types.ts";

/**
 * Merge user plugin config over defaults. Plain objects (llm, embedding) are
 * merged one level deep so a partial override like `{ llm: { model: "x" } }`
 * doesn't wipe other default fields in `llm`. Arrays and non-plain values are
 * replaced wholesale.
 */
export function mergeConfig(defaults: GmConfig, user: Record<string, any>): GmConfig {
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

/** Extract and validate the plugin's config object from the host API. */
export function loadPluginConfig(api: OpenClawPluginApi): GmConfig {
  const raw =
    api.pluginConfig && typeof api.pluginConfig === "object"
      ? (api.pluginConfig as Record<string, any>)
      : {};
  return mergeConfig(DEFAULT_CONFIG, raw);
}
