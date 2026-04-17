/**
 * graph-memory — Provider/model resolver
 *
 * SRP: read the default provider/model from OpenClaw's api.config. Plugin-
 * specific LLM configuration (apiKey/baseURL/model overrides) lives in
 * cfg.llm and is handled by createCompleteFn.
 */

/** Parse provider/model from OpenClaw API config. */
export function readProviderModel(apiConfig: unknown): { provider: string; model: string } {
  let raw = "";

  if (apiConfig && typeof apiConfig === "object") {
    const m = (apiConfig as any).agents?.defaults?.model;
    if (typeof m === "string" && m.trim()) {
      raw = m.trim();
    } else if (m && typeof m === "object" && typeof m.primary === "string" && m.primary.trim()) {
      raw = m.primary.trim();
    }
  }

  if (!raw) {
    raw = (process.env.OPENCLAW_PROVIDER ?? "anthropic") + "/claude-haiku-4-5-20251001";
  }

  if (raw.includes("/")) {
    const [provider, ...rest] = raw.split("/");
    const model = rest.join("/").trim();
    if (provider?.trim() && model) {
      return { provider: provider.trim(), model };
    }
  }

  const provider = (process.env.OPENCLAW_PROVIDER ?? "anthropic").trim();
  return { provider, model: raw };
}
