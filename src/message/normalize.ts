/**
 * graph-memory — Message normalization and text extraction
 *
 * SRP: convert whatever shape the host hands us into a canonical form and
 * strip OpenClaw metadata wrappers. No slicing or token math here.
 */

// ─── Prompt Cleaning ─────────────────────────────────────────

/** Strip OpenClaw metadata wrappers from raw prompt text. */
export function cleanPrompt(raw: string): string {
  let prompt = raw.trim();

  if (prompt.includes("Sender (untrusted metadata)")) {
    const jsonStart = prompt.indexOf("```json");
    if (jsonStart >= 0) {
      const jsonEnd = prompt.indexOf("```", jsonStart + 7);
      if (jsonEnd >= 0) {
        prompt = prompt.slice(jsonEnd + 3).trim();
      }
    }
    if (prompt.includes("Sender (untrusted metadata)")) {
      const lines = prompt
        .split("\n")
        .filter((l) => l.trim() && !l.includes("Sender") && !l.startsWith("```") && !l.startsWith("{"));
      prompt = lines.join("\n").trim();
    }
  }

  prompt = prompt.replace(/^\/\w+\s+/, "").trim();
  prompt = prompt.replace(/^\[[\w\s\-:]+\]\s*/, "").trim();

  return prompt;
}

// ─── Message Content Normalization ───────────────────────────

/** Normalize message content arrays so content.filter() won't crash. */
export function normalizeMessageContent(messages: any[]): any[] {
  return messages.map((msg: any) => {
    if (!msg || typeof msg !== "object") return msg;
    const c = msg.content;
    if (Array.isArray(c)) {
      let changed = false;
      const fixed = c.map((block: any) => {
        if (block && typeof block === "object" && block.type === "text" && !("text" in block)) {
          changed = true;
          return { ...block, text: "" };
        }
        return block;
      });
      if (changed) return { ...msg, content: fixed };
      return msg;
    }
    if (typeof c === "string") {
      return { ...msg, content: [{ type: "text", text: c }] };
    }
    if (c == null) {
      return { ...msg, content: [{ type: "text", text: "" }] };
    }
    return msg;
  });
}

// ─── Text Extraction ─────────────────────────────────────────

/** Extract plain text from an assistant message (strip tool_use/thinking blocks). */
export function extractAssistantText(msg: any): string {
  if (typeof msg.content === "string") return msg.content;
  if (!Array.isArray(msg.content)) return "";
  return msg.content
    .filter((b: any) => b && typeof b === "object" && b.type === "text" && typeof b.text === "string")
    .map((b: any) => b.text)
    .join("\n")
    .trim();
}

/** Extract plain text from a user message (strip OpenClaw metadata wrappers). */
export function extractUserText(msg: any): string {
  let raw: string;
  if (typeof msg.content === "string") {
    raw = msg.content;
  } else if (!Array.isArray(msg.content)) {
    raw = String(msg.content ?? "");
  } else {
    raw = msg.content
      .filter((b: any) => b && typeof b === "object" && b.type === "text" && typeof b.text === "string")
      .map((b: any) => b.text)
      .join("\n")
      .trim();
  }

  const fenceEnd = raw.lastIndexOf("```");
  if (fenceEnd >= 0 && raw.includes("Sender")) {
    raw = raw.slice(fenceEnd + 3).trim();
  }

  raw = raw.replace(/^\/\w+\s+/, "").trim();
  raw = raw.replace(/^\[[\w\s\-:]+\]\s*/, "").trim();

  return raw;
}
