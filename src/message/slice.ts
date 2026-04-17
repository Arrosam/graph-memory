/**
 * graph-memory — Token-budget message slicing
 *
 * SRP: trim a conversation history to fit a token budget while preserving
 * the last turn's tool-chain integrity. Uses extractAssistantText /
 * extractUserText from normalize.ts to condense older turns.
 */

import { extractAssistantText, extractUserText } from "./normalize.ts";

// Upper bound on turns scanned from the tail. Actual trimming is budget-driven;
// this cap just bounds work when the conversation is huge.
const MAX_KEEP_TURNS = 50;

export interface SliceResult {
  messages: any[];
  tokens: number;
  dropped: number;
}

export function estimateMsgTokens(msg: any): number {
  const text = typeof msg.content === "string" ? msg.content : JSON.stringify(msg.content ?? "");
  return Math.ceil(text.length / 3);
}

/**
 * Slice messages by token budget, keeping the most recent turns.
 *
 * - Latest turn: preserved in full (tool_result truncated if oversized)
 * - Older turns: user + assistant text only (no tool schema / thinking)
 * - Drops oldest turns first until within token budget
 * - If the latest turn alone exceeds budget (long tool-use chains),
 *   drops oldest tool-call groups within the turn
 *
 * @param maxTokens 0 or undefined = no limit, use MAX_KEEP_TURNS as cap
 */
export function sliceLastTurn(messages: any[], maxTokens?: number): SliceResult {
  if (!messages.length) {
    return { messages: [], tokens: 0, dropped: 0 };
  }

  // Identify user turn start indices (reverse order)
  const userIndices: number[] = [];
  for (let i = messages.length - 1; i >= 0; i--) {
    if (messages[i].role === "user") {
      userIndices.push(i);
      if (userIndices.length >= MAX_KEEP_TURNS) break;
    }
  }
  if (!userIndices.length) {
    return { messages: [], tokens: 0, dropped: messages.length };
  }

  const lastTurnUserIdx = userIndices[0];

  // Latest turn: full messages with oversized tool_result truncation
  const TOOL_MAX = 6000;
  let lastTurnMsgs = messages.slice(lastTurnUserIdx).map((msg: any) => {
    if (msg.role !== "tool" && msg.role !== "toolResult") return msg;
    if (typeof msg.content !== "string") return msg;
    if (msg.content.length <= TOOL_MAX) return msg;
    const head = Math.floor(TOOL_MAX * 0.6);
    const tail = Math.floor(TOOL_MAX * 0.3);
    return {
      ...msg,
      content:
        msg.content.slice(0, head) +
        `\n...[truncated ${msg.content.length - head - tail} chars]...\n` +
        msg.content.slice(-tail),
    };
  });

  let lastTurnTokens = 0;
  for (const msg of lastTurnMsgs) lastTurnTokens += estimateMsgTokens(msg);

  // Older turns: user + assistant text only, grouped by turn
  type TurnSlice = { msgs: any[]; tokens: number };
  const olderTurns: TurnSlice[] = [];

  for (let t = userIndices.length - 1; t >= 1; t--) {
    const startIdx = userIndices[t];
    const endIdx = userIndices[t - 1];
    const turnMsgs: any[] = [];
    let turnTokens = 0;

    for (let i = startIdx; i < endIdx; i++) {
      const msg = messages[i];
      if (!msg) continue;
      if (msg.role === "user") {
        const text = extractUserText(msg);
        if (text) {
          const m = { role: "user", content: text };
          turnMsgs.push(m);
          turnTokens += estimateMsgTokens(m);
        }
      } else if (msg.role === "assistant") {
        const text = extractAssistantText(msg);
        if (text) {
          const m = { role: "assistant", content: text };
          turnMsgs.push(m);
          turnTokens += estimateMsgTokens(m);
        }
      }
    }
    if (turnMsgs.length) olderTurns.push({ msgs: turnMsgs, tokens: turnTokens });
  }

  // Drop oldest turns until within token budget
  let totalTokens = lastTurnTokens;
  for (const t of olderTurns) totalTokens += t.tokens;

  let droppedTurns = 0;
  if (maxTokens && maxTokens > 0) {
    while (olderTurns.length > 0 && totalTokens > maxTokens) {
      const oldest = olderTurns.shift()!;
      totalTokens -= oldest.tokens;
      droppedTurns++;
    }
  }

  // If the latest turn alone still exceeds budget (long tool-use chains),
  // trim oldest tool-call groups within the turn, keeping user message
  // and the most recent groups.
  if (maxTokens && maxTokens > 0 && totalTokens > maxTokens) {
    const olderTurnTokens = olderTurns.reduce((s, t) => s + t.tokens, 0);
    const latestBudget = Math.max(maxTokens - olderTurnTokens, 0);
    const trimmed = trimToolCallGroups(lastTurnMsgs, latestBudget);
    totalTokens = olderTurnTokens + trimmed.tokens;
    lastTurnMsgs = trimmed.msgs;
    lastTurnTokens = trimmed.tokens;
  }

  const keptMsgs = [...olderTurns.flatMap((t) => t.msgs), ...lastTurnMsgs];
  const dropped = messages.length - keptMsgs.length;

  return { messages: keptMsgs, tokens: totalTokens, dropped };
}

/**
 * Trim tool-call groups from the beginning of a single turn to fit a token budget.
 *
 * A "tool-call group" is an assistant message (which may contain tool_use blocks)
 * followed by its tool/toolResult messages. Groups are atomic — we never split a
 * tool_use from its result.
 *
 * Always keeps: the user message(s) at the start + at least the most recent group.
 */
function trimToolCallGroups(
  turnMsgs: any[],
  maxTokens: number,
): { msgs: any[]; tokens: number } {
  if (!turnMsgs.length) return { msgs: [], tokens: 0 };

  // Separate leading user messages from the assistant/tool chain
  const userMsgs: any[] = [];
  let chainStart = 0;
  for (let i = 0; i < turnMsgs.length; i++) {
    if (turnMsgs[i].role === "user") {
      userMsgs.push(turnMsgs[i]);
      chainStart = i + 1;
    } else {
      break;
    }
  }
  const chainMsgs = turnMsgs.slice(chainStart);
  if (!chainMsgs.length) {
    const tokens = userMsgs.reduce((s, m) => s + estimateMsgTokens(m), 0);
    return { msgs: userMsgs, tokens };
  }

  // Group chain into tool-call groups: each starts with an assistant message
  // and includes all following tool/toolResult messages until the next assistant
  const groups: any[][] = [];
  let current: any[] = [];
  for (const msg of chainMsgs) {
    if (msg.role === "assistant" && current.length > 0) {
      groups.push(current);
      current = [msg];
    } else {
      current.push(msg);
    }
  }
  if (current.length) groups.push(current);

  const userTokens = userMsgs.reduce((s, m) => s + estimateMsgTokens(m), 0);

  // Keep groups from the end (most recent) until budget is reached.
  // Always keep at least the last group so the agent has context on its latest action.
  let keptTokens = userTokens;
  let keepFrom = groups.length;

  for (let i = groups.length - 1; i >= 0; i--) {
    const groupTokens = groups[i].reduce((s: number, m: any) => s + estimateMsgTokens(m), 0);
    if (keptTokens + groupTokens > maxTokens && keepFrom < groups.length) {
      break; // over budget, but we already have at least one group
    }
    keepFrom = i;
    keptTokens += groupTokens;
  }

  const keptMsgs = [...userMsgs, ...groups.slice(keepFrom).flat()];
  return { msgs: keptMsgs, tokens: keptTokens };
}
