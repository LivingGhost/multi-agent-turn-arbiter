import { createHash } from "node:crypto";
import type { OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

type ArbiterConfig = {
  enabled?: boolean;
  enabledChannels?: string[];
  activeRunLeaseMs?: number;
  reservationLeaseMs?: number;
  postSendGraceMs?: number;
  stateIdleMs?: number;
  maxBacklogTurns?: number;
  maxPromptChars?: number;
  failOpen?: boolean;
  debug?: boolean;
};

type LockPhase = "reserved" | "active";

type AgentAccountInfo = {
  channelId: string;
  accountId: string;
  agentId: string;
  displayNames: string[];
};

type VisibleMessage = {
  messageId: string;
  kind: "external" | "agent";
  content: string;
  senderKey?: string;
  senderName?: string;
  senderAgentId?: string;
  timestamp: number;
};

type LatestVisibleRecord = {
  message: VisibleMessage;
  updatedAt: number;
};

type VisibleHistoryRecord = {
  messages: VisibleMessage[];
  updatedAt: number;
};

type ActiveRunRecord = {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  agentId: string;
  sourceMessageId: string;
  startedAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
};

type DeferredWakeRecord = {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  agentId: string;
  sourceMessageId: string;
  enqueuedAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
};

type SendLockRecord = {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  createdAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
};

type BootstrapLockRecord = {
  conversationKey: string;
  agentId: string;
  phase: LockPhase;
  createdAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
};

type SourceLockRecord = {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  phase: LockPhase;
  createdAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
};

type PostSendGraceRecord = {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  createdAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
};

const latestVisibleByConversation = new Map<string, LatestVisibleRecord>();
const visibleHistoryByConversation = new Map<string, VisibleHistoryRecord>();
const activeRunsByConversationAgent = new Map<string, ActiveRunRecord>();
const deferredWakesByConversationAgent = new Map<string, DeferredWakeRecord>();
const sendLocksByConversation = new Map<string, SendLockRecord>();
const bootstrapLocksByConversation = new Map<string, BootstrapLockRecord>();
const sourceLocksByConversation = new Map<string, SourceLockRecord>();
const postSendGracesByConversationAgent = new Map<string, PostSendGraceRecord>();
const pendingWakeConversations = new Map<string, number>();

function nowMs(): number {
  return Date.now();
}

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function makeHash(input: string): string {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function isEnabledForChannel(enabledChannels: Set<string>, channelId: string): boolean {
  return enabledChannels.size === 0 || enabledChannels.has(channelId);
}

function getMetadataString(
  metadata: Record<string, unknown> | undefined,
  keys: string[],
): string | undefined {
  for (const key of keys) {
    const value = trimString(metadata?.[key]);
    if (value) {
      return value;
    }
  }
  return undefined;
}

function resolveInboundSenderKey(metadata?: Record<string, unknown>): string | undefined {
  return getMetadataString(metadata, [
    "senderId",
    "sender_id",
    "authorId",
    "author_id",
    "userId",
    "user_id",
    "fromId",
    "from_id",
  ]);
}

function resolveMessageId(event: {
  timestamp?: number;
  from: string;
  content: string;
  metadata?: Record<string, unknown>;
}): string {
  const meta = event.metadata ?? {};
  const direct =
    trimString(meta["messageId"]) ||
    trimString(meta["message_id"]) ||
    trimString(meta["id"]) ||
    trimString(meta["eventId"]) ||
    trimString(meta["event_id"]);
  if (direct) {
    return direct;
  }
  return `derived-${makeHash(`${event.timestamp ?? 0}:${event.from}:${event.content}`)}`;
}

function resolveInboundConversationKey(
  channelId: string,
  conversationId: string | undefined,
  metadata?: Record<string, unknown>,
): string | null {
  const threadId = trimString(metadata?.["threadId"]);
  const base = threadId || trimString(conversationId);
  if (!channelId || !base) {
    return null;
  }
  return `${channelId}:${base}`;
}

function resolveOutboundConversationKey(
  channelId: string,
  conversationId: string | undefined,
  to: string,
  metadata?: Record<string, unknown>,
): string | null {
  const threadId = trimString(metadata?.["threadId"]);
  const base = threadId || trimString(conversationId) || trimString(to);
  if (!channelId || !base) {
    return null;
  }
  return `${channelId}:${base}`;
}

function resolveConversationKeyFromSessionKey(
  sessionKey: string | undefined,
  channelId: string | undefined,
): string | null {
  const normalizedSessionKey = trimString(sessionKey);
  const normalizedChannelId = trimString(channelId);
  if (!normalizedSessionKey || !normalizedChannelId) {
    return null;
  }
  const stripped = normalizedSessionKey.replace(/^agent:[^:]+:/, "");
  if (!stripped.startsWith(`${normalizedChannelId}:`)) {
    return null;
  }
  return stripped;
}

function buildDerivedSessionKey(agentId: string, conversationKey: string): string {
  return `agent:${trimString(agentId)}:${conversationKey}`;
}

function buildPendingSourceMessageId(conversationKey: string): string {
  return `pending:${conversationKey}`;
}

function makeConversationAgentKey(conversationKey: string, agentId: string): string {
  return `${conversationKey}::${agentId}`;
}

function buildAgentAccounts(config: OpenClawConfig): AgentAccountInfo[] {
  const bindings = Array.isArray(config.bindings) ? config.bindings : [];
  const agents =
    config.agents?.list && Array.isArray(config.agents.list)
      ? config.agents.list.filter((entry) => entry && typeof entry === "object")
      : [];

  const agentNameById = new Map<string, string>();
  for (const entry of agents) {
    const id = trimString(entry.id);
    if (!id) {
      continue;
    }
    const name =
      trimString(entry.identity?.name) ||
      trimString((entry as { name?: string }).name) ||
      id;
    agentNameById.set(id, name);
  }

  const out: AgentAccountInfo[] = [];
  for (const binding of bindings) {
    const channelId = trimString(binding.match?.channel);
    const accountId = trimString(binding.match?.accountId);
    const agentId = trimString(binding.agentId);
    if (!channelId || !accountId || !agentId) {
      continue;
    }
    const agentName = agentNameById.get(agentId);
    out.push({
      channelId,
      accountId,
      agentId,
      displayNames: uniqueStrings([accountId, agentName ?? agentId]),
    });
  }
  return out;
}

function buildAgentLabelById(infos: AgentAccountInfo[]): Map<string, string> {
  const labels = new Map<string, string>();
  for (const info of infos) {
    if (!labels.has(info.agentId)) {
      labels.set(info.agentId, info.displayNames[0] ?? info.agentId);
    }
  }
  return labels;
}

function resolveOutboundAgentId(
  accountId: string | undefined,
  metadata: Record<string, unknown> | undefined,
  agentByAccount: Map<string, string>,
): string | undefined {
  const directAgentId = trimString(metadata?.["agentId"]);
  if (directAgentId) {
    return directAgentId;
  }
  const normalizedAccountId = trimString(accountId);
  if (!normalizedAccountId) {
    return undefined;
  }
  return agentByAccount.get(normalizedAccountId);
}

function inferTargetAgents(
  content: string,
  channelId: string,
  infos: AgentAccountInfo[],
): string[] {
  const normalized = trimString(content);
  const relevant = infos.filter((info) => info.channelId === channelId);
  const allAgents = uniqueStrings(relevant.map((info) => info.agentId));
  const matchedAgents = new Set<string>();

  for (const info of relevant) {
    if (
      info.displayNames.some((name) => {
        const candidate = trimString(name);
        return candidate && normalized.includes(candidate);
      })
    ) {
      matchedAgents.add(info.agentId);
    }
  }

  return matchedAgents.size > 0 ? uniqueStrings([...matchedAgents]) : allAgents;
}

function getAllChannelAgents(
  channelId: string,
  infos: AgentAccountInfo[],
  options?: { excludeAgentId?: string },
): string[] {
  const excluded = trimString(options?.excludeAgentId);
  return uniqueStrings(
    infos
      .filter((info) => info.channelId === channelId)
      .map((info) => info.agentId)
      .filter((agentId) => agentId !== excluded),
  );
}

function getEligibleAgentsForVisibleMessage(
  message: VisibleMessage,
  channelId: string,
  infos: AgentAccountInfo[],
): string[] {
  if (message.kind === "external") {
    return inferTargetAgents(message.content, channelId, infos);
  }
  return getAllChannelAgents(channelId, infos, { excludeAgentId: message.senderAgentId });
}

function makeVisibleMessage(params: {
  messageId: string;
  kind: "external" | "agent";
  content: string;
  senderKey?: string;
  senderName?: string;
  senderAgentId?: string;
  timestamp?: number;
}): VisibleMessage {
  return {
    messageId: params.messageId,
    kind: params.kind,
    content: params.content,
    senderKey: params.senderKey,
    senderName: params.senderName,
    senderAgentId: params.senderAgentId,
    timestamp: typeof params.timestamp === "number" ? params.timestamp : nowMs(),
  };
}

function isOnlyNoReplyOutput(texts: string[]): boolean {
  const normalized = texts.map((entry) => entry.trim()).filter(Boolean);
  return normalized.length > 0 && normalized.every((entry) => entry === "NO_REPLY");
}

function getTurnSpeakerKey(message: VisibleMessage): string {
  if (message.kind === "agent") {
    const agentId = trimString(message.senderAgentId);
    if (agentId) {
      return `agent:${agentId}`;
    }
  }
  const senderKey = trimString(message.senderKey);
  if (senderKey) {
    return `${message.kind}:${senderKey}`;
  }
  const senderName = trimString(message.senderName);
  if (senderName) {
    return `${message.kind}:${senderName}`;
  }
  return `${message.kind}:message:${message.messageId}`;
}

function selectNewestTurnEntries(
  messages: Array<{ message: VisibleMessage; index: number }>,
  maxTurns: number,
): Array<{ message: VisibleMessage; index: number }> {
  if (maxTurns === -1 || messages.length === 0) {
    return messages;
  }

  let turnsSeen = 0;
  let currentTurnKey: string | undefined;
  let startIndex = messages.length;

  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const entry = messages[index];
    const turnKey = getTurnSpeakerKey(entry.message);
    if (turnKey !== currentTurnKey) {
      turnsSeen += 1;
      currentTurnKey = turnKey;
      if (turnsSeen > maxTurns) {
        break;
      }
    }
    startIndex = index;
  }

  return messages.slice(startIndex);
}

function trimVisibleHistoryToTailTurns(
  messages: VisibleMessage[],
  maxBacklogTurns: number,
): VisibleMessage[] {
  if (maxBacklogTurns === -1 || messages.length === 0) {
    return messages;
  }
  return selectNewestTurnEntries(
    messages.map((message, index) => ({ message, index })),
    maxBacklogTurns,
  ).map(({ message }) => message);
}

function setLatestVisible(conversationKey: string, message: VisibleMessage): void {
  latestVisibleByConversation.set(conversationKey, {
    message,
    updatedAt: nowMs(),
  });
}

function getLatestVisibleForConversation(
  conversationKey: string,
  stateIdleMs: number,
): VisibleMessage | undefined {
  const existing = latestVisibleByConversation.get(conversationKey);
  if (!existing) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt > stateIdleMs) {
    latestVisibleByConversation.delete(conversationKey);
    return undefined;
  }
  return existing.message;
}

function appendConversationVisibleMessage(
  conversationKey: string,
  message: VisibleMessage,
  maxBacklogTurns: number,
): void {
  const existing = visibleHistoryByConversation.get(conversationKey);
  if (!existing) {
    visibleHistoryByConversation.set(conversationKey, {
      messages: trimVisibleHistoryToTailTurns([message], maxBacklogTurns),
      updatedAt: nowMs(),
    });
    return;
  }

  const existingIndex = existing.messages.findIndex((entry) => entry.messageId === message.messageId);
  if (existingIndex >= 0) {
    existing.messages[existingIndex] = message;
  } else {
    existing.messages.push(message);
  }
  existing.messages = trimVisibleHistoryToTailTurns(existing.messages, maxBacklogTurns);
  existing.updatedAt = nowMs();
  visibleHistoryByConversation.set(conversationKey, existing);
}

function getConversationVisibleMessages(
  conversationKey: string,
  stateIdleMs: number,
): VisibleMessage[] {
  const existing = visibleHistoryByConversation.get(conversationKey);
  if (!existing) {
    return [];
  }
  if (nowMs() - existing.updatedAt > stateIdleMs) {
    visibleHistoryByConversation.delete(conversationKey);
    return [];
  }
  return existing.messages;
}

function rememberActiveRun(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  agentId: string;
  sourceMessageId: string;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
}): void {
  const key = makeConversationAgentKey(params.conversationKey, params.agentId);
  const existing = activeRunsByConversationAgent.get(key);
  activeRunsByConversationAgent.set(key, {
    conversationKey: params.conversationKey,
    channelId: params.channelId,
    conversationId: params.conversationId,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId || existing?.sourceMessageId || "",
    startedAt: existing?.startedAt ?? nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey ?? existing?.sessionKey,
    sessionId: params.sessionId ?? existing?.sessionId,
    runId: params.runId ?? existing?.runId,
  });
}

function getActiveRun(
  conversationKey: string,
  agentId: string,
  activeRunLeaseMs: number,
): ActiveRunRecord | undefined {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const existing = activeRunsByConversationAgent.get(key);
  if (!existing) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt <= activeRunLeaseMs) {
    return existing;
  }
  activeRunsByConversationAgent.delete(key);
  return undefined;
}

function clearActiveRun(conversationKey: string, agentId: string): void {
  activeRunsByConversationAgent.delete(makeConversationAgentKey(conversationKey, agentId));
}

function rememberDeferredWake(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  agentId: string;
  sourceMessageId: string;
  sessionKey?: string;
  sessionId?: string;
}): void {
  const key = makeConversationAgentKey(params.conversationKey, params.agentId);
  const existing = deferredWakesByConversationAgent.get(key);
  deferredWakesByConversationAgent.set(key, {
    conversationKey: params.conversationKey,
    channelId: params.channelId,
    conversationId: params.conversationId,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId,
    enqueuedAt: existing?.enqueuedAt ?? nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey ?? existing?.sessionKey,
    sessionId: params.sessionId ?? existing?.sessionId,
  });
}

function getDeferredWake(
  conversationKey: string,
  agentId: string,
  stateIdleMs: number,
): DeferredWakeRecord | undefined {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const existing = deferredWakesByConversationAgent.get(key);
  if (!existing) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt <= stateIdleMs) {
    return existing;
  }
  deferredWakesByConversationAgent.delete(key);
  return undefined;
}

function clearDeferredWake(conversationKey: string, agentId: string): void {
  deferredWakesByConversationAgent.delete(makeConversationAgentKey(conversationKey, agentId));
}

function conversationHasDeferredWake(conversationKey: string): boolean {
  for (const deferredWake of deferredWakesByConversationAgent.values()) {
    if (deferredWake.conversationKey === conversationKey) {
      return true;
    }
  }
  return false;
}

function markConversationNeedsWake(conversationKey: string | null | undefined): void {
  const normalizedConversationKey = trimString(conversationKey);
  if (!normalizedConversationKey) {
    return;
  }
  pendingWakeConversations.set(normalizedConversationKey, nowMs());
}

function markConversationsNeedingWake(conversationKeys: Iterable<string>): void {
  for (const conversationKey of conversationKeys) {
    markConversationNeedsWake(conversationKey);
  }
}

function reconcileDeferredWakesForSupersededSource(params: {
  conversationKey: string;
  supersededSourceMessageId: string;
  replacementSourceMessageId: string;
  replacementEligibleAgents: string[];
}): void {
  for (const deferredWake of deferredWakesByConversationAgent.values()) {
    if (
      deferredWake.conversationKey !== params.conversationKey ||
      deferredWake.sourceMessageId !== params.supersededSourceMessageId
    ) {
      continue;
    }
    if (params.replacementEligibleAgents.includes(deferredWake.agentId)) {
      rememberDeferredWake({
        conversationKey: deferredWake.conversationKey,
        channelId: deferredWake.channelId,
        conversationId: deferredWake.conversationId,
        agentId: deferredWake.agentId,
        sourceMessageId: params.replacementSourceMessageId,
        sessionKey: deferredWake.sessionKey,
        sessionId: deferredWake.sessionId,
      });
      continue;
    }
    clearDeferredWake(deferredWake.conversationKey, deferredWake.agentId);
  }
}

function rebaseDeferredWakesForLatestVisible(params: {
  conversationKey: string;
  replacementSourceMessageId: string;
  replacementEligibleAgents: string[];
}): void {
  for (const deferredWake of [...deferredWakesByConversationAgent.values()]) {
    if (
      deferredWake.conversationKey !== params.conversationKey ||
      deferredWake.sourceMessageId === params.replacementSourceMessageId
    ) {
      continue;
    }
    if (params.replacementEligibleAgents.includes(deferredWake.agentId)) {
      rememberDeferredWake({
        conversationKey: deferredWake.conversationKey,
        channelId: deferredWake.channelId,
        conversationId: deferredWake.conversationId,
        agentId: deferredWake.agentId,
        sourceMessageId: params.replacementSourceMessageId,
        sessionKey: deferredWake.sessionKey,
        sessionId: deferredWake.sessionId,
      });
      continue;
    }
    clearDeferredWake(deferredWake.conversationKey, deferredWake.agentId);
  }
}

function getSendLock(
  conversationKey: string,
  activeRunLeaseMs: number,
): SendLockRecord | undefined {
  const existing = sendLocksByConversation.get(conversationKey);
  if (!existing) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt <= activeRunLeaseMs) {
    return existing;
  }
  sendLocksByConversation.delete(conversationKey);
  return undefined;
}

function setSendLock(params: {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  sessionKey?: string;
  sessionId?: string;
}): void {
  sendLocksByConversation.set(params.conversationKey, {
    conversationKey: params.conversationKey,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId,
    createdAt: nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey,
    sessionId: params.sessionId,
  });
}

function clearSendLockForAgent(conversationKey: string, agentId: string): void {
  const existing = sendLocksByConversation.get(conversationKey);
  if (existing?.agentId === agentId) {
    sendLocksByConversation.delete(conversationKey);
  }
}

function getLockLeaseMs(
  phase: LockPhase,
  reservationLeaseMs: number,
  activeRunLeaseMs: number,
): number {
  return phase === "active" ? activeRunLeaseMs : reservationLeaseMs;
}

function getBootstrapLock(
  conversationKey: string,
  reservationLeaseMs: number,
  activeRunLeaseMs: number,
): BootstrapLockRecord | undefined {
  const existing = bootstrapLocksByConversation.get(conversationKey);
  if (!existing) {
    return undefined;
  }
  const leaseMs = getLockLeaseMs(existing.phase, reservationLeaseMs, activeRunLeaseMs);
  if (nowMs() - existing.updatedAt <= leaseMs) {
    return existing;
  }
  bootstrapLocksByConversation.delete(conversationKey);
  return undefined;
}

function setBootstrapLock(params: {
  conversationKey: string;
  agentId: string;
  phase: LockPhase;
  sessionKey?: string;
  sessionId?: string;
}): void {
  const existing = bootstrapLocksByConversation.get(params.conversationKey);
  bootstrapLocksByConversation.set(params.conversationKey, {
    conversationKey: params.conversationKey,
    agentId: params.agentId,
    phase: params.phase,
    createdAt: existing?.createdAt ?? nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey,
    sessionId: params.sessionId,
  });
}

function clearBootstrapLockForAgent(conversationKey: string, agentId: string): void {
  const existing = bootstrapLocksByConversation.get(conversationKey);
  if (existing?.agentId === agentId) {
    bootstrapLocksByConversation.delete(conversationKey);
  }
}

function getSourceLock(
  conversationKey: string,
  reservationLeaseMs: number,
  activeRunLeaseMs: number,
): SourceLockRecord | undefined {
  const existing = sourceLocksByConversation.get(conversationKey);
  if (!existing) {
    return undefined;
  }
  const leaseMs = getLockLeaseMs(existing.phase, reservationLeaseMs, activeRunLeaseMs);
  if (nowMs() - existing.updatedAt <= leaseMs) {
    return existing;
  }
  sourceLocksByConversation.delete(conversationKey);
  return undefined;
}

function setSourceLock(params: {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  phase: LockPhase;
  sessionKey?: string;
  sessionId?: string;
}): void {
  const existing = sourceLocksByConversation.get(params.conversationKey);
  sourceLocksByConversation.set(params.conversationKey, {
    conversationKey: params.conversationKey,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId,
    phase: params.phase,
    createdAt: existing?.createdAt ?? nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey,
    sessionId: params.sessionId,
  });
}

function clearSourceLockForAgent(
  conversationKey: string,
  agentId: string,
  sourceMessageId?: string,
): void {
  const existing = sourceLocksByConversation.get(conversationKey);
  if (!existing || existing.agentId !== agentId) {
    return;
  }
  if (sourceMessageId && existing.sourceMessageId !== sourceMessageId) {
    return;
  }
  sourceLocksByConversation.delete(conversationKey);
}

function rememberPostSendGrace(params: {
  conversationKey: string;
  agentId: string;
  sourceMessageId: string;
  sessionKey?: string;
  sessionId?: string;
}): void {
  const key = makeConversationAgentKey(params.conversationKey, params.agentId);
  const existing = postSendGracesByConversationAgent.get(key);
  postSendGracesByConversationAgent.set(key, {
    conversationKey: params.conversationKey,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId,
    createdAt: existing?.createdAt ?? nowMs(),
    updatedAt: nowMs(),
    sessionKey: params.sessionKey ?? existing?.sessionKey,
    sessionId: params.sessionId ?? existing?.sessionId,
  });
}

function clearPostSendGrace(
  conversationKey: string,
  agentId: string,
  sourceMessageId?: string,
): void {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const existing = postSendGracesByConversationAgent.get(key);
  if (!existing) {
    return;
  }
  if (sourceMessageId && existing.sourceMessageId !== sourceMessageId) {
    return;
  }
  postSendGracesByConversationAgent.delete(key);
}

function getPostSendGrace(
  conversationKey: string,
  agentId: string,
  postSendGraceMs: number,
  sourceMessageId?: string,
): PostSendGraceRecord | undefined {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const existing = postSendGracesByConversationAgent.get(key);
  if (!existing) {
    return undefined;
  }
  if (sourceMessageId && existing.sourceMessageId !== sourceMessageId) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt <= postSendGraceMs) {
    return existing;
  }
  postSendGracesByConversationAgent.delete(key);
  return undefined;
}

function formatSpeakerLabel(message: VisibleMessage, agentLabelById: Map<string, string>): string {
  if (message.kind === "external") {
    return message.senderName || message.senderKey || "External";
  }
  if (message.senderAgentId) {
    return agentLabelById.get(message.senderAgentId) ?? message.senderName ?? message.senderAgentId;
  }
  return message.senderName || "Agent";
}

function formatTranscriptEntry(
  message: VisibleMessage,
  index: number,
  agentLabelById: Map<string, string>,
): string {
  const speakerKind = message.kind === "external" ? "External" : "Agent";
  const speaker = formatSpeakerLabel(message, agentLabelById);
  return `[${index + 1}] ${speakerKind} ${speaker} (messageId=${message.messageId})\n${message.content}`;
}

function buildTranscriptContext(params: {
  visibleMessages: VisibleMessage[];
  focusMessage?: VisibleMessage;
  latestVisible?: VisibleMessage;
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): string {
  const headerLines = [
    "Authoritative raw visible conversation backlog for the shared multi-agent baton conversation.",
  ];
  if (params.focusMessage) {
    headerLines.push(
      `Focus visible message: ${formatSpeakerLabel(params.focusMessage, params.agentLabelById)} (messageId=${params.focusMessage.messageId}).`,
    );
  }
  if (params.latestVisible) {
    headerLines.push(
      `Latest visible message in the room: ${formatSpeakerLabel(params.latestVisible, params.agentLabelById)} (messageId=${params.latestVisible.messageId}).`,
    );
  }
  headerLines.push("Visible backlog (oldest first, trimmed from the start if needed):");
  const header = headerLines.join("\n\n");

  if (params.visibleMessages.length === 0) {
    return header;
  }

  const entryTexts = params.visibleMessages.map((message, index) =>
    formatTranscriptEntry(message, index, params.agentLabelById),
  );
  if (params.maxPromptChars === -1) {
    return [header, ...entryTexts].join("\n\n");
  }

  const budget = Math.max(0, params.maxPromptChars - header.length - 2);
  const selected: string[] = [];
  let used = 0;
  for (let index = entryTexts.length - 1; index >= 0; index -= 1) {
    const candidate = entryTexts[index];
    const nextUsed = selected.length === 0 ? candidate.length : used + 2 + candidate.length;
    if (selected.length > 0 && nextUsed > budget) {
      break;
    }
    selected.push(candidate);
    used = selected.length === 1 ? candidate.length : nextUsed;
  }

  return [header, ...selected.reverse()].join("\n\n");
}

function buildPromptContext(params: {
  visibleMessages: VisibleMessage[];
  focusMessage?: VisibleMessage;
  latestVisible?: VisibleMessage;
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): {
  prependSystemContext: string;
  prependContext: string;
} {
  const prependSystemContext = [
    "You are participating in a shared multi-agent conversation.",
    "Reply to the focus visible message using the visible backlog below.",
    "If you decide not to add a reply, output only NO_REPLY.",
  ].join("\n");
  return {
    prependSystemContext,
    prependContext: buildTranscriptContext(params),
  };
}

function buildInitialPromptContext(): {
  prependSystemContext: string;
  prependContext: string;
} {
  return {
    prependSystemContext: [
      "You are participating in a shared multi-agent conversation.",
      "No fresh visible backlog is currently cached by the plugin.",
      "Reply only if the existing conversation history already gives you enough context.",
      "If you decide not to add a reply, output only NO_REPLY.",
    ].join("\n"),
    prependContext: "Reply to the latest visible message already present in the normal conversation history.",
  };
}

function denyContext(message: string): { prependSystemContext: string } {
  return { prependSystemContext: `${message} Output only NO_REPLY.` };
}

function cleanupState(
  stateIdleMs: number,
  reservationLeaseMs: number,
  activeRunLeaseMs: number,
  postSendGraceMs: number,
): string[] {
  const now = nowMs();
  const conversationsToWake = new Set<string>();
  for (const [conversationKey, latestVisible] of latestVisibleByConversation) {
    if (now - latestVisible.updatedAt > stateIdleMs) {
      latestVisibleByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, visibleHistory] of visibleHistoryByConversation) {
    if (now - visibleHistory.updatedAt > stateIdleMs) {
      visibleHistoryByConversation.delete(conversationKey);
    }
  }
  for (const [conversationAgentKey, activeRun] of activeRunsByConversationAgent) {
    if (now - activeRun.updatedAt > activeRunLeaseMs) {
      activeRunsByConversationAgent.delete(conversationAgentKey);
      conversationsToWake.add(activeRun.conversationKey);
    }
  }
  for (const [conversationAgentKey, deferredWake] of deferredWakesByConversationAgent) {
    if (now - deferredWake.updatedAt > stateIdleMs) {
      deferredWakesByConversationAgent.delete(conversationAgentKey);
    }
  }
  for (const [conversationKey, sendLock] of sendLocksByConversation) {
    if (now - sendLock.updatedAt > activeRunLeaseMs) {
      sendLocksByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, bootstrapLock] of bootstrapLocksByConversation) {
    const leaseMs = getLockLeaseMs(bootstrapLock.phase, reservationLeaseMs, activeRunLeaseMs);
    if (now - bootstrapLock.updatedAt > leaseMs) {
      bootstrapLocksByConversation.delete(conversationKey);
      conversationsToWake.add(conversationKey);
    }
  }
  for (const [conversationKey, sourceLock] of sourceLocksByConversation) {
    const leaseMs = getLockLeaseMs(sourceLock.phase, reservationLeaseMs, activeRunLeaseMs);
    if (now - sourceLock.updatedAt > leaseMs) {
      sourceLocksByConversation.delete(conversationKey);
      conversationsToWake.add(conversationKey);
    }
  }
  for (const [conversationAgentKey, postSendGrace] of postSendGracesByConversationAgent) {
    if (now - postSendGrace.updatedAt <= postSendGraceMs) {
      continue;
    }
    postSendGracesByConversationAgent.delete(conversationAgentKey);
    clearActiveRun(postSendGrace.conversationKey, postSendGrace.agentId);
    clearBootstrapLockForAgent(postSendGrace.conversationKey, postSendGrace.agentId);
    clearSourceLockForAgent(
      postSendGrace.conversationKey,
      postSendGrace.agentId,
      postSendGrace.sourceMessageId,
    );
    clearSendLockForAgent(postSendGrace.conversationKey, postSendGrace.agentId);
    conversationsToWake.add(postSendGrace.conversationKey);
  }
  for (const [conversationKey, markedAt] of pendingWakeConversations) {
    if (now - markedAt > stateIdleMs) {
      pendingWakeConversations.delete(conversationKey);
    }
  }
  return [...conversationsToWake];
}

function logDebug(api: OpenClawPluginApi, debug: boolean, message: string): void {
  if (!debug) {
    return;
  }
  api.logger.info?.(`multi-agent-turn-arbiter: ${message}`);
}

export default {
  id: "multi-agent-turn-arbiter",
  name: "Multi-Agent Turn Arbiter",
  description:
    "Keeps a short visible backlog and lets busy agents finish before catching up from the latest retained visible conversation state.",

  register(api: OpenClawPluginApi) {
    const cfg = (api.pluginConfig ?? {}) as ArbiterConfig;
    const enabled = cfg.enabled !== false;
    const enabledChannels = new Set(
      (cfg.enabledChannels ?? []).map((entry) => entry.trim()).filter(Boolean),
    );
    const activeRunLeaseMs =
      typeof cfg.activeRunLeaseMs === "number" ? Math.max(1000, cfg.activeRunLeaseMs) : 180000;
    const reservationLeaseMs =
      typeof cfg.reservationLeaseMs === "number" ? Math.max(1000, cfg.reservationLeaseMs) : 10000;
    const postSendGraceMs =
      typeof cfg.postSendGraceMs === "number" ? Math.max(1000, cfg.postSendGraceMs) : 15000;
    const stateIdleMs =
      typeof cfg.stateIdleMs === "number" ? Math.max(1000, cfg.stateIdleMs) : 300000;
    const maxBacklogTurns =
      cfg.maxBacklogTurns === -1
        ? -1
        : typeof cfg.maxBacklogTurns === "number"
          ? Math.max(1, Math.floor(cfg.maxBacklogTurns))
          : 6;
    const maxPromptChars =
      cfg.maxPromptChars === -1
        ? -1
        : typeof cfg.maxPromptChars === "number"
          ? Math.max(4000, Math.floor(cfg.maxPromptChars))
          : 30000;
    const failOpen = cfg.failOpen !== false;
    const debug = cfg.debug === true;

    if (!enabled) {
      return;
    }

    const agentAccounts = buildAgentAccounts(api.config);
    const agentByAccount = new Map(agentAccounts.map((info) => [info.accountId, info.agentId]));
    const agentLabelById = buildAgentLabelById(agentAccounts);
    const managedAgentBySenderKey = new Map<string, string>();

    const rememberManagedSender = (
      channelId: string,
      senderId: string | undefined,
      agentId: string | undefined,
    ): void => {
      const normalizedSenderId = trimString(senderId);
      const normalizedAgentId = trimString(agentId);
      if (!channelId || !normalizedSenderId || !normalizedAgentId) {
        return;
      }
      managedAgentBySenderKey.set(`${channelId}:${normalizedSenderId}`, normalizedAgentId);
    };

    const rememberManagedSenderFromMetadata = (
      channelId: string,
      metadata: Record<string, unknown> | undefined,
      agentId?: string,
    ): void => {
      const senderId = resolveInboundSenderKey(metadata);
      if (!senderId) {
        return;
      }

      const directAgentId = trimString(agentId) || trimString(metadata?.["agentId"]);
      if (directAgentId && agentLabelById.has(directAgentId)) {
        rememberManagedSender(channelId, senderId, directAgentId);
        return;
      }

      const managedAccountId = getMetadataString(metadata, [
        "senderManagedAccountId",
        "sender_managed_account_id",
        "senderAccountId",
        "sender_account_id",
        "accountId",
        "account_id",
      ]);
      if (!managedAccountId) {
        return;
      }

      const mappedAgentId = agentByAccount.get(managedAccountId);
      if (mappedAgentId) {
        rememberManagedSender(channelId, senderId, mappedAgentId);
      }
    };

    const resolveInboundManagedAgentId = (
      channelId: string,
      metadata?: Record<string, unknown>,
    ): string | undefined => {
      const senderId = resolveInboundSenderKey(metadata);
      if (senderId) {
        const cached = managedAgentBySenderKey.get(`${channelId}:${senderId}`);
        if (cached) {
          return cached;
        }
      }

      const directAgentId = trimString(metadata?.["agentId"]);
      if (directAgentId && agentLabelById.has(directAgentId)) {
        rememberManagedSender(channelId, senderId, directAgentId);
        return directAgentId;
      }

      const inboundAccountId = getMetadataString(metadata, [
        "senderManagedAccountId",
        "sender_managed_account_id",
        "senderAccountId",
        "sender_account_id",
        "accountId",
        "account_id",
      ]);
      if (inboundAccountId) {
        const mapped = agentByAccount.get(inboundAccountId);
        if (mapped) {
          rememberManagedSender(channelId, senderId, mapped);
          return mapped;
        }
      }

      return undefined;
    };

    const resolvePromptState = (
      conversationKey: string,
      agentId: string,
    ): {
      visibleMessages: VisibleMessage[];
      latestVisible?: VisibleMessage;
      focusMessage?: VisibleMessage;
      deferredWake?: DeferredWakeRecord;
    } => {
      const visibleMessages = getConversationVisibleMessages(conversationKey, stateIdleMs);
      const latestVisible =
        getLatestVisibleForConversation(conversationKey, stateIdleMs) ?? visibleMessages.at(-1);
      const deferredWake = getDeferredWake(conversationKey, agentId, stateIdleMs);
      let focusMessage: VisibleMessage | undefined;
      if (deferredWake) {
        focusMessage =
          visibleMessages.find((message) => message.messageId === deferredWake.sourceMessageId) ??
          (latestVisible?.messageId === deferredWake.sourceMessageId ? latestVisible : undefined);
      }
      if (!focusMessage) {
        focusMessage = latestVisible;
      }
      return {
        visibleMessages,
        latestVisible,
        focusMessage,
        deferredWake,
      };
    };

    const maybeLaunchDeferredWake = async (
      conversationKey: string,
      agentId: string,
    ): Promise<void> => {
      const deferredWake = getDeferredWake(conversationKey, agentId, stateIdleMs);
      if (!deferredWake) {
        return;
      }
      if (getActiveRun(conversationKey, agentId, activeRunLeaseMs)) {
        return;
      }

      const visibleMessages = getConversationVisibleMessages(conversationKey, stateIdleMs);
      const latestVisible =
        getLatestVisibleForConversation(conversationKey, stateIdleMs) ?? visibleMessages.at(-1);
      const pendingSourceMessageId = buildPendingSourceMessageId(conversationKey);
      const bootstrapLock = getBootstrapLock(
        conversationKey,
        reservationLeaseMs,
        activeRunLeaseMs,
      );
      const latestVisibleSenderActive =
        latestVisible?.kind === "agent" &&
        latestVisible.senderAgentId &&
        latestVisible.senderAgentId !== agentId
          ? getActiveRun(conversationKey, latestVisible.senderAgentId, activeRunLeaseMs)
          : undefined;
      if (latestVisibleSenderActive) {
        return;
      }
      const focusMessage =
        visibleMessages.find((message) => message.messageId === deferredWake.sourceMessageId) ??
        (latestVisible?.messageId === deferredWake.sourceMessageId ? latestVisible : undefined) ??
        latestVisible;

      if (!focusMessage && bootstrapLock && bootstrapLock.agentId !== agentId) {
        return;
      }

      if (!focusMessage && deferredWake.sourceMessageId !== pendingSourceMessageId) {
        clearDeferredWake(conversationKey, agentId);
        return;
      }

      if (focusMessage) {
        const eligibleAgents = getEligibleAgentsForVisibleMessage(
          focusMessage,
          deferredWake.channelId,
          agentAccounts,
        );
        if (!eligibleAgents.includes(agentId)) {
          clearDeferredWake(conversationKey, agentId);
          return;
        }
      }

      const sessionKey =
        trimString(deferredWake.sessionKey) || buildDerivedSessionKey(agentId, conversationKey);
      const sessionId = trimString(deferredWake.sessionId) || undefined;
      clearDeferredWake(conversationKey, agentId);

      try {
        await api.runtime.subagent.run({
          sessionKey,
          message: "Internal baton wake.",
          extraSystemPrompt: [
            "This is an internal baton wake signal.",
            "Ignore the literal incoming user text.",
            "Reply only from the visible baton conversation context added by the plugin.",
            "If you decide not to add a reply, output only NO_REPLY.",
          ].join("\n"),
          deliver: true,
          idempotencyKey: `deferred-baton:${conversationKey}:${agentId}:${focusMessage?.messageId || pendingSourceMessageId}`,
        });
        logDebug(
          api,
          debug,
          `launched deferred wake for ${agentId} in ${conversationKey}; source=${focusMessage?.messageId || pendingSourceMessageId}`,
        );
      } catch (err) {
        rememberDeferredWake({
          conversationKey,
          channelId: deferredWake.channelId,
          conversationId: deferredWake.conversationId,
          agentId,
          sourceMessageId: focusMessage?.messageId || pendingSourceMessageId,
          sessionKey,
          sessionId,
        });
        api.logger.warn?.(
          `multi-agent-turn-arbiter: failed to launch deferred wake for ${agentId} (${String(err)})`,
        );
      }
    };

    const maybeLaunchDeferredWakesForConversation = async (
      conversationKey: string,
    ): Promise<void> => {
      const visibleMessages = getConversationVisibleMessages(conversationKey, stateIdleMs);
      const latestVisible =
        getLatestVisibleForConversation(conversationKey, stateIdleMs) ?? visibleMessages.at(-1);
      const deferredWakes = [...deferredWakesByConversationAgent.values()]
        .filter((deferredWake) => deferredWake.conversationKey === conversationKey)
        .sort((left, right) => {
          const leftMessage =
            visibleMessages.find((message) => message.messageId === left.sourceMessageId) ??
            (latestVisible?.messageId === left.sourceMessageId ? latestVisible : undefined);
          const rightMessage =
            visibleMessages.find((message) => message.messageId === right.sourceMessageId) ??
            (latestVisible?.messageId === right.sourceMessageId ? latestVisible : undefined);
          const leftTimestamp = leftMessage?.timestamp ?? left.updatedAt;
          const rightTimestamp = rightMessage?.timestamp ?? right.updatedAt;
          return rightTimestamp - leftTimestamp || right.updatedAt - left.updatedAt;
        });

      for (const deferredWake of deferredWakes) {
        await maybeLaunchDeferredWake(conversationKey, deferredWake.agentId);
      }
    };

    const cleanupAndMarkPendingWakes = (): void => {
      const conversationsToWake = cleanupState(
        stateIdleMs,
        reservationLeaseMs,
        activeRunLeaseMs,
        postSendGraceMs,
      );
      markConversationsNeedingWake(conversationsToWake);
    };

    const maybeLaunchPendingWakesForCurrentConversation = async (
      conversationKey: string | null | undefined,
    ): Promise<void> => {
      const normalizedConversationKey = trimString(conversationKey);
      if (!normalizedConversationKey || !pendingWakeConversations.has(normalizedConversationKey)) {
        return;
      }
      pendingWakeConversations.delete(normalizedConversationKey);
      await maybeLaunchDeferredWakesForConversation(normalizedConversationKey);
      if (conversationHasDeferredWake(normalizedConversationKey)) {
        pendingWakeConversations.set(normalizedConversationKey, nowMs());
      }
    };

    api.on("message_received", async (event, ctx) => {
      let currentConversationKey: string | null = null;
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        cleanupAndMarkPendingWakes();

        const conversationKey = resolveInboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.metadata,
        );
        currentConversationKey = conversationKey;
        if (!conversationKey) {
          return;
        }
        const conversationHadPendingWake = pendingWakeConversations.has(conversationKey);

        const conversationId = trimString(event.metadata?.["threadId"]) || trimString(ctx.conversationId);
        if (!conversationId) {
          return;
        }

        const messageId = resolveMessageId(event);
        const senderName =
          getMetadataString(event.metadata, [
            "senderName",
            "sender_name",
            "authorName",
            "author_name",
          ]) ?? "";
        const senderKey = resolveInboundSenderKey(event.metadata);
        const senderAgentId = resolveInboundManagedAgentId(ctx.channelId, event.metadata);
        const kind: "external" | "agent" = senderAgentId ? "agent" : "external";
        const visibleMessage = makeVisibleMessage({
          messageId,
          kind,
          content: event.content,
          senderKey,
          senderName: senderName || undefined,
          senderAgentId,
          timestamp: event.timestamp,
        });

        if (kind === "agent" && trimString(event.content) === "NO_REPLY") {
          logDebug(api, debug, `ignored visible NO_REPLY from ${senderAgentId ?? "unknown"} in ${conversationKey}`);
          return;
        }

        setLatestVisible(conversationKey, visibleMessage);
        appendConversationVisibleMessage(conversationKey, visibleMessage, maxBacklogTurns);

        const sendLock = getSendLock(conversationKey, activeRunLeaseMs);
        const senderActiveRun = senderAgentId
          ? getActiveRun(conversationKey, senderAgentId, activeRunLeaseMs)
          : undefined;
        if (senderAgentId && sendLock?.agentId === senderAgentId && senderActiveRun) {
          setSendLock({
            conversationKey,
            agentId: senderAgentId,
            sourceMessageId: sendLock.sourceMessageId,
            sessionKey: sendLock.sessionKey,
            sessionId: sendLock.sessionId,
          });
        }

        const eligibleAgents = getEligibleAgentsForVisibleMessage(
          visibleMessage,
          ctx.channelId,
          agentAccounts,
        );
        if (conversationHadPendingWake) {
          rebaseDeferredWakesForLatestVisible({
            conversationKey,
            replacementSourceMessageId: visibleMessage.messageId,
            replacementEligibleAgents: eligibleAgents,
          });
        }
        for (const targetAgentId of eligibleAgents) {
          const activeRun = getActiveRun(conversationKey, targetAgentId, activeRunLeaseMs);
          if (!activeRun && (!senderActiveRun || targetAgentId === senderAgentId)) {
            continue;
          }
          rememberDeferredWake({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            agentId: targetAgentId,
            sourceMessageId: visibleMessage.messageId,
            sessionKey: activeRun?.sessionKey || buildDerivedSessionKey(targetAgentId, conversationKey),
            sessionId: activeRun?.sessionId,
          });
          logDebug(
            api,
            debug,
            `deferred latest visible ${visibleMessage.messageId} for busy ${targetAgentId} in ${conversationKey}`,
          );
        }

        if (!senderAgentId) {
          return;
        }

        if (senderActiveRun) {
          rememberPostSendGrace({
            conversationKey,
            agentId: senderAgentId,
            sourceMessageId: senderActiveRun.sourceMessageId,
            sessionKey: senderActiveRun.sessionKey,
            sessionId: senderActiveRun.sessionId,
          });
          rememberActiveRun({
            conversationKey,
            channelId: senderActiveRun.channelId,
            conversationId: senderActiveRun.conversationId,
            agentId: senderAgentId,
            sourceMessageId: senderActiveRun.sourceMessageId,
            sessionKey: senderActiveRun.sessionKey,
            sessionId: senderActiveRun.sessionId,
            runId: senderActiveRun.runId,
          });
          return;
        }

        clearPostSendGrace(conversationKey, senderAgentId);
        clearActiveRun(conversationKey, senderAgentId);
        clearSendLockForAgent(conversationKey, senderAgentId);

        const senderDeferredWake = getDeferredWake(conversationKey, senderAgentId, stateIdleMs);
        if (senderDeferredWake && senderDeferredWake.sourceMessageId !== visibleMessage.messageId) {
          await maybeLaunchDeferredWake(conversationKey, senderAgentId);
        }
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: message_received hook failed (${String(err)})`,
        );
      } finally {
        await maybeLaunchPendingWakesForCurrentConversation(currentConversationKey);
      }
    });

    api.on("before_prompt_build", async (_event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const channelId = trimString(ctx.channelId);
        const sessionKey = trimString(ctx.sessionKey) || undefined;
        const sessionId = trimString(ctx.sessionId) || undefined;
        const conversationKey = resolveConversationKeyFromSessionKey(sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const promptState = resolvePromptState(conversationKey, agentId);
        const pendingSourceMessageId = buildPendingSourceMessageId(conversationKey);
        const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
        const bootstrapLock = getBootstrapLock(
          conversationKey,
          reservationLeaseMs,
          activeRunLeaseMs,
        );
        let sourceLock =
          promptState.focusMessage
            ? getSourceLock(conversationKey, reservationLeaseMs, activeRunLeaseMs)
            : undefined;
        const latestVisibleSenderActive =
          promptState.latestVisible?.kind === "agent" &&
          promptState.latestVisible.senderAgentId &&
          promptState.latestVisible.senderAgentId !== agentId
            ? getActiveRun(conversationKey, promptState.latestVisible.senderAgentId, activeRunLeaseMs)
            : undefined;

        if (!promptState.focusMessage) {
          if (bootstrapLock && bootstrapLock.agentId !== agentId) {
            rememberDeferredWake({
              conversationKey,
              channelId,
              conversationId:
                trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
              agentId,
              sourceMessageId: pendingSourceMessageId,
              sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId,
            });
            return denyContext("Another managed agent currently owns the initial conversation turn.");
          }
          if (activeRun) {
            if (activeRun.sourceMessageId === pendingSourceMessageId) {
              setBootstrapLock({
                conversationKey,
                agentId,
                phase: "active",
                sessionKey: sessionKey || activeRun.sessionKey,
                sessionId: sessionId || activeRun.sessionId,
              });
            }
            return denyContext("You already have an active run in this conversation.");
          }
          setBootstrapLock({
            conversationKey,
            agentId,
            phase: "reserved",
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
          return buildInitialPromptContext();
        }

        const eligibleAgents = getEligibleAgentsForVisibleMessage(
          promptState.focusMessage,
          channelId,
          agentAccounts,
        );
        if (!eligibleAgents.includes(agentId)) {
          if (bootstrapLock?.agentId === agentId) {
            rememberDeferredWake({
              conversationKey,
              channelId,
              conversationId:
                trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
              agentId,
              sourceMessageId: pendingSourceMessageId,
              sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId,
            });
          } else if (promptState.deferredWake) {
            clearDeferredWake(conversationKey, agentId);
          }
          return denyContext("This visible message is not addressed to you.");
        }

        if (
          promptState.latestVisible?.kind === "agent" &&
          promptState.latestVisible.senderAgentId === agentId &&
          (!promptState.deferredWake ||
            promptState.deferredWake.sourceMessageId === promptState.latestVisible.messageId)
        ) {
          if (bootstrapLock?.agentId === agentId) {
            rememberDeferredWake({
              conversationKey,
              channelId,
              conversationId:
                trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
              agentId,
              sourceMessageId: pendingSourceMessageId,
              sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId,
            });
          }
          return denyContext("You may not reply directly to your own latest visible message.");
        }

        if (bootstrapLock) {
          if (bootstrapLock.agentId !== agentId) {
            rememberDeferredWake({
              conversationKey,
              channelId,
              conversationId:
                trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
              agentId,
              sourceMessageId: promptState.focusMessage.messageId,
              sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId,
            });
            return denyContext("Another managed agent currently owns the initial conversation turn.");
          }

          if (!sourceLock || sourceLock.agentId === agentId) {
            const promotedPhase: LockPhase =
              bootstrapLock.phase === "active" || !!activeRun ? "active" : "reserved";
            setSourceLock({
              conversationKey,
              agentId,
              sourceMessageId: promptState.focusMessage.messageId,
              phase: promotedPhase,
              sessionKey:
                sessionKey || bootstrapLock.sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId: sessionId || bootstrapLock.sessionId,
            });
            clearBootstrapLockForAgent(conversationKey, agentId);
            sourceLock = getSourceLock(conversationKey, reservationLeaseMs, activeRunLeaseMs);
          }
        }

        if (latestVisibleSenderActive && promptState.latestVisible) {
          rememberDeferredWake({
            conversationKey,
            channelId,
            conversationId:
              trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
            agentId,
            sourceMessageId: promptState.latestVisible.messageId,
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
          return denyContext("Another managed agent is still completing its visible turn.");
        }

        if (
          promptState.focusMessage &&
          sourceLock &&
          sourceLock.sourceMessageId === promptState.focusMessage.messageId &&
          sourceLock.agentId !== agentId
        ) {
          rememberDeferredWake({
            conversationKey,
            channelId,
            conversationId:
              trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
            agentId,
            sourceMessageId: promptState.focusMessage.messageId,
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
          return denyContext("Another managed agent currently owns this visible message.");
        }

        if (activeRun) {
          if (
            promptState.focusMessage &&
            activeRun.sourceMessageId === promptState.focusMessage.messageId &&
            (!sourceLock || sourceLock.agentId === agentId)
          ) {
            setSourceLock({
              conversationKey,
              agentId,
              sourceMessageId: promptState.focusMessage.messageId,
              phase: "active",
              sessionKey: sessionKey || activeRun.sessionKey,
              sessionId: sessionId || activeRun.sessionId,
            });
          }
          if (activeRun.sourceMessageId !== promptState.focusMessage.messageId) {
            rememberDeferredWake({
              conversationKey,
              channelId,
              conversationId:
                trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length),
              agentId,
              sourceMessageId: promptState.focusMessage.messageId,
              sessionKey: sessionKey || activeRun.sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId: sessionId || activeRun.sessionId,
            });
            return denyContext("You already have an active run for an earlier visible message.");
          }
          return denyContext("You already have an active run for this visible message.");
        }

        if (promptState.focusMessage) {
          setSourceLock({
            conversationKey,
            agentId,
            sourceMessageId: promptState.focusMessage.messageId,
            phase: "reserved",
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
        }

        return buildPromptContext({
          visibleMessages:
            promptState.visibleMessages.length > 0
              ? promptState.visibleMessages
              : [promptState.focusMessage],
          focusMessage: promptState.focusMessage,
          latestVisible: promptState.latestVisible,
          agentLabelById,
          maxPromptChars,
        });
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: before_prompt_build hook failed (${String(err)})`,
        );
      }
    });

    api.on("llm_input", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const channelId = trimString(ctx.channelId);
        const sessionKey = trimString(ctx.sessionKey) || undefined;
        const sessionId = trimString(ctx.sessionId) || undefined;
        const conversationKey = resolveConversationKeyFromSessionKey(sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const promptState = resolvePromptState(conversationKey, agentId);
        const pendingSourceMessageId = buildPendingSourceMessageId(conversationKey);
        const sourceMessageId =
          trimString(promptState.deferredWake?.sourceMessageId) ||
          trimString(promptState.focusMessage?.messageId) ||
          pendingSourceMessageId;
        const conversationId =
          trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length);

        if (sourceMessageId === pendingSourceMessageId) {
          const bootstrapLock = getBootstrapLock(
            conversationKey,
            reservationLeaseMs,
            activeRunLeaseMs,
          );
          if (!bootstrapLock || bootstrapLock.agentId !== agentId) {
            logDebug(
              api,
              debug,
              `ignored llm_input for ${agentId}; bootstrap owner is ${bootstrapLock?.agentId ?? "none"} in ${conversationKey}`,
            );
            return;
          }
          setBootstrapLock({
            conversationKey,
            agentId,
            phase: "active",
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
        } else {
          const sourceLock = getSourceLock(
            conversationKey,
            reservationLeaseMs,
            activeRunLeaseMs,
          );
          if (
            !sourceLock ||
            sourceLock.sourceMessageId !== sourceMessageId ||
            sourceLock.agentId !== agentId
          ) {
            logDebug(
              api,
              debug,
              `ignored llm_input for ${agentId}; source=${sourceMessageId} owner=${sourceLock?.agentId ?? "none"} lockSource=${sourceLock?.sourceMessageId ?? "none"} in ${conversationKey}`,
            );
            return;
          }
          setSourceLock({
            conversationKey,
            agentId,
            sourceMessageId,
            phase: "active",
            sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId,
          });
        }
        rememberActiveRun({
          conversationKey,
          channelId,
          conversationId,
          agentId,
          sourceMessageId,
          sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
          sessionId,
          runId: event.runId,
        });
        if (promptState.deferredWake?.sourceMessageId === sourceMessageId) {
          clearDeferredWake(conversationKey, agentId);
        }
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: llm_input hook failed (${String(err)})`);
      }
    });

    api.on("llm_output", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
        if (!activeRun) {
          return;
        }
        if (activeRun.runId && activeRun.runId !== event.runId) {
          return;
        }

        if (!isOnlyNoReplyOutput(event.assistantTexts)) {
          rememberActiveRun({
            conversationKey,
            channelId: activeRun.channelId,
            conversationId: activeRun.conversationId,
            agentId,
            sourceMessageId: activeRun.sourceMessageId,
            sessionKey: activeRun.sessionKey,
            sessionId: activeRun.sessionId,
            runId: activeRun.runId,
          });
          return;
        }

        clearBootstrapLockForAgent(conversationKey, agentId);
        clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
        clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
        clearActiveRun(conversationKey, agentId);
        clearSendLockForAgent(conversationKey, agentId);
        markConversationNeedsWake(conversationKey);
        await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: llm_output hook failed (${String(err)})`);
      }
    });

    api.on("message_sending", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const conversationKey = resolveOutboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.to,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const accountId = trimString(ctx.accountId);
        const agentId = resolveOutboundAgentId(accountId || undefined, event.metadata, agentByAccount);
        if (!agentId) {
          return;
        }
        rememberManagedSenderFromMetadata(ctx.channelId, event.metadata, agentId);

        const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
        if (!activeRun) {
          markConversationNeedsWake(conversationKey);
          await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
          logDebug(api, debug, `cancelled outbound from ${agentId}; no active run for ${conversationKey}`);
          return { cancel: true };
        }

        const pendingSourceMessageId = buildPendingSourceMessageId(conversationKey);
        if (trimString(event.content) === "NO_REPLY") {
          clearBootstrapLockForAgent(conversationKey, agentId);
          clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
          clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
          clearActiveRun(conversationKey, agentId);
          clearSendLockForAgent(conversationKey, agentId);
          markConversationNeedsWake(conversationKey);
          await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
          return { cancel: true };
        }

        if (activeRun.sourceMessageId === pendingSourceMessageId) {
          const bootstrapLock = getBootstrapLock(
            conversationKey,
            reservationLeaseMs,
            activeRunLeaseMs,
          );
          if (!bootstrapLock || bootstrapLock.agentId !== agentId) {
            clearBootstrapLockForAgent(conversationKey, agentId);
            clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
            clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
            clearActiveRun(conversationKey, agentId);
            clearSendLockForAgent(conversationKey, agentId);
            markConversationNeedsWake(conversationKey);
            await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
            logDebug(
              api,
              debug,
              `cancelled outbound from ${agentId}; bootstrap owner is ${bootstrapLock?.agentId ?? "none"} in ${conversationKey}`,
            );
            return { cancel: true };
          }
        } else {
          const sourceLock = getSourceLock(
            conversationKey,
            reservationLeaseMs,
            activeRunLeaseMs,
          );
          if (
            !sourceLock ||
            sourceLock.sourceMessageId !== activeRun.sourceMessageId ||
            sourceLock.agentId !== agentId
          ) {
            clearBootstrapLockForAgent(conversationKey, agentId);
            clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
            clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
            clearActiveRun(conversationKey, agentId);
            clearSendLockForAgent(conversationKey, agentId);
            markConversationNeedsWake(conversationKey);
            await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
            logDebug(
              api,
              debug,
              `cancelled outbound from ${agentId}; source owner is ${sourceLock?.agentId ?? "none"} lockSource=${sourceLock?.sourceMessageId ?? "none"} activeSource=${activeRun.sourceMessageId} in ${conversationKey}`,
            );
            return { cancel: true };
          }
        }

        const deferredWake = getDeferredWake(conversationKey, agentId, stateIdleMs);
        if (
          deferredWake &&
          deferredWake.sourceMessageId &&
          deferredWake.sourceMessageId !== activeRun.sourceMessageId
        ) {
          clearBootstrapLockForAgent(conversationKey, agentId);
          clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
          clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
          clearActiveRun(conversationKey, agentId);
          clearSendLockForAgent(conversationKey, agentId);
          markConversationNeedsWake(conversationKey);
          await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
          logDebug(
            api,
            debug,
            `cancelled outbound from ${agentId}; stale source=${activeRun.sourceMessageId} deferred=${deferredWake.sourceMessageId}`,
          );
          return { cancel: true };
        }

        const latestVisible = getLatestVisibleForConversation(conversationKey, stateIdleMs);
        const activePostSendGrace = getPostSendGrace(
          conversationKey,
          agentId,
          postSendGraceMs,
          activeRun.sourceMessageId,
        );
        const sameAgentFollowUpChunkAllowed =
          !!latestVisible &&
          latestVisible.kind === "agent" &&
          latestVisible.senderAgentId === agentId &&
          latestVisible.messageId !== activeRun.sourceMessageId &&
          !!activePostSendGrace;
        const latestVisibleSupersedesActiveRun =
          activeRun.sourceMessageId !== pendingSourceMessageId &&
          !!latestVisible &&
          latestVisible.messageId !== activeRun.sourceMessageId &&
          (latestVisible.kind === "external" ||
            latestVisible.senderAgentId !== agentId ||
            !sameAgentFollowUpChunkAllowed);
        if (latestVisibleSupersedesActiveRun && latestVisible) {
          const latestEligibleAgents = getEligibleAgentsForVisibleMessage(
            latestVisible,
            activeRun.channelId,
            agentAccounts,
          );
          reconcileDeferredWakesForSupersededSource({
            conversationKey,
            supersededSourceMessageId: activeRun.sourceMessageId,
            replacementSourceMessageId: latestVisible.messageId,
            replacementEligibleAgents: latestEligibleAgents,
          });
          if (latestEligibleAgents.includes(agentId)) {
            rememberDeferredWake({
              conversationKey,
              channelId: activeRun.channelId,
              conversationId: activeRun.conversationId,
              agentId,
              sourceMessageId: latestVisible.messageId,
              sessionKey: activeRun.sessionKey,
              sessionId: activeRun.sessionId,
            });
          }
          clearBootstrapLockForAgent(conversationKey, agentId);
          clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
          clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
          clearActiveRun(conversationKey, agentId);
          clearSendLockForAgent(conversationKey, agentId);
          markConversationNeedsWake(conversationKey);
          await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
          logDebug(
            api,
            debug,
            `cancelled outbound from ${agentId}; stale source=${activeRun.sourceMessageId} latest=${latestVisible.messageId}`,
          );
          return { cancel: true };
        }

        const sendLock = getSendLock(conversationKey, activeRunLeaseMs);
        if (
          sendLock &&
          sendLock.sourceMessageId === activeRun.sourceMessageId &&
          sendLock.agentId !== agentId
        ) {
          clearBootstrapLockForAgent(conversationKey, agentId);
          clearSourceLockForAgent(conversationKey, agentId, activeRun.sourceMessageId);
          clearPostSendGrace(conversationKey, agentId, activeRun.sourceMessageId);
          clearActiveRun(conversationKey, agentId);
          clearSendLockForAgent(conversationKey, agentId);
          markConversationNeedsWake(conversationKey);
          await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
          logDebug(
            api,
            debug,
            `cancelled outbound from ${agentId}; source=${activeRun.sourceMessageId} already won by ${sendLock.agentId}`,
          );
          return { cancel: true };
        }

        setSendLock({
          conversationKey,
          agentId,
          sourceMessageId: activeRun.sourceMessageId,
          sessionKey: activeRun.sessionKey,
          sessionId: activeRun.sessionId,
        });
        if (activeRun.sourceMessageId === pendingSourceMessageId) {
          setBootstrapLock({
            conversationKey,
            agentId,
            phase: "active",
            sessionKey: activeRun.sessionKey,
            sessionId: activeRun.sessionId,
          });
        } else {
          setSourceLock({
            conversationKey,
            agentId,
            sourceMessageId: activeRun.sourceMessageId,
            phase: "active",
            sessionKey: activeRun.sessionKey,
            sessionId: activeRun.sessionId,
          });
        }
        rememberActiveRun({
          conversationKey,
          channelId: activeRun.channelId,
          conversationId: activeRun.conversationId,
          agentId,
          sourceMessageId: activeRun.sourceMessageId,
          sessionKey: activeRun.sessionKey,
          sessionId: activeRun.sessionId,
          runId: activeRun.runId,
        });
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: message_sending hook failed (${String(err)})`);
        if (failOpen) {
          return;
        }
        return { cancel: true };
      }
    });

    api.on("message_sent", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const conversationKey = resolveOutboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.to,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const accountId = trimString(ctx.accountId);
        const agentId = resolveOutboundAgentId(accountId || undefined, event.metadata, agentByAccount);
        if (!agentId) {
          return;
        }
        rememberManagedSenderFromMetadata(ctx.channelId, event.metadata, agentId);

        if (event.success) {
          const pendingSourceMessageId = buildPendingSourceMessageId(conversationKey);
          const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
          if (activeRun) {
            rememberActiveRun({
              conversationKey,
              channelId: activeRun.channelId,
              conversationId: activeRun.conversationId,
              agentId,
              sourceMessageId: activeRun.sourceMessageId,
              sessionKey: activeRun.sessionKey,
              sessionId: activeRun.sessionId,
              runId: activeRun.runId,
            });
            if (activeRun.sourceMessageId === pendingSourceMessageId) {
              setBootstrapLock({
                conversationKey,
                agentId,
                phase: "active",
                sessionKey: activeRun.sessionKey,
                sessionId: activeRun.sessionId,
              });
            } else {
              setSourceLock({
                conversationKey,
                agentId,
                sourceMessageId: activeRun.sourceMessageId,
                phase: "active",
                sessionKey: activeRun.sessionKey,
                sessionId: activeRun.sessionId,
              });
            }
            rememberPostSendGrace({
              conversationKey,
              agentId,
              sourceMessageId: activeRun.sourceMessageId,
              sessionKey: activeRun.sessionKey,
              sessionId: activeRun.sessionId,
            });
          }
          const sendLock = getSendLock(conversationKey, activeRunLeaseMs);
          if (sendLock?.agentId === agentId) {
            setSendLock({
              conversationKey,
              agentId,
              sourceMessageId: sendLock.sourceMessageId,
              sessionKey: sendLock.sessionKey,
              sessionId: sendLock.sessionId,
            });
          }
          return;
        }

        const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
        clearBootstrapLockForAgent(conversationKey, agentId);
        clearSourceLockForAgent(conversationKey, agentId, activeRun?.sourceMessageId);
        clearPostSendGrace(conversationKey, agentId, activeRun?.sourceMessageId);
        clearActiveRun(conversationKey, agentId);
        clearSendLockForAgent(conversationKey, agentId);
        markConversationNeedsWake(conversationKey);
        await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: message_sent hook failed (${String(err)})`);
      }
    });

    api.on("agent_end", async (_event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }
        cleanupAndMarkPendingWakes();

        const activeRun = getActiveRun(conversationKey, agentId, activeRunLeaseMs);
        const sendLock = getSendLock(conversationKey, activeRunLeaseMs);
        clearBootstrapLockForAgent(conversationKey, agentId);
        clearSourceLockForAgent(conversationKey, agentId, activeRun?.sourceMessageId);
        clearPostSendGrace(conversationKey, agentId, activeRun?.sourceMessageId);
        clearActiveRun(conversationKey, agentId);
        if (sendLock?.agentId === agentId) {
          clearSendLockForAgent(conversationKey, agentId);
        }
        markConversationNeedsWake(conversationKey);
        await maybeLaunchPendingWakesForCurrentConversation(conversationKey);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: agent_end hook failed (${String(err)})`);
      }
    });
  },
};
