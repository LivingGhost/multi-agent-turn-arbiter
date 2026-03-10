import { createHash } from "node:crypto";
import type { OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

type ArbiterConfig = {
  enabled?: boolean;
  enabledChannels?: string[];
  leaseMs?: number;
  activeRunLeaseMs?: number;
  epochIdleMs?: number;
  maxBotHops?: number;
  maxBacklogTurns?: number;
  maxPromptChars?: number;
  settleMs?: number;
  botReopenCooldownMs?: number;
  failOpen?: boolean;
  debug?: boolean;
};

type BotReopenGuard = {
  blockedUntil: number;
  sourceMessageId: string;
};

type PendingSendAllowance = {
  agentId: string;
  grantedAt: number;
  lastActivityAt: number;
  sourceMessageId?: string;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
  approvedOutputTextNormalized?: string;
  accountId?: string;
};

type ConversationLatestVisibleRecord = {
  message: VisibleMessage;
  updatedAt: number;
};

type ConversationVisibleHistoryRecord = {
  messages: VisibleMessage[];
  updatedAt: number;
};

type ActiveTurnRecord = {
  conversationKey: string;
  agentId: string;
  sourceMessageId?: string;
  claimedAt?: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
};

type DeferredBatonRecord = {
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
  senderName?: string;
  senderAgentId?: string;
  timestamp: number;
};

type ClaimRecord = {
  agentId: string;
  accountId?: string;
  sourceMessageId: string;
  claimedAt: number;
  lastActivityAt: number;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
  approvedOutputTextNormalized?: string;
  sendCompletedAt?: number;
  lastVisibleAt?: number;
  observedMessageCount: number;
  observedMessageIds?: string[];
  expectedTextNormalized?: string;
  observedTextNormalized?: string;
};

type EpochRecord = {
  epochId: string;
  conversationKey: string;
  channelId: string;
  conversationId: string;
  createdAt: number;
  updatedAt: number;
  rootMessageId: string;
  targetAgents: string[];
  visibleMessages: VisibleMessage[];
  claim?: ClaimRecord;
  declinedByMessageId: Record<string, string[]>;
  hopCount: number;
};

const epochsByConversation = new Map<string, EpochRecord>();
const settleTimersByConversation = new Map<string, ReturnType<typeof setTimeout>>();
const botReopenGuardsByConversation = new Map<string, BotReopenGuard>();
const pendingAllowancesByConversation = new Map<string, PendingSendAllowance>();
const latestVisibleByConversation = new Map<string, ConversationLatestVisibleRecord>();
const visibleHistoryByConversation = new Map<string, ConversationVisibleHistoryRecord>();
const activeTurnsByConversationAgent = new Map<string, ActiveTurnRecord>();
const deferredBatonsByConversationAgent = new Map<string, DeferredBatonRecord>();

function clearSettleTimer(conversationKey: string): void {
  const timer = settleTimersByConversation.get(conversationKey);
  if (!timer) {
    return;
  }
  clearTimeout(timer);
  settleTimersByConversation.delete(conversationKey);
}

function deleteEpoch(conversationKey: string): void {
  clearSettleTimer(conversationKey);
  epochsByConversation.delete(conversationKey);
}

function setBotReopenGuard(conversationKey: string, sourceMessageId: string, cooldownMs: number): void {
  if (cooldownMs <= 0) {
    botReopenGuardsByConversation.delete(conversationKey);
    return;
  }
  botReopenGuardsByConversation.set(conversationKey, {
    blockedUntil: nowMs() + cooldownMs,
    sourceMessageId,
  });
}

function clearBotReopenGuard(conversationKey: string): void {
  botReopenGuardsByConversation.delete(conversationKey);
}

function getActiveBotReopenGuard(conversationKey: string): BotReopenGuard | undefined {
  const guard = botReopenGuardsByConversation.get(conversationKey);
  if (!guard) {
    return undefined;
  }
  if (guard.blockedUntil > nowMs()) {
    return guard;
  }
  botReopenGuardsByConversation.delete(conversationKey);
  return undefined;
}

function clearPendingAllowance(conversationKey: string): void {
  pendingAllowancesByConversation.delete(conversationKey);
}

function touchPendingAllowance(allowance: PendingSendAllowance): void {
  allowance.lastActivityAt = nowMs();
}

function setLatestVisible(conversationKey: string, message: VisibleMessage): void {
  latestVisibleByConversation.set(conversationKey, {
    message,
    updatedAt: nowMs(),
  });
}

function getLatestVisibleForConversation(
  conversationKey: string,
  idleMs: number,
): VisibleMessage | undefined {
  const existing = latestVisibleByConversation.get(conversationKey);
  if (!existing) {
    return undefined;
  }
  if (nowMs() - existing.updatedAt > idleMs) {
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
  idleMs: number,
): VisibleMessage[] {
  const existing = visibleHistoryByConversation.get(conversationKey);
  if (!existing) {
    return [];
  }
  if (nowMs() - existing.updatedAt > idleMs) {
    visibleHistoryByConversation.delete(conversationKey);
    return [];
  }
  return existing.messages;
}

function getActivePendingAllowance(
  conversationKey: string,
  leaseMs: number,
  activeRunLeaseMs: number,
): PendingSendAllowance | undefined {
  const allowance = pendingAllowancesByConversation.get(conversationKey);
  if (!allowance) {
    return undefined;
  }
  const activeLeaseMs = allowance.runId ? Math.max(leaseMs, activeRunLeaseMs) : leaseMs;
  if (nowMs() - allowance.lastActivityAt <= activeLeaseMs) {
    return allowance;
  }
  pendingAllowancesByConversation.delete(conversationKey);
  return undefined;
}

function nowMs(): number {
  return Date.now();
}

function sleepMs(ms: number): Promise<void> {
  if (ms <= 0) {
    return Promise.resolve();
  }
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function normalizeComparableText(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function isEnabledForChannel(enabledChannels: Set<string>, channelId: string): boolean {
  return enabledChannels.size === 0 || enabledChannels.has(channelId);
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
  const basis = `${event.timestamp ?? 0}:${event.from}:${event.content}`;
  return `derived-${makeHash(basis)}`;
}

function resolveInboundConversationKey(
  channelId: string,
  conversationId: string | undefined,
  metadata?: Record<string, unknown>,
): string | null {
  const threadId = trimString(metadata?.["threadId"]);
  const base = threadId || trimString(conversationId);
  if (!base) {
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
  if (!base) {
    return null;
  }
  return `${channelId}:${base}`;
}

function resolveConversationKeyFromSessionKey(
  sessionKey: string | undefined,
  channelId: string | undefined,
): string | null {
  const key = trimString(sessionKey);
  const channel = trimString(channelId);
  if (!key || !channel) {
    return null;
  }
  const stripped = key.replace(/^agent:[^:]+:/, "");
  if (!stripped.startsWith(`${channel}:`)) {
    return null;
  }
  return stripped;
}

function buildDerivedSessionKey(agentId: string, conversationKey: string): string {
  return `agent:${trimString(agentId)}:${conversationKey}`;
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

function makeConversationAgentKey(conversationKey: string, agentId: string): string {
  return `${conversationKey}::${agentId}`;
}

function rememberActiveTurn(params: {
  conversationKey: string;
  agentId: string;
  sourceMessageId?: string;
  claimedAt?: number;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
}): void {
  const key = makeConversationAgentKey(params.conversationKey, params.agentId);
  const existing = activeTurnsByConversationAgent.get(key);
  activeTurnsByConversationAgent.set(key, {
    conversationKey: params.conversationKey,
    agentId: params.agentId,
    sourceMessageId: params.sourceMessageId ?? existing?.sourceMessageId,
    claimedAt: params.claimedAt ?? existing?.claimedAt,
    updatedAt: nowMs(),
    sessionKey: params.sessionKey ?? existing?.sessionKey,
    sessionId: params.sessionId ?? existing?.sessionId,
    runId: params.runId ?? existing?.runId,
  });
}

function getActiveTurn(
  conversationKey: string,
  agentId: string,
  leaseMs: number,
  activeRunLeaseMs: number,
): ActiveTurnRecord | undefined {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const activeTurn = activeTurnsByConversationAgent.get(key);
  if (!activeTurn) {
    return undefined;
  }
  const activeLeaseMs = activeTurn.runId ? Math.max(leaseMs, activeRunLeaseMs) : leaseMs;
  if (nowMs() - activeTurn.updatedAt <= activeLeaseMs) {
    return activeTurn;
  }
  activeTurnsByConversationAgent.delete(key);
  return undefined;
}

function clearActiveTurn(conversationKey: string, agentId: string): void {
  activeTurnsByConversationAgent.delete(makeConversationAgentKey(conversationKey, agentId));
}

function rememberDeferredBaton(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  agentId: string;
  sourceMessageId: string;
  sessionKey?: string;
  sessionId?: string;
}): void {
  const key = makeConversationAgentKey(params.conversationKey, params.agentId);
  const existing = deferredBatonsByConversationAgent.get(key);
  deferredBatonsByConversationAgent.set(key, {
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

function getDeferredBaton(
  conversationKey: string,
  agentId: string,
  idleMs: number,
): DeferredBatonRecord | undefined {
  const key = makeConversationAgentKey(conversationKey, agentId);
  const baton = deferredBatonsByConversationAgent.get(key);
  if (!baton) {
    return undefined;
  }
  if (nowMs() - baton.updatedAt <= idleMs) {
    return baton;
  }
  deferredBatonsByConversationAgent.delete(key);
  return undefined;
}

function clearDeferredBaton(conversationKey: string, agentId: string): void {
  deferredBatonsByConversationAgent.delete(makeConversationAgentKey(conversationKey, agentId));
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

  if (matchedAgents.size > 0) {
    return uniqueStrings([...matchedAgents]);
  }

  return allAgents;
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

function buildAgentLabelById(infos: AgentAccountInfo[]): Map<string, string> {
  const labels = new Map<string, string>();
  for (const info of infos) {
    if (!labels.has(info.agentId)) {
      labels.set(info.agentId, info.displayNames[0] ?? info.agentId);
    }
  }
  return labels;
}

function makeVisibleMessage(params: {
  messageId: string;
  kind: "external" | "agent";
  content: string;
  senderName?: string;
  senderAgentId?: string;
  timestamp?: number;
}): VisibleMessage {
  return {
    messageId: params.messageId,
    kind: params.kind,
    content: params.content,
    senderName: params.senderName,
    senderAgentId: params.senderAgentId,
    timestamp: typeof params.timestamp === "number" ? params.timestamp : nowMs(),
  };
}

function makeEpoch(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  rootMessage: VisibleMessage;
  targetAgents: string[];
}): EpochRecord {
  const timestamp = params.rootMessage.timestamp;
  return {
    epochId: `${params.conversationKey}:${params.rootMessage.messageId}`,
    conversationKey: params.conversationKey,
    channelId: params.channelId,
    conversationId: params.conversationId,
    createdAt: timestamp,
    updatedAt: timestamp,
    rootMessageId: params.rootMessage.messageId,
    targetAgents: uniqueStrings(params.targetAgents),
    visibleMessages: [params.rootMessage],
    declinedByMessageId: {},
    hopCount: 0,
  };
}

function makePendingEpoch(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  targetAgents: string[];
}): EpochRecord {
  const timestamp = nowMs();
  return {
    epochId: `${params.conversationKey}:pending:${timestamp}`,
    conversationKey: params.conversationKey,
    channelId: params.channelId,
    conversationId: params.conversationId,
    createdAt: timestamp,
    updatedAt: timestamp,
    rootMessageId: `pending-${makeHash(`${params.conversationKey}:${timestamp}`)}`,
    targetAgents: uniqueStrings(params.targetAgents),
    visibleMessages: [],
    declinedByMessageId: {},
    hopCount: 0,
  };
}

function getLatestVisible(epoch: EpochRecord): VisibleMessage | undefined {
  return epoch.visibleMessages.at(-1);
}

function clearClaim(epoch: EpochRecord): void {
  if (!epoch.claim) {
    return;
  }
  clearActiveTurn(epoch.conversationKey, epoch.claim.agentId);
  clearSettleTimer(epoch.conversationKey);
  epoch.claim = undefined;
  epoch.updatedAt = nowMs();
}

function touchClaim(claim: ClaimRecord): void {
  claim.lastActivityAt = nowMs();
}

function maybeExpireClaim(epoch: EpochRecord, leaseMs: number, activeRunLeaseMs: number): boolean {
  const claim = epoch.claim;
  if (!claim) {
    return false;
  }
  const activeLeaseMs =
    claim.runId && !claim.sendCompletedAt ? Math.max(leaseMs, activeRunLeaseMs) : leaseMs;
  const lastActivityAt = claim.lastActivityAt || claim.claimedAt;
  if (nowMs() - lastActivityAt <= activeLeaseMs) {
    return false;
  }
  clearClaim(epoch);
  return true;
}

function cleanupEpochs(epochIdleMs: number, leaseMs: number, activeRunLeaseMs: number): void {
  const now = nowMs();
  for (const [conversationKey, guard] of botReopenGuardsByConversation) {
    if (now >= guard.blockedUntil) {
      botReopenGuardsByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, allowance] of pendingAllowancesByConversation) {
    const activeLeaseMs = allowance.runId ? Math.max(leaseMs, activeRunLeaseMs) : leaseMs;
    if (now - allowance.lastActivityAt > activeLeaseMs) {
      pendingAllowancesByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, epoch] of epochsByConversation) {
    maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);
    if (now - epoch.updatedAt > epochIdleMs) {
      deleteEpoch(conversationKey);
    }
  }
  for (const [conversationKey, latestVisible] of latestVisibleByConversation) {
    if (now - latestVisible.updatedAt > epochIdleMs) {
      latestVisibleByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, visibleHistory] of visibleHistoryByConversation) {
    if (now - visibleHistory.updatedAt > epochIdleMs) {
      visibleHistoryByConversation.delete(conversationKey);
    }
  }
  for (const [conversationAgentKey, activeTurn] of activeTurnsByConversationAgent) {
    const activeLeaseMs = activeTurn.runId ? Math.max(leaseMs, activeRunLeaseMs) : leaseMs;
    if (now - activeTurn.updatedAt > activeLeaseMs) {
      activeTurnsByConversationAgent.delete(conversationAgentKey);
    }
  }
  for (const [conversationAgentKey, deferredBaton] of deferredBatonsByConversationAgent) {
    if (now - deferredBaton.updatedAt > epochIdleMs) {
      deferredBatonsByConversationAgent.delete(conversationAgentKey);
    }
  }
}

function appendVisibleMessage(epoch: EpochRecord, message: VisibleMessage): boolean {
  const existingIndex = epoch.visibleMessages.findIndex((entry) => entry.messageId === message.messageId);
  if (existingIndex >= 0) {
    epoch.visibleMessages[existingIndex] = message;
    epoch.updatedAt = nowMs();
    return false;
  }
  epoch.visibleMessages.push(message);
  epoch.updatedAt = nowMs();
  return true;
}

function isSameVisibleMessage(a: VisibleMessage, b: VisibleMessage): boolean {
  return (
    a.kind === b.kind &&
    trimString(a.senderAgentId) === trimString(b.senderAgentId) &&
    trimString(a.senderName) === trimString(b.senderName) &&
    normalizeComparableText(a.content) === normalizeComparableText(b.content)
  );
}

function hasVisibleMessage(epoch: EpochRecord, messageId: string): boolean {
  return epoch.visibleMessages.some((entry) => entry.messageId === messageId);
}

function recomputeClaimObservedText(epoch: EpochRecord, claim: ClaimRecord): void {
  const observedMessageIds = claim.observedMessageIds ?? [];
  const observedParts = observedMessageIds
    .map((messageId) => epoch.visibleMessages.find((entry) => entry.messageId === messageId)?.content ?? "")
    .filter((content) => Boolean(normalizeComparableText(content)));
  claim.observedMessageCount = observedMessageIds.length;
  claim.observedTextNormalized =
    observedParts.length > 0 ? normalizeComparableText(observedParts.join("\n")) : undefined;
}

function getEligibleAgents(epoch: EpochRecord): string[] {
  const latest = getLatestVisible(epoch);
  if (!latest) {
    return [];
  }
  if (latest.kind === "agent" && latest.senderAgentId) {
    return epoch.targetAgents.filter((agentId) => agentId !== latest.senderAgentId);
  }
  return [...epoch.targetAgents];
}

function getDeclinedAgents(epoch: EpochRecord, messageId: string): Set<string> {
  return new Set(epoch.declinedByMessageId[messageId] ?? []);
}

function markDeclined(epoch: EpochRecord, agentId: string, messageId: string): void {
  const current = new Set(epoch.declinedByMessageId[messageId] ?? []);
  current.add(agentId);
  epoch.declinedByMessageId[messageId] = [...current];
  epoch.updatedAt = nowMs();
}

function haveAllEligibleAgentsDeclined(epoch: EpochRecord, messageId: string): boolean {
  const latest = getLatestVisible(epoch);
  if (!latest || latest.messageId !== messageId) {
    return false;
  }
  const eligible = getEligibleAgents(epoch);
  if (eligible.length === 0) {
    return true;
  }
  const declined = getDeclinedAgents(epoch, messageId);
  return eligible.every((agentId) => declined.has(agentId));
}

function formatSpeakerLabel(message: VisibleMessage, agentLabelById: Map<string, string>): string {
  if (message.kind === "external") {
    return message.senderName || "External";
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

function getTurnSpeakerKey(message: VisibleMessage): string {
  if (message.kind === "agent") {
    const agentId = trimString(message.senderAgentId);
    if (agentId) {
      return `agent:${agentId}`;
    }
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

function buildTailTranscript(params: {
  messages: VisibleMessage[];
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
  prependSystemContext: string;
  latestLabel: string;
}): string {
  const header = [
    "Authoritative raw visible conversation tail for the current baton conversation.",
    `Current visible message: ${params.latestLabel}.`,
    "Transcript tail for current conversation (oldest first, truncated from the start if needed):",
  ].join("\n\n");
  const entries = params.messages.map((message, index) => ({
    message,
    index,
    text: formatTranscriptEntry(message, index, params.agentLabelById),
  }));
  if (entries.length === 0) {
    return header;
  }

  const tailTexts = entries.map(({ message, index }) =>
    formatTranscriptEntry(message, index, params.agentLabelById),
  );
  if (params.maxPromptChars === -1) {
    return [header, ...tailTexts].join("\n\n");
  }

  const budget = Math.max(0, params.maxPromptChars - `${params.prependSystemContext}\n\n${header}`.length);
  const selected: string[] = [tailTexts.at(-1) ?? ""];
  let used = selected[0].length;
  for (let index = tailTexts.length - 2; index >= 0; index -= 1) {
    const candidate = tailTexts[index];
    const nextUsed = used + 2 + candidate.length;
    if (nextUsed > budget) {
      break;
    }
    selected.push(candidate);
    used = nextUsed;
  }

  return [header, ...selected.reverse()].join("\n\n");
}

function buildPromptContext(params: {
  epoch: EpochRecord;
  visibleMessages: VisibleMessage[];
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): { prependSystemContext: string; prependContext: string } | null {
  const latest = getLatestVisible(params.epoch);
  if (!latest) {
    return null;
  }
  const latestLabel = formatSpeakerLabel(latest, params.agentLabelById);
  const prependSystemContext = [
    "You are participating in a multi-agent baton conversation.",
    "If you decide not to add a reply after reading the conversation context, output only NO_REPLY.",
  ].join("\n");
  const prependContext = buildTailTranscript({
    messages: params.visibleMessages,
    agentLabelById: params.agentLabelById,
    maxPromptChars: params.maxPromptChars,
    prependSystemContext,
    latestLabel: `${latestLabel} (messageId=${latest.messageId})`,
  });
  return {
    prependSystemContext,
    prependContext,
  };
}

function buildPendingPromptContext(params: {
  visibleMessages: VisibleMessage[];
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): {
  prependSystemContext: string;
  prependContext: string;
} {
  const latest = params.visibleMessages.at(-1);
  const prependSystemContext = [
    "You are participating in a multi-agent baton conversation.",
    "You currently own the pending baton for the next visible reply.",
    "If you decide not to add a reply, output only NO_REPLY.",
  ].join("\n");
  const prependContext = latest
    ? buildTailTranscript({
        messages: params.visibleMessages,
        agentLabelById: params.agentLabelById,
        maxPromptChars: params.maxPromptChars,
        prependSystemContext,
        latestLabel: `${formatSpeakerLabel(latest, params.agentLabelById)} (messageId=${latest.messageId})`,
      })
    : "Reply to the latest visible message already present in the conversation history.";
  return {
    prependSystemContext,
    prependContext,
  };
}

function denyContext(message: string): { prependSystemContext: string } {
  return { prependSystemContext: `${message} Output only NO_REPLY.` };
}

function isOnlyNoReplyOutput(texts: string[]): boolean {
  const normalized = texts.map((entry) => entry.trim()).filter(Boolean);
  return normalized.length > 0 && normalized.every((entry) => entry === "NO_REPLY");
}

function logDebug(api: OpenClawPluginApi, debug: boolean, message: string): void {
  if (!debug) {
    return;
  }
  api.logger.info?.(`multi-agent-turn-arbiter: ${message}`);
}

function isSameAllowanceOwner(params: {
  allowance: PendingSendAllowance;
  agentId: string;
  sessionKey?: string;
  sessionId?: string;
}): boolean {
  const { allowance, agentId, sessionKey, sessionId } = params;
  if (allowance.agentId !== agentId) {
    return false;
  }
  if (allowance.sessionKey && sessionKey && allowance.sessionKey !== sessionKey) {
    return false;
  }
  if (allowance.sessionId && sessionId && allowance.sessionId !== sessionId) {
    return false;
  }
  return true;
}

function makeClaimFromPendingAllowance(
  allowance: PendingSendAllowance,
  sourceMessageId: string,
): ClaimRecord {
  return {
    agentId: allowance.agentId,
    accountId: allowance.accountId,
    sourceMessageId,
    claimedAt: allowance.grantedAt,
    lastActivityAt: allowance.lastActivityAt,
    sessionKey: allowance.sessionKey,
    sessionId: allowance.sessionId,
    runId: allowance.runId,
    approvedOutputTextNormalized: allowance.approvedOutputTextNormalized,
    observedMessageCount: 0,
    observedMessageIds: [],
  };
}

export default {
  id: "multi-agent-turn-arbiter",
  name: "Multi-Agent Turn Arbiter",
  description:
    "Maintains a single active AI speaker per visible message while preserving a raw in-memory transcript for the current conversation epoch.",

  register(api: OpenClawPluginApi) {
    const cfg = (api.pluginConfig ?? {}) as ArbiterConfig;
    const enabled = cfg.enabled !== false;
    const enabledChannels = new Set(
      (cfg.enabledChannels ?? []).map((entry) => entry.trim()).filter(Boolean),
    );
    const leaseMs = typeof cfg.leaseMs === "number" ? Math.max(1000, cfg.leaseMs) : 30000;
    const activeRunLeaseMs =
      typeof cfg.activeRunLeaseMs === "number" ? Math.max(1000, cfg.activeRunLeaseMs) : 300000;
    const epochIdleMs =
      typeof cfg.epochIdleMs === "number" ? Math.max(60000, cfg.epochIdleMs) : 300000;
    const maxBotHops =
      cfg.maxBotHops === -1
        ? -1
        : typeof cfg.maxBotHops === "number"
          ? Math.max(1, Math.floor(cfg.maxBotHops))
          : 16;
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
    const settleMs = typeof cfg.settleMs === "number" ? Math.max(250, cfg.settleMs) : 10000;
    const botReopenCooldownMs =
      typeof cfg.botReopenCooldownMs === "number"
        ? Math.max(0, Math.floor(cfg.botReopenCooldownMs))
        : 30000;
    const failOpen = cfg.failOpen !== false;
    const debug = cfg.debug === true;
    const claimRecheckGraceMs = 250;

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

    const resolveInboundManagedAgentId = (
      channelId: string,
      metadata?: Record<string, unknown>,
    ): string | undefined => {
      const senderId = getMetadataString(metadata, [
        "senderId",
        "sender_id",
        "authorId",
        "author_id",
        "userId",
        "user_id",
        "fromId",
        "from_id",
      ]);
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

    const rememberDeferredBatonForBusyAgent = (params: {
      conversationKey: string;
      channelId: string;
      conversationId: string;
      agentId: string;
      sourceMessageId: string;
      sessionKey?: string;
      sessionId?: string;
    }): void => {
      rememberDeferredBaton(params);
      logDebug(
        api,
        debug,
        `deferred baton for ${params.agentId} in ${params.conversationKey}; source=${params.sourceMessageId}`,
      );
    };

    const maybeLaunchDeferredBatonsForConversation = async (
      conversationKey: string,
    ): Promise<void> => {
      const existingEpoch = epochsByConversation.get(conversationKey);
      if (existingEpoch?.claim) {
        return;
      }

      const conversationVisibleMessages = getConversationVisibleMessages(conversationKey, epochIdleMs);
      const latestVisible = getLatestVisibleForConversation(conversationKey, epochIdleMs);
      if (conversationVisibleMessages.length === 0 && !latestVisible) {
        return;
      }

      const messageIndexById = new Map(
        conversationVisibleMessages.map((message, index) => [message.messageId, index]),
      );
      const deferredEntries = [...deferredBatonsByConversationAgent.values()]
        .filter((entry) => entry.conversationKey === conversationKey)
        .map((entry) => {
          const resolvedSourceMessage =
            conversationVisibleMessages.find((message) => message.messageId === entry.sourceMessageId) ??
            latestVisible;
          const resolvedSourceIndex = resolvedSourceMessage
            ? (messageIndexById.get(resolvedSourceMessage.messageId) ?? Number.MAX_SAFE_INTEGER)
            : -1;
          return {
            entry,
            resolvedSourceMessage,
            resolvedSourceIndex,
          };
        })
        .sort((left, right) => {
          if (right.resolvedSourceIndex !== left.resolvedSourceIndex) {
            return right.resolvedSourceIndex - left.resolvedSourceIndex;
          }
          if (right.entry.updatedAt !== left.entry.updatedAt) {
            return right.entry.updatedAt - left.entry.updatedAt;
          }
          return right.entry.enqueuedAt - left.entry.enqueuedAt;
        });

      for (const deferredEntry of deferredEntries) {
        const deferredBaton = deferredEntry.entry;
        const agentId = deferredBaton.agentId;
        if (getActiveTurn(conversationKey, agentId, leaseMs, activeRunLeaseMs)) {
          continue;
        }

        const sourceMessage = deferredEntry.resolvedSourceMessage;
        if (!sourceMessage) {
          clearDeferredBaton(conversationKey, agentId);
          continue;
        }

        const targetAgents = getAllChannelAgents(deferredBaton.channelId, agentAccounts, {
          excludeAgentId: sourceMessage.kind === "agent" ? sourceMessage.senderAgentId : undefined,
        });
        if (!targetAgents.includes(agentId)) {
          clearDeferredBaton(conversationKey, agentId);
          continue;
        }

        clearSettleTimer(conversationKey);
        const epoch = makeEpoch({
          conversationKey,
          channelId: deferredBaton.channelId,
          conversationId: deferredBaton.conversationId,
          rootMessage: sourceMessage,
          targetAgents,
        });
        const sessionKey =
          trimString(deferredBaton.sessionKey) || buildDerivedSessionKey(agentId, conversationKey);
        const sessionId = trimString(deferredBaton.sessionId) || undefined;
        const claimedAt = nowMs();
        epoch.claim = {
          agentId,
          sourceMessageId: sourceMessage.messageId,
          claimedAt,
          lastActivityAt: claimedAt,
          sessionKey,
          sessionId,
          observedMessageCount: 0,
          observedMessageIds: [],
        };
        epochsByConversation.set(conversationKey, epoch);
        rememberActiveTurn({
          conversationKey,
          agentId,
          sourceMessageId: sourceMessage.messageId,
          claimedAt,
          sessionKey,
          sessionId,
        });
        clearDeferredBaton(conversationKey, agentId);

        try {
          await api.runtime.subagent.run({
            sessionKey,
            message: "Internal baton wake.",
            extraSystemPrompt: [
              "This is an internal baton wake signal.",
              "Ignore the literal incoming user text.",
              "Reply only from the baton conversation context already added by the plugin.",
              "If you decide not to add a reply, output only NO_REPLY.",
            ].join("\n"),
            deliver: true,
            idempotencyKey: `deferred-baton:${conversationKey}:${agentId}:${sourceMessage.messageId}`,
          });
          logDebug(
            api,
            debug,
            `launched deferred baton for ${agentId} in ${conversationKey}; source=${sourceMessage.messageId}`,
          );
        } catch (err) {
          clearActiveTurn(conversationKey, agentId);
          rememberDeferredBatonForBusyAgent({
            conversationKey,
            channelId: deferredBaton.channelId,
            conversationId: deferredBaton.conversationId,
            agentId,
            sourceMessageId: sourceMessage.messageId,
            sessionKey,
            sessionId,
          });
          api.logger.warn?.(
            `multi-agent-turn-arbiter: failed to launch deferred baton for ${agentId} (${String(err)})`,
          );
        }
        return;
      }
    };

    const scheduleClaimFinalize = (
      conversationKey: string,
      expectedEpochId: string,
      expectedClaimedAt: number,
    ): void => {
      clearSettleTimer(conversationKey);
      const timer = setTimeout(() => {
        settleTimersByConversation.delete(conversationKey);

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch || epoch.epochId !== expectedEpochId) {
          return;
        }

        maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

        const claim = epoch.claim;
        if (!claim || claim.claimedAt !== expectedClaimedAt || !claim.sendCompletedAt) {
          return;
        }

        const now = nowMs();
        const expectedTextNormalized = claim.expectedTextNormalized ?? "";
        const observedTextNormalized = claim.observedTextNormalized ?? "";
        const waitingForInitialVisibility = claim.observedMessageCount <= 0;
        const waitingForVisiblePrefix =
          Boolean(expectedTextNormalized) &&
          (!observedTextNormalized ||
            (expectedTextNormalized.startsWith(observedTextNormalized) &&
              expectedTextNormalized !== observedTextNormalized));
        if ((waitingForInitialVisibility || waitingForVisiblePrefix) && now - claim.sendCompletedAt < leaseMs) {
          scheduleClaimFinalize(conversationKey, expectedEpochId, expectedClaimedAt);
          return;
        }

        const settledAt = Math.max(claim.sendCompletedAt, claim.lastVisibleAt ?? 0);
        if (now - settledAt < settleMs) {
          scheduleClaimFinalize(conversationKey, expectedEpochId, expectedClaimedAt);
          return;
        }

        const observedMessageCount = claim.observedMessageCount;
        const visibleMessageCount = epoch.visibleMessages.length;
        const releasedAgentId = claim.agentId;
        clearClaim(epoch);

        let shouldPersistEpoch = true;

        if (observedMessageCount > 0) {
          epoch.hopCount += 1;
          if (maxBotHops !== -1 && epoch.hopCount >= maxBotHops) {
            shouldPersistEpoch = false;
            deleteEpoch(conversationKey);
          } else {
            epoch.updatedAt = nowMs();
          }
          logDebug(
            api,
            debug,
            `finalized claim for ${conversationKey}; messages=${observedMessageCount} textComplete=${expectedTextNormalized === observedTextNormalized} hops=${epoch.hopCount}`,
          );
        } else {
          if (visibleMessageCount === 0) {
            shouldPersistEpoch = false;
            deleteEpoch(conversationKey);
          } else {
            epoch.updatedAt = nowMs();
          }
          logDebug(
            api,
            debug,
            `cleared claim for ${conversationKey} after send without visible confirmation`,
          );
        }

        if (shouldPersistEpoch) {
          epochsByConversation.set(conversationKey, epoch);
        }
        clearActiveTurn(conversationKey, releasedAgentId);
        void maybeLaunchDeferredBatonsForConversation(conversationKey);
      }, settleMs);
      settleTimersByConversation.set(conversationKey, timer);
    };

    const maybeWaitForSettlingClaimRelease = async (
      conversationKey: string,
      epoch: EpochRecord,
      latest: VisibleMessage | undefined,
      waitingAgentId: string,
    ): Promise<EpochRecord | undefined> => {
      const claim = epoch.claim;
      if (!claim || claim.agentId === waitingAgentId || !claim.sendCompletedAt) {
        return undefined;
      }
      if (!latest || latest.kind !== "agent" || latest.senderAgentId !== claim.agentId) {
        return undefined;
      }

      const settledAt = Math.max(claim.sendCompletedAt, claim.lastVisibleAt ?? 0);
      const deadline = settledAt + settleMs + claimRecheckGraceMs;
      const waitMs = deadline - nowMs();
      if (waitMs <= 0) {
        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);
        return epochsByConversation.get(conversationKey) ?? epoch;
      }

      logDebug(
        api,
        debug,
        `waiting ${waitMs}ms for settling claim in ${conversationKey}; owner=${claim.agentId} waiter=${waitingAgentId}`,
      );
      await sleepMs(waitMs);
      cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);
      return epochsByConversation.get(conversationKey) ?? epoch;
    };

    api.on("message_received", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, ctx.channelId)) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveInboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }

        const conversationId = trimString(event.metadata?.["threadId"]) || trimString(ctx.conversationId);
        if (!conversationId) {
          return;
        }

        const messageId = resolveMessageId(event);
        const senderName = getMetadataString(event.metadata, ["senderName", "sender_name", "authorName", "author_name"]) ?? "";
        const senderAgentId = resolveInboundManagedAgentId(ctx.channelId, event.metadata);
        const kind: "external" | "agent" = senderAgentId ? "agent" : "external";
        const visibleMessage = makeVisibleMessage({
          messageId,
          kind,
          content: event.content,
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
        const pendingAllowanceForLatest = getActivePendingAllowance(
          conversationKey,
          leaseMs,
          activeRunLeaseMs,
        );
        if (
          pendingAllowanceForLatest &&
          (!senderAgentId || senderAgentId !== pendingAllowanceForLatest.agentId)
        ) {
          pendingAllowanceForLatest.sourceMessageId = visibleMessage.messageId;
          touchPendingAllowance(pendingAllowanceForLatest);
          pendingAllowancesByConversation.set(conversationKey, pendingAllowanceForLatest);
        }

        const existing = epochsByConversation.get(conversationKey);
        if (senderAgentId) {
          const senderDeferredBaton = getDeferredBaton(
            conversationKey,
            senderAgentId,
            epochIdleMs,
          );
          const senderClaimSourceMessageId =
            trimString(existing?.claim?.agentId) === senderAgentId
              ? trimString(existing?.claim?.sourceMessageId)
              : "";
          if (
            senderDeferredBaton &&
            senderDeferredBaton.sourceMessageId &&
            senderDeferredBaton.sourceMessageId !== senderClaimSourceMessageId
          ) {
            logDebug(
              api,
              debug,
              `preserved deferred baton for ${senderAgentId} in ${conversationKey}; deferred=${senderDeferredBaton.sourceMessageId} active=${senderClaimSourceMessageId || "none"}`,
            );
          } else {
            clearDeferredBaton(conversationKey, senderAgentId);
          }
        }
        const existingVisibleMessage = existing?.visibleMessages.find((entry) => entry.messageId === messageId);
        if (existing && existingVisibleMessage) {
          const unchanged = isSameVisibleMessage(existingVisibleMessage, visibleMessage);
          appendVisibleMessage(existing, visibleMessage);
          if (kind === "agent" && senderAgentId && existing.claim?.agentId === senderAgentId) {
            const claim = existing.claim;
            claim.lastVisibleAt = nowMs();
            touchClaim(claim);
            clearBotReopenGuard(conversationKey);
            claim.observedMessageIds = uniqueStrings([...(claim.observedMessageIds ?? []), messageId]);
            recomputeClaimObservedText(existing, claim);
            if (!claim.sendCompletedAt) {
              claim.sendCompletedAt = claim.lastVisibleAt;
              existing.updatedAt = claim.sendCompletedAt;
              logDebug(
                api,
                debug,
                `inferred send completion from visible message ${messageId} for ${conversationKey}`,
              );
            }
            scheduleClaimFinalize(conversationKey, existing.epochId, claim.claimedAt);
          }
          existing.updatedAt = nowMs();
          epochsByConversation.set(conversationKey, existing);
          logDebug(
            api,
            debug,
            `${unchanged ? "reused" : "refreshed"} existing visible message ${messageId} for ${conversationKey}`,
          );
          return;
        }

        if (kind === "external") {
          const existingLatest = existing ? getLatestVisible(existing) : undefined;
          if (
            existing &&
            existingLatest?.kind === "external" &&
            trimString(existingLatest.content) === trimString(event.content)
          ) {
            existing.updatedAt = nowMs();
            epochsByConversation.set(conversationKey, existing);
            logDebug(api, debug, `reused existing external epoch ${existing.epochId} for ${conversationKey}`);
            return;
          }

          const targetAgents = inferTargetAgents(event.content, ctx.channelId, agentAccounts);
          if (targetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            epochsByConversation.delete(conversationKey);
            return;
          }
          const immediateTargetAgents: string[] = [];
          for (const targetAgentId of targetAgents) {
            const activeTurn = getActiveTurn(
              conversationKey,
              targetAgentId,
              leaseMs,
              activeRunLeaseMs,
            );
            if (!activeTurn) {
              immediateTargetAgents.push(targetAgentId);
              continue;
            }
            rememberDeferredBatonForBusyAgent({
              conversationKey,
              channelId: ctx.channelId,
              conversationId,
              agentId: targetAgentId,
              sourceMessageId: visibleMessage.messageId,
              sessionKey: activeTurn.sessionKey || buildDerivedSessionKey(targetAgentId, conversationKey),
              sessionId: activeTurn.sessionId,
            });
          }
          if (immediateTargetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            deleteEpoch(conversationKey);
            return;
          }
          clearSettleTimer(conversationKey);
          const epoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents: immediateTargetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (pendingAllowance && epoch.targetAgents.includes(pendingAllowance.agentId)) {
            epoch.claim = makeClaimFromPendingAllowance(pendingAllowance, visibleMessage.messageId);
          }
          clearPendingAllowance(conversationKey);
          setBotReopenGuard(conversationKey, visibleMessage.messageId, botReopenCooldownMs);
          epochsByConversation.set(conversationKey, epoch);
          logDebug(
            api,
            debug,
            `opened epoch ${epoch.epochId} targets=${epoch.targetAgents.join(",") || "none"}`,
          );
          return;
        }

        if (!senderAgentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        const botReopenGuard = getActiveBotReopenGuard(conversationKey);
        if (!epoch) {
          if (botReopenGuard) {
            logDebug(
              api,
              debug,
              `suppressed agent-root epoch from ${senderAgentId} for ${conversationKey}; guard source=${botReopenGuard.sourceMessageId}`,
            );
            return;
          }
          const targetAgents = getAllChannelAgents(ctx.channelId, agentAccounts, {
            excludeAgentId: senderAgentId,
          });
          if (targetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            epochsByConversation.delete(conversationKey);
            return;
          }
          const immediateTargetAgents: string[] = [];
          for (const targetAgentId of targetAgents) {
            const activeTurn = getActiveTurn(
              conversationKey,
              targetAgentId,
              leaseMs,
              activeRunLeaseMs,
            );
            if (!activeTurn) {
              immediateTargetAgents.push(targetAgentId);
              continue;
            }
            rememberDeferredBatonForBusyAgent({
              conversationKey,
              channelId: ctx.channelId,
              conversationId,
              agentId: targetAgentId,
              sourceMessageId: visibleMessage.messageId,
              sessionKey: activeTurn.sessionKey || buildDerivedSessionKey(targetAgentId, conversationKey),
              sessionId: activeTurn.sessionId,
            });
          }
          if (immediateTargetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            deleteEpoch(conversationKey);
            return;
          }
          clearSettleTimer(conversationKey);
          const nextEpoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents: immediateTargetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (pendingAllowance && nextEpoch.targetAgents.includes(pendingAllowance.agentId)) {
            nextEpoch.claim = makeClaimFromPendingAllowance(pendingAllowance, visibleMessage.messageId);
          }
          clearPendingAllowance(conversationKey);
          setBotReopenGuard(conversationKey, visibleMessage.messageId, botReopenCooldownMs);
          epochsByConversation.set(conversationKey, nextEpoch);
          logDebug(
            api,
            debug,
            `opened agent-root epoch ${nextEpoch.epochId} from ${senderAgentId}; targets=${targetAgents.join(",") || "none"}`,
          );
          return;
        }

        maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

        const claim = epoch.claim;
        if (!claim) {
          if (botReopenGuard) {
            logDebug(
              api,
              debug,
              `suppressed stale agent visible message ${messageId} from ${senderAgentId} during reopen guard for ${conversationKey}`,
            );
            return;
          }
          const targetAgents = getAllChannelAgents(ctx.channelId, agentAccounts, {
            excludeAgentId: senderAgentId,
          });
          if (targetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            epochsByConversation.delete(conversationKey);
            return;
          }
          const immediateTargetAgents: string[] = [];
          for (const targetAgentId of targetAgents) {
            const activeTurn = getActiveTurn(
              conversationKey,
              targetAgentId,
              leaseMs,
              activeRunLeaseMs,
            );
            if (!activeTurn) {
              immediateTargetAgents.push(targetAgentId);
              continue;
            }
            rememberDeferredBatonForBusyAgent({
              conversationKey,
              channelId: ctx.channelId,
              conversationId,
              agentId: targetAgentId,
              sourceMessageId: visibleMessage.messageId,
              sessionKey: activeTurn.sessionKey || buildDerivedSessionKey(targetAgentId, conversationKey),
              sessionId: activeTurn.sessionId,
            });
          }
          if (immediateTargetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            deleteEpoch(conversationKey);
            return;
          }
          clearSettleTimer(conversationKey);
          const nextEpoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents: immediateTargetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (pendingAllowance && nextEpoch.targetAgents.includes(pendingAllowance.agentId)) {
            nextEpoch.claim = makeClaimFromPendingAllowance(pendingAllowance, visibleMessage.messageId);
          }
          clearPendingAllowance(conversationKey);
          setBotReopenGuard(conversationKey, visibleMessage.messageId, botReopenCooldownMs);
          epochsByConversation.set(conversationKey, nextEpoch);
          logDebug(
            api,
            debug,
            `reopened epoch ${nextEpoch.epochId} from visible agent ${senderAgentId}; targets=${targetAgents.join(",") || "none"}`,
          );
          return;
        }
        if (claim.agentId !== senderAgentId) {
          logDebug(
            api,
            debug,
            `ignored stale visible agent message ${messageId} from ${senderAgentId} for ${conversationKey}`,
          );
          return;
        }

        const appended = appendVisibleMessage(epoch, visibleMessage);
        claim.lastVisibleAt = nowMs();
        touchClaim(claim);
        clearBotReopenGuard(conversationKey);
        claim.observedMessageIds = uniqueStrings([...(claim.observedMessageIds ?? []), messageId]);
        if (appended) {
          recomputeClaimObservedText(epoch, claim);
        } else {
          recomputeClaimObservedText(epoch, claim);
        }
        if (!claim.sendCompletedAt) {
          claim.sendCompletedAt = claim.lastVisibleAt;
          epoch.updatedAt = claim.sendCompletedAt;
          logDebug(
            api,
            debug,
            `inferred send completion from visible message ${messageId} for ${conversationKey}`,
          );
        }
        scheduleClaimFinalize(conversationKey, epoch.epochId, claim.claimedAt);
        epochsByConversation.set(conversationKey, epoch);
        logDebug(
          api,
          debug,
          `observed visible agent message ${messageId} from ${senderAgentId}; count=${claim.observedMessageCount} sendComplete=${Boolean(claim.sendCompletedAt)}`,
        );
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: message_received hook failed (${String(err)})`,
        );
      }
    });

    api.on("before_prompt_build", async (_event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const channelId = trimString(ctx.channelId);
        const sessionKey = trimString(ctx.sessionKey) || undefined;
        const sessionId = trimString(ctx.sessionId) || undefined;
        const conversationKey = resolveConversationKeyFromSessionKey(sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const latestVisibleForConversation = getLatestVisibleForConversation(conversationKey, epochIdleMs);
        let epoch = epochsByConversation.get(conversationKey);
        const activeTurn = getActiveTurn(conversationKey, agentId, leaseMs, activeRunLeaseMs);
        const currentSourceMessageId =
          trimString(getLatestVisible(epoch)?.messageId) || trimString(latestVisibleForConversation?.messageId);
        const fallbackConversationId =
          epoch?.conversationId || conversationKey.slice(`${channelId}:`.length);
        if (
          activeTurn &&
          currentSourceMessageId &&
          trimString(activeTurn.sourceMessageId) &&
          currentSourceMessageId !== trimString(activeTurn.sourceMessageId)
        ) {
          rememberDeferredBatonForBusyAgent({
            conversationKey,
            channelId,
            conversationId: trimString(ctx.conversationId) || fallbackConversationId,
            agentId,
            sourceMessageId: currentSourceMessageId,
            sessionKey: sessionKey || activeTurn.sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId: sessionId || activeTurn.sessionId,
          });
          return denyContext("You already have an active baton run for an earlier visible message.");
        }

        if (!epoch) {
          const existingAllowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (existingAllowance && !isSameAllowanceOwner({ allowance: existingAllowance, agentId, sessionKey, sessionId })) {
            logDebug(
              api,
              debug,
              `yielding ${agentId}; pending allowance for ${conversationKey} owned by ${existingAllowance.agentId}`,
            );
            return denyContext("Another targeted agent currently owns the pending baton.");
          }
          if (!existingAllowance) {
            const targetAgents = getAllChannelAgents(channelId, agentAccounts);
            if (!targetAgents.includes(agentId)) {
              return;
            }
            pendingAllowancesByConversation.set(conversationKey, {
              agentId,
              grantedAt: nowMs(),
              lastActivityAt: nowMs(),
              sourceMessageId: latestVisibleForConversation?.messageId,
              sessionKey,
              sessionId,
            });
            logDebug(
              api,
              debug,
              `granted pending allowance for ${conversationKey} to ${agentId}`,
            );
          } else {
            if (latestVisibleForConversation?.messageId) {
              existingAllowance.sourceMessageId = latestVisibleForConversation.messageId;
            }
            touchPendingAllowance(existingAllowance);
            pendingAllowancesByConversation.set(conversationKey, existingAllowance);
          }
          const conversationVisibleMessages = getConversationVisibleMessages(conversationKey, epochIdleMs);
          const pendingVisibleMessages =
            conversationVisibleMessages.length > 0
              ? conversationVisibleMessages
              : latestVisibleForConversation
                ? [latestVisibleForConversation]
                : [];
          return buildPendingPromptContext({
            visibleMessages: pendingVisibleMessages,
            agentLabelById,
            maxPromptChars,
          });
        }

        let waitedForSettlingClaim = false;
        while (true) {
          maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

          if (!epoch.targetAgents.includes(agentId)) {
            return denyContext("This conversation step is not addressed to you.");
          }

          const latest = getLatestVisible(epoch);
          if (!latest) {
            if (epoch.claim && epoch.claim.agentId !== agentId) {
              logDebug(
                api,
                debug,
                `yielding ${agentId}; pending epoch ${epoch.epochId} owned by ${epoch.claim.agentId}`,
              );
              return denyContext("Another targeted agent currently owns the pending baton.");
            }

            if (!epoch.claim) {
              const pendingAllowance = getActivePendingAllowance(
                conversationKey,
                leaseMs,
                activeRunLeaseMs,
              );
              if (pendingAllowance && !isSameAllowanceOwner({ allowance: pendingAllowance, agentId, sessionKey, sessionId })) {
                return denyContext("Another targeted agent currently owns the pending baton.");
              }
              if (pendingAllowance) {
                epoch.claim = makeClaimFromPendingAllowance(pendingAllowance, epoch.rootMessageId);
                clearPendingAllowance(conversationKey);
              } else {
                const claimedAt = nowMs();
                epoch.claim = {
                  agentId,
                  sourceMessageId: epoch.rootMessageId,
                  claimedAt,
                  lastActivityAt: claimedAt,
                  sessionKey,
                  sessionId,
                  observedMessageCount: 0,
                  observedMessageIds: [],
                };
              }
              epoch.updatedAt = nowMs();
              epochsByConversation.set(conversationKey, epoch);
            }

            return;
          }

          if (latest.kind === "agent" && latest.senderAgentId === agentId) {
            return denyContext("You may not respond directly to your own latest visible message.");
          }

          const eligibleAgents = getEligibleAgents(epoch);
          if (!eligibleAgents.includes(agentId)) {
            return denyContext("You are not eligible to answer the current visible message.");
          }

          if (getDeclinedAgents(epoch, latest.messageId).has(agentId)) {
            return denyContext("You already declined the current visible message.");
          }

          if (haveAllEligibleAgentsDeclined(epoch, latest.messageId)) {
            deleteEpoch(conversationKey);
            return denyContext("All eligible agents already declined this visible message.");
          }

          if (epoch.claim && epoch.claim.agentId !== agentId) {
            if (!waitedForSettlingClaim) {
              const refreshedEpoch = await maybeWaitForSettlingClaimRelease(
                conversationKey,
                epoch,
                latest,
                agentId,
              );
              if (refreshedEpoch) {
                epoch = refreshedEpoch;
                waitedForSettlingClaim = true;
                continue;
              }
            }
            logDebug(
              api,
              debug,
              `yielding ${agentId}; epoch ${epoch.epochId} owned by ${epoch.claim.agentId}`,
            );
            return denyContext("Another targeted agent currently owns this visible message.");
          }

          if (!epoch.claim) {
            const claimedAt = nowMs();
            epoch.claim = {
              agentId,
              sourceMessageId: latest.messageId,
              claimedAt,
              lastActivityAt: claimedAt,
              sessionKey,
              sessionId,
              observedMessageCount: 0,
              observedMessageIds: [],
            };
            epoch.updatedAt = nowMs();
            epochsByConversation.set(conversationKey, epoch);
            logDebug(
              api,
              debug,
              `claimed epoch ${epoch.epochId} by ${agentId} for ${latest.messageId}`,
            );
          }

          const conversationVisibleMessages = getConversationVisibleMessages(conversationKey, epochIdleMs);
          if (!epoch) {
            const pendingVisibleMessages =
              conversationVisibleMessages.length > 0
                ? conversationVisibleMessages
                : latestVisibleForConversation
                  ? [latestVisibleForConversation]
                  : [];
            return buildPendingPromptContext({
              visibleMessages: pendingVisibleMessages,
              agentLabelById,
              maxPromptChars,
            });
          }
          const prompt = buildPromptContext({
            epoch,
            visibleMessages:
              conversationVisibleMessages.length > 0 ? conversationVisibleMessages : epoch.visibleMessages,
            agentLabelById,
            maxPromptChars,
          });
          if (!prompt) {
            return denyContext("No visible message is available for the current baton epoch.");
          }

          return prompt;
        }
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

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch) {
          const allowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (!allowance || !isSameAllowanceOwner({
            allowance,
            agentId,
            sessionKey: trimString(ctx.sessionKey) || undefined,
            sessionId: trimString(ctx.sessionId) || undefined,
          })) {
            return;
          }
          allowance.runId = event.runId;
          allowance.approvedOutputTextNormalized = undefined;
          touchPendingAllowance(allowance);
          pendingAllowancesByConversation.set(conversationKey, allowance);
          rememberActiveTurn({
            conversationKey,
            agentId,
            sourceMessageId: allowance.sourceMessageId,
            claimedAt: allowance.grantedAt,
            sessionKey: allowance.sessionKey || trimString(ctx.sessionKey) || buildDerivedSessionKey(agentId, conversationKey),
            sessionId: allowance.sessionId || trimString(ctx.sessionId) || undefined,
            runId: event.runId,
          });
          return;
        }

        maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

        const claim = epoch.claim;
        if (!claim || claim.agentId !== agentId) {
          return;
        }

        if (claim.sessionKey && ctx.sessionKey && claim.sessionKey !== ctx.sessionKey) {
          return;
        }
        if (claim.sessionId && ctx.sessionId && claim.sessionId !== ctx.sessionId) {
          return;
        }

        claim.runId = event.runId;
        claim.approvedOutputTextNormalized = undefined;
        touchClaim(claim);
        epoch.updatedAt = nowMs();
        epochsByConversation.set(conversationKey, epoch);
        rememberActiveTurn({
          conversationKey,
          agentId,
          sourceMessageId: claim.sourceMessageId,
          claimedAt: claim.claimedAt,
          sessionKey: claim.sessionKey || trimString(ctx.sessionKey) || buildDerivedSessionKey(agentId, conversationKey),
          sessionId: claim.sessionId || trimString(ctx.sessionId) || undefined,
          runId: event.runId,
        });
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: llm_input hook failed (${String(err)})`,
        );
      }
    });

    api.on("llm_output", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch) {
          const allowance = getActivePendingAllowance(
            conversationKey,
            leaseMs,
            activeRunLeaseMs,
          );
          if (!allowance || !isSameAllowanceOwner({
            allowance,
            agentId,
            sessionKey: trimString(ctx.sessionKey) || undefined,
            sessionId: trimString(ctx.sessionId) || undefined,
          })) {
            return;
          }
          if (!allowance.runId || event.runId !== allowance.runId) {
            return;
          }
          if (!isOnlyNoReplyOutput(event.assistantTexts)) {
            const approvedOutputTextNormalized = normalizeComparableText(event.assistantTexts.join("\n\n"));
            if (approvedOutputTextNormalized) {
              allowance.approvedOutputTextNormalized = approvedOutputTextNormalized;
              touchPendingAllowance(allowance);
              pendingAllowancesByConversation.set(conversationKey, allowance);
            }
            return;
          }
          clearPendingAllowance(conversationKey);
          clearActiveTurn(conversationKey, agentId);
          void maybeLaunchDeferredBatonsForConversation(conversationKey);
          return;
        }

        maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

        const claim = epoch.claim;
        if (!claim || claim.agentId !== agentId) {
          return;
        }

        if (claim.sessionKey && ctx.sessionKey && claim.sessionKey !== ctx.sessionKey) {
          return;
        }
        if (claim.sessionId && ctx.sessionId && claim.sessionId !== ctx.sessionId) {
          return;
        }
        if (!claim.runId || event.runId !== claim.runId) {
          return;
        }
        if (!isOnlyNoReplyOutput(event.assistantTexts)) {
          const approvedOutputTextNormalized = normalizeComparableText(event.assistantTexts.join("\n\n"));
          if (approvedOutputTextNormalized) {
            claim.approvedOutputTextNormalized = approvedOutputTextNormalized;
            touchClaim(claim);
            epoch.updatedAt = nowMs();
            epochsByConversation.set(conversationKey, epoch);
          }
          return;
        }

        const latest = getLatestVisible(epoch);
        if (!latest || latest.messageId !== claim.sourceMessageId) {
          clearClaim(epoch);
          epochsByConversation.set(conversationKey, epoch);
          clearActiveTurn(conversationKey, agentId);
          void maybeLaunchDeferredBatonsForConversation(conversationKey);
          return;
        }

        markDeclined(epoch, agentId, claim.sourceMessageId);
        clearClaim(epoch);
        if (haveAllEligibleAgentsDeclined(epoch, claim.sourceMessageId)) {
          deleteEpoch(conversationKey);
        } else {
          epochsByConversation.set(conversationKey, epoch);
        }
        clearActiveTurn(conversationKey, agentId);
        void maybeLaunchDeferredBatonsForConversation(conversationKey);
        logDebug(
          api,
          debug,
          `agent ${agentId} declined ${claim.sourceMessageId} in epoch ${epoch.epochId}`,
        );
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: llm_output hook failed (${String(err)})`,
        );
      }
    });

    api.on("message_sending", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, ctx.channelId)) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveOutboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.to,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }

        const accountId = trimString(ctx.accountId);
        const agentId = resolveOutboundAgentId(accountId || undefined, event.metadata, agentByAccount);
        if (!agentId) {
          return;
        }

        let epoch = epochsByConversation.get(conversationKey);
        const sessionKey = trimString(ctx.sessionKey) || undefined;
        const sessionId = trimString(ctx.sessionId) || undefined;
        const pendingAllowance = getActivePendingAllowance(
          conversationKey,
          leaseMs,
          activeRunLeaseMs,
        );
        const latestVisibleForConversation = getLatestVisibleForConversation(conversationKey, epochIdleMs);
        if (!epoch) {
          if (getActiveBotReopenGuard(conversationKey)) {
            return { cancel: true };
          }
          if (!pendingAllowance || !isSameAllowanceOwner({ allowance: pendingAllowance, agentId, sessionKey, sessionId })) {
            logDebug(
              api,
              debug,
              `cancelled outbound from ${agentId}; conversation=${conversationKey} has no epoch and no active pending allowance`,
            );
            return { cancel: true };
          }
          const conversationId =
            trimString(event.metadata?.["threadId"]) ||
            trimString(ctx.conversationId) ||
            trimString(event.to);
          const targetAgents = getAllChannelAgents(ctx.channelId, agentAccounts);
          if (!conversationId || targetAgents.length === 0) {
            return;
          }
          epoch = makePendingEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            targetAgents,
          });
          epoch.claim = makeClaimFromPendingAllowance(pendingAllowance, epoch.rootMessageId);
          clearPendingAllowance(conversationKey);
          epochsByConversation.set(conversationKey, epoch);
          logDebug(api, debug, `created pending epoch ${epoch.epochId} claimed by ${agentId}`);
        }

        if (!epoch.targetAgents.includes(agentId)) {
          return { cancel: true };
        }

        if (trimString(event.content) === "NO_REPLY") {
          return { cancel: true };
        }

        maybeExpireClaim(epoch, leaseMs, activeRunLeaseMs);

        let claim = epoch.claim;
        if (!claim && pendingAllowance && isSameAllowanceOwner({ allowance: pendingAllowance, agentId, sessionKey, sessionId })) {
          const latestVisible = getLatestVisible(epoch);
          epoch.claim = makeClaimFromPendingAllowance(
            pendingAllowance,
            latestVisible?.messageId ?? epoch.rootMessageId,
          );
          clearPendingAllowance(conversationKey);
          epoch.updatedAt = nowMs();
          epochsByConversation.set(conversationKey, epoch);
          claim = epoch.claim;
          logDebug(
            api,
            debug,
            `adopted pending allowance for ${agentId} into epoch ${epoch.epochId}`,
          );
        }

        if (!claim || claim.agentId !== agentId) {
          if (latestVisibleForConversation) {
            rememberDeferredBatonForBusyAgent({
              conversationKey,
              channelId: ctx.channelId,
              conversationId: trimString(ctx.conversationId) || trimString(event.to),
              agentId,
              sourceMessageId: latestVisibleForConversation.messageId,
              sessionKey: sessionKey || buildDerivedSessionKey(agentId, conversationKey),
              sessionId,
            });
          }
          clearActiveTurn(conversationKey, agentId);
          void maybeLaunchDeferredBatonsForConversation(conversationKey);
          logDebug(
            api,
            debug,
            `cancelled outbound from ${agentId}; epoch=${epoch.epochId} claim=${claim?.agentId ?? "none"}`,
          );
          return { cancel: true };
        }

        if (claim.sessionKey && ctx.sessionKey && claim.sessionKey !== ctx.sessionKey) {
          return { cancel: true };
        }

        if (claim.sessionId && ctx.sessionId && claim.sessionId !== ctx.sessionId) {
          return { cancel: true };
        }

        const latest = getLatestVisible(epoch);
        if (!latest) {
          epoch.updatedAt = nowMs();
          epochsByConversation.set(conversationKey, epoch);
          return;
        }

        if (latestVisibleForConversation && latestVisibleForConversation.messageId !== claim.sourceMessageId) {
          rememberDeferredBatonForBusyAgent({
            conversationKey,
            channelId: ctx.channelId,
            conversationId: epoch.conversationId,
            agentId,
            sourceMessageId: latestVisibleForConversation.messageId,
            sessionKey: claim.sessionKey || sessionKey || buildDerivedSessionKey(agentId, conversationKey),
            sessionId: claim.sessionId || sessionId,
          });
          clearClaim(epoch);
          epochsByConversation.set(conversationKey, epoch);
          clearActiveTurn(conversationKey, agentId);
          void maybeLaunchDeferredBatonsForConversation(conversationKey);
          logDebug(
            api,
            debug,
            `cancelled outbound from ${agentId}; source=${claim.sourceMessageId} stale against latest=${latestVisibleForConversation.messageId}`,
          );
          return { cancel: true };
        }

        const normalizedExpectedText = normalizeComparableText(event.content);
        const approvedOutputTextNormalized = claim.approvedOutputTextNormalized ?? "";
        if (normalizedExpectedText) {
          if (!approvedOutputTextNormalized) {
            logDebug(
              api,
              debug,
              `cancelled outbound from ${agentId}; epoch=${epoch.epochId} missing approved llm_output`,
            );
            return { cancel: true };
          }
          if (
            !approvedOutputTextNormalized.includes(normalizedExpectedText) &&
            !normalizedExpectedText.includes(approvedOutputTextNormalized)
          ) {
            logDebug(
              api,
              debug,
              `cancelled outbound from ${agentId}; epoch=${epoch.epochId} approved output mismatch`,
            );
            return { cancel: true };
          }
          claim.expectedTextNormalized = normalizeComparableText(
            `${claim.expectedTextNormalized ?? ""}\n${event.content}`,
          );
        }

        if (!claim.accountId && accountId) {
          claim.accountId = accountId;
        }
        touchClaim(claim);
        epoch.updatedAt = nowMs();
        epochsByConversation.set(conversationKey, epoch);
        rememberActiveTurn({
          conversationKey,
          agentId,
          sourceMessageId: claim.sourceMessageId,
          claimedAt: claim.claimedAt,
          sessionKey: claim.sessionKey || sessionKey || buildDerivedSessionKey(agentId, conversationKey),
          sessionId: claim.sessionId || sessionId,
          runId: claim.runId,
        });

        return;
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: message_sending hook failed (${String(err)})`,
        );
        if (failOpen) {
          return;
        }
        return { cancel: true };
      }
    });

    api.on("message_sent", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, ctx.channelId)) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveOutboundConversationKey(
          ctx.channelId,
          ctx.conversationId,
          event.to,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch) {
          return;
        }

        const accountId = trimString(ctx.accountId);
        const agentId = resolveOutboundAgentId(accountId || undefined, event.metadata, agentByAccount);
        if (!agentId || !epoch.targetAgents.includes(agentId)) {
          return;
        }

        if (!event.success) {
          if (epoch.claim?.agentId === agentId) {
            clearClaim(epoch);
            epochsByConversation.set(conversationKey, epoch);
            clearActiveTurn(conversationKey, agentId);
            void maybeLaunchDeferredBatonsForConversation(conversationKey);
          }
          return;
        }

        const claim = epoch.claim;
        if (!claim || claim.agentId !== agentId) {
          return;
        }

        if (claim.sessionKey && ctx.sessionKey && claim.sessionKey !== ctx.sessionKey) {
          return;
        }

        if (claim.sessionId && ctx.sessionId && claim.sessionId !== ctx.sessionId) {
          return;
        }

        claim.sendCompletedAt = nowMs();
        touchClaim(claim);
        epoch.updatedAt = claim.sendCompletedAt;
        epochsByConversation.set(conversationKey, epoch);
        rememberActiveTurn({
          conversationKey,
          agentId,
          sourceMessageId: claim.sourceMessageId,
          claimedAt: claim.claimedAt,
          sessionKey: claim.sessionKey || trimString(ctx.sessionKey) || buildDerivedSessionKey(agentId, conversationKey),
          sessionId: claim.sessionId || trimString(ctx.sessionId) || undefined,
          runId: claim.runId,
        });
        scheduleClaimFinalize(conversationKey, epoch.epochId, claim.claimedAt);
        logDebug(
          api,
          debug,
          `observed successful outbound from ${agentId} for epoch ${epoch.epochId}; waiting ${settleMs}ms to settle visible messages`,
        );
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: message_sent hook failed (${String(err)})`,
        );
      }
    });

    api.on("agent_end", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, trimString(ctx.channelId))) {
          return;
        }

        if (event.success) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch) {
          return;
        }

        const claim = epoch.claim;
        if (!claim || claim.agentId !== agentId) {
          return;
        }

        clearClaim(epoch);
        epochsByConversation.set(conversationKey, epoch);
        clearActiveTurn(conversationKey, agentId);
        void maybeLaunchDeferredBatonsForConversation(conversationKey);
        logDebug(api, debug, `cleared failed claim for ${agentId} in epoch ${epoch.epochId}`);
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: agent_end hook failed (${String(err)})`,
        );
      }
    });
  },
};
