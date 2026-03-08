import { createHash } from "node:crypto";
import type { OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

type ArbiterConfig = {
  enabled?: boolean;
  enabledChannels?: string[];
  leaseMs?: number;
  epochIdleMs?: number;
  maxBotHops?: number;
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
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
  approvedOutputTextNormalized?: string;
  accountId?: string;
};

type AgentAccountInfo = {
  channelId: string;
  accountId: string;
  agentId: string;
  displayNames: string[];
};

type VisibleMessage = {
  messageId: string;
  kind: "human" | "agent";
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
  closedAt?: number;
  status: "open" | "closed";
  closeReason?: string;
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

function clearSettleTimer(conversationKey: string): void {
  const timer = settleTimersByConversation.get(conversationKey);
  if (!timer) {
    return;
  }
  clearTimeout(timer);
  settleTimersByConversation.delete(conversationKey);
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

function getActivePendingAllowance(
  conversationKey: string,
  leaseMs: number,
): PendingSendAllowance | undefined {
  const allowance = pendingAllowancesByConversation.get(conversationKey);
  if (!allowance) {
    return undefined;
  }
  const activeLeaseMs = allowance.runId ? Math.max(leaseMs, 120000) : leaseMs;
  if (nowMs() - allowance.lastActivityAt <= activeLeaseMs) {
    return allowance;
  }
  pendingAllowancesByConversation.delete(conversationKey);
  return undefined;
}

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

function normalizeText(value: string): string {
  return value.toLowerCase();
}

function normalizeComparableText(value: string): string {
  return normalizeText(value).replace(/\s+/g, " ").trim();
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

function resolveSenderAgentId(
  channelId: string,
  senderName: string,
  infos: AgentAccountInfo[],
): string | undefined {
  const normalized = normalizeText(senderName);
  if (!normalized) {
    return undefined;
  }
  for (const info of infos) {
    if (info.channelId !== channelId) {
      continue;
    }
    if (info.displayNames.some((name) => normalizeText(name) === normalized)) {
      return info.agentId;
    }
  }
  return undefined;
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
  const normalized = normalizeText(content);
  const relevant = infos.filter((info) => info.channelId === channelId);
  const allAgents = uniqueStrings(relevant.map((info) => info.agentId));
  const matchedAgents = new Set<string>();

  for (const info of relevant) {
    if (
      info.displayNames.some((name) => {
        const lowered = normalizeText(name);
        return lowered && normalized.includes(lowered);
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
  kind: "human" | "agent";
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
    status: "open",
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
    status: "open",
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
  clearSettleTimer(epoch.conversationKey);
  epoch.claim = undefined;
  epoch.updatedAt = nowMs();
}

function touchClaim(claim: ClaimRecord): void {
  claim.lastActivityAt = nowMs();
}

function closeEpoch(epoch: EpochRecord, reason: string): void {
  clearSettleTimer(epoch.conversationKey);
  epoch.status = "closed";
  epoch.closeReason = reason;
  epoch.claim = undefined;
  epoch.closedAt = nowMs();
  epoch.updatedAt = epoch.closedAt;
}

function maybeExpireClaim(epoch: EpochRecord, leaseMs: number): boolean {
  const claim = epoch.claim;
  if (!claim) {
    return false;
  }
  const activeLeaseMs = claim.runId && !claim.sendCompletedAt ? Math.max(leaseMs, 120000) : leaseMs;
  const lastActivityAt = claim.lastActivityAt || claim.claimedAt;
  if (nowMs() - lastActivityAt <= activeLeaseMs) {
    return false;
  }
  clearClaim(epoch);
  return true;
}

function cleanupEpochs(epochIdleMs: number, leaseMs: number): void {
  const now = nowMs();
  for (const [conversationKey, guard] of botReopenGuardsByConversation) {
    if (now >= guard.blockedUntil) {
      botReopenGuardsByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, allowance] of pendingAllowancesByConversation) {
    const activeLeaseMs = allowance.runId ? Math.max(leaseMs, 120000) : leaseMs;
    if (now - allowance.lastActivityAt > activeLeaseMs) {
      pendingAllowancesByConversation.delete(conversationKey);
    }
  }
  for (const [conversationKey, epoch] of epochsByConversation) {
    maybeExpireClaim(epoch, leaseMs);
    if (epoch.status === "open" && now - epoch.updatedAt > epochIdleMs) {
      closeEpoch(epoch, "idle_timeout");
    }
    if (epoch.status === "closed") {
      const cutoff = epoch.closedAt ?? epoch.updatedAt;
      if (now - cutoff > epochIdleMs) {
        epochsByConversation.delete(conversationKey);
      }
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
  if (message.kind === "human") {
    return message.senderName || "Human";
  }
  if (message.senderAgentId) {
    return agentLabelById.get(message.senderAgentId) ?? message.senderName ?? message.senderAgentId;
  }
  return message.senderName || "Agent";
}

function buildTranscript(epoch: EpochRecord, agentLabelById: Map<string, string>): string {
  return epoch.visibleMessages
    .map((message, index) => {
      const speakerKind = message.kind === "human" ? "Human" : "Agent";
      const speaker = formatSpeakerLabel(message, agentLabelById);
      return `[${index + 1}] ${speakerKind} ${speaker} (messageId=${message.messageId})\n${message.content}`;
    })
    .join("\n\n");
}

function buildPromptContext(params: {
  epoch: EpochRecord;
  agentId: string;
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): string | null {
  const latest = getLatestVisible(params.epoch);
  if (!latest) {
    return null;
  }
  const transcript = buildTranscript(params.epoch, params.agentLabelById);
  const eligibleLabels = getEligibleAgents(params.epoch).map(
    (entry) => params.agentLabelById.get(entry) ?? entry,
  );
  const latestLabel = formatSpeakerLabel(latest, params.agentLabelById);
  const prompt = [
    "You are participating in a multi-agent baton conversation.",
    "Exactly one AI agent may speak for the current visible message.",
    "The transcript below is the authoritative raw message log for the current epoch. It is not a summary.",
    `Current visible message: ${latestLabel} (messageId=${latest.messageId}).`,
    `Eligible agents for this visible message: ${eligibleLabels.join(", ") || "none"}.`,
    "Read the full transcript before deciding whether to speak.",
    "Reply only if you can add something materially distinct, useful, or corrective after the latest visible message.",
    "Do not restate points that are already present in the transcript.",
    "If you should yield, output only NO_REPLY.",
    "",
    "Transcript for current epoch (oldest first):",
    transcript,
  ].join("\n");

  if (prompt.length > params.maxPromptChars) {
    return null;
  }
  return prompt;
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
    const leaseMs = typeof cfg.leaseMs === "number" ? Math.max(1000, cfg.leaseMs) : 15000;
    const epochIdleMs =
      typeof cfg.epochIdleMs === "number" ? Math.max(60000, cfg.epochIdleMs) : 3600000;
    const maxBotHops =
      typeof cfg.maxBotHops === "number" ? Math.max(1, Math.floor(cfg.maxBotHops)) : 16;
    const maxPromptChars =
      typeof cfg.maxPromptChars === "number" ? Math.max(4000, Math.floor(cfg.maxPromptChars)) : 60000;
    const settleMs = typeof cfg.settleMs === "number" ? Math.max(250, cfg.settleMs) : 1500;
    const botReopenCooldownMs =
      typeof cfg.botReopenCooldownMs === "number"
        ? Math.max(0, Math.floor(cfg.botReopenCooldownMs))
        : 15000;
    const failOpen = cfg.failOpen !== false;
    const debug = cfg.debug === true;

    if (!enabled) {
      return;
    }

    const agentAccounts = buildAgentAccounts(api.config);
    const agentByAccount = new Map(agentAccounts.map((info) => [info.accountId, info.agentId]));
    const agentLabelById = buildAgentLabelById(agentAccounts);

    const scheduleClaimFinalize = (
      conversationKey: string,
      expectedEpochId: string,
      expectedClaimedAt: number,
    ): void => {
      clearSettleTimer(conversationKey);
      const timer = setTimeout(() => {
        settleTimersByConversation.delete(conversationKey);

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch || epoch.status !== "open" || epoch.epochId !== expectedEpochId) {
          return;
        }

        maybeExpireClaim(epoch, leaseMs);

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
        clearClaim(epoch);

        if (observedMessageCount > 0) {
          epoch.hopCount += 1;
          if (epoch.hopCount >= maxBotHops) {
            closeEpoch(epoch, "max_bot_hops");
          } else {
            epoch.updatedAt = nowMs();
          }
          logDebug(
            api,
            debug,
            `finalized claim for ${conversationKey}; messages=${observedMessageCount} textComplete=${expectedTextNormalized === observedTextNormalized} hops=${epoch.hopCount} status=${epoch.status}`,
          );
        } else {
          if (visibleMessageCount === 0) {
            closeEpoch(epoch, "no_visible_confirmation");
          } else {
            epoch.updatedAt = nowMs();
          }
          logDebug(
            api,
            debug,
            `cleared claim for ${conversationKey} after send without visible confirmation status=${epoch.status}`,
          );
        }

        epochsByConversation.set(conversationKey, epoch);
      }, settleMs);
      settleTimersByConversation.set(conversationKey, timer);
    };

    api.on("message_received", async (event, ctx) => {
      try {
        if (!isEnabledForChannel(enabledChannels, ctx.channelId)) {
          return;
        }

        cleanupEpochs(epochIdleMs, leaseMs);

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
        const senderName = trimString(event.metadata?.["senderName"]);
        const senderAgentId = resolveSenderAgentId(ctx.channelId, senderName, agentAccounts);
        const kind: "human" | "agent" = senderAgentId ? "agent" : "human";
        const visibleMessage = makeVisibleMessage({
          messageId,
          kind,
          content: event.content,
          senderName: senderName || undefined,
          senderAgentId,
          timestamp: event.timestamp,
        });

        const existing = epochsByConversation.get(conversationKey);
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
            if (claim.sendCompletedAt) {
              scheduleClaimFinalize(conversationKey, existing.epochId, claim.claimedAt);
            }
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

        if (kind === "human") {
          const existingLatest = existing ? getLatestVisible(existing) : undefined;
          if (
            existing &&
            existing.status === "open" &&
            existingLatest?.kind === "human" &&
            trimString(existingLatest.content) === trimString(event.content)
          ) {
            existing.updatedAt = nowMs();
            epochsByConversation.set(conversationKey, existing);
            logDebug(api, debug, `reused existing human epoch ${existing.epochId} for ${conversationKey}`);
            return;
          }

          const targetAgents = inferTargetAgents(event.content, ctx.channelId, agentAccounts);
          if (targetAgents.length === 0) {
            clearSettleTimer(conversationKey);
            epochsByConversation.delete(conversationKey);
            return;
          }
          clearSettleTimer(conversationKey);
          const epoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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
        if (!epoch || epoch.status !== "open") {
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
          clearSettleTimer(conversationKey);
          const nextEpoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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

        maybeExpireClaim(epoch, leaseMs);

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
          clearSettleTimer(conversationKey);
          const nextEpoch = makeEpoch({
            conversationKey,
            channelId: ctx.channelId,
            conversationId,
            rootMessage: visibleMessage,
            targetAgents,
          });
          const pendingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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
        if (claim.sendCompletedAt) {
          scheduleClaimFinalize(conversationKey, epoch.epochId, claim.claimedAt);
        }
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

        cleanupEpochs(epochIdleMs, leaseMs);

        const channelId = trimString(ctx.channelId);
        const sessionKey = trimString(ctx.sessionKey) || undefined;
        const sessionId = trimString(ctx.sessionId) || undefined;
        const conversationKey = resolveConversationKeyFromSessionKey(sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        let epoch = epochsByConversation.get(conversationKey);
        if (!epoch) {
          const existingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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
              sessionKey,
              sessionId,
            });
            logDebug(
              api,
              debug,
              `granted pending allowance for ${conversationKey} to ${agentId}`,
            );
          } else {
            touchPendingAllowance(existingAllowance);
            pendingAllowancesByConversation.set(conversationKey, existingAllowance);
          }
          return;
        }

        maybeExpireClaim(epoch, leaseMs);

        if (epoch.status !== "open") {
          return denyContext("This baton epoch is already closed.");
        }

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
            const pendingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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
          closeEpoch(epoch, "all_agents_declined");
          epochsByConversation.set(conversationKey, epoch);
          return denyContext("All eligible agents already declined this visible message.");
        }

        if (epoch.claim && epoch.claim.agentId !== agentId) {
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

        const prompt = buildPromptContext({
          epoch,
          agentId,
          agentLabelById,
          maxPromptChars,
        });
        if (!prompt) {
          closeEpoch(epoch, "prompt_budget_exceeded");
          epochsByConversation.set(conversationKey, epoch);
          return denyContext(
            "The raw transcript for this epoch exceeds the configured prompt budget, so this baton epoch is closed.",
          );
        }

        return { prependSystemContext: prompt };
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

        cleanupEpochs(epochIdleMs, leaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch || epoch.status !== "open") {
          const allowance = getActivePendingAllowance(conversationKey, leaseMs);
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
          return;
        }

        maybeExpireClaim(epoch, leaseMs);

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

        cleanupEpochs(epochIdleMs, leaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch || epoch.status !== "open") {
          const allowance = getActivePendingAllowance(conversationKey, leaseMs);
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
          return;
        }

        maybeExpireClaim(epoch, leaseMs);

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
          return;
        }

        markDeclined(epoch, agentId, claim.sourceMessageId);
        clearClaim(epoch);
        if (haveAllEligibleAgentsDeclined(epoch, claim.sourceMessageId)) {
          closeEpoch(epoch, "all_agents_declined");
        }
        epochsByConversation.set(conversationKey, epoch);
        logDebug(
          api,
          debug,
          `agent ${agentId} declined ${claim.sourceMessageId} in epoch ${epoch.epochId}; status=${epoch.status}`,
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

        cleanupEpochs(epochIdleMs, leaseMs);

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
        const pendingAllowance = getActivePendingAllowance(conversationKey, leaseMs);
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

        if (epoch.status !== "open") {
          return { cancel: true };
        }

        maybeExpireClaim(epoch, leaseMs);

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

        cleanupEpochs(epochIdleMs, leaseMs);

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

        cleanupEpochs(epochIdleMs, leaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, ctx.channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const epoch = epochsByConversation.get(conversationKey);
        if (!epoch || epoch.status !== "open") {
          return;
        }

        const claim = epoch.claim;
        if (!claim || claim.agentId !== agentId) {
          return;
        }

        clearClaim(epoch);
        epochsByConversation.set(conversationKey, epoch);
        logDebug(api, debug, `cleared failed claim for ${agentId} in epoch ${epoch.epochId}`);
      } catch (err) {
        api.logger.warn?.(
          `multi-agent-turn-arbiter: agent_end hook failed (${String(err)})`,
        );
      }
    });
  },
};
