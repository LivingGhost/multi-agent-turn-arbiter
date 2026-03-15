import { createHash } from "node:crypto";
import type { OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

type ArbiterConfig = {
  enabled?: boolean;
  enabledChannels?: string[];
  activeRunLeaseMs?: number;
  postSendGraceMs?: number;
  stateIdleMs?: number;
  maxBacklogTurns?: number;
  maxPromptChars?: number;
  failOpen?: boolean;
  debug?: boolean;
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
  senderKey?: string;
  senderName?: string;
  senderAgentId?: string;
  timestamp: number;
};

type ConversationTurn = {
  ownerAgentId: string;
  sourceKind: "bootstrap" | "visible";
  sourceMessageId: string;
  sourceMessage?: VisibleMessage;
  phase: "planned" | "running";
  attemptedAgentIds: string[];
  createdAt: number;
  updatedAt: number;
  sessionKey?: string;
  sessionId?: string;
  runId?: string;
  hasVisibleOutput: boolean;
  chunkWindowOpenedAt?: number;
  abortRequestedAt?: number;
  launchId: string;
  enqueuedAt?: number;
};

type PendingTurn = {
  sourceMessageId: string;
  sourceMessage?: VisibleMessage;
  attemptedAgentIds: string[];
  updatedAt: number;
};

type ConversationState = {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  latestVisible?: VisibleMessage;
  history: VisibleMessage[];
  current?: ConversationTurn;
  pending?: PendingTurn;
  lastStarterAgentId?: string;
  nextLaunchSeq: number;
  updatedAt: number;
};

const conversationsByKey = new Map<string, ConversationState>();
const managedAgentBySenderKey = new Map<string, string>();

function nowMs(): number {
  return Date.now();
}

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function makeHash(input: string): string {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
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

function resolveRunId(metadata?: Record<string, unknown>): string | undefined {
  return getMetadataString(metadata, ["runId", "run_id"]);
}

function resolveMessageId(event: {
  timestamp?: number;
  from: string;
  content: string;
  metadata?: Record<string, unknown>;
}): string {
  const direct = getMetadataString(event.metadata, [
    "messageId",
    "message_id",
    "id",
    "eventId",
    "event_id",
  ]);
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
  const threadId = getMetadataString(metadata, ["threadId", "thread_id"]);
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
  const threadId = getMetadataString(metadata, ["threadId", "thread_id"]);
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

function buildBootstrapSourceMessageId(conversationKey: string): string {
  return `bootstrap:${conversationKey}`;
}

function makeConversationSenderKey(channelId: string, senderKey: string): string {
  return `${channelId}::${senderKey}`;
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
  const directAgentId = getMetadataString(metadata, ["agentId", "agent_id"]);
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
  const matched = new Set<string>();

  for (const info of relevant) {
    if (
      info.displayNames.some((name) => {
        const candidate = trimString(name);
        return candidate && normalized.includes(candidate);
      })
    ) {
      matched.add(info.agentId);
    }
  }

  return matched.size > 0 ? [...matched] : allAgents;
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

function trimVisibleHistoryToTailTurns(
  messages: VisibleMessage[],
  maxBacklogTurns: number,
): VisibleMessage[] {
  if (maxBacklogTurns === -1 || messages.length === 0) {
    return messages;
  }

  let turnsSeen = 0;
  let currentTurnKey: string | undefined;
  let startIndex = messages.length;

  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const turnKey = getTurnSpeakerKey(messages[index]);
    if (turnKey !== currentTurnKey) {
      turnsSeen += 1;
      currentTurnKey = turnKey;
      if (turnsSeen > maxBacklogTurns) {
        break;
      }
    }
    startIndex = index;
  }

  return messages.slice(startIndex);
}

function formatSpeakerLabel(
  message: VisibleMessage,
  agentLabelById: Map<string, string>,
): string {
  if (message.kind === "agent") {
    const agentId = trimString(message.senderAgentId);
    const label = agentId ? agentLabelById.get(agentId) ?? agentId : message.senderName ?? "agent";
    return `Agent ${label}`;
  }
  const label = trimString(message.senderName) || trimString(message.senderKey) || "external";
  return `External ${label}`;
}

function formatTranscriptEntry(
  message: VisibleMessage,
  agentLabelById: Map<string, string>,
): string {
  const label = formatSpeakerLabel(message, agentLabelById);
  return `${label} (messageId=${message.messageId}):\n${message.content}`;
}

function buildTranscriptContext(
  messages: VisibleMessage[],
  maxPromptChars: number,
  agentLabelById: Map<string, string>,
): string {
  if (messages.length === 0) {
    return "";
  }
  const entries = messages.map((message) => formatTranscriptEntry(message, agentLabelById));
  if (maxPromptChars === -1) {
    return entries.join("\n\n");
  }

  const selected: string[] = [];
  let used = 0;
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index];
    const nextUsed = used + entry.length + (selected.length > 0 ? 2 : 0);
    if (selected.length > 0 && nextUsed > maxPromptChars) {
      break;
    }
    if (selected.length === 0 && entry.length > maxPromptChars) {
      selected.unshift(entry.slice(-maxPromptChars));
      break;
    }
    selected.unshift(entry);
    used = nextUsed;
  }
  return selected.join("\n\n");
}

function buildPromptContext(params: {
  state: ConversationState;
  agentLabelById: Map<string, string>;
  maxPromptChars: number;
}): { prependSystemContext: string; prependContext?: string } {
  const effectiveSource = params.state.latestVisible;
  const promptIntro = [
    "You are the single current owner for this multi-agent conversation turn.",
    "Reply to the latest visible message and the retained visible backlog only.",
    "If no reply is needed, output only NO_REPLY.",
    effectiveSource
      ? `Current visible source: messageId=${effectiveSource.messageId}.`
      : "No fresh visible source is currently cached.",
  ].join(" ");

  const transcript = buildTranscriptContext(
    params.state.history,
    params.maxPromptChars,
    params.agentLabelById,
  );

  return {
    prependSystemContext: promptIntro,
    ...(transcript ? { prependContext: `Visible backlog:\n\n${transcript}` } : {}),
  };
}

function buildInitialPromptContext(): { prependSystemContext: string } {
  return {
    prependSystemContext:
      "You currently own the initial conversation start for this shared multi-agent conversation. No fresh visible backlog is cached yet. If no reply is needed, output only NO_REPLY.",
  };
}

function denyContext(reason: string): { prependSystemContext: string } {
  return {
    prependSystemContext: `${reason} Output only NO_REPLY.`,
  };
}

function rememberManagedSender(
  channelId: string,
  senderKey: string | undefined,
  agentId: string | undefined,
): void {
  const normalizedSenderKey = trimString(senderKey);
  const normalizedAgentId = trimString(agentId);
  if (!channelId || !normalizedSenderKey || !normalizedAgentId) {
    return;
  }
  managedAgentBySenderKey.set(
    makeConversationSenderKey(channelId, normalizedSenderKey),
    normalizedAgentId,
  );
}

function resolveInboundManagedAgentId(params: {
  channelId: string;
  metadata?: Record<string, unknown>;
  senderKey?: string;
  agentByAccount: Map<string, string>;
}): string | undefined {
  const directAgentId = getMetadataString(params.metadata, [
    "agentId",
    "agent_id",
    "senderAgentId",
    "sender_agent_id",
  ]);
  if (directAgentId) {
    rememberManagedSender(params.channelId, params.senderKey, directAgentId);
    return directAgentId;
  }

  const managedAccountId = getMetadataString(params.metadata, [
    "senderManagedAccountId",
    "sender_managed_account_id",
    "managedAccountId",
    "managed_account_id",
  ]);
  if (managedAccountId) {
    const resolved = params.agentByAccount.get(managedAccountId);
    if (resolved) {
      rememberManagedSender(params.channelId, params.senderKey, resolved);
      return resolved;
    }
  }

  const senderKey = trimString(params.senderKey);
  if (!senderKey) {
    return undefined;
  }
  return managedAgentBySenderKey.get(makeConversationSenderKey(params.channelId, senderKey));
}

function getOrCreateConversationState(params: {
  conversationKey: string;
  channelId: string;
  conversationId: string;
}): ConversationState {
  const existing = conversationsByKey.get(params.conversationKey);
  if (existing) {
    existing.channelId = params.channelId || existing.channelId;
    existing.conversationId = params.conversationId || existing.conversationId;
    existing.updatedAt = nowMs();
    return existing;
  }
  const created: ConversationState = {
    conversationKey: params.conversationKey,
    channelId: params.channelId,
    conversationId: params.conversationId,
    history: [],
    nextLaunchSeq: 1,
    updatedAt: nowMs(),
  };
  conversationsByKey.set(params.conversationKey, created);
  return created;
}

function findMessageInState(state: ConversationState, messageId: string): VisibleMessage | undefined {
  if (state.latestVisible?.messageId === messageId) {
    return state.latestVisible;
  }
  return state.history.find((message) => message.messageId === messageId);
}

function appendVisibleMessageToState(
  state: ConversationState,
  message: VisibleMessage,
  maxBacklogTurns: number,
): void {
  const existingIndex = state.history.findIndex((entry) => entry.messageId === message.messageId);
  if (existingIndex >= 0) {
    state.history[existingIndex] = message;
  } else {
    state.history.push(message);
  }
  state.history = trimVisibleHistoryToTailTurns(state.history, maxBacklogTurns);
  state.latestVisible = message;
  state.updatedAt = nowMs();
}

function chooseRoundRobinAgent(eligible: string[], lastStarterAgentId?: string): string {
  if (eligible.length === 0) {
    throw new Error("No eligible agents to choose from.");
  }
  const lastIndex = lastStarterAgentId ? eligible.indexOf(lastStarterAgentId) : -1;
  if (lastIndex === -1) {
    return eligible[0];
  }
  return eligible[(lastIndex + 1) % eligible.length];
}

function planTurnFromVisibleSource(params: {
  state: ConversationState;
  sourceMessage: VisibleMessage;
  attemptedAgentIds: string[];
  agentAccounts: AgentAccountInfo[];
  preferredAgentId?: string;
}): ConversationTurn | undefined {
  const eligible = getEligibleAgentsForVisibleMessage(
    params.sourceMessage,
    params.state.channelId,
    params.agentAccounts,
  ).filter((agentId) => !params.attemptedAgentIds.includes(agentId));

  if (eligible.length === 0) {
    params.state.current = undefined;
    params.state.pending = undefined;
    params.state.updatedAt = nowMs();
    return undefined;
  }

  const ownerAgentId =
    params.preferredAgentId && eligible.includes(params.preferredAgentId)
      ? params.preferredAgentId
      : chooseRoundRobinAgent(eligible, params.state.lastStarterAgentId);
  params.state.lastStarterAgentId = ownerAgentId;

  const current: ConversationTurn = {
    ownerAgentId,
    sourceKind: "visible",
    sourceMessageId: params.sourceMessage.messageId,
    sourceMessage: params.sourceMessage,
    phase: "planned",
    attemptedAgentIds: uniqueStrings([...params.attemptedAgentIds, ownerAgentId]),
    createdAt: nowMs(),
    updatedAt: nowMs(),
    hasVisibleOutput: false,
    launchId: `${params.state.conversationKey}:${params.sourceMessage.messageId}:${params.state.nextLaunchSeq}:${ownerAgentId}`,
  };
  params.state.current = current;
  params.state.nextLaunchSeq += 1;
  params.state.updatedAt = nowMs();
  return current;
}

function planBootstrapTurn(params: {
  state: ConversationState;
  agentAccounts: AgentAccountInfo[];
}): ConversationTurn | undefined {
  const eligible = getAllChannelAgents(params.state.channelId, params.agentAccounts);
  if (eligible.length === 0) {
    return undefined;
  }
  const ownerAgentId = chooseRoundRobinAgent(eligible, params.state.lastStarterAgentId);
  params.state.lastStarterAgentId = ownerAgentId;
  const current: ConversationTurn = {
    ownerAgentId,
    sourceKind: "bootstrap",
    sourceMessageId: buildBootstrapSourceMessageId(params.state.conversationKey),
    phase: "planned",
    attemptedAgentIds: [ownerAgentId],
    createdAt: nowMs(),
    updatedAt: nowMs(),
    hasVisibleOutput: false,
    launchId: `${params.state.conversationKey}:bootstrap:${params.state.nextLaunchSeq}:${ownerAgentId}`,
  };
  params.state.current = current;
  params.state.nextLaunchSeq += 1;
  params.state.updatedAt = nowMs();
  return current;
}

function getEffectiveSourceMessageId(state: ConversationState, turn: ConversationTurn): string {
  if (turn.sourceKind === "bootstrap" && state.latestVisible) {
    return state.latestVisible.messageId;
  }
  return turn.sourceMessageId;
}

function logDebug(api: OpenClawPluginApi, debug: boolean, message: string): void {
  if (!debug) {
    return;
  }
  api.logger.debug?.(`multi-agent-turn-arbiter: ${message}`);
}

async function requestAbortForCurrent(
  api: OpenClawPluginApi,
  state: ConversationState,
  debug: boolean,
): Promise<void> {
  const current = state.current;
  if (!current?.runId || current.abortRequestedAt) {
    return;
  }
  current.abortRequestedAt = nowMs();
  try {
    await api.runtime.subagent.abort({
      runId: current.runId,
      ...(current.sessionKey ? { sessionKey: current.sessionKey } : {}),
    });
    current.updatedAt = nowMs();
    logDebug(
      api,
      debug,
      `requested abort for ${current.ownerAgentId} run=${current.runId} source=${current.sourceMessageId}`,
    );
  } catch (err) {
    api.logger.warn?.(`multi-agent-turn-arbiter: failed to abort stale run (${String(err)})`);
  }
}

async function enqueueCurrentTurn(
  api: OpenClawPluginApi,
  state: ConversationState,
  debug: boolean,
): Promise<void> {
  const current = state.current;
  if (!current || current.phase !== "planned" || current.enqueuedAt) {
    return;
  }

  current.sessionKey = buildDerivedSessionKey(current.ownerAgentId, state.conversationKey);
  try {
    const result = await api.runtime.subagent.enqueue({
      sessionKey: current.sessionKey,
      message: "Continue the shared multi-agent conversation and respond if appropriate.",
      deliver: true,
      idempotencyKey: `multi-agent-turn-arbiter:${current.launchId}`,
    });
    current.runId = result.runId;
    current.enqueuedAt = nowMs();
    current.updatedAt = nowMs();
    state.updatedAt = nowMs();
    logDebug(
      api,
      debug,
      `enqueued ${current.ownerAgentId} for source=${current.sourceMessageId} in ${state.conversationKey}`,
    );
  } catch (err) {
    api.logger.warn?.(`multi-agent-turn-arbiter: failed to enqueue follow-up (${String(err)})`);
    if (current.sourceKind === "visible" && current.sourceMessage) {
      state.pending = {
        sourceMessageId: current.sourceMessageId,
        sourceMessage: current.sourceMessage,
        attemptedAgentIds: current.attemptedAgentIds,
        updatedAt: nowMs(),
      };
    }
    state.current = undefined;
    state.updatedAt = nowMs();
  }
}

async function launchPendingTurnIfNeeded(
  api: OpenClawPluginApi,
  state: ConversationState,
  agentAccounts: AgentAccountInfo[],
  debug: boolean,
): Promise<void> {
  if (state.current || !state.pending) {
    return;
  }
  const sourceMessage =
    state.pending.sourceMessage ??
    findMessageInState(state, state.pending.sourceMessageId) ??
    state.latestVisible;
  if (!sourceMessage) {
    state.pending = undefined;
    state.updatedAt = nowMs();
    return;
  }

  const attemptedAgentIds = state.pending.attemptedAgentIds;
  state.pending = undefined;
  const planned = planTurnFromVisibleSource({
    state,
    sourceMessage,
    attemptedAgentIds,
    agentAccounts,
  });
  if (!planned) {
    return;
  }
  await enqueueCurrentTurn(api, state, debug);
}

async function finishCurrentTurn(
  api: OpenClawPluginApi,
  state: ConversationState,
  agentAccounts: AgentAccountInfo[],
  debug: boolean,
): Promise<void> {
  const finished = state.current;
  if (!finished) {
    return;
  }
  state.current = undefined;
  state.updatedAt = nowMs();

  if (!finished.hasVisibleOutput && finished.sourceKind === "visible" && finished.sourceMessage) {
    if (!state.pending || state.pending.sourceMessageId === finished.sourceMessageId) {
      state.pending = {
        sourceMessageId: finished.sourceMessageId,
        sourceMessage: finished.sourceMessage,
        attemptedAgentIds: finished.attemptedAgentIds,
        updatedAt: nowMs(),
      };
    }
  } else if (
    finished.hasVisibleOutput &&
    !state.pending &&
    state.latestVisible &&
    state.latestVisible.kind === "agent" &&
    state.latestVisible.senderAgentId === finished.ownerAgentId &&
    state.latestVisible.messageId !== finished.sourceMessageId
  ) {
    state.pending = {
      sourceMessageId: state.latestVisible.messageId,
      sourceMessage: state.latestVisible,
      attemptedAgentIds: [],
      updatedAt: nowMs(),
    };
  }

  await launchPendingTurnIfNeeded(api, state, agentAccounts, debug);
}

function cleanupConversations(
  stateIdleMs: number,
  activeRunLeaseMs: number,
): void {
  const now = nowMs();
  for (const [conversationKey, state] of conversationsByKey) {
    if (now - state.updatedAt > stateIdleMs) {
      conversationsByKey.delete(conversationKey);
      continue;
    }

    const current = state.current;
    if (!current || now - current.updatedAt <= activeRunLeaseMs) {
      continue;
    }

    state.current = undefined;
    if (
      current.sourceKind === "visible" &&
      current.sourceMessage &&
      state.latestVisible?.messageId === current.sourceMessageId
    ) {
      state.pending = {
        sourceMessageId: current.sourceMessageId,
        sourceMessage: current.sourceMessage,
        attemptedAgentIds: current.attemptedAgentIds,
        updatedAt: now,
      };
    } else if (state.latestVisible) {
      if (!state.pending || state.pending.sourceMessageId !== state.latestVisible.messageId) {
        state.pending = {
          sourceMessageId: state.latestVisible.messageId,
          sourceMessage: state.latestVisible,
          attemptedAgentIds: [],
          updatedAt: now,
        };
      }
    } else if (current.sourceKind === "visible" && current.sourceMessage) {
      state.pending = {
        sourceMessageId: current.sourceMessageId,
        sourceMessage: current.sourceMessage,
        attemptedAgentIds: current.attemptedAgentIds,
        updatedAt: now,
      };
    }
    state.updatedAt = now;
  }
}

export default {
  id: "multi-agent-turn-arbiter",
  name: "Multi-Agent Turn Arbiter",
  description:
    "Serializes visible shared-conversation replies across managed agents using enqueue/abort primitives.",
  register(api: OpenClawPluginApi) {
    const cfg = (api.pluginConfig as ArbiterConfig | undefined) ?? {};
    if (cfg.enabled === false) {
      return;
    }

    const enabledChannels = new Set(
      Array.isArray(cfg.enabledChannels)
        ? cfg.enabledChannels.map((value) => trimString(value)).filter(Boolean)
        : [],
    );
    const activeRunLeaseMs = Math.max(1000, Math.floor(cfg.activeRunLeaseMs ?? 180000));
    const postSendGraceMs = Math.max(1000, Math.floor(cfg.postSendGraceMs ?? 15000));
    const stateIdleMs = Math.max(1000, Math.floor(cfg.stateIdleMs ?? 300000));
    const maxBacklogTurns =
      cfg.maxBacklogTurns === -1
        ? -1
        : Math.max(1, Math.floor(cfg.maxBacklogTurns ?? 6));
    const maxPromptChars =
      cfg.maxPromptChars === -1
        ? -1
        : Math.max(4000, Math.floor(cfg.maxPromptChars ?? 30000));
    const failOpen = typeof cfg.failOpen === "boolean" ? cfg.failOpen : true;
    const debug = cfg.debug === true;

    const agentAccounts = buildAgentAccounts(api.config as OpenClawConfig);
    const agentByAccount = new Map(agentAccounts.map((entry) => [entry.accountId, entry.agentId]));
    const agentLabelById = buildAgentLabelById(agentAccounts);

    api.on("message_received", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveInboundConversationKey(
          channelId,
          ctx.conversationId,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }

        const conversationId =
          trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length);
        const state = getOrCreateConversationState({
          conversationKey,
          channelId,
          conversationId,
        });

        const senderKey = resolveInboundSenderKey(event.metadata);
        const senderName = getMetadataString(event.metadata, ["senderName", "authorName"]);
        const senderAgentId = resolveInboundManagedAgentId({
          channelId,
          metadata: event.metadata,
          senderKey,
          agentByAccount,
        });

        const visibleMessage = makeVisibleMessage({
          messageId: resolveMessageId(event),
          kind: senderAgentId ? "agent" : "external",
          content: event.content,
          senderKey,
          senderName,
          senderAgentId,
          timestamp: event.timestamp,
        });

        appendVisibleMessageToState(state, visibleMessage, maxBacklogTurns);

        const current = state.current;
        if (!current) {
          state.pending = undefined;
          return;
        }

        if (visibleMessage.kind === "agent" && visibleMessage.senderAgentId === current.ownerAgentId) {
          current.hasVisibleOutput = true;
          current.chunkWindowOpenedAt ??= nowMs();
          current.updatedAt = nowMs();
          state.pending = {
            sourceMessageId: visibleMessage.messageId,
            sourceMessage: visibleMessage,
            attemptedAgentIds: [],
            updatedAt: nowMs(),
          };
          state.updatedAt = nowMs();
          return;
        }

        state.pending = {
          sourceMessageId: visibleMessage.messageId,
          sourceMessage: visibleMessage,
          attemptedAgentIds: [],
          updatedAt: nowMs(),
        };
        state.updatedAt = nowMs();
        await requestAbortForCurrent(api, state, debug);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: message_received hook failed (${String(err)})`);
      }
    });

    api.on("before_prompt_build", async (_event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const agentId = trimString(ctx.agentId);
        if (!agentId) {
          return;
        }

        const conversationKey =
          resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId) ||
          resolveOutboundConversationKey(
            channelId,
            ctx.conversationId,
            trimString(ctx.conversationId),
            undefined,
          );
        if (!conversationKey) {
          return;
        }

        const conversationId =
          trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length);
        const state = getOrCreateConversationState({
          conversationKey,
          channelId,
          conversationId,
        });

        if (
          state.current?.sourceKind === "bootstrap" &&
          state.current.phase === "planned" &&
          state.latestVisible
        ) {
          planTurnFromVisibleSource({
            state,
            sourceMessage: state.latestVisible,
            attemptedAgentIds: [],
            agentAccounts,
            preferredAgentId: state.current.ownerAgentId,
          });
        }

        if (!state.current) {
          if (state.latestVisible) {
            planTurnFromVisibleSource({
              state,
              sourceMessage: state.latestVisible,
              attemptedAgentIds: [],
              agentAccounts,
            });
          } else {
            planBootstrapTurn({ state, agentAccounts });
          }
        }

        if (!state.current) {
          return denyContext("No managed agent is eligible for this conversation turn.");
        }

        if (state.current.ownerAgentId !== agentId) {
          return denyContext("Another managed agent currently owns this visible message.");
        }

        if (state.current.phase === "running" && state.current.runId) {
          return denyContext(
            "This managed agent already has an in-flight turn for this conversation.",
          );
        }

        if (!state.latestVisible) {
          return buildInitialPromptContext();
        }

        return buildPromptContext({
          state,
          agentLabelById,
          maxPromptChars,
        });
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: before_prompt_build hook failed (${String(err)})`);
      }
    });

    api.on("llm_input", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const state = conversationsByKey.get(conversationKey);
        if (!state?.current || state.current.ownerAgentId !== agentId) {
          return;
        }

        state.current.phase = "running";
        state.current.sessionKey = trimString(ctx.sessionKey) || state.current.sessionKey;
        state.current.sessionId = trimString(ctx.sessionId) || state.current.sessionId;
        state.current.runId = event.runId;
        state.current.updatedAt = nowMs();
        state.updatedAt = nowMs();
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: llm_input hook failed (${String(err)})`);
      }
    });

    api.on("llm_output", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const state = conversationsByKey.get(conversationKey);
        if (!state?.current || state.current.ownerAgentId !== agentId) {
          return;
        }
        if (state.current.runId && state.current.runId !== event.runId) {
          return;
        }

        state.current.updatedAt = nowMs();
        state.updatedAt = nowMs();
        if (!isOnlyNoReplyOutput(event.assistantTexts)) {
          return;
        }

        await finishCurrentTurn(api, state, agentAccounts, debug);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: llm_output hook failed (${String(err)})`);
      }
    });

    api.on("message_sending", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveOutboundConversationKey(
          channelId,
          ctx.conversationId,
          event.to,
          event.metadata,
        );
        if (!conversationKey) {
          return;
        }

        const state = conversationsByKey.get(conversationKey);
        const accountId = trimString(ctx.accountId);
        const agentId = resolveOutboundAgentId(accountId || undefined, event.metadata, agentByAccount);
        if (!agentId || !state?.current || state.current.ownerAgentId !== agentId) {
          return { cancel: true };
        }

        const runId = resolveRunId(event.metadata);
        if (state.current.runId && runId && state.current.runId !== runId) {
          return { cancel: true };
        }

        if (trimString(event.content) === "NO_REPLY") {
          await finishCurrentTurn(api, state, agentAccounts, debug);
          return { cancel: true };
        }

        const effectiveSourceMessageId = getEffectiveSourceMessageId(state, state.current);
        const latestVisible = state.latestVisible;
        const chunkWindowIsOpen =
          !!state.current.chunkWindowOpenedAt &&
          nowMs() - state.current.chunkWindowOpenedAt <= postSendGraceMs;
        const sameAgentFollowUpChunkAllowed =
          !!latestVisible &&
          latestVisible.kind === "agent" &&
          latestVisible.senderAgentId === agentId &&
          latestVisible.messageId !== effectiveSourceMessageId &&
          state.current.hasVisibleOutput &&
          chunkWindowIsOpen;

        const latestVisibleSupersedesCurrent =
          !!latestVisible &&
          latestVisible.messageId !== effectiveSourceMessageId &&
          !sameAgentFollowUpChunkAllowed;
        if (latestVisibleSupersedesCurrent) {
          state.pending = {
            sourceMessageId: latestVisible.messageId,
            sourceMessage: latestVisible,
            attemptedAgentIds: [],
            updatedAt: nowMs(),
          };
          await finishCurrentTurn(api, state, agentAccounts, debug);
          logDebug(
            api,
            debug,
            `cancelled stale send from ${agentId}; source=${effectiveSourceMessageId} latest=${latestVisible.messageId}`,
          );
          return { cancel: true };
        }

        state.current.updatedAt = nowMs();
        state.updatedAt = nowMs();
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: message_sending hook failed (${String(err)})`);
        if (!failOpen) {
          return { cancel: true };
        }
      }
    });

    api.on("message_sent", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveOutboundConversationKey(
          channelId,
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

        const state = conversationsByKey.get(conversationKey);
        if (!state?.current || state.current.ownerAgentId !== agentId) {
          return;
        }

        state.current.updatedAt = nowMs();
        state.updatedAt = nowMs();
        if (!event.success) {
          await finishCurrentTurn(api, state, agentAccounts, debug);
        }
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: message_sent hook failed (${String(err)})`);
      }
    });

    api.on("agent_end", async (_event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabledForChannel(enabledChannels, channelId)) {
          return;
        }

        cleanupConversations(stateIdleMs, activeRunLeaseMs);

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        const agentId = trimString(ctx.agentId);
        if (!conversationKey || !agentId) {
          return;
        }

        const state = conversationsByKey.get(conversationKey);
        if (!state?.current || state.current.ownerAgentId !== agentId) {
          return;
        }

        await finishCurrentTurn(api, state, agentAccounts, debug);
      } catch (err) {
        api.logger.warn?.(`multi-agent-turn-arbiter: agent_end hook failed (${String(err)})`);
      }
    });
  },
};
