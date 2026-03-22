import { createHash } from "node:crypto";
import type { OpenClawConfig, OpenClawPluginApi } from "openclaw/plugin-sdk";

// --- Configuration ---

type ArbiterConfig = {
  enabled?: boolean;
  enabledChannels?: string[];
  maxBufferMessages?: number;
  stateIdleMs?: number;
  maxPromptChars?: number;
  failOpen?: boolean;
  debug?: boolean;
};

// --- Types ---

type AgentAccountInfo = {
  channelId: string;
  accountId: string;
  agentId: string;
  displayNames: string[];
};

type BufferedMessage = {
  messageId: string;
  kind: "external" | "agent";
  content: string;
  senderAgentId?: string;
  senderName?: string;
  senderKey?: string;
  timestamp: number;
};

type AgentRunState = {
  runId?: string;
  thinking: boolean;
  cancelled?: boolean;
};

type ConversationState = {
  conversationKey: string;
  channelId: string;
  conversationId: string;
  buffer: BufferedMessage[];
  agentRuns: Map<string, AgentRunState>;
  pendingResponse: boolean;
  updatedAt: number;
};

// --- Utilities ---

function trimString(value: unknown): string {
  return typeof value === "string" ? value.trim() : "";
}

function uniqueStrings(values: string[]): string[] {
  return [...new Set(values.filter(Boolean))];
}

function makeHash(input: string): string {
  return createHash("sha1").update(input).digest("hex").slice(0, 12);
}

function generateId(): string {
  return `arbiter-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function getMetadataString(
  metadata: Record<string, unknown> | undefined,
  keys: string[],
): string | undefined {
  for (const key of keys) {
    const value = trimString(metadata?.[key]);
    if (value) return value;
  }
  return undefined;
}

function resolveInboundSenderKey(metadata?: Record<string, unknown>): string | undefined {
  return getMetadataString(metadata, [
    "senderId", "sender_id", "authorId", "author_id",
    "userId", "user_id", "fromId", "from_id",
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
    "messageId", "message_id", "id", "eventId", "event_id",
  ]);
  if (direct) return direct;
  return `derived-${makeHash(`${event.timestamp ?? 0}:${event.from}:${event.content}`)}`;
}

function resolveInboundConversationKey(
  channelId: string,
  conversationId: string | undefined,
  metadata?: Record<string, unknown>,
): string | null {
  const threadId = getMetadataString(metadata, ["threadId", "thread_id"]);
  const base = threadId || trimString(conversationId);
  if (!channelId || !base) return null;
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
  if (!channelId || !base) return null;
  return `${channelId}:${base}`;
}

function resolveConversationKeyFromSessionKey(
  sessionKey: string | undefined,
  channelId: string | undefined,
): string | null {
  const normalizedSessionKey = trimString(sessionKey);
  const normalizedChannelId = trimString(channelId);
  if (!normalizedSessionKey || !normalizedChannelId) return null;
  const stripped = normalizedSessionKey.replace(/^agent:[^:]+:/, "");
  if (!stripped.startsWith(`${normalizedChannelId}:`)) return null;
  return stripped;
}

function makeConversationSenderKey(channelId: string, senderKey: string): string {
  return `${channelId}::${senderKey}`;
}

// --- Agent account resolution ---

const managedAgentBySenderKey = new Map<string, string>();

function buildAgentAccounts(config: OpenClawConfig): AgentAccountInfo[] {
  const bindings = Array.isArray(config.bindings) ? config.bindings : [];
  const agents =
    config.agents?.list && Array.isArray(config.agents.list)
      ? config.agents.list.filter((entry) => entry && typeof entry === "object")
      : [];

  const agentNameById = new Map<string, string>();
  for (const entry of agents) {
    const id = trimString(entry.id);
    if (!id) continue;
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
    if (!channelId || !accountId || !agentId) continue;
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

function rememberManagedSender(
  channelId: string,
  senderKey: string | undefined,
  agentId: string | undefined,
): void {
  const normalizedSenderKey = trimString(senderKey);
  const normalizedAgentId = trimString(agentId);
  if (!channelId || !normalizedSenderKey || !normalizedAgentId) return;
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
    "agentId", "agent_id", "senderAgentId", "sender_agent_id",
  ]);
  if (directAgentId) {
    rememberManagedSender(params.channelId, params.senderKey, directAgentId);
    return directAgentId;
  }

  const managedAccountId = getMetadataString(params.metadata, [
    "senderManagedAccountId", "sender_managed_account_id",
    "managedAccountId", "managed_account_id",
  ]);
  if (managedAccountId) {
    const resolved = params.agentByAccount.get(managedAccountId);
    if (resolved) {
      rememberManagedSender(params.channelId, params.senderKey, resolved);
      return resolved;
    }
  }

  const senderKey = trimString(params.senderKey);
  if (!senderKey) return undefined;
  return managedAgentBySenderKey.get(makeConversationSenderKey(params.channelId, senderKey));
}

function resolveOutboundAgentId(
  accountId: string | undefined,
  metadata: Record<string, unknown> | undefined,
  agentByAccount: Map<string, string>,
): string | undefined {
  const directAgentId = getMetadataString(metadata, ["agentId", "agent_id"]);
  if (directAgentId) return directAgentId;
  const normalizedAccountId = trimString(accountId);
  if (!normalizedAccountId) return undefined;
  return agentByAccount.get(normalizedAccountId);
}

// --- Transcript formatting ---

function formatBufferTranscript(
  buffer: BufferedMessage[],
  agentLabelById: Map<string, string>,
  maxChars: number,
): string {
  const entries = buffer.map((m) => {
    const sender = m.senderAgentId
      ? agentLabelById.get(m.senderAgentId) ?? m.senderAgentId
      : m.senderName ?? m.senderKey ?? "user";
    return `${sender} (messageId=${m.messageId}):\n${m.content}`;
  });

  if (maxChars === -1) return entries.join("\n\n");

  const selected: string[] = [];
  let used = 0;
  for (let i = entries.length - 1; i >= 0; i--) {
    const entry = entries[i];
    const next = used + entry.length + (selected.length > 0 ? 2 : 0);
    if (selected.length > 0 && next > maxChars) break;
    selected.unshift(entry);
    used = next;
  }
  return selected.join("\n\n");
}

// --- Core state ---

const conversations = new Map<string, ConversationState>();
const seenMessageIds = new Set<string>();
const quietAgents = new Set<string>();

// Agent resolution state — module-level so it survives re-registration.
// Only updated when register receives a config with valid bindings.
let currentAgentByAccount = new Map<string, string>();
let currentAgentLabelById = new Map<string, string>();
let currentManagedAgentIds = new Set<string>();
let agentStateInitialized = false;

function getConversation(
  conversationKey: string,
  channelId: string,
  conversationId: string,
): ConversationState {
  let state = conversations.get(conversationKey);
  if (!state) {
    state = {
      conversationKey,
      channelId,
      conversationId,
      buffer: [],
      agentRuns: new Map(),
      pendingResponse: false,
      updatedAt: Date.now(),
    };
    conversations.set(conversationKey, state);
  }
  state.updatedAt = Date.now();
  return state;
}

function addToBuffer(conv: ConversationState, msg: BufferedMessage, max: number): void {
  conv.buffer.push(msg);
  if (conv.buffer.length > max) {
    conv.buffer = conv.buffer.slice(-max);
  }
}

function pruneSeenIds(): void {
  if (seenMessageIds.size > 10_000) {
    const arr = [...seenMessageIds];
    seenMessageIds.clear();
    for (const id of arr.slice(-5_000)) seenMessageIds.add(id);
  }
}

function cleanupStaleConversations(stateIdleMs: number): void {
  const now = Date.now();
  for (const [key, state] of conversations) {
    if (now - state.updatedAt > stateIdleMs) {
      conversations.delete(key);
    }
  }
}

// --- Plugin ---

export default {
  id: "multi-agent-turn-arbiter",
  name: "Multi-Agent Conversation Arbiter",
  description:
    "Enables natural multi-agent conversations where each agent autonomously decides whether to participate.",
  register(api: OpenClawPluginApi) {
    const cfg = (api.pluginConfig as ArbiterConfig | undefined) ?? {};
    if (cfg.enabled === false) return;

    const enabledChannels = new Set(
      Array.isArray(cfg.enabledChannels)
        ? cfg.enabledChannels.map((v) => trimString(v)).filter(Boolean)
        : [],
    );
    const maxBuffer = Math.max(10, Math.floor(cfg.maxBufferMessages ?? 50));
    const stateIdleMs = Math.max(60_000, Math.floor(cfg.stateIdleMs ?? 300_000));
    const maxPromptChars = cfg.maxPromptChars === -1 ? -1 : Math.max(4_000, Math.floor(cfg.maxPromptChars ?? 30_000));
    const failOpen = typeof cfg.failOpen === "boolean" ? cfg.failOpen : true;
    const debug = cfg.debug === true;

    const agentAccounts = buildAgentAccounts(api.config as OpenClawConfig);
    // Update agent state only when config has valid bindings.
    // Partial configs (e.g. from buildMediaUnderstandingRegistry) lack bindings —
    // in that case, keep the previously initialized state.
    if (agentAccounts.length > 0) {
      currentAgentByAccount = new Map(agentAccounts.map((e) => [e.accountId, e.agentId]));
      currentAgentLabelById = buildAgentLabelById(agentAccounts);
      currentManagedAgentIds = new Set(agentAccounts.map((e) => e.agentId));
      if (!agentStateInitialized) {
        api.logger.info?.(`arbiter: initialized with ${agentAccounts.length} agent(s): [${[...currentManagedAgentIds].join(",")}]`);
      }
      agentStateInitialized = true;
    } else if (!agentStateInitialized) {
      return;
    }
    // Use module-level state for all closures below.
    const agentByAccount = currentAgentByAccount;
    const agentLabelById = currentAgentLabelById;
    const managedAgentIds = currentManagedAgentIds;

    function isEnabled(channelId: string): boolean {
      return enabledChannels.size === 0 || enabledChannels.has(channelId);
    }

    function log(message: string): void {
      if (debug) api.logger.debug?.(`arbiter: ${message}`);
    }

    // --- Cancel thinking ---

    function cancelThinkingForAgent(conv: ConversationState, agentId: string): void {
      const run = conv.agentRuns.get(agentId);
      if (!run?.runId) return;
      void api.runtime.agent.abort({ runId: run.runId }).catch(() => {});
      log(`cancelled ${agentId} runId=${run.runId}`);
      run.runId = undefined;
      run.thinking = false;
      run.cancelled = true;
    }

    function cancelOtherAgents(conv: ConversationState, excludeAgentId?: string): void {
      for (const agentId of managedAgentIds) {
        if (agentId === excludeAgentId) continue;
        cancelThinkingForAgent(conv, agentId);
      }
    }

    // --- /quiet command ---

    api.registerCommand({
      name: "quiet",
      description: "Silence this agent. It resumes when you send a new message.",
      requireAuth: true,
      handler: async (ctx) => {
        const agentId = agentByAccount.get(trimString(ctx.accountId));
        if (agentId) {
          quietAgents.add(agentId);
          for (const conv of conversations.values()) {
            cancelThinkingForAgent(conv, agentId);
          }
          log(`quiet: ${agentId}`);
          return { text: `${agentId} silenced. Send a new message to resume.` };
        }
        return { text: "Could not identify agent." };
      },
    });

    // --- message_received: buffer + cancel stale + trigger rethink ---

    api.on("message_received", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabled(channelId)) return;

        cleanupStaleConversations(stateIdleMs);
        pruneSeenIds();

        const messageId = resolveMessageId(event);
        if (seenMessageIds.has(messageId)) return;
        seenMessageIds.add(messageId);

        const conversationKey = resolveInboundConversationKey(
          channelId, ctx.conversationId, event.metadata,
        );
        if (!conversationKey) return;

        const senderKey = resolveInboundSenderKey(event.metadata);
        const senderAgentId = resolveInboundManagedAgentId({
          channelId, metadata: event.metadata, senderKey, agentByAccount,
        });

        const conversationId =
          trimString(ctx.conversationId) || conversationKey.slice(`${channelId}:`.length);
        const conv = getConversation(conversationKey, channelId, conversationId);

        addToBuffer(conv, {
          messageId,
          kind: senderAgentId ? "agent" : "external",
          content: event.content,
          senderAgentId,
          senderName: getMetadataString(event.metadata, ["senderName", "authorName"]),
          senderKey,
          timestamp: event.timestamp ?? Date.now(),
        }, maxBuffer);

        // New message received — clear pending response flag so fresh
        // dispatches triggered by this message can proceed.
        conv.pendingResponse = false;

        // Reset quiet mode on external (user) message
        if (!senderAgentId) {
          quietAgents.clear();
        }

        // Cancel stale thinking for other agents — they'll re-run via
        // normal Discord dispatch when the new message reaches them.
        log(`message_received channelId=${channelId} senderAgentId=${senderAgentId} convKey=${conv.conversationKey}`);
        cancelOtherAgents(conv, senderAgentId);
      } catch (err) {
        api.logger.warn?.(`arbiter: message_received failed: ${err}`);
      }
    });

    // --- before_agent_start: record runId for abort tracking ---

    api.on("before_agent_start", async (_event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabled(channelId)) return;

        const agentId = trimString(ctx.agentId);
        if (!agentId || !managedAgentIds.has(agentId)) return;

        // If this agent is quieted, abort immediately regardless of conversation state.
        if (quietAgents.has(agentId) && ctx.runId) {
          void api.runtime.agent.abort({ runId: ctx.runId }).catch(() => {});
          log(`quiet abort ${agentId} runId=${ctx.runId}`);
          return;
        }

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        if (!conversationKey) return;

        const conv = conversations.get(conversationKey);
        if (!conv) return;

        // If another agent has already responded but the echo hasn't arrived
        // yet, this dispatch is stale. Abort — it will be re-triggered when
        // the responding agent's echo arrives via message_received.
        if (conv.pendingResponse && ctx.runId) {
          void api.runtime.agent.abort({ runId: ctx.runId }).catch(() => {});
          log(`aborted stale dispatch ${agentId}: another agent already responded`);
          return;
        }

        let run = conv.agentRuns.get(agentId);
        if (!run) {
          run = { thinking: false };
          conv.agentRuns.set(agentId, run);
        }
        run.runId = ctx.runId;
        run.thinking = true;
        run.cancelled = false;
        log(`agent_start ${agentId} runId=${ctx.runId} convKey=${conversationKey}`);
      } catch (err) {
        api.logger.warn?.(`arbiter: before_agent_start failed: ${err}`);
      }
    });

    // --- before_prompt_build: inject conversation context ---

    api.on("before_prompt_build", async (_event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabled(channelId)) return;

        const agentId = trimString(ctx.agentId);
        if (!agentId || !managedAgentIds.has(agentId)) return;

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        if (!conversationKey) return;

        const conv = conversations.get(conversationKey);
        if (!conv) return;

        const transcript = formatBufferTranscript(conv.buffer, agentLabelById, maxPromptChars);

        log(`before_prompt_build ${agentId} bufferSize=${conv.buffer.length} transcriptLen=${transcript.length}`);

        return {
          prependSystemContext: [
            "You are a participant in this multi-agent conversation.",
            "Respond naturally if you have something to contribute.",
            "If you have nothing to add, output only NO_REPLY.",
          ].join(" "),
          ...(transcript ? { prependContext: `Recent conversation:\n\n${transcript}` } : {}),
        };
      } catch (err) {
        api.logger.warn?.(`arbiter: before_prompt_build failed: ${err}`);
      }
    });

    // --- message_sending: gate outbound messages ---

    api.on("message_sending", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabled(channelId)) return;

        const agentId = resolveOutboundAgentId(
          trimString(ctx.accountId) || undefined, event.metadata, agentByAccount,
        );

        if (agentId && quietAgents.has(agentId)) return { cancel: true };

        if (trimString(event.content) === "NO_REPLY") return { cancel: true };

        // Resolve conversation early — pendingResponse check must run even
        // when agentId resolution fails (accountId format mismatch).
        const conversationKey = resolveOutboundConversationKey(
          channelId, ctx.conversationId, event.to, event.metadata,
        );
        const conv = conversationKey ? conversations.get(conversationKey) : undefined;

        if (!conv && conversations.size > 0) {
          api.logger.warn?.(`arbiter: message_sending conv miss: key=${conversationKey} convId=${ctx.conversationId} to=${event.to} accountId=${ctx.accountId} known=[${[...conversations.keys()].join(",")}]`);
        }

        // Block if another agent already responded in this round.
        // This check does NOT depend on agentId resolution.
        if (conv?.pendingResponse) {
          log(`cancelled send in ${conversationKey}: another agent already responded`);
          return { cancel: true };
        }

        // If we can't identify this as a managed agent, still mark the
        // response so other agents' stale dispatches are blocked.
        if (!agentId || !managedAgentIds.has(agentId)) {
          if (conv) {
            conv.pendingResponse = true;
            cancelOtherAgents(conv);
          }
          return;
        }

        // --- Managed agent path (agentId resolved) ---

        if (!conversationKey) {
          if (!failOpen) return { cancel: true };
          return;
        }

        const run = conv?.agentRuns.get(agentId);

        // Stale run check: if this run has been superseded, cancel
        const eventRunId = resolveRunId(event.metadata);
        if (run?.runId && eventRunId && run.runId !== eventRunId) {
          log(`cancelled stale send from ${agentId}: run superseded`);
          return { cancel: true };
        }

        // If this agent was cancelled (another agent responded first), block
        if (run?.cancelled) {
          log(`cancelled stale send from ${agentId}: superseded by another agent`);
          return { cancel: true };
        }

        // Record this message in buffer immediately (before Discord echo)
        if (conv) {
          const messageId = generateId();
          seenMessageIds.add(messageId);
          addToBuffer(conv, {
            messageId,
            kind: "agent",
            content: event.content,
            senderAgentId: agentId,
            timestamp: Date.now(),
          }, maxBuffer);
        }

        // Mark agent as done thinking and abort other agents still thinking
        // in this conversation — the first response wins.
        if (run) {
          run.runId = undefined;
          run.thinking = false;
        }
        if (conv) {
          conv.pendingResponse = true;
          cancelOtherAgents(conv, agentId);
        }

        log(`send from ${agentId} in ${conversationKey}`);
      } catch (err) {
        api.logger.warn?.(`arbiter: message_sending failed: ${err}`);
        if (!failOpen) return { cancel: true };
      }
    });

    // --- message_sent: no action needed ---
    // Other agents are triggered via Discord's message echo (normal dispatch).
    // The arbiter no longer needs to enqueue them.

    // --- agent_end: cleanup run state ---

    api.on("agent_end", async (event, ctx) => {
      try {
        const channelId = trimString(ctx.channelId);
        if (!isEnabled(channelId)) return;

        const agentId = trimString(ctx.agentId);
        if (!agentId || !managedAgentIds.has(agentId)) return;

        const conversationKey = resolveConversationKeyFromSessionKey(ctx.sessionKey, channelId);
        if (!conversationKey) return;

        const conv = conversations.get(conversationKey);
        const run = conv?.agentRuns.get(agentId);
        if (run) {
          run.runId = undefined;
          run.thinking = false;
        }

        // Extract agent's response from event.messages and add to buffer.
        // This is necessary because message_sending doesn't fire for extension
        // plugins, and the re-dispatch carries the original user message.
        if (conv && Array.isArray(event.messages) && event.messages.length > 0) {
          const msg = event.messages[event.messages.length - 1] as Record<string, unknown> | undefined;
          let responseText = "";
          const rawContent = msg?.content;
          if (typeof rawContent === "string") {
            responseText = rawContent.trim();
          } else if (Array.isArray(rawContent)) {
            responseText = rawContent
              .filter((b: unknown) => b && typeof b === "object" && (b as { type?: string }).type === "text")
              .map((b: unknown) => (b as { text?: string }).text ?? "")
              .join("\n")
              .trim();
          }
          // Strip internal markers (e.g. [[reply_to_current]])
          responseText = responseText.replace(/\[\[[^\]]*\]\]\s*/g, "").trim();
          if (responseText && responseText !== "NO_REPLY") {
            const messageId = generateId();
            seenMessageIds.add(messageId);
            addToBuffer(conv, {
              messageId,
              kind: "agent",
              content: responseText,
              senderAgentId: agentId,
              timestamp: Date.now(),
            }, maxBuffer);
            log(`agent_end buffered ${agentId}: ${responseText.slice(0, 50)} (bufSize=${conv.buffer.length})`);
          }
        }

        log(`agent_end ${agentId} convKey=${conversationKey}`);
      } catch (err) {
        api.logger.warn?.(`arbiter: agent_end failed: ${err}`);
      }
    });
  },
};
