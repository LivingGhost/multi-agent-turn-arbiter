import { describe, it, expect, vi, beforeEach } from "vitest";

// --- Mock API factory ---

type HookHandler = (...args: unknown[]) => unknown;
type CommandHandler = (ctx: Record<string, unknown>) => unknown;

const AGENT_A = "agent-a";
const AGENT_B = "agent-b";

function createMockApi(overrides?: {
  pluginConfig?: Record<string, unknown>;
  bindings?: Array<{ agentId: string; match: { channel: string; accountId: string } }>;
  agents?: Array<{ id: string; identity?: { name: string } }>;
}) {
  const hooks = new Map<string, HookHandler[]>();
  const commands = new Map<string, CommandHandler>();
  const abortCalls: Array<{ runId: string }> = [];

  const api = {
    pluginConfig: overrides?.pluginConfig ?? {},
    config: {
      bindings: overrides?.bindings ?? [
        { agentId: AGENT_A, match: { channel: "discord", accountId: AGENT_A } },
        { agentId: AGENT_B, match: { channel: "discord", accountId: AGENT_B } },
      ],
      agents: {
        list: overrides?.agents ?? [
          { id: AGENT_A, identity: { name: "Agent A" } },
          { id: AGENT_B, identity: { name: "Agent B" } },
        ],
      },
    },
    logger: {
      info: vi.fn(),
      warn: vi.fn(),
      debug: vi.fn(),
    },
    runtime: {
      agent: {
        abort: vi.fn(async (params: { runId: string }) => {
          abortCalls.push(params);
          return { aborted: true };
        }),
      },
      subagent: {
        abort: vi.fn(async () => ({ aborted: true })),
        run: vi.fn(async () => ({ runId: "mock-run" })),
        enqueue: vi.fn(async () => ({ runId: "mock-run" })),
      },
    },
    on: vi.fn((hookName: string, handler: HookHandler) => {
      const list = hooks.get(hookName) ?? [];
      list.push(handler);
      hooks.set(hookName, list);
    }),
    registerCommand: vi.fn((cmd: { name: string; handler: CommandHandler }) => {
      commands.set(cmd.name, cmd.handler);
    }),
    registerTool: vi.fn(),
  };

  return {
    api,
    hooks,
    commands,
    abortCalls,
    async fireHook(name: string, event: unknown, ctx: unknown) {
      const handlers = hooks.get(name) ?? [];
      let result: unknown;
      for (const handler of handlers) {
        result = await handler(event, ctx);
      }
      return result;
    },
  };
}

async function loadPlugin() {
  const mod = await import("./index.js");
  return mod.default as { register: (api: unknown) => void };
}

// --- Tests ---

describe("multi-agent-turn-arbiter", () => {
  let plugin: { register: (api: unknown) => void };

  beforeEach(async () => {
    vi.resetModules();
    plugin = await loadPlugin();
  });

  it("registers conversation_history tool", () => {
    const { api } = createMockApi();
    plugin.register(api as never);

    expect(api.registerTool).toHaveBeenCalledWith(
      expect.objectContaining({ name: "conversation_history" }),
    );
  });

  it("registers expected hooks", () => {
    const { api, hooks } = createMockApi();
    plugin.register(api as never);

    expect(hooks.has("message_received")).toBe(true);
    expect(hooks.has("before_agent_start")).toBe(true);
    expect(hooks.has("before_prompt_build")).toBe(true);
    expect(hooks.has("agent_end")).toBe(true);
  });

  it("does not register hooks when enabled is false", () => {
    const { api, hooks } = createMockApi({ pluginConfig: { enabled: false } });
    plugin.register(api as never);

    expect(hooks.size).toBe(0);
  });

  it("registers /quiet command", () => {
    const { api, commands } = createMockApi();
    plugin.register(api as never);

    expect(commands.has("quiet")).toBe(true);
  });

  it("buffers messages on message_received", async () => {
    const { api, fireHook } = createMockApi();
    plugin.register(api as never);

    await fireHook("message_received", {
      content: "hello",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-1" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    const result = await fireHook("before_prompt_build", {}, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:123`,
    });

    expect(result).toBeDefined();
    const ctx = result as { prependContext?: string };
    expect(ctx.prependContext).toContain("--- Recent context (oldest → newest");
    expect(ctx.prependContext).toContain("hello");
    expect(ctx.prependContext).toContain("user|");
    expect(ctx.prependContext).toMatch(/\d{4}-\d{2}-\d{2}T/);
    expect(ctx.prependContext).toContain("msg:msg-1");
    // messageId should appear only once (in header as msg:), not duplicated in metadata
    expect(ctx.prependContext).not.toContain("messageId:");
  });

  it("cancels other agents on new message via agent.abort", async () => {
    const { api, fireHook, abortCalls } = createMockApi();
    plugin.register(api as never);

    await fireHook("message_received", {
      content: "setup",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-setup" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:123`,
      runId: "run-a-1",
    });

    await fireHook("message_received", {
      content: "new message",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-2" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    expect(abortCalls.some((c) => c.runId === "run-a-1")).toBe(true);
  });

  it("does not cancel the sender agent", async () => {
    const { api, fireHook, abortCalls } = createMockApi();
    plugin.register(api as never);

    await fireHook("message_received", {
      content: "setup",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-sender-setup" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_B,
      sessionKey: `agent:${AGENT_B}:discord:channel:123`,
      runId: "run-b-1",
    });

    await fireHook("message_received", {
      content: "agent-b says hello",
      from: AGENT_B,
      timestamp: Date.now(),
      metadata: {
        messageId: "msg-3",
        senderAgentId: AGENT_B,
        senderManagedAccountId: AGENT_B,
      },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    expect(abortCalls.some((c) => c.runId === "run-b-1")).toBe(false);
  });


  it("cleans up run state on agent_end", async () => {
    const { api, fireHook, abortCalls } = createMockApi();
    plugin.register(api as never);

    await fireHook("message_received", {
      content: "setup",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-end-setup" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:123`,
      runId: "run-end-test",
    });

    await fireHook("agent_end", { messages: [], success: true }, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:123`,
    });

    abortCalls.length = 0;
    await fireHook("message_received", {
      content: "after end",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-after-end" },
    }, {
      channelId: "discord",
      conversationId: "channel:123",
    });

    expect(abortCalls.some((c) => c.runId === "run-end-test")).toBe(false);
  });

  it("/quiet silences a specific agent and aborts on next run", async () => {
    const { api, fireHook, commands, abortCalls } = createMockApi();
    plugin.register(api as never);

    const quietHandler = commands.get("quiet")!;
    const result = await quietHandler({ accountId: AGENT_A });
    expect((result as { text: string }).text).toContain(AGENT_A);

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:456`,
      runId: "run-quiet-test",
    });

    expect(abortCalls.some((c) => c.runId === "run-quiet-test")).toBe(true);
  });

  it("/quiet does not affect other agents", async () => {
    const { api, fireHook, commands, abortCalls } = createMockApi();
    plugin.register(api as never);

    const quietHandler = commands.get("quiet")!;
    await quietHandler({ accountId: AGENT_A });

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_B,
      sessionKey: `agent:${AGENT_B}:discord:channel:456`,
      runId: "run-b-not-quiet",
    });

    expect(abortCalls.some((c) => c.runId === "run-b-not-quiet")).toBe(false);
  });

  it("user message resets quiet mode", async () => {
    const { api, fireHook, commands, abortCalls } = createMockApi();
    plugin.register(api as never);

    const quietHandler = commands.get("quiet")!;
    await quietHandler({ accountId: AGENT_A });

    await fireHook("message_received", {
      content: "hey",
      from: "user",
      timestamp: Date.now(),
      metadata: { messageId: "msg-reset-quiet" },
    }, {
      channelId: "discord",
      conversationId: "channel:456",
    });

    await fireHook("before_agent_start", { prompt: "test" }, {
      channelId: "discord",
      agentId: AGENT_A,
      sessionKey: `agent:${AGENT_A}:discord:channel:456`,
      runId: "run-after-reset",
    });

    expect(abortCalls.some((c) => c.runId === "run-after-reset")).toBe(false);
  });


  it("skips registration when config has no bindings", () => {
    const { api, hooks } = createMockApi({ bindings: [] });
    plugin.register(api as never);

    expect(hooks.size).toBe(0);
  });
});
