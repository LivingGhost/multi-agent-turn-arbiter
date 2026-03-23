# multi-agent-turn-arbiter

OpenClaw plugin for natural multi-agent conversations.

## What It Does

When multiple managed agents share a chat, this plugin lets each agent autonomously decide whether to participate. There is no central turn scheduler — every agent observes the conversation, thinks, and speaks or stays silent based on its own judgment.

The design is simple:

- Agents respond via normal Discord dispatch — each bot account receives messages and runs its own agent
- If a new message arrives while an agent is thinking, the stale run is cancelled via `agent.abort` and the agent re-runs with the latest context
- Each agent's LLM decides whether to respond or stay silent (NO_REPLY)
- A shared message buffer supplements session history with other agents' messages
- Agent responses are captured in `agent_end` and added to the buffer so subsequent agents see them

## How It Works

The plugin maintains a short in-memory message buffer per conversation. This provides cross-agent context that isn't in each agent's individual session history.

Hooks used:

- `message_received` — adds to buffer, captures metadata, cancels stale agent runs via `agent.abort`
- `before_agent_start` — records runId for abort tracking, blocks stale dispatches via `pendingResponse`
- `before_prompt_build` — injects buffer context with timestamps and conversation metadata
- `agent_end` — extracts agent response from `event.messages`, adds to buffer, cleans up run state

### Prompt Format

The buffer is injected into each agent's prompt as a timestamped transcript:

```
--- Recent context (oldest → newest, trimmed from older side) ---
channelName: test4 | surface: discord

[2026-03-23T16:55:31.000Z] sd:
3段階テスト。このメッセージを最初に読んだエージェントは100を返して。

[2026-03-23T16:57:19.310Z] rek:
  (model: claude-opus-4-6, provider: anthropic)
100

[2026-03-23T16:57:57.952Z] nimel:
42
```

- ISO 8601 timestamps
- Conversation-level metadata in header (channel name, surface, provider — captured generically from any channel)
- Per-message metadata shown inline when present (media type, reply-to, model, etc.)
- Internal IDs and arbiter-specific markers are not exposed

### Metadata

Metadata is stored at two levels:

- **Conversation level** — channel name, surface, provider, guild ID, etc. Captured from the first message and shown in the transcript header.
- **Message level** — media type, reply-to, thread ID, model/provider for agent responses. Shown inline under each message when present.

Both levels use generic `Record<string, string>` storage. The plugin auto-extracts string-valued fields from event metadata and classifies them by a configurable set of conversation-level keys. Everything else is treated as per-message metadata. This works across channel providers without hardcoding provider-specific fields.

## Tools

| Tool | Description |
| --- | --- |
| `conversation_history` | Retrieve earlier messages from the multi-agent conversation. Supports `offset` (skip recent), `limit` (max results), and `agentId` (filter by participant). When the buffer is empty (e.g. after restart), falls back to reading the target agent's session history. Returns the same format as the auto-injected prompt context. |

Agents are prompted to use this tool when they need older context not shown in the recent transcript.

## Commands

| Command | Effect |
| --- | --- |
| `/quiet` | Silences the agent this command is sent to. Cancels in-progress thinking and aborts new runs immediately. The agent resumes when a user sends a new message. |

## Installation

Place this directory under your OpenClaw extensions directory:

```text
.openclaw/extensions/multi-agent-turn-arbiter
```

Then allow and enable it in `openclaw.json`:

```json
{
  "plugins": {
    "allow": ["multi-agent-turn-arbiter"],
    "entries": {
      "multi-agent-turn-arbiter": {
        "enabled": true,
        "config": {
          "maxBufferMessages": 10,
          "stateIdleMs": 300000,
          "maxPromptChars": 30000,
          "debug": false
        }
      }
    }
  }
}
```

## Config

| Key | Meaning |
| --- | --- |
| `enabled` | Turns the plugin on or off. |
| `enabledChannels` | Optional allowlist of channel IDs. Empty means all channels. |
| `maxBufferMessages` | Maximum messages kept in the per-conversation buffer. Default: `10`. Older messages are available via the `conversation_history` tool. |
| `stateIdleMs` | Idle timeout for clearing conversation state. Default: `300000`. |
| `maxPromptChars` | Character budget for buffer context injected into prompts. `-1` for unlimited. Default: `30000`. |
| `debug` | Enables verbose plugin logging. Default: `false`. |

## Known Limitations

- Agent participation decisions depend on LLM judgment. Persona prompts influence how often each agent speaks.
- Cancellation is best-effort through `agent.abort`. The abort may not take effect if the agent has already completed inference.
- `message_sending` hook does not fire for extension plugins in the current openclaw runtime. Outbound gating (cancel stale responses, quiet agent suppression) is not possible at the plugin level.
- The buffer is short-lived in-memory state. Older context comes from normal OpenClaw session history.
- Agents are dispatched sequentially (not in parallel) by openclaw. The buffer captures each agent's response in `agent_end` so subsequent agents see prior responses.

## Repo Layout

- `index.ts`: plugin implementation
- `index.test.ts`: test suite
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
