# multi-agent-turn-arbiter

OpenClaw plugin for natural multi-agent conversations.

## What It Does

When multiple managed agents share a chat, this plugin lets each agent autonomously decide whether to participate. There is no central turn scheduler — every agent observes the conversation, thinks, and speaks or stays silent based on its own judgment.

The design is simple:

- Agents respond via normal Discord dispatch — each bot account receives messages and runs its own agent
- If a new message arrives while an agent is thinking, the stale run is cancelled via `agent.abort` and the agent re-runs with the latest context
- Sending is gated so stale responses from superseded runs are dropped
- Each agent's LLM decides whether to respond or stay silent (NO_REPLY)
- A shared message buffer supplements session history with other agents' messages

## How It Works

The plugin maintains a short in-memory message buffer per conversation. This provides cross-agent context that isn't in each agent's individual session history.

Hooks used:

- `message_received` — adds to buffer, cancels stale agent runs via `agent.abort`
- `before_agent_start` — records runId for abort tracking
- `before_prompt_build` — injects buffer context and participation prompt
- `message_sending` — drops NO_REPLY, stale-run output, and quiet agent output
- `agent_end` — cleans up run state

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
          "maxBufferMessages": 50,
          "stateIdleMs": 300000,
          "maxPromptChars": 30000,
          "failOpen": true,
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
| `maxBufferMessages` | Maximum messages kept in the per-conversation buffer. Default: `50`. |
| `stateIdleMs` | Idle timeout for clearing conversation state. Default: `300000`. |
| `maxPromptChars` | Character budget for buffer context injected into prompts. `-1` for unlimited. Default: `30000`. |
| `failOpen` | If true, allow delivery on plugin errors instead of blocking. Default: `true`. |
| `debug` | Enables verbose plugin logging. Default: `false`. |

## Limits

- Agent participation decisions depend on LLM judgment. Persona prompts influence how often each agent speaks.
- Cancellation is best-effort through `agent.abort`. If a run has already produced output, `message_sending` drops it.
- The buffer is short-lived in-memory state. Older context comes from normal OpenClaw session history.
- Use `/quiet` on an agent to silence it individually. It resumes on the next user message.

## Repo Layout

- `index.ts`: plugin implementation
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
