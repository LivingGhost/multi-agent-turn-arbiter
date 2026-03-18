# multi-agent-turn-arbiter

OpenClaw plugin for natural multi-agent conversations.

## What It Does

When multiple managed agents share a chat, this plugin lets each agent autonomously decide whether to participate. There is no central turn scheduler — every agent observes the conversation, thinks, and speaks or stays silent based on its own judgment.

The design is simple:

- Every new message triggers all agents to think in parallel
- Each agent's LLM decides whether to respond or stay silent (NO_REPLY)
- If a new message arrives while an agent is thinking, the stale thought is cancelled and the agent re-thinks with the latest context
- Sending is gated so stale responses from superseded runs are dropped
- After an agent speaks, other agents are triggered to consider responding

## How It Works

The plugin maintains a short in-memory message buffer per conversation to supplement session history with messages that haven't been persisted yet (e.g., immediately after a send).

Hooks used:

- `message_received` — adds to buffer, triggers all agents to think (cancels stale runs)
- `before_prompt_build` — injects buffer context and participation prompt
- `message_sending` — drops NO_REPLY and stale-run output
- `message_sent` — triggers other agents to react to the new message
- `agent_end` — cleans up run state

## Commands

| Command | Effect |
| --- | --- |
| `/quiet` | Immediately silences all agents. Cancels in-progress thinking and blocks further responses. Agents automatically resume when the user sends a new message. |

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
- If a model run has already started, cancellation is best-effort through `subagent.abort(...)`.
- The buffer is short-lived in-memory state. Older context comes from normal OpenClaw session history.
- Use `/quiet` if agents become too chatty. They resume on your next message.

## Repo Layout

- `index.ts`: plugin implementation
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
