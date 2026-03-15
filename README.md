# multi-agent-turn-arbiter

OpenClaw plugin for keeping visible multi-agent replies orderly in one shared conversation.

## What It Does

When multiple managed agents are present in the same chat, this plugin keeps a short in-memory visible backlog per conversation and schedules a single current owner for each visible source.

The model is intentionally simple:

- a conversation keeps only four pieces of live state:
  - `latestVisible`
  - retained visible `history`
  - one `current` owner turn
  - one `pending` latest source to catch up to
- only the current owner may progress past `llm_input` and send visible output
- if a newer visible message arrives while an owner is still running, that run is best-effort aborted and the newer source becomes pending
- when the current owner ends, the pending latest source is enqueued once through core `subagent.enqueue(...)`
- stale visible sends are canceled before delivery
- same-agent follow-up chunks are allowed only during a short fixed post-send grace window

The goal is practical visible-turn coordination. It does not rely on request-scoped plugin wake launches and it does not promise hard cancellation unless the core `abort` primitive succeeds.

## How It Works

The plugin keeps short-lived in-memory state per conversation:

- the latest visible message seen in the room
- a short retained visible backlog for prompt reconstruction
- a single current owner turn
- a single pending latest source to hand off after the current owner ends
- a short post-send grace window for valid same-run chunking

It uses these OpenClaw hooks:

- `message_received`
- `before_prompt_build`
- `llm_input`
- `llm_output`
- `message_sending`
- `message_sent`
- `agent_end`

## Requirements

This plugin works best when inbound and outbound hook metadata are rich enough to correlate visible room messages and agent sends.

In practice, it works best with an OpenClaw build that exposes:

- inbound managed-sender metadata like `senderManagedAccountId`
- outbound hook metadata like:
  - `conversationId`
  - `threadId`
  - `sessionKey`
  - `agentId`
- plugin runtime primitives:
  - `subagent.enqueue(...)`
  - `subagent.abort(...)`

Without that metadata, visible sender attribution and stale-send suppression are less reliable.

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
          "enabledChannels": [],
          "activeRunLeaseMs": 180000,
          "postSendGraceMs": 15000,
          "stateIdleMs": 300000,
          "maxBacklogTurns": 6,
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
| `activeRunLeaseMs` | Maximum age for an in-flight run before the plugin treats it as stale and releases it. Default: `180000`. |
| `postSendGraceMs` | Fixed grace window after the current owner’s first visible output during which same-run follow-up chunks are still allowed. Default: `15000`. |
| `stateIdleMs` | Idle timeout for clearing retained in-memory conversation state such as latest visible messages, visible backlog, pending catch-up sources, and current owner state. Default: `300000`. |
| `maxBacklogTurns` | Maximum number of newest visible conversation turns to retain in plugin memory. Consecutive visible messages from the same speaker count as one turn. Set `-1` for unlimited. Default: `6`. |
| `maxPromptChars` | Character budget for plugin-added visible backlog context. Set `-1` for unlimited. Older retained backlog is dropped from the start if needed. Default: `30000`. |
| `failOpen` | If true, prefer letting delivery continue on plugin errors instead of blocking output. Default: `true`. |
| `debug` | Enables verbose plugin logging. Default: `false`. |

## Limits

- It serializes visible output, not hidden generation.
- If a model run has already started, the plugin can only best-effort abort it through core `subagent.abort(...)`.
- A slow agent may keep running in the background, but if newer visible room state supersedes it, its stale visible send is cancelled before delivery.
- If a newer visible message arrives while an owner is still active, the conversation keeps only the latest pending source. Older pending sources are dropped.
- Initial empty-state serialization still uses a bootstrap owner. If the plugin was restarted and has no fresh visible backlog yet, the winner must rely on normal session history for that first reply.
- If one visible turn is split across multiple managed-agent messages, follow-up chunks are allowed only while the fixed post-send grace window for that same owner turn is still active.
- The plugin keeps only short-lived in-memory state. Older context is expected to come from normal OpenClaw session history, not from this plugin.

## Repo Layout

- `index.ts`: plugin implementation
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
