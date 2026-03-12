# multi-agent-turn-arbiter

OpenClaw plugin for keeping visible multi-agent replies orderly in one shared conversation.

## What It Does

When multiple managed agents are present in the same chat, this plugin keeps a short in-memory visible backlog per conversation and uses that backlog to coordinate replies.

The model is intentionally simple:

- new runs look at the latest retained visible backlog
- if the plugin has no fresh visible backlog yet, only one managed agent may own the initial conversation start at a time
- only one managed agent may own a given visible source message at a time
- non-owners may still run internally, but they are ignored at `llm_input` and hard-cancelled at `message_sending`
- if an agent is already busy, newer visible messages are deferred for that same agent
- a busy run may continue in the background, but stale visible output is dropped before send
- if a managed agent is still visibly in the middle of its turn, other agents wait until that run ends
- once it finishes, that agent can be woken once to catch up from the latest deferred visible message
- if two agents try to send against the same visible source, only the first send is allowed through

The goal is practical visible-turn coordination. It does not hard-cancel model generations that already started.

## How It Works

The plugin keeps short-lived in-memory state per conversation:

- the latest visible message seen in the room
- a short retained visible backlog for prompt reconstruction
- a short bootstrap lock for the very first reply when no fresh visible backlog is cached yet
- a short reserved/active source lock so only one managed agent may answer a given visible source at a time
- active in-flight runs per agent
- one deferred wake source per conversation+agent
- a short send lock so only one visible send wins for the same source message
- a short post-send grace window so success without visible echo does not leak locks until the full active-run lease

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
          "reservationLeaseMs": 10000,
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
| `reservationLeaseMs` | Maximum age for a reserved initial/source lock before it is discarded if no real run starts. Expired reservations immediately re-open deferred waiters. Default: `10000`. |
| `postSendGraceMs` | How long to keep a successful send alive while waiting for visible echo or `agent_end` before forcing release. Default: `15000`. |
| `stateIdleMs` | Idle timeout for clearing retained in-memory conversation state such as latest visible messages, visible backlog, deferred wakes, and send locks. Default: `300000`. |
| `maxBacklogTurns` | Maximum number of newest visible conversation turns to retain in plugin memory. Consecutive visible messages from the same speaker count as one turn. Set `-1` for unlimited. Default: `6`. |
| `maxPromptChars` | Character budget for plugin-added visible backlog context. Set `-1` for unlimited. Older retained backlog is dropped from the start if needed. Default: `30000`. |
| `failOpen` | If true, prefer letting delivery continue on plugin errors instead of blocking output. Default: `true`. |
| `debug` | Enables verbose plugin logging. Default: `false`. |

## Limits

- It serializes visible output, not hidden generation.
- If a model run has already started, the plugin cannot hard-cancel it.
- A slow agent may keep running in the background, but if newer visible room state supersedes it, its stale visible send is cancelled before delivery.
- Initial empty-state serialization uses a short bootstrap lock. If the plugin was restarted and still has no fresh visible backlog, the winner must rely on normal session history for that first reply.
- Cleanup-triggered deferred wakes are launched only for the current conversation after the current inbound visible message has been recorded into plugin backlog state.
- If one visible turn is split across multiple managed-agent messages, the plugin holds handoff until that sender run ends.
- The plugin keeps only short-lived in-memory state. Older context is expected to come from normal OpenClaw session history, not from this plugin.

## Repo Layout

- `index.ts`: plugin implementation
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
