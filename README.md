# multi-agent-turn-arbiter

OpenClaw plugin for serializing visible multi-agent turns in a shared conversation.

## What it does

When multiple bot agents are present in the same chat, this plugin tries to keep the conversation visually sequential:

- one bot claims the current turn
- other bots yield instead of visibly replying at the same time
- chunked or split bot output is given a short settle window before the baton moves
- once a bot reply becomes visible, other eligible bots can react to that visible message in the next hop
- stale late-arriving bot messages are ignored as new roots when possible

The goal is practical coordination of visible output. It does not hard-cancel model generations that already started.

## How it works

The plugin keeps short-lived in-memory state per conversation:

- a short retained visible backlog for the conversation
- the current claim holder
- active in-flight runs per agent
- deferred wake state for busy agents
- per-message decline state
- dedupe and stale-message guards

It uses OpenClaw hooks such as:

- `message_received`
- `before_prompt_build`
- `llm_input`
- `llm_output`
- `message_sending`
- `message_sent`
- `agent_end`

## Requirements

This plugin expects outbound hook metadata to include enough information to correlate sends back to the active conversation turn.

In practice, it works best with an OpenClaw build that exposes:

- inbound managed-sender metadata like `senderManagedAccountId`
- outbound hook metadata like:

- `conversationId`
- `threadId`
- `sessionKey`
- `agentId`

Without that metadata, loser/stale send suppression is less reliable.

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
          "leaseMs": 30000,
          "activeRunLeaseMs": 300000,
          "settleMs": 10000,
          "botReopenCooldownMs": 30000,
          "epochIdleMs": 300000,
          "maxBotHops": 16,
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
| `leaseMs` | How long a claim stays valid without activity. Default: `30000`. |
| `activeRunLeaseMs` | Minimum lease for in-flight runs that already have a `runId`, so slow models are not treated as idle too early. Default: `300000`. |
| `settleMs` | How long to wait for split/chunked visible output before finalizing a bot turn. Default: `10000`. |
| `botReopenCooldownMs` | Cooldown that suppresses stale bot-root reopen attempts after a new visible root appears. Default: `30000`. |
| `epochIdleMs` | Idle timeout for deleting a live epoch that stops progressing. Finished epochs are deleted immediately. Default: `300000`. |
| `maxBotHops` | Maximum number of bot-to-bot hops before the epoch closes. Set `-1` for unlimited. Default: `16`. |
| `maxBacklogTurns` | Maximum number of newest visible conversation turns to retain in plugin memory for deferred catch-up and prompt reconstruction. Consecutive visible messages from the same speaker count as one turn. Set `-1` for unlimited. Default: `6`. |
| `maxPromptChars` | Character budget for plugin-added visible transcript context. Set `-1` for unlimited. The latest visible message is always kept, and older retained backlog messages are included from newest to oldest until the budget is filled. Default: `30000`. |
| `failOpen` | If true, prefer letting delivery continue on plugin errors instead of blocking output. Default: `true`. |
| `debug` | Enables verbose plugin logging. Default: `false`. |

## Limits

- It serializes visible output, not hidden generation.
- If a model run has already started, the plugin cannot guarantee hard cancellation.
- Continuing multi-hop bot dialogue depends on visible bot messages being re-observed as inbound events by the host channel integration.
- The plugin keeps only short-lived in-memory state. Finished epochs are deleted immediately, idle live epochs age out after `epochIdleMs`, and retained visible backlog is capped by `maxBacklogTurns`. It is not a persistent transcript store.

## Repo layout

- `index.ts`: plugin implementation
- `openclaw.plugin.json`: plugin manifest and config schema
- `package.json`: package metadata
