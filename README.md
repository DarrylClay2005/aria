# Aria

![Aria bot icon](assets/bot-icon.png)

Aria is the operator brain for the Discord bot swarm. She watches the music bots, records health events, posts structured Discord webhook alerts, sends scoped Telegram operator notices, and answers `@Aria` and `/aria` requests with runtime-aware context.

## What She Does

- Monitors the 12 music bots for stale heartbeats, voice timeouts, queue drift, playback stalls, and recovery candidates.
- Keeps Discord webhook alerts for operational feeds while using Telegram only for scoped owner notices.
- Runs Medic repair logic that can inspect queue and backup state before attempting playback recovery.
- Adds real current date, local time, UTC time, and timezone context to Aria responses so she does not answer as if it is an older year.
- Handles bot error events, drift summaries, stale node checks, database health checks, and recovery probes.
- Coordinates with SwarmPanel by writing and reading the shared operational tables used by the panel.
- Keeps AI behavior anchored to Aria Blaze-inspired personality while giving the runtime context needed for practical operator work.

## Main Systems

- **Aria Core:** response generation, persona instruction, runtime date and timezone grounding, and command context.
- **Medic:** issue detection, queue recovery, stale node review, and drift triage.
- **Autonomy:** event intake, repair candidate selection, cooldowns, and recovery decision logic.
- **Telegram Bridge:** owner-scoped alerts for database and system problems without forwarding noisy Python logs.
- **Webhook Feeds:** structured Discord embeds for operational events, warnings, and errors.
- **Database:** shared swarm telemetry and health state used by Aria and SwarmPanel.

## Servers And Data

- Runtime: Python Discord app.
- Database: shared MySQL telemetry and bot schemas.
- Operator feeds: Discord webhooks and Telegram owner chat IDs.
- Connected projects: SwarmPanel plus all 12 music bots.

## Guardrails

- Telegram alerts are scoped and cooldown-based to avoid spam.
- Python logging remains local or webhook-oriented rather than direct Telegram firehose output.
- Secrets belong in ignored environment files, never in committed code.
- Aria should use runtime context for dates and clearly admit when live-changing facts need a current source.

## Copyright

(c) HeavenlyXenusVR. Discord: <https://discord.com/users/1304564041863266347>
