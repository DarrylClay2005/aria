# Aria Swarm Recovery Authority

This build keeps Aria as the authority for music-bot recovery and doctoring.

## Intended split

- Music bots keep normal startup auto-resume/autoplay behavior.
- Music bots do **not** self-trigger leave/rejoin recovery for transient voice stutter or disconnect.
- Aria watches bot playback state, queue/backup state, direct-order tables, and bot error-event tables.
- Aria queues explicit `RECOVER` direct orders only after the state is old enough to avoid reacting to short internet lag.

## Important guardrails

- `ARIA_RECOVERY_MIN_AGE_SECONDS` controls how old non-playing recoverable state must be before Aria acts.
- `ARIA_PLAYBACK_DRIFT_MIN_AGE_SECONDS` controls drift detection.
- `ARIA_RECOVERY_GUARD_SECONDS` de-dupes repeated `RECOVER` orders.
- Voice connect timeout / recovery-exhausted bot errors arm a pause guard instead of triggering immediate retry storms.

The direct-order schema is now normalized across `core/autonomy.py`, `core/swarm_control.py`, `cogs/swarm_admin.py`, and `cogs/ai_core.py`, including `created_at`, `attempts`, `last_error`, and optional `claimed_at` for compatibility.
