# Aria Code Audit Tracker

Audit target: `Aria(12).zip`  
Audit date: 2026-05-18  
Scope: runtime source, cogs, core modules, package modules, tests, Docker/package hygiene. Secrets, `.env`, venvs, logs, git metadata, local model binaries, and backup folders were not included in the deliverable patch.

## Validation Summary

| Check | Original | Patched |
|---|---:|---:|
| Active Python files scanned | 61 | 61 |
| Python syntax errors | 0 | 0 |
| `compileall` | not final | pass |
| Bundled pytest suite | failed during collection | **46 passed** |
| `print()` calls in active source | 4 | **0** |
| pass-only exception handlers | 58 | **11** |
| pass-only broad `Exception` handlers | many in autonomy/swarm areas | **0** |

Remaining pass-only handlers are intentional control-flow / Discord API cases: `asyncio.CancelledError`, `asyncio.TimeoutError`, and `discord.Forbidden` permission-denial cleanup paths.

## Fixed Findings

### ARIA-001 — Telegram commands could fail once and never retry
- **Severity:** High
- **Files:** `core/telegram_bridge.py`
- **Problem:** `_poll_loop()` treated a verified Telegram username as proof that command registration succeeded. If `getMe` succeeded but `setMyCommands` failed during startup, Aria could keep polling forever without re-registering commands. This matches the “Telegram commands not showing” symptom.
- **Fix:** Added `self._commands_registered`, `_register_commands()`, and retry logic that only marks registration complete after `setMyCommands` actually succeeds.
- **Status:** Fixed and tested.

### ARIA-002 — Test suite import crash from incomplete Discord stub
- **Severity:** High
- **Files:** `tests/test_image_ops_shuffle.py`
- **Problem:** Test collection failed before running any tests because the local `discord.app_commands` stub lacked decorators used by `cogs.image_ops`.
- **Fix:** Added identity stubs for `app_commands.describe` and `app_commands.rename`.
- **Status:** Fixed. `46 passed`.

### ARIA-003 — Error webhook subsystem used `print()` instead of logging
- **Severity:** Medium
- **Files:** `core/webhooks.py`
- **Problem:** Error persistence/dispatch failures were printed to stderr. In Docker logs this is harder to classify, filter, and route than structured logger output.
- **Fix:** Replaced stderr `print()` calls with `logger.warning()` / `logger.debug()`.
- **Status:** Fixed.

### ARIA-004 — Error webhook logging handler swallowed its own failures silently
- **Severity:** Medium
- **Files:** `core/webhooks.py`
- **Problem:** `AriaErrorWebhookHandler.emit()` had `except Exception: pass`, hiding dispatch recursion/failure details.
- **Fix:** Converted to debug logging so recursive failures stay non-fatal but visible.
- **Status:** Fixed.

### ARIA-005 — Swarm schema migrations swallowed all ALTER/INDEX errors
- **Severity:** High
- **Files:** `core/swarm_control.py`
- **Problem:** schema compatibility ALTERs and index creation used broad `except Exception: pass`, hiding permission failures, syntax mismatches, and bad table states.
- **Fix:** Added `_run_schema_statement()` and `_is_benign_schema_error()`. Duplicate-column/index cases are debug-level; unexpected migration issues become warnings.
- **Status:** Fixed.

### ARIA-006 — Invalid drone names could accidentally target the whole swarm
- **Severity:** High
- **Files:** `core/swarm_control.py`
- **Problem:** Several methods normalized a user-provided drone name, then used the entire swarm if normalization returned `None`. That is safe when `drone` is omitted, but dangerous when `drone="typo"`.
- **Fix:** Added `unknown_drone_message()` and explicit guards for invalid provided drone names in leave, loop, queue view, shuffle, and filter paths.
- **Status:** Fixed.

### ARIA-007 — Autonomy infra execution used shell commands without a local safety validator
- **Severity:** High
- **Files:** `core/autonomy.py`
- **Problem:** Infra actions are guarded by env toggles, but the actual execution still used `asyncio.create_subprocess_shell()`. If a configured command were malformed or compromised, shell chaining/substitution could run extra commands.
- **Fix:** Added `_infra_command_allowed()` and rejection of newline, chaining, pipe, redirection, and substitution tokens before shell execution.
- **Status:** Fixed conservatively. Legit restart commands should stay simple, e.g. `docker restart container_name`.

### ARIA-008 — Autonomy duplicate assignment
- **Severity:** Low
- **Files:** `core/autonomy.py`
- **Problem:** `issue_type = issue.get("type")` was duplicated back-to-back.
- **Fix:** Removed duplicate assignment.
- **Status:** Fixed.

### ARIA-009 — Autonomy webhook/notification failures were silently ignored
- **Severity:** Medium
- **Files:** `core/autonomy.py`, `aria/aria_monitor.py`
- **Problem:** Best-effort ops/error notifications could fail silently, making it look like Aria did nothing.
- **Fix:** Converted broad pass-only handlers to debug logging.
- **Status:** Fixed.

### ARIA-010 — Autonomy repair/schema best-effort failures hid useful DB details
- **Severity:** Medium
- **Files:** `core/autonomy.py`
- **Problem:** schema patching, queue normalization, direct-order de-duping, and repair journaling had broad pass-only handlers.
- **Fix:** Converted broad pass-only `Exception` handlers to debug logging so failures remain non-fatal but diagnosable.
- **Status:** Fixed.

### ARIA-011 — Aria core learning/context writes could fail silently
- **Severity:** Medium
- **Files:** `aria/aria_core.py`
- **Problem:** prompt observation, recent context lookup, response learning writes, prompt-fragment generation, insult seed generation, and live swarm context fallbacks used silent exception handling.
- **Fix:** Added logger and debug messages for those best-effort paths.
- **Status:** Fixed.

### ARIA-012 — Command failure learning could mask original user-facing error
- **Severity:** Medium
- **Files:** `aria/aria_core.py`
- **Problem:** `_execute_intents()` recorded failure in the database inside the exception path. If that write failed, it could mask the original command error.
- **Fix:** Wrapped failure recording in its own guarded block.
- **Status:** Fixed.

### ARIA-013 — Slash/prefix error response send failures were silent
- **Severity:** Low
- **Files:** `aria.py`
- **Problem:** When Aria tried to tell the user a command failed, HTTP failures were swallowed.
- **Fix:** Added debug logging for failed user-facing error responses.
- **Status:** Fixed.

### ARIA-014 — Invalid Telegram admin chat ID entries were silent
- **Severity:** Low
- **Files:** `aria.py`
- **Problem:** Bad `ARIA_TELEGRAM_ADMIN_CHAT_IDS` chunks were ignored with no clue.
- **Fix:** Added debug logging for invalid chunks without printing secrets.
- **Status:** Fixed.

### ARIA-015 — Swarm admin direct-order/schema paths had silent migration failures
- **Severity:** Medium
- **Files:** `cogs/swarm_admin.py`
- **Problem:** the slash-command admin cog contained pass-only broad handlers around direct-order migration and backup/history lookups.
- **Fix:** Converted broad pass-only handlers to debug logging.
- **Status:** Fixed.

### ARIA-016 — Image/video UI cleanup failures were silent
- **Severity:** Low
- **Files:** `cogs/image_ops.py`, `cogs/video_ops.py`
- **Problem:** message edit/followup cleanup and unload cleanup could fail silently.
- **Fix:** Added debug logging while keeping failures non-fatal.
- **Status:** Fixed.

### ARIA-017 — Pipeline helper fallback failures were silent
- **Severity:** Low
- **Files:** `core/image_pipeline.py`, `core/video_pipeline.py`
- **Problem:** Real-ESRGAN chmod failures and Bing video redirect decode failures had pass-only handlers.
- **Fix:** Added module loggers and debug logging.
- **Status:** Fixed.

### ARIA-018 — Ultimatum moderation failures were silent
- **Severity:** Low
- **Files:** `cogs/ultimatum.py`
- **Problem:** timeout failures were ignored silently.
- **Fix:** Added debug logging.
- **Status:** Fixed.

### ARIA-019 — Docker build context could still carry local junk/heavy artifacts
- **Severity:** Medium
- **Files:** `.dockerignore`
- **Problem:** `.env` was already excluded, but local backups, model folders, sqlite files, and the local Real-ESRGAN binary were not fully covered.
- **Fix:** Hardened `.dockerignore` with backup, model, binary, and local DB excludes.
- **Status:** Fixed.

## Dead Code / Bad Code Notes

### No obvious removable runtime dead code was safely deleted
A static scan finds many functions that look unused, but most are Discord command callbacks, cog lifecycle hooks, context-menu callbacks, task handlers, or framework entry points. Removing those would be reckless. The safer conclusion is: **no source-level runtime dead code was removed**.

### Non-source junk excluded from deliverable
The uploaded zip contained local/dev artifacts such as `.git`, `.venv`, `.pytest_cache`, logs, backup folders, model folders, and a local Real-ESRGAN binary. These are not source fixes, but they are bad build/deploy hygiene. They were excluded from the final patch zip and `.dockerignore` was tightened.

## Remaining Intentional Patterns

These were left alone because they are expected control flow and changing them would create log spam or incorrect behavior:

- `asyncio.CancelledError` during task shutdown.
- `asyncio.TimeoutError` for user interaction views that time out normally.
- `discord.Forbidden` for optional reaction/emoji cleanup where missing permissions are expected.

## Files Modified

- `.dockerignore`
- `aria.py`
- `aria/aria_core.py`
- `aria/aria_monitor.py`
- `cogs/image_ops.py`
- `cogs/swarm_admin.py`
- `cogs/ultimatum.py`
- `cogs/video_ops.py`
- `core/autonomy.py`
- `core/image_pipeline.py`
- `core/swarm_control.py`
- `core/telegram_bridge.py`
- `core/video_pipeline.py`
- `core/webhooks.py`
- `tests/test_image_ops_shuffle.py`

## Apply Notes

Replace your Aria source with the patched files from the patch zip, but keep your existing `.env` on your machine. The patch zip intentionally does not include `.env` or secret-bearing runtime folders.
