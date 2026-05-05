from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

try:
    import aiomysql
except ImportError:  # pragma: no cover
    aiomysql = None

from core.database import db
from core.learning import LearningEngine
from core.swarm_control import DRONE_NAMES
from core.webhooks import send_error_webhook_log, send_ops_webhook_log, send_webhook_log

logger = logging.getLogger("Autonomy")


BOT_SCHEMAS = {
    drone: {
        "schema": f"discord_music_{drone}",
        "queue": f"{drone}_queue",
        "backup": f"{drone}_queue_backup",
        "playback": f"{drone}_playback_state",
        "home": f"{drone}_bot_home_channels",
        "direct": f"{drone}_swarm_direct_orders",
        "override": f"{drone}_swarm_overrides",
        "errors": f"{drone}_error_events",
    }
    for drone in DRONE_NAMES
}


@dataclass
class RepairResult:
    success: bool
    action: str
    scope: str
    details: str = ""


def resolve_bot_from_ctx(ctx):
    bot = getattr(ctx, "bot", None) or getattr(ctx, "client", None)
    if bot is not None:
        return bot
    state = getattr(ctx, "_state", None)
    getter = getattr(state, "_get_client", None)
    if callable(getter):
        return getter()
    return None


class AutonomousEngine:
    def __init__(self, bot):
        self.bot = bot
        self.enabled = True
        self.learning = LearningEngine()
        self._cooldowns: dict[str, float] = {}
        self._cooldown_seconds = 45.0
        self._diagnostic_cache: dict[str, dict[str, Any]] = {}

        self._max_repair_attempts = 3
        self._repair_followup_delay = 6.0
        self._infra_followup_delay = 15.0
        self._recover_guard_seconds = max(60, int(os.getenv('ARIA_RECOVERY_GUARD_SECONDS', '300') or '300'))
        self._queue_rebuild_guard_seconds = max(30, int(os.getenv('ARIA_QUEUE_REBUILD_GUARD_SECONDS', '180') or '180'))
        self._state_normalize_guard_seconds = max(30, int(os.getenv('ARIA_STATE_GUARD_SECONDS', '120') or '120'))
        self._infra_timeout_seconds = max(15, int(os.getenv('ARIA_INFRA_TIMEOUT_SECONDS', '45') or '45'))
        self._infra_enabled = str(os.getenv('ARIA_ENABLE_INFRA_CONTROL', '0')).strip().lower() in {'1', 'true', 'yes', 'on'}
        self._infra_allow_execute = str(os.getenv('ARIA_ALLOW_INFRA_EXEC', '0')).strip().lower() in {'1', 'true', 'yes', 'on'}

    async def ensure_repair_tables(self) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_repair_tasks (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        symptom_signature VARCHAR(255) NOT NULL,
                        issue_type VARCHAR(64) NOT NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        strategy_index INT NOT NULL DEFAULT 0,
                        attempt_count INT NOT NULL DEFAULT 0,
                        max_attempts INT NOT NULL DEFAULT 3,
                        status VARCHAR(24) NOT NULL DEFAULT 'pending',
                        due_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        issue_json LONGTEXT NOT NULL,
                        last_result TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY uniq_open_task (symptom_signature, status)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_operator_decisions (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        issue_type VARCHAR(64) NOT NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        priority_score DOUBLE NOT NULL DEFAULT 0,
                        urgency_label VARCHAR(32) NOT NULL DEFAULT 'normal',
                        details_json LONGTEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_created (created_at),
                        INDEX idx_bot_guild (bot_name, guild_id)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_infra_tasks (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        target_name VARCHAR(64) NOT NULL,
                        action_name VARCHAR(64) NOT NULL,
                        issue_type VARCHAR(64) NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        status VARCHAR(24) NOT NULL DEFAULT 'pending',
                        priority_score DOUBLE NOT NULL DEFAULT 0,
                        command_text TEXT NULL,
                        reason_text TEXT NULL,
                        issue_json LONGTEXT NULL,
                        due_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        attempt_count INT NOT NULL DEFAULT 0,
                        max_attempts INT NOT NULL DEFAULT 2,
                        last_result TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_status_due (status, due_at),
                        INDEX idx_target_status (target_name, status)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_infra_history (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        target_name VARCHAR(64) NOT NULL,
                        action_name VARCHAR(64) NOT NULL,
                        issue_type VARCHAR(64) NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        success BOOLEAN NOT NULL DEFAULT FALSE,
                        execution_mode VARCHAR(24) NOT NULL DEFAULT 'planned',
                        command_text TEXT NULL,
                        result_text TEXT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_target_created (target_name, created_at)
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_repair_guards (
                        guard_key VARCHAR(255) PRIMARY KEY,
                        guard_scope VARCHAR(96) NOT NULL,
                        action_name VARCHAR(64) NOT NULL,
                        issue_type VARCHAR(64) NOT NULL,
                        bot_name VARCHAR(64) NULL,
                        guild_id BIGINT NULL,
                        details_json LONGTEXT NULL,
                        last_triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        INDEX idx_scope_time (guard_scope, last_triggered_at)
                    )
                    """
                )

    def _issue_priority_score(self, issue: dict[str, Any]) -> float:
        issue_type = str(issue.get('type') or 'unknown')
        base = {
            'voice_disconnect': 0.98,
            'recover_from_queue': 0.95,
            'queue_rebuild_needed': 0.92,
            'predictive_stall_risk': 0.88,
            'stalled_playback_candidate': 0.84,
            'invalid_playback_state': 0.82,
            'invalid_position': 0.72,
            'stale_orders': 0.63,
            'predictive_queue_drift': 0.58,
            'health_trending_down': 0.56,
            'drone_health_outlier': 0.55,
            'guild_hotspot': 0.54,
            'drone_outlier': 0.45,
            'stale_swarm_node': 0.40,
            'missing_recovery_anchor': 0.28,
            'missing_automation_channel': 0.18,
        }.get(issue_type, 0.35)
        queue_count = int(issue.get('queue_count') or 0)
        backup_count = int(issue.get('backup_count') or 0)
        if issue.get('current_track'):
            base += 0.08
        if queue_count > 0:
            base += min(0.10, queue_count * 0.01)
        if backup_count > queue_count:
            base += min(0.08, (backup_count - queue_count) * 0.01)
        if issue.get('home_vc_id'):
            base += 0.05
        if issue.get('predictive_pressure'):
            try:
                base += min(0.10, float(issue.get('predictive_pressure') or 0) * 0.10)
            except Exception:
                pass
        if issue.get('issue_count') and issue.get('baseline_issue_count'):
            try:
                ratio = float(issue['issue_count']) / max(float(issue['baseline_issue_count']), 1.0)
                base += min(0.12, max(0.0, ratio - 1.0) * 0.08)
            except Exception:
                pass
        return max(0.0, min(1.0, base))

    def _urgency_label(self, score: float) -> str:
        if score >= 0.90:
            return 'critical'
        if score >= 0.72:
            return 'high'
        if score >= 0.48:
            return 'normal'
        return 'low'

    async def _record_operator_decision(self, issue: dict[str, Any]) -> None:
        if not db.pool:
            return
        await self.ensure_repair_tables()
        score = float(issue.get('_priority_score') if issue.get('_priority_score') is not None else self._issue_priority_score(issue))
        urgency = self._urgency_label(score)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_operator_decisions (issue_type, bot_name, guild_id, priority_score, urgency_label, details_json)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        str(issue.get('type') or 'unknown')[:64],
                        str(issue.get('drone') or '')[:64] or None,
                        issue.get('guild_id'),
                        score,
                        urgency[:32],
                        json.dumps(issue, default=str),
                    ),
                )

    def _rank_issues(self, issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = []
        seen: set[tuple[Any, ...]] = set()
        for issue in issues:
            key = (issue.get('type'), issue.get('drone'), issue.get('guild_id'))
            if key in seen:
                continue
            seen.add(key)
            score = self._issue_priority_score(issue)
            ranked.append({**issue, '_priority_score': score, '_urgency': self._urgency_label(score)})
        ranked.sort(key=lambda item: (item.get('_priority_score', 0.0), int(item.get('queue_count') or 0), int(item.get('backup_count') or 0)), reverse=True)
        return ranked

    async def _detect_health_outliers(self, cur) -> list[dict[str, Any]]:
        issues: list[dict[str, Any]] = []
        try:
            await cur.execute(
                """
                SELECT bot_name, guild_id, health_score, status_label
                FROM aria_swarm_health
                """
            )
            rows = await cur.fetchall() or []
        except Exception:
            return issues
        bot_scores: dict[str, list[float]] = {}
        guild_bad_counts: dict[int, int] = {}
        total_by_guild: dict[int, int] = {}
        for row in rows:
            bot = row.get('bot_name') if isinstance(row, dict) else row[0]
            gid = row.get('guild_id') if isinstance(row, dict) else row[1]
            score = float((row.get('health_score') if isinstance(row, dict) else row[2]) or 0)
            status = (row.get('status_label') if isinstance(row, dict) else row[3]) or 'unknown'
            bot_scores.setdefault(str(bot), []).append(score)
            if gid is not None:
                total_by_guild[int(gid)] = total_by_guild.get(int(gid), 0) + 1
                if status in {'degraded', 'critical'} or score < 0.45:
                    guild_bad_counts[int(gid)] = guild_bad_counts.get(int(gid), 0) + 1
        for bot, scores in bot_scores.items():
            if len(scores) < 2:
                continue
            mean = sum(scores) / len(scores)
            worst = min(scores)
            if worst <= max(0.30, mean - 0.30):
                issues.append({'type': 'drone_health_outlier', 'drone': bot, 'health_mean': round(mean, 4), 'health_floor': round(worst, 4)})
        for gid, bad in guild_bad_counts.items():
            total = max(total_by_guild.get(gid, 1), 1)
            if bad >= 2 and bad / total >= 0.5:
                issues.append({'type': 'guild_hotspot', 'guild_id': gid, 'bad_bot_count': bad, 'bot_count': total})
        return issues

    async def _choose_action_plan(self, issue: dict[str, Any]) -> list[str]:
        issue_type = issue.get("type")
        base_plans = {
            "recover_from_queue": ["recover_resume", "clear_stale_orders", "queue_rebuild", "recover_resume"],
            "stalled_playback_candidate": ["normalize_playback_state", "queue_rebuild", "recover_resume"],
            "queue_rebuild_needed": ["queue_rebuild", "recover_resume"],
            "invalid_playback_state": ["normalize_playback_state", "queue_rebuild", "recover_resume"],
            "invalid_position": ["normalize_playback_state", "recover_resume"],
            "stale_orders": ["clear_stale_orders", "recover_resume"],
            "predictive_queue_drift": ["predictive_queue_rebalance", "queue_rebuild"],
            "predictive_stall_risk": ["normalize_playback_state", "recover_resume", "queue_rebuild"],
            "drone_health_outlier": ["outlier_report"],
            "guild_hotspot": ["hotspot_report"],
        }
        plan = list(base_plans.get(issue_type, [self._default_action_for_issue(issue_type)]))
        hints = []
        try:
            hints = await self.learning.action_success_hints(
                symptom_signature=self._signature(issue),
                repair_scope=self._scoped_repair_scope(issue),
                limit=6,
            )
            hints.extend(await self.learning.action_success_hints(symptom_signature=self._signature(issue), limit=6))
        except Exception:
            hints = []
        ranked = []
        known_actions = [
            "normalize_playback_state", "queue_rebuild", "clear_stale_orders", "recover_resume", "predictive_queue_rebalance"
        ]
        for hint in hints:
            for action in known_actions:
                if f"-> {action} " in hint or hint.endswith(f"-> {action}"):
                    ranked.append(action)
        merged = []
        for action in ranked + plan:
            if action and action not in merged:
                merged.append(action)
        return merged or plan

    def _default_action_for_issue(self, issue_type: str | None) -> str:
        return {
            "queue_rebuild_needed": "queue_rebuild",
            "recover_from_queue": "recover_resume",
            "invalid_playback_state": "normalize_playback_state",
            "invalid_position": "normalize_playback_state",
            "stale_orders": "clear_stale_orders",
            "stalled_playback_candidate": "recover_resume",
            "predictive_queue_drift": "predictive_queue_rebalance",
            "predictive_stall_risk": "recover_resume",
            "drone_health_outlier": "outlier_report",
            "guild_hotspot": "hotspot_report",
        }.get(issue_type or "", "observe")

    def _infra_target_for_issue(self, issue: dict[str, Any]) -> str | None:
        drone = str(issue.get("drone") or "").strip().lower()
        issue_type = str(issue.get("type") or "")
        if drone in BOT_SCHEMAS and issue_type != "guild_hotspot":
            return drone
        if issue_type in {"stale_swarm_node", "drone_outlier", "drone_health_outlier", "guild_hotspot"}:
            return "lavalink"
        return None

    def _infra_command_for_target(self, target: str) -> str | None:
        normalized = str(target or "").strip().upper().replace("-", "_")
        candidates = [f"ARIA_RESTART_{normalized}_CMD", f"ARIA_INFRA_{normalized}_CMD"]
        if normalized in {"SWARMPANEL", "PANEL"}:
            candidates.extend(["ARIA_RESTART_PANEL_CMD", "ARIA_INFRA_PANEL_CMD"])
        if normalized == "LAVALINK":
            candidates.extend(["ARIA_RESTART_LAVALINK_CMD", "ARIA_INFRA_LAVALINK_CMD"])
        for name in candidates:
            value = str(os.getenv(name, "") or "").strip()
            if value:
                return value
        return None

    async def _record_infra_history(self, *, target: str, action: str, issue: dict[str, Any], success: bool, execution_mode: str, command_text: str | None, result_text: str | None) -> None:
        if not db.pool:
            return
        await self.ensure_repair_tables()
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_infra_history (target_name, action_name, issue_type, bot_name, guild_id, success, execution_mode, command_text, result_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        target[:64],
                        action[:64],
                        str(issue.get("type") or "unknown")[:64],
                        (str(issue.get("drone") or "")[:64] or None),
                        issue.get("guild_id"),
                        1 if success else 0,
                        execution_mode[:24],
                        (command_text or "")[:4000] or None,
                        (result_text or "")[:4000] or None,
                    ),
                )

    async def _queue_infra_task(self, issue: dict[str, Any], *, target: str, action: str, reason: str) -> bool:
        if not db.pool:
            return False
        await self.ensure_repair_tables()
        command_text = self._infra_command_for_target(target)
        priority = float(issue.get("_priority_score") if issue.get("_priority_score") is not None else self._issue_priority_score(issue))
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_infra_tasks (target_name, action_name, issue_type, bot_name, guild_id, status, priority_score, command_text, reason_text, issue_json, due_at, max_attempts, last_result)
                    VALUES (%s, %s, %s, %s, %s, 'pending', %s, %s, %s, %s, DATE_ADD(NOW(), INTERVAL %s SECOND), %s, %s)
                    """,
                    (
                        target[:64],
                        action[:64],
                        str(issue.get("type") or "unknown")[:64],
                        (str(issue.get("drone") or "")[:64] or None),
                        issue.get("guild_id"),
                        priority,
                        command_text,
                        reason[:4000],
                        json.dumps(issue, default=str),
                        int(self._infra_followup_delay),
                        2,
                        "queued by medic escalation",
                    ),
                )
        await self._record_infra_history(target=target, action=action, issue=issue, success=False, execution_mode="planned", command_text=command_text, result_text=reason)
        try:
            await send_ops_webhook_log("Aria Infra Escalation", f"Queued {action} for {target}.", fields=[("Reason", reason[:512], False), ("Issue", str(issue.get('type') or 'unknown')[:128], True)])
        except Exception:
            pass
        return True

    def _should_escalate_infra(self, issue: dict[str, Any], *, attempts: int = 0) -> bool:
        issue_type = str(issue.get("type") or "")
        priority = float(issue.get("_priority_score") if issue.get("_priority_score") is not None else self._issue_priority_score(issue))
        if issue_type in {"stale_swarm_node", "drone_health_outlier", "drone_outlier", "guild_hotspot"}:
            return True
        if attempts >= max(1, self._max_repair_attempts - 1) and priority >= 0.78:
            return True
        if issue_type in {"recover_from_queue", "stalled_playback_candidate", "predictive_stall_risk"} and priority >= 0.92:
            return True
        return False

    async def _execute_infra_task(self, task: dict[str, Any]) -> tuple[bool, str, str]:
        command_text = str(task.get("command_text") or self._infra_command_for_target(task.get("target_name")) or "").strip()
        if not self._infra_enabled:
            return False, "disabled", "infrastructure control is disabled"
        if not command_text:
            return False, "manual", "no configured command for target"
        if not self._infra_allow_execute:
            return False, "planned", f"command prepared but execution disabled: {command_text}"
        try:
            proc = await asyncio.create_subprocess_shell(command_text, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self._infra_timeout_seconds)
            out = (stdout or b"").decode("utf-8", "ignore").strip()
            err = (stderr or b"").decode("utf-8", "ignore").strip()
            combined = ("\n".join([p for p in [out, err] if p]) or f"exit={proc.returncode}").strip()
            return proc.returncode == 0, "executed", combined[:4000]
        except asyncio.TimeoutError:
            return False, "executed", f"command timed out after {self._infra_timeout_seconds}s"
        except Exception as exc:
            return False, "executed", f"{type(exc).__name__}: {exc}"

    async def run_pending_infra_tasks(self) -> None:
        if not db.pool:
            return
        await self.ensure_repair_tables()
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    "SELECT id, target_name, action_name, issue_type, bot_name, guild_id, command_text, reason_text, issue_json, attempt_count, max_attempts FROM aria_infra_tasks WHERE status='pending' AND due_at <= NOW() ORDER BY priority_score DESC, due_at ASC LIMIT 10"
                )
                tasks = await cur.fetchall() or []
        for task in tasks:
            try:
                issue = json.loads(task.get("issue_json") or "{}")
            except Exception:
                issue = {"type": task.get("issue_type"), "drone": task.get("bot_name"), "guild_id": task.get("guild_id")}
            success, mode, result_text = await self._execute_infra_task(task)
            new_attempts = int(task.get("attempt_count") or 0) + 1
            max_attempts = int(task.get("max_attempts") or 2)
            status = "resolved" if success else ("manual_needed" if mode in {"manual", "planned", "disabled"} else ("failed" if new_attempts >= max_attempts else "pending"))
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if status == "pending":
                        await cur.execute(
                            "UPDATE aria_infra_tasks SET attempt_count=%s, due_at=DATE_ADD(NOW(), INTERVAL %s SECOND), last_result=%s WHERE id=%s",
                            (new_attempts, int(self._infra_followup_delay), result_text[:4000], task["id"])
                        )
                    else:
                        await cur.execute(
                            "UPDATE aria_infra_tasks SET status=%s, attempt_count=%s, last_result=%s WHERE id=%s",
                            (status, new_attempts, result_text[:4000], task["id"])
                        )
            await self._record_infra_history(target=str(task.get("target_name") or "unknown"), action=str(task.get("action_name") or "restart"), issue=issue, success=success, execution_mode=mode, command_text=str(task.get("command_text") or "") or None, result_text=result_text)
            try:
                if success:
                    await send_ops_webhook_log("Aria Infra Action", f"Executed {task.get('action_name')} for {task.get('target_name')}.", fields=[("Result", result_text[:512], False)])
                elif status == "manual_needed":
                    await send_ops_webhook_log("Aria Infra Manual Action Needed", f"Prepared {task.get('action_name')} for {task.get('target_name')}, but manual follow-through is needed.", fields=[("Reason", result_text[:512], False)])
                elif status == "failed":
                    await send_error_webhook_log("Aria Infra Failure", result_text[:512] or "unknown infra failure", traceback_text=None)
            except Exception:
                pass

    async def _schedule_repair_followup(self, issue: dict[str, Any], *, strategy_index: int, attempt_count: int, last_result: str = "") -> None:
        if not db.pool:
            return
        await self.ensure_repair_tables()
        symptom_signature = self._signature(issue)
        issue_json = json.dumps(issue, default=str)
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO aria_repair_tasks
                    (symptom_signature, issue_type, bot_name, guild_id, strategy_index, attempt_count, max_attempts, status, due_at, issue_json, last_result)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', DATE_ADD(NOW(), INTERVAL %s SECOND), %s, %s)
                    ON DUPLICATE KEY UPDATE
                        strategy_index=VALUES(strategy_index),
                        attempt_count=VALUES(attempt_count),
                        max_attempts=VALUES(max_attempts),
                        due_at=VALUES(due_at),
                        issue_json=VALUES(issue_json),
                        last_result=VALUES(last_result),
                        status='pending'
                    """,
                    (
                        symptom_signature,
                        str(issue.get('type', 'unknown'))[:64],
                        str(issue.get('drone') or '')[:64] or None,
                        issue.get('guild_id'),
                        strategy_index,
                        attempt_count,
                        self._max_repair_attempts,
                        int(self._repair_followup_delay),
                        issue_json,
                        last_result[:4000] if last_result else None,
                    ),
                )

    async def run_pending_repairs(self) -> None:
        if not db.pool:
            return
        await self.ensure_repair_tables()
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    "SELECT id, symptom_signature, issue_type, strategy_index, attempt_count, max_attempts, issue_json, last_result FROM aria_repair_tasks WHERE status='pending' AND due_at <= NOW() ORDER BY due_at ASC LIMIT 20"
                )
                tasks = await cur.fetchall() or []
        for task in tasks:
            try:
                issue = json.loads(task.get('issue_json') or '{}')
            except Exception:
                issue = {}
            if not issue:
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("UPDATE aria_repair_tasks SET status='failed', last_result=%s WHERE id=%s", ('invalid issue payload', task['id']))
                continue
            resolved = False
            async with db.pool.acquire() as conn:
                async with conn.cursor(self._dict_cursor()) as cur:
                    try:
                        resolved = await self._verify_resolution(cur, issue)
                    except Exception:
                        resolved = False
            if resolved:
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("UPDATE aria_repair_tasks SET status='resolved', last_result=%s WHERE id=%s", ('verified by follow-up', task['id']))
                await self._journal_repair(issue, RepairResult(True, 'followup_verified', issue.get('drone', 'swarm'), details='repair verified on follow-up'))
                continue
            next_attempt = int(task.get('attempt_count') or 0) + 1
            max_attempts = int(task.get('max_attempts') or self._max_repair_attempts)
            if next_attempt >= max_attempts:
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("UPDATE aria_repair_tasks SET status='failed', attempt_count=%s, last_result=%s WHERE id=%s", (next_attempt, 'repair plan exhausted', task['id']))
                await self._journal_repair(issue, RepairResult(False, 'repair_plan_exhausted', issue.get('drone', 'swarm'), details='repair plan exhausted after follow-up retries'), error='repair_plan_exhausted')
                if self._should_escalate_infra(issue, attempts=next_attempt):
                    target = self._infra_target_for_issue(issue)
                    if target:
                        await self._queue_infra_task(issue, target=target, action='restart', reason='repair plan exhausted after follow-up retries')
                continue
            next_strategy_index = int(task.get('strategy_index') or 0) + 1
            success = await self.fix_issue({**issue, '_strategy_index': next_strategy_index, '_attempt_count': next_attempt, '_from_followup': True})
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE aria_repair_tasks SET status=%s, attempt_count=%s, strategy_index=%s, due_at=DATE_ADD(NOW(), INTERVAL %s SECOND), last_result=%s WHERE id=%s",
                        ('pending', next_attempt, next_strategy_index, int(self._repair_followup_delay), 'next repair step executed' if success else 'follow-up repair did not verify; retrying next strategy', task['id'])
                    )

    @staticmethod
    def _dict_cursor():
        return aiomysql.DictCursor if aiomysql else None

    def _signature(self, issue: dict[str, Any]) -> str:
        keys = [issue.get("type", "unknown"), str(issue.get("drone", "swarm")), str(issue.get("guild_id", 0))]
        return "|".join(keys)[:255]

    def _cooldown_key(self, issue: dict[str, Any], action: str) -> str:
        return f"{self._signature(issue)}::{action}"

    def _cooldown_ready(self, issue: dict[str, Any], action: str) -> bool:
        key = self._cooldown_key(issue, action)
        now = time.time()
        last = self._cooldowns.get(key, 0.0)
        if now - last < self._cooldown_seconds:
            return False
        self._cooldowns[key] = now
        return True

    def _scoped_repair_scope(self, issue: dict[str, Any]) -> str:
        drone = issue.get("drone", "swarm")
        guild_id = issue.get("guild_id", "global")
        return f"{drone}:{guild_id}"[:64]

    def _guard_window_seconds(self, issue: dict[str, Any], strategy: str) -> int:
        if strategy == "recover_resume" or str(issue.get("type") or "") in {"recover_from_queue", "stalled_playback_candidate", "predictive_stall_risk"}:
            return self._recover_guard_seconds
        if strategy in {"queue_rebuild", "predictive_queue_rebalance"} or str(issue.get("type") or "") == "queue_rebuild_needed":
            return self._queue_rebuild_guard_seconds
        if strategy in {"normalize_playback_state", "clear_stale_orders"}:
            return self._state_normalize_guard_seconds
        return 0

    async def _recent_direct_order_exists(self, issue: dict[str, Any], command: str, within_seconds: int) -> bool:
        drone = str(issue.get("drone") or "").strip().lower()
        guild_id = issue.get("guild_id")
        if drone not in BOT_SCHEMAS or not guild_id or within_seconds <= 0 or not db.pool:
            return False
        cfg = BOT_SCHEMAS[drone]
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor(self._dict_cursor()) as cur:
                    await cur.execute(
                        f"""
                        SELECT COUNT(*) AS c
                        FROM {cfg['schema']}.{cfg['direct']}
                        WHERE bot_name=%s
                          AND guild_id=%s
                          AND command=%s
                          AND TIMESTAMPDIFF(SECOND, COALESCE(created_at, NOW()), NOW()) <= %s
                        """,
                        (drone, guild_id, command.upper(), int(within_seconds)),
                    )
                    row = await cur.fetchone()
        except Exception:
            return False
        return int((row or {}).get("c", 0)) > 0

    async def _consume_repair_guard(self, issue: dict[str, Any], strategy: str, *, window_seconds: int) -> tuple[bool, str]:
        if not db.pool or window_seconds <= 0:
            return True, ""
        await self.ensure_repair_tables()
        guard_key = self._cooldown_key(issue, strategy)
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                await cur.execute(
                    """
                    SELECT TIMESTAMPDIFF(SECOND, last_triggered_at, NOW()) AS age_seconds
                    FROM aria_repair_guards
                    WHERE guard_key=%s
                    LIMIT 1
                    """,
                    (guard_key,),
                )
                row = await cur.fetchone()
                age_seconds = int((row or {}).get("age_seconds", window_seconds + 1) or window_seconds + 1)
                if row and age_seconds < int(window_seconds):
                    remaining = max(1, int(window_seconds) - age_seconds)
                    return False, f"{strategy} is guarded for another {remaining}s"
                await cur.execute(
                    """
                    INSERT INTO aria_repair_guards (guard_key, guard_scope, action_name, issue_type, bot_name, guild_id, details_json, last_triggered_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        guard_scope=VALUES(guard_scope),
                        action_name=VALUES(action_name),
                        issue_type=VALUES(issue_type),
                        bot_name=VALUES(bot_name),
                        guild_id=VALUES(guild_id),
                        details_json=VALUES(details_json),
                        last_triggered_at=NOW()
                    """,
                    (
                        guard_key,
                        self._scoped_repair_scope(issue)[:96],
                        strategy[:64],
                        str(issue.get("type") or "unknown")[:64],
                        (str(issue.get("drone") or "")[:64] or None),
                        issue.get("guild_id"),
                        json.dumps(issue, default=str)[:4000],
                    ),
                )
        return True, ""

    def _policy_scope_keys(self, issue: dict[str, Any]) -> list[str]:
        drone = str(issue.get("drone", "swarm"))
        guild_id = str(issue.get("guild_id", "global"))
        return [f"{drone}:{guild_id}"[:96], f"bot:{drone}"[:96], "global"]

    async def _policy_profile(self, issue: dict[str, Any]) -> dict[str, Any]:
        profile = {"confidence_bias": 0.0, "preferred_cooldown_seconds": self._cooldown_seconds}
        issue_type = str(issue.get("type") or "unknown")
        for scope_key in self._policy_scope_keys(issue):
            try:
                hint = await self.learning.get_policy_hint(scope_key, issue_type)
            except Exception:
                hint = None
            if not hint:
                continue
            try:
                profile["confidence_bias"] += float(hint.get("confidence_bias") or 0.0)
            except Exception:
                pass
            try:
                cooldown = float(hint.get("preferred_cooldown_seconds") or self._cooldown_seconds)
                profile["preferred_cooldown_seconds"] = min(profile["preferred_cooldown_seconds"], cooldown)
            except Exception:
                pass
        profile["confidence_bias"] = max(-0.25, min(0.25, profile["confidence_bias"]))
        return profile

    async def _record_predictive_signal(self, issue: dict[str, Any], signal_type: str, strength: float, details: dict[str, Any] | None = None) -> None:
        scope_key = self._scoped_repair_scope(issue)
        try:
            await self.learning.record_predictive_signal(
                scope_key=scope_key,
                signal_type=signal_type,
                signal_strength=strength,
                details_json=json.dumps(details or issue, default=str)[:4000],
            )
        except Exception:
            logger.exception("Failed to record predictive signal")

    async def _predictive_pressure(self, issue: dict[str, Any], signal_type: str) -> float:
        scope_key = self._scoped_repair_scope(issue)
        try:
            recent = await self.learning.recent_predictive_signals(scope_key=scope_key, signal_type=signal_type, limit=5)
        except Exception:
            recent = []
        if not recent:
            return 0.0
        strengths = [max(0.0, float(row.get("signal_strength") or 0.0)) for row in recent]
        return min(1.0, sum(strengths) / max(len(strengths), 1))

    def _score_confidence(self, issue: dict[str, Any]) -> float:
        score = 0.35 + float(issue.get("policy_confidence_bias", 0.0) or 0.0)
        issue_type = issue.get("type")
        if issue_type in {"invalid_playback_state", "invalid_position", "stale_orders"}:
            score += 0.35
        if issue_type in {"queue_rebuild_needed", "recover_from_queue", "stalled_playback_candidate", "predictive_stall_risk"}:
            score += 0.28
        if issue_type in {"stale_swarm_node", "drone_outlier"}:
            score += 0.12
        if issue.get("guild_id"):
            score += 0.05
        if issue.get("home_vc_id"):
            score += 0.08
        if issue.get("queue_count", 0) > 0:
            score += 0.08
        if issue.get("backup_count", 0) > 0:
            score += 0.06
        if issue.get("current_track"):
            score += 0.1
        if issue.get("updated_stale"):
            score += 0.08
        if issue.get("predictive_pressure"):
            score += min(0.18, float(issue.get("predictive_pressure") or 0.0) * 0.25)
        return max(0.0, min(0.98, score))

    def _repair_plan(self, issue: dict[str, Any], action: str) -> list[str]:
        issue_type = issue.get("type")
        if issue_type == "recover_from_queue":
            return ["normalize_state", "queue_rebuild", "recover", "resume", "verify"]
        if issue_type == "queue_rebuild_needed":
            return ["queue_rebuild", "verify"]
        if issue_type in {"invalid_playback_state", "invalid_position", "stalled_playback_candidate", "predictive_stall_risk"}:
            return ["normalize_state", "recover", "verify"]
        if issue_type == "stale_orders":
            return ["clear_stale_orders", "verify"]
        return [action, "verify"]

    async def _journal_repair(self, issue: dict[str, Any], result: RepairResult, error: str | None = None) -> None:
        if not db.pool:
            return
        symptom_signature = self._signature(issue)
        meta = json.dumps({"issue": issue, "details": result.details}, default=str)[:8000]
        await self.learning.remember_repair_outcome(
            symptom_signature=symptom_signature,
            repair_action=result.action,
            repair_scope=result.scope,
            success=result.success,
            last_error=error,
            meta_json=meta,
        )
        await self.learning.remember_repair_outcome(
            symptom_signature=symptom_signature,
            repair_action=result.action,
            repair_scope=self._scoped_repair_scope(issue),
            success=result.success,
            last_error=error,
            meta_json=meta,
        )
        try:
            await self.learning.update_policy_hint(
                scope_key=self._scoped_repair_scope(issue),
                issue_type=str(issue.get("type", "unknown")),
                success=result.success,
                confidence=self._score_confidence(issue),
                notes=(result.details or error or "")[:2000] or None,
            )
            if issue.get("drone"):
                await self.learning.update_policy_hint(
                    scope_key=f"bot:{issue.get('drone')}"[:96],
                    issue_type=str(issue.get("type", "unknown")),
                    success=result.success,
                    confidence=self._score_confidence(issue),
                    notes=(result.details or error or "")[:2000] or None,
                )
        except Exception:
            logger.exception("Failed to update policy hint")
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS aria_repair_journal (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            symptom_signature VARCHAR(255) NOT NULL,
                            issue_type VARCHAR(64) NOT NULL,
                            repair_action VARCHAR(128) NOT NULL,
                            repair_scope VARCHAR(64) NOT NULL,
                            success BOOLEAN NOT NULL DEFAULT FALSE,
                            confidence DOUBLE NOT NULL DEFAULT 0,
                            repair_plan TEXT NULL,
                            details TEXT NULL,
                            error_text TEXT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    await cur.execute(
                        """
                        INSERT INTO aria_repair_journal
                        (symptom_signature, issue_type, repair_action, repair_scope, success, confidence, repair_plan, details, error_text)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            symptom_signature,
                            issue.get("type", "unknown")[:64],
                            result.action[:128],
                            result.scope[:64],
                            1 if result.success else 0,
                            self._score_confidence(issue),
                            json.dumps(self._repair_plan(issue, result.action), default=str)[:2000],
                            result.details[:4000] if result.details else None,
                            error[:4000] if error else None,
                        ),
                    )
        except Exception:
            logger.exception("Failed to journal repair")

    async def ensure_bot_schema(self, cur, drone: str) -> None:
        cfg = BOT_SCHEMAS[drone]
        schema = cfg["schema"]
        await cur.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['queue']} (guild_id BIGINT, position INT, track_data LONGTEXT, requested_by BIGINT NULL, PRIMARY KEY (guild_id, position))"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['backup']} (guild_id BIGINT, position INT, track_data LONGTEXT, requested_by BIGINT NULL, PRIMARY KEY (guild_id, position))"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['playback']} (guild_id BIGINT PRIMARY KEY, channel_id BIGINT NULL, text_channel_id BIGINT NULL, current_track LONGTEXT NULL, position_seconds DOUBLE DEFAULT 0, is_playing BOOLEAN DEFAULT FALSE, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['home']} (guild_id BIGINT PRIMARY KEY, home_vc_id BIGINT NULL)"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['direct']} (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data LONGTEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
        )
        await cur.execute(
            f"CREATE TABLE IF NOT EXISTS {schema}.{cfg['override']} (guild_id BIGINT, bot_name VARCHAR(50), command VARCHAR(20), PRIMARY KEY(guild_id, bot_name))"
        )
        # Compatibility helpers for legacy music-bot schemas.
        for stmt in [
            f"ALTER TABLE {schema}.{cfg['playback']} ADD COLUMN text_channel_id BIGINT NULL",
            f"ALTER TABLE {schema}.{cfg['playback']} ADD COLUMN current_track LONGTEXT NULL",
            f"ALTER TABLE {schema}.{cfg['playback']} ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            f"ALTER TABLE {schema}.{cfg['queue']} ADD COLUMN position INT NULL",
            f"ALTER TABLE {schema}.{cfg['queue']} ADD COLUMN track_data LONGTEXT NULL",
            f"ALTER TABLE {schema}.{cfg['queue']} ADD COLUMN requested_by BIGINT NULL",
            f"ALTER TABLE {schema}.{cfg['backup']} ADD COLUMN position INT NULL",
            f"ALTER TABLE {schema}.{cfg['backup']} ADD COLUMN track_data LONGTEXT NULL",
            f"ALTER TABLE {schema}.{cfg['backup']} ADD COLUMN requested_by BIGINT NULL",
            f"ALTER TABLE {schema}.{cfg['home']} ADD COLUMN bot_name VARCHAR(50) NULL",
            f"ALTER TABLE {schema}.{cfg['direct']} ADD COLUMN attempts INT NOT NULL DEFAULT 0",
            f"ALTER TABLE {schema}.{cfg['direct']} ADD COLUMN last_error TEXT NULL",
            f"ALTER TABLE {schema}.{cfg['direct']} ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            f"ALTER TABLE {schema}.{cfg['override']} ADD COLUMN attempts INT NOT NULL DEFAULT 0",
            f"ALTER TABLE {schema}.{cfg['override']} ADD COLUMN last_error TEXT NULL",
        ]:
            try:
                await cur.execute(stmt)
            except Exception:
                pass
        for stmt in [
            f"UPDATE {schema}.{cfg['playback']} SET current_track = COALESCE(NULLIF(current_track, ''), NULLIF(video_url, ''), NULLIF(title, '')) WHERE current_track IS NULL OR current_track = ''",
            f"UPDATE {schema}.{cfg['queue']} SET requested_by = COALESCE(requested_by, requester_id) WHERE requested_by IS NULL",
            f"UPDATE {schema}.{cfg['queue']} SET track_data = COALESCE(NULLIF(track_data, ''), NULLIF(video_url, ''), NULLIF(title, '')) WHERE track_data IS NULL OR track_data = ''",
            f"UPDATE {schema}.{cfg['queue']} SET position = id WHERE position IS NULL AND id IS NOT NULL",
            f"UPDATE {schema}.{cfg['backup']} SET requested_by = COALESCE(requested_by, requester_id) WHERE requested_by IS NULL",
            f"UPDATE {schema}.{cfg['backup']} SET track_data = COALESCE(NULLIF(track_data, ''), NULLIF(video_url, ''), NULLIF(title, '')) WHERE track_data IS NULL OR track_data = ''",
            f"UPDATE {schema}.{cfg['backup']} SET position = id WHERE position IS NULL AND id IS NOT NULL",
            f"UPDATE {schema}.{cfg['home']} SET bot_name = COALESCE(bot_name, '{drone}') WHERE bot_name IS NULL OR bot_name = ''",
        ]:
            try:
                await cur.execute(stmt)
            except Exception:
                pass
        try:
            await cur.execute(f"ALTER TABLE {schema}.{cfg['queue']} ADD INDEX idx_guild_position (guild_id, position)")
        except Exception:
            pass
        try:
            await cur.execute(f"ALTER TABLE {schema}.{cfg['backup']} ADD INDEX idx_guild_position (guild_id, position)")
        except Exception:
            pass

    async def _fetchone(self, cur, query: str, params=()):
        await cur.execute(query, params)
        return await cur.fetchone()

    async def _fetchall(self, cur, query: str, params=()):
        await cur.execute(query, params)
        return await cur.fetchall()

    async def _table_columns(self, cur, schema: str, table: str) -> set[str]:
        await cur.execute(
            """
            SELECT COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
            """,
            (schema, table),
        )
        rows = await cur.fetchall() or []
        cols = set()
        for row in rows:
            if isinstance(row, dict):
                cols.add(str(row.get("COLUMN_NAME") or row.get("column_name") or ""))
            else:
                cols.add(str(row[0]))
        return cols

    async def _compat_playback_select(self, cur, drone: str, *, where: str = "", include_updated: bool = True) -> str:
        cfg = BOT_SCHEMAS[drone]
        cols = await self._table_columns(cur, cfg["schema"], cfg["playback"])
        channel_expr = "channel_id" if "channel_id" in cols else "NULL AS channel_id"
        text_expr = "text_channel_id" if "text_channel_id" in cols else "NULL AS text_channel_id"
        pos_expr = "position_seconds" if "position_seconds" in cols else "0 AS position_seconds"
        play_expr = "is_playing" if "is_playing" in cols else "FALSE AS is_playing"
        if "current_track" in cols and "video_url" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(current_track, ''), NULLIF(video_url, ''), NULLIF(title, '')) AS current_track"
        elif "current_track" in cols and "video_url" in cols:
            track_expr = "COALESCE(NULLIF(current_track, ''), NULLIF(video_url, '')) AS current_track"
        elif "current_track" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(current_track, ''), NULLIF(title, '')) AS current_track"
        elif "video_url" in cols and "title" in cols:
            track_expr = "COALESCE(NULLIF(video_url, ''), NULLIF(title, '')) AS current_track"
        elif "video_url" in cols:
            track_expr = "NULLIF(video_url, '') AS current_track"
        elif "title" in cols:
            track_expr = "NULLIF(title, '') AS current_track"
        elif "current_track" in cols:
            track_expr = "NULLIF(current_track, '') AS current_track"
        else:
            track_expr = "NULL AS current_track"
        updated_expr = "updated_at" if (include_updated and "updated_at" in cols) else ("NULL AS updated_at" if include_updated else None)
        selects = ["guild_id", channel_expr, text_expr, track_expr, pos_expr, play_expr]
        if include_updated and updated_expr:
            selects.append(updated_expr)
        select_sql = ", ".join(selects)
        return f"SELECT {select_sql} FROM {cfg['schema']}.{cfg['playback']}" + (f" WHERE {where}" if where else "")

    async def _fetch_playback_row(self, cur, drone: str, guild_id: int) -> dict[str, Any] | None:
        query = await self._compat_playback_select(cur, drone, where="guild_id=%s")
        return await self._fetchone(cur, query, (guild_id,))

    async def _fetch_queue_rows(self, cur, drone: str, table_key: str, guild_id: int):
        cfg = BOT_SCHEMAS[drone]
        table = cfg[table_key]
        cols = await self._table_columns(cur, cfg["schema"], table)
        legacy = "video_url" in cols
        order_col = "position" if "position" in cols else ("id" if "id" in cols else "guild_id")
        if legacy:
            query = f"SELECT id, guild_id, bot_name, video_url, title, requester_id, COALESCE(requested_by, requester_id) AS requested_by, COALESCE(track_data, video_url, title) AS track_data FROM {cfg['schema']}.{table} WHERE guild_id=%s ORDER BY {order_col} ASC"
        else:
            query = f"SELECT guild_id, position, track_data, requested_by FROM {cfg['schema']}.{table} WHERE guild_id=%s ORDER BY {order_col} ASC"
        return (await self._fetchall(cur, query, (guild_id,))) or []

    async def _replace_queue_from_rows(self, cur, drone: str, table_key: str, guild_id: int, rows: list[dict[str, Any]]):
        cfg = BOT_SCHEMAS[drone]
        table = cfg[table_key]
        cols = await self._table_columns(cur, cfg["schema"], table)
        await cur.execute(f"DELETE FROM {cfg['schema']}.{table} WHERE guild_id=%s", (guild_id,))
        legacy = "video_url" in cols
        pos = 1
        for row in rows:
            track = row.get("track_data") or row.get("video_url") or row.get("title")
            requester = row.get("requested_by") if row.get("requested_by") is not None else row.get("requester_id")
            if legacy:
                title = row.get("title") or (str(track)[:250] if track else None)
                await cur.execute(
                    f"INSERT INTO {cfg['schema']}.{table} (guild_id, bot_name, video_url, title, requester_id) VALUES (%s, %s, %s, %s, %s)",
                    (guild_id, drone, track, title, requester),
                )
            else:
                await cur.execute(
                    f"INSERT INTO {cfg['schema']}.{table} (guild_id, position, track_data, requested_by) VALUES (%s, %s, %s, %s)",
                    (guild_id, pos, track, requester),
                )
            pos += 1

    async def detect_automation_issues(self):
        issues = []
        if not db.pool:
            return issues
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                try:
                    await cur.execute("SELECT id, guild_id, channel_id FROM aria_automations")
                    automations = await cur.fetchall()
                except Exception:
                    automations = []
        for automation in automations:
            channel = self.bot.get_channel(automation["channel_id"])
            if channel is None:
                issues.append({"type": "missing_automation_channel", "automation_id": automation["id"], "guild_id": automation["guild_id"]})
                continue
            me = channel.guild.me or channel.guild.get_member(self.bot.user.id)
            if me and not channel.permissions_for(me).send_messages:
                issues.append({"type": "automation_no_permission", "automation_id": automation["id"], "guild_id": automation["guild_id"], "channel_id": automation["channel_id"]})
        return issues

    async def detect_swarm_issues(self):
        issues = []
        if not db.pool:
            return issues
        async with db.pool.acquire() as conn:
            async with conn.cursor(self._dict_cursor()) as cur:
                for drone, cfg in BOT_SCHEMAS.items():
                    try:
                        await self.ensure_bot_schema(cur, drone)
                        stale_nodes = await self._fetchall(
                            cur,
                            f"SELECT bot_name FROM {cfg['schema']}.swarm_health WHERE last_pulse < NOW() - INTERVAL 3 MINUTE",
                        )
                        for row in stale_nodes or []:
                            issues.append({"type": "stale_swarm_node", "drone": row.get("bot_name", drone)})
                    except Exception:
                        pass
                    try:
                        playback_query = await self._compat_playback_select(cur, drone)
                        playback_rows = await self._fetchall(cur, playback_query)
                    except Exception:
                        playback_rows = []
                    for row in playback_rows or []:
                        guild_id = row.get("guild_id")
                        if not guild_id:
                            continue
                        try:
                            queue_count_row = await self._fetchone(cur, f"SELECT COUNT(*) AS c FROM {cfg['schema']}.{cfg['queue']} WHERE guild_id=%s", (guild_id,))
                            backup_count_row = await self._fetchone(cur, f"SELECT COUNT(*) AS c FROM {cfg['schema']}.{cfg['backup']} WHERE guild_id=%s", (guild_id,))
                            home_row = await self._fetchone(cur, f"SELECT home_vc_id FROM {cfg['schema']}.{cfg['home']} WHERE guild_id=%s", (guild_id,))
                            pending_orders = await self._fetchone(cur, f"SELECT COUNT(*) AS c FROM {cfg['schema']}.{cfg['direct']} WHERE guild_id=%s AND created_at < NOW() - INTERVAL 5 MINUTE", (guild_id,))
                        except Exception:
                            continue
                        queue_count = int((queue_count_row or {}).get("c", 0))
                        backup_count = int((backup_count_row or {}).get("c", 0))
                        home_vc_id = (home_row or {}).get("home_vc_id")
                        is_playing = bool(row.get("is_playing"))
                        updated_stale = False
                        try:
                            updated_at = row.get("updated_at")
                            if updated_at is not None:
                                age = (time.time() - updated_at.timestamp())
                                updated_stale = age > 150
                        except Exception:
                            updated_stale = False
                        if pending_orders and int((pending_orders or {}).get("c", 0)):
                            issues.append({"type": "stale_orders", "drone": drone, "guild_id": guild_id, "queue_count": queue_count, "backup_count": backup_count})
                        if is_playing and not row.get("current_track") and queue_count == 0:
                            issues.append({"type": "invalid_playback_state", "drone": drone, "guild_id": guild_id, "queue_count": queue_count, "backup_count": backup_count})
                        if (queue_count == 0 and backup_count > 0) or (row.get("current_track") and queue_count == 0):
                            issues.append({"type": "queue_rebuild_needed", "drone": drone, "guild_id": guild_id, "home_vc_id": home_vc_id, "queue_count": queue_count, "backup_count": backup_count, "current_track": row.get("current_track")})
                        if home_vc_id and (queue_count > 0 or backup_count > 0 or row.get("current_track")) and not is_playing:
                            issues.append({"type": "recover_from_queue", "drone": drone, "guild_id": guild_id, "home_vc_id": home_vc_id, "queue_count": queue_count, "backup_count": backup_count, "current_track": row.get("current_track")})
                        if row.get("position_seconds") is not None and float(row.get("position_seconds") or 0) < 0:
                            issues.append({"type": "invalid_position", "drone": drone, "guild_id": guild_id, "queue_count": queue_count, "backup_count": backup_count})
                        if is_playing and updated_stale and (queue_count > 0 or backup_count > 0 or row.get("current_track")):
                            issue = {"type": "stalled_playback_candidate", "drone": drone, "guild_id": guild_id, "home_vc_id": home_vc_id, "queue_count": queue_count, "backup_count": backup_count, "current_track": row.get("current_track"), "updated_stale": True}
                            issues.append(issue)
                            await self._record_predictive_signal(issue, "stall_risk", 0.72, details={"age_stale": True, "queue_count": queue_count, "backup_count": backup_count})
                            pressure = await self._predictive_pressure(issue, "stall_risk")
                            if pressure >= 0.65:
                                issues.append({**issue, "type": "predictive_stall_risk", "predictive_pressure": pressure})
                        if backup_count > 0 and queue_count > 0 and backup_count >= queue_count + 3:
                            issue = {"type": "predictive_queue_drift", "drone": drone, "guild_id": guild_id, "home_vc_id": home_vc_id, "queue_count": queue_count, "backup_count": backup_count, "current_track": row.get("current_track")}
                            issues.append(issue)
                            await self._record_predictive_signal(issue, "queue_drift", min(1.0, 0.4 + ((backup_count - queue_count) / max(queue_count, 1)) * 0.08), details={"queue_count": queue_count, "backup_count": backup_count})
                        if (queue_count > 0 or backup_count > 0 or row.get("current_track")) and not home_vc_id and not row.get("channel_id"):
                            issues.append({"type": "missing_recovery_anchor", "drone": drone, "guild_id": guild_id, "queue_count": queue_count, "backup_count": backup_count})
        if db.pool:
            try:
                async with db.pool.acquire() as conn:
                    async with conn.cursor(self._dict_cursor()) as cur:
                        issues.extend(await self._detect_health_outliers(cur))
            except Exception:
                logger.exception('Failed health outlier detection')
        issue_counts: dict[str, int] = {}
        for item in issues:
            drone = item.get("drone")
            if drone:
                issue_counts[drone] = issue_counts.get(drone, 0) + 1
        if issue_counts:
            counts = sorted(issue_counts.values())
            baseline = counts[len(counts)//2]
            for drone, count in issue_counts.items():
                if count >= max(3, baseline + 2):
                    issues.append({"type": "drone_outlier", "drone": drone, "issue_count": count, "baseline_issue_count": baseline})
        return issues

    async def detect_issues(self):
        issues = []
        for guild in self.bot.guilds:
            vc = guild.voice_client
            if vc and not vc.is_connected():
                issues.append({"type": "voice_disconnect", "guild_id": guild.id})
        issues.extend(await self.detect_automation_issues())
        issues.extend(await self.detect_swarm_issues())
        return self._rank_issues(issues)

    async def _repair_queue_rebuild(self, cur, drone: str, guild_id: int) -> RepairResult:
        playback = await self._fetch_playback_row(cur, drone, guild_id)
        queue_rows = await self._fetch_queue_rows(cur, drone, 'queue', guild_id)
        backup_rows = await self._fetch_queue_rows(cur, drone, 'backup', guild_id)
        inserted = 0
        if not queue_rows and backup_rows:
            await self._replace_queue_from_rows(cur, drone, 'queue', guild_id, backup_rows)
            inserted += len(backup_rows)
        elif not queue_rows and playback and playback.get('current_track'):
            await self._replace_queue_from_rows(cur, drone, 'queue', guild_id, [{
                'track_data': playback.get('current_track'),
                'title': str(playback.get('current_track') or '')[:250],
                'requested_by': None,
            }])
            inserted += 1
        await cur.execute(
            f"UPDATE {BOT_SCHEMAS[drone]['schema']}.{BOT_SCHEMAS[drone]['playback']} SET is_playing = FALSE, position_seconds = GREATEST(0, COALESCE(position_seconds,0)) WHERE guild_id=%s",
            (guild_id,),
        )
        return RepairResult(True, "queue_rebuild", drone, details=f"rebuilt/restored {inserted} row(s)")

    async def _enqueue_direct_order(self, cur, drone: str, guild_id: int, command: str, *, vc_id: int | None = None, text_channel_id: int | None = None, data: str | None = None):
        cfg = BOT_SCHEMAS[drone]
        await self.ensure_bot_schema(cur, drone)
        # De-dupe by command so Aria does not flood a music bot with repeated recovery orders.
        try:
            await cur.execute(
                f"DELETE FROM {cfg['schema']}.{cfg['direct']} WHERE bot_name=%s AND guild_id=%s AND command=%s",
                (drone, guild_id, command),
            )
        except Exception:
            pass
        await cur.execute(
            f"INSERT INTO {cfg['schema']}.{cfg['direct']} (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
            (drone, guild_id, vc_id if vc_id else None, text_channel_id if text_channel_id else None, command, data or "aria"),
        )

    async def _repair_recover_from_queue(self, cur, issue: dict[str, Any]) -> RepairResult:
        drone = issue["drone"]
        guild_id = issue["guild_id"]
        cfg = BOT_SCHEMAS[drone]
        playback = await self._fetch_playback_row(cur, drone, guild_id)
        vc_id = issue.get("home_vc_id") or (playback or {}).get("channel_id")
        text_channel_id = (playback or {}).get("text_channel_id")
        if not vc_id:
            return RepairResult(False, "recover", drone, "no home or playback channel available")
        await self._enqueue_direct_order(cur, drone, guild_id, "RECOVER", vc_id=vc_id, text_channel_id=text_channel_id, data="aria_auto_recovery")
        await cur.execute(
            f"UPDATE {cfg['schema']}.{cfg['playback']} SET is_playing = FALSE, is_paused = FALSE WHERE guild_id=%s",
            (guild_id,),
        )
        return RepairResult(True, "recover", drone, details=f"queued RECOVER for guild {guild_id}")

    async def _repair_invalid_state(self, cur, drone: str, guild_id: int) -> RepairResult:
        cfg = BOT_SCHEMAS[drone]
        await cur.execute(
            f"UPDATE {cfg['schema']}.{cfg['playback']} SET is_playing = FALSE, position_seconds = GREATEST(0, COALESCE(position_seconds,0)), current_track = NULLIF(current_track, '') WHERE guild_id=%s",
            (guild_id,),
        )
        return RepairResult(True, "normalize_playback_state", drone, details="normalized playback row")

    async def _repair_stale_orders(self, cur, drone: str, guild_id: int) -> RepairResult:
        cfg = BOT_SCHEMAS[drone]
        await cur.execute(f"DELETE FROM {cfg['schema']}.{cfg['direct']} WHERE guild_id=%s AND created_at < NOW() - INTERVAL 5 MINUTE", (guild_id,))
        return RepairResult(True, "clear_stale_orders", drone, details="deleted stale pending orders")

    async def _verify_resolution(self, cur, issue: dict[str, Any]) -> bool:
        drone = issue.get("drone")
        if drone not in BOT_SCHEMAS:
            return True
        cfg = BOT_SCHEMAS[drone]
        guild_id = issue.get("guild_id")
        if not guild_id:
            return True
        queue_count_row = await self._fetchone(cur, f"SELECT COUNT(*) AS c FROM {cfg['schema']}.{cfg['queue']} WHERE guild_id=%s", (guild_id,))
        playback = await self._fetch_playback_row(cur, drone, guild_id)
        queue_count = int((queue_count_row or {}).get("c", 0))
        issue_type = issue.get("type")
        if issue_type == "queue_rebuild_needed":
            return queue_count > 0
        if issue_type in {"recover_from_queue", "stalled_playback_candidate", "predictive_stall_risk"}:
            playback = playback or {}
            return bool(
                playback.get("channel_id")
                and (playback.get("is_playing") or playback.get("current_track") or queue_count > 0)
            )
        if issue_type in {"invalid_playback_state", "invalid_position"}:
            pos_ok = True
            if playback is not None and issue_type == "invalid_position":
                pos_row = await self._fetchone(cur, f"SELECT position_seconds FROM {cfg['schema']}.{cfg['playback']} WHERE guild_id=%s", (guild_id,))
                pos_ok = float((pos_row or {}).get("position_seconds") or 0) >= 0
            return pos_ok and not (playback or {}).get("is_playing") if playback else pos_ok
        if issue_type == "stale_orders":
            stale_row = await self._fetchone(cur, f"SELECT COUNT(*) AS c FROM {cfg['schema']}.{cfg['direct']} WHERE guild_id=%s AND created_at < NOW() - INTERVAL 5 MINUTE", (guild_id,))
            return int((stale_row or {}).get("c", 0)) == 0
        return True

    async def _repair_predictive_drift(self, cur, issue: dict[str, Any]) -> RepairResult:
        await self._repair_queue_rebuild(cur, issue['drone'], issue['guild_id'])
        return RepairResult(True, 'predictive_queue_rebalance', issue['drone'], details='rebuilt live queue from strongest persisted state before visible failure')


    async def _execute_strategy(self, cur, issue: dict[str, Any], strategy: str) -> RepairResult:
        drone = issue['drone']
        guild_id = issue.get('guild_id')
        if strategy == 'queue_rebuild':
            return await self._repair_queue_rebuild(cur, drone, guild_id)
        if strategy == 'recover_resume':
            await self._repair_invalid_state(cur, drone, guild_id)
            await self._repair_queue_rebuild(cur, drone, guild_id)
            return await self._repair_recover_from_queue(cur, issue)
        if strategy == 'normalize_playback_state':
            return await self._repair_invalid_state(cur, drone, guild_id)
        if strategy == 'clear_stale_orders':
            return await self._repair_stale_orders(cur, drone, guild_id)
        if strategy == 'predictive_queue_rebalance':
            return await self._repair_predictive_drift(cur, issue)
        return RepairResult(False, strategy, drone, details='unsupported strategy')

    async def fix_issue(self, issue):
        symptom = self._signature(issue)
        try:
            if issue["type"] == "voice_disconnect":
                guild = self.bot.get_guild(issue["guild_id"])
                vc = guild.voice_client if guild else None
                if vc:
                    await vc.disconnect()
                    await asyncio.sleep(1)
                    result = RepairResult(True, "disconnect_reset", "aria", "disconnected stale voice client")
                    await self._journal_repair(issue, result)
                    return True
            elif issue["type"] == "missing_automation_channel":
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("DELETE FROM aria_automations WHERE id = %s", (issue["automation_id"],))
                result = RepairResult(True, "delete_missing_automation", "aria")
                await self._journal_repair(issue, result)
                return True
            elif issue["type"] in {"stale_swarm_node", "queue_rebuild_needed", "recover_from_queue", "invalid_playback_state", "invalid_position", "stale_orders", "stalled_playback_candidate", "predictive_queue_drift", "predictive_stall_risk", "missing_recovery_anchor", "drone_outlier", "drone_health_outlier", "guild_hotspot"}:
                drone = issue.get("drone")
                action_map = {
                    "queue_rebuild_needed": "queue_rebuild",
                    "recover_from_queue": "recover_resume",
                    "invalid_playback_state": "normalize_playback_state",
                    "invalid_position": "normalize_playback_state",
                    "stale_orders": "clear_stale_orders",
                    "stalled_playback_candidate": "recover_resume",
                    "predictive_queue_drift": "predictive_queue_rebalance",
            "predictive_stall_risk": "recover_resume",
                    "missing_recovery_anchor": "needs_manual_anchor",
                    "drone_outlier": "outlier_report",
                    "drone_health_outlier": "outlier_report",
                    "guild_hotspot": "hotspot_report",
                    "stale_swarm_node": "stale_swarm_report",
                }
                action = action_map[issue["type"]]
                policy_profile = await self._policy_profile(issue)
                issue = {**issue, "policy_confidence_bias": policy_profile.get("confidence_bias", 0.0)}
                confidence = self._score_confidence(issue)
                if issue["type"] not in {"stale_swarm_node", "drone_outlier", "drone_health_outlier", "guild_hotspot", "missing_recovery_anchor"} and confidence < 0.45:
                    await self._journal_repair(issue, RepairResult(False, f"observe_{action}", drone, details=f"confidence too low: {confidence:.2f}"), error="low_confidence")
                    return False
                await self._record_operator_decision(issue)
                if issue["type"] not in {"stale_swarm_node", "drone_outlier", "drone_health_outlier", "guild_hotspot", "missing_recovery_anchor"}:
                    original_cooldown = self._cooldown_seconds
                    self._cooldown_seconds = float(policy_profile.get("preferred_cooldown_seconds", self._cooldown_seconds) or self._cooldown_seconds)
                    cooldown_ok = self._cooldown_ready(issue, action)
                    self._cooldown_seconds = original_cooldown
                    if not cooldown_ok:
                        return False
                strategy_plan = await self._choose_action_plan(issue)
                strategy_index = int(issue.get('_strategy_index', 0) or 0)
                current_strategy = strategy_plan[min(strategy_index, max(len(strategy_plan) - 1, 0))] if strategy_plan else action
                if not issue.get("_from_followup"):
                    guard_window = self._guard_window_seconds(issue, current_strategy)
                    if current_strategy == "recover_resume" and guard_window > 0:
                        if await self._recent_direct_order_exists(issue, "RECOVER", guard_window):
                            result = RepairResult(True, "recover_guarded", drone or "swarm", details=f"recent RECOVER order already exists within {guard_window}s")
                            await self._journal_repair(issue, result)
                            return True
                    if guard_window > 0:
                        allowed, guard_reason = await self._consume_repair_guard(issue, current_strategy, window_seconds=guard_window)
                        if not allowed:
                            result = RepairResult(True, f"guarded_{current_strategy}", drone or "swarm", details=guard_reason)
                            await self._journal_repair(issue, result)
                            return True
                if issue["type"] == "guild_hotspot":
                    await send_ops_webhook_log("Aria Medic Guild Hotspot", f"Guild {issue.get('guild_id', 'n/a')} has multiple degraded swarm bots.", fields=[("Bad Bots", str(issue.get('bad_bot_count', 0)), True), ("Bot Count", str(issue.get('bot_count', 0)), True), ("Issue", symptom, False)])
                    result = RepairResult(True, "hotspot_report", "swarm", details="reported guild hotspot requiring operator attention")
                    await self._journal_repair(issue, result)
                    return True
                if drone not in BOT_SCHEMAS:
                    return False
                async with db.pool.acquire() as conn:
                    async with conn.cursor(self._dict_cursor()) as cur:
                        await self.ensure_bot_schema(cur, drone)
                        if issue["type"] == "missing_recovery_anchor":
                            await send_ops_webhook_log("Aria Medic Alert", f"{drone} has recoverable queue state but no home/playback anchor.", fields=[("Issue", symptom, False)])
                            result = RepairResult(False, "needs_manual_anchor", drone, details="queue exists but no home channel/playback channel anchor")
                        elif issue["type"] == "drone_outlier":
                            await send_ops_webhook_log("Aria Medic Drift Alert", f"{drone} is an issue outlier in the swarm.", fields=[("Issue Count", str(issue.get('issue_count', 0)), True), ("Baseline", str(issue.get('baseline_issue_count', 0)), True), ("Issue", symptom, False)])
                            result = RepairResult(True, "outlier_report", drone, details="reported comparative swarm outlier")
                        elif issue["type"] == "drone_health_outlier":
                            await send_ops_webhook_log("Aria Medic Health Outlier", f"{drone} is underperforming versus the swarm baseline.", fields=[("Mean", str(issue.get('health_mean', 0)), True), ("Floor", str(issue.get('health_floor', 0)), True), ("Issue", symptom, False)])
                            result = RepairResult(True, "outlier_report", drone, details="reported health outlier versus swarm baseline")
                        elif issue["type"] == "guild_hotspot":
                            await send_ops_webhook_log("Aria Medic Guild Hotspot", f"Guild {issue.get('guild_id', 'n/a')} has multiple degraded swarm bots.", fields=[("Bad Bots", str(issue.get('bad_bot_count', 0)), True), ("Bot Count", str(issue.get('bot_count', 0)), True), ("Issue", symptom, False)])
                            result = RepairResult(True, "hotspot_report", drone or 'swarm', details="reported guild hotspot requiring operator attention")
                        elif issue["type"] == "stale_swarm_node":
                            await send_ops_webhook_log("Aria Medic Alert", f"Detected stale swarm node: {drone}", fields=[("Issue", symptom, False)])
                            result = RepairResult(True, "stale_swarm_report", drone, details="reported stale swarm node")
                        else:
                            result = await self._execute_strategy(cur, issue, current_strategy)
                            verified = await self._verify_resolution(cur, issue) if result.success else False
                            if not verified:
                                result = RepairResult(False, f"{result.action}_pending", drone, details=(result.details + f" | waiting for verification via follow-up strategy={current_strategy}").strip())
                await self._journal_repair(issue, result)
                if issue["type"] not in {"stale_swarm_node", "drone_outlier", "drone_health_outlier", "guild_hotspot", "missing_recovery_anchor"}:
                    if result.success:
                        await send_webhook_log(
                            "Aria Medic Repair",
                            f"Executed {result.action} on {drone} for guild {issue.get('guild_id', 'n/a')}.",
                            fields=[("Details", result.details or "n/a", False)],
                        )
                    elif not issue.get("_from_followup"):
                        strategy_plan = await self._choose_action_plan(issue)
                        next_index = int(issue.get('_strategy_index', 0) or 0) + 1
                        attempt_count = int(issue.get('_attempt_count', 0) or 0) + 1
                        if next_index < len(strategy_plan) and attempt_count < self._max_repair_attempts:
                            await self._schedule_repair_followup(issue, strategy_index=next_index, attempt_count=attempt_count, last_result=result.details or result.action)
                            await send_webhook_log(
                                "Aria Medic Follow-up Scheduled",
                                f"Queued follow-up repair for {drone} guild {issue.get('guild_id', 'n/a')}.",
                                fields=[("Next Step", strategy_plan[next_index], True), ("Last Result", (result.details or result.action)[:256], False)],
                            )
                        elif self._should_escalate_infra(issue, attempts=attempt_count):
                            target = self._infra_target_for_issue(issue)
                            if target:
                                await self._queue_infra_task(issue, target=target, action='restart', reason=(result.details or result.action or 'repair failed')[:4000])
                return result.success
        except Exception as e:
            logger.exception(e)
            try:
                await self._journal_repair(issue, RepairResult(False, issue.get("type", "repair"), issue.get("drone", "swarm")), error=str(e))
                await send_error_webhook_log("Aria Medic Repair Error", str(e), traceback_text=None)
            except Exception:
                pass
        return False


    async def handle_event(self, event: dict[str, Any]) -> bool:
        event_type = event.get("event_type")
        payload = dict(event.get("payload") or {})
        if event_type == "recoverable_state_detected":
            issue = {
                "type": "recover_from_queue",
                "drone": event.get("bot_name"),
                "guild_id": event.get("guild_id"),
                "home_vc_id": payload.get("home_vc_id"),
                "queue_count": payload.get("queue_count", 0),
                "backup_count": payload.get("backup_count", 0),
                "current_track": payload.get("current_track"),
            }
            return await self.fix_issue(issue)
        if event_type == "playback_state_drift":
            issue = {
                "type": "stalled_playback_candidate",
                "drone": event.get("bot_name"),
                "guild_id": event.get("guild_id"),
                "home_vc_id": payload.get("home_vc_id"),
                "queue_count": payload.get("queue_count", 0),
                "backup_count": payload.get("backup_count", 0),
                "current_track": payload.get("current_track"),
                "updated_stale": True,
            }
            return await self.fix_issue(issue)
        if event_type == "bot_error_logged":
            issue = {
                "type": "stale_swarm_node",
                "drone": event.get("bot_name"),
                "guild_id": event.get("guild_id"),
                "error_type": payload.get("error_type"),
            }
            return await self.fix_issue(issue)
        if event_type == "health_trending_down":
            issue = {
                "type": "predictive_stall_risk",
                "drone": event.get("bot_name"),
                "guild_id": event.get("guild_id"),
                "home_vc_id": payload.get("home_vc_id"),
                "queue_count": payload.get("queue_count", 0),
                "backup_count": payload.get("backup_count", 0),
                "current_track": payload.get("current_track"),
                "predictive_pressure": 0.75,
            }
            return await self.fix_issue(issue)
        return False

    async def run_summary(self):
        findings = await self.detect_issues()
        resolved = 0
        unresolved = []
        for issue in findings:
            fixed = await self.fix_issue(issue)
            if fixed:
                resolved += 1
            else:
                unresolved.append(issue["type"])

        if not findings:
            return "Diagnostics complete. No automation or swarm faults were detected."

        parts = [f"Diagnostics complete. Found {len(findings)} issue(s) and resolved {resolved}."]
        if findings:
            top = findings[0]
            parts.append(f"Top priority: {top.get('type')} ({top.get('_urgency', 'normal')}).")
        if unresolved:
            parts.append("Still watching: " + ", ".join(sorted(set(unresolved))) + ".")
        return " ".join(parts)

    async def run_once(self):
        if not self.enabled:
            return
        for issue in await self.detect_issues():
            await self.fix_issue(issue)
