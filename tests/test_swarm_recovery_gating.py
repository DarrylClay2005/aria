from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from core.autonomy import AutonomousEngine
from core.event_bus import EventBus


def test_event_bus_skips_fresh_recoverable_state(monkeypatch):
    monkeypatch.setenv("ARIA_RECOVERY_EVENT_MIN_AGE_SECONDS", "120")
    bus = EventBus(SimpleNamespace())

    state = {
        "queue_count": 12,
        "backup_count": 4,
        "current_track": True,
        "is_playing": False,
        "is_paused": False,
        "home_vc_id": 123,
        "channel_id": 123,
        "updated_seconds": 18,
    }

    assert bus._should_emit_recoverable_state(state) is False


def test_event_bus_emits_stale_recoverable_state(monkeypatch):
    monkeypatch.setenv("ARIA_RECOVERY_EVENT_MIN_AGE_SECONDS", "120")
    bus = EventBus(SimpleNamespace())

    state = {
        "queue_count": 12,
        "backup_count": 4,
        "current_track": True,
        "is_playing": False,
        "is_paused": False,
        "home_vc_id": 123,
        "channel_id": 123,
        "updated_seconds": 245,
    }

    assert bus._should_emit_recoverable_state(state) is True


def test_autonomy_recover_from_queue_rejects_active_playback():
    engine = AutonomousEngine(SimpleNamespace())

    should_act, reason = engine._should_auto_act_on_issue(
        {
            "type": "recover_from_queue",
            "guild_id": 42,
            "home_vc_id": 123,
            "channel_id": 123,
            "queue_count": 5,
            "backup_count": 2,
            "current_track": "https://example.com/song",
            "is_playing": True,
            "is_paused": False,
            "updated_seconds": 300,
        }
    )

    assert should_act is False
    assert "already playing" in reason


@pytest.mark.asyncio
async def test_handle_event_voice_timeout_arms_pause_without_recovering():
    engine = AutonomousEngine(SimpleNamespace())
    engine._arm_repair_guard = AsyncMock()
    engine.fix_issue = AsyncMock()

    result = await engine.handle_event(
        {
            "event_type": "bot_error_logged",
            "bot_name": "nexus",
            "guild_id": 42,
            "payload": {
                "error_category": "voice_connect_timeout",
                "track_query": "test song",
            },
            "created_at": "2026-05-06 15:00:00",
        }
    )

    assert result is False
    engine._arm_repair_guard.assert_awaited_once()
    engine.fix_issue.assert_not_called()


@pytest.mark.asyncio
async def test_handle_event_ignores_fresh_recoverable_state():
    engine = AutonomousEngine(SimpleNamespace())
    engine.fix_issue = AsyncMock(return_value=True)

    result = await engine.handle_event(
        {
            "event_type": "recoverable_state_detected",
            "bot_name": "nexus",
            "guild_id": 42,
            "payload": {
                "channel_id": 123,
                "home_vc_id": 123,
                "queue_count": 5,
                "backup_count": 1,
                "current_track": True,
                "is_playing": False,
                "is_paused": False,
                "updated_seconds": 15,
            },
            "created_at": "2099-05-06 15:00:00",
        }
    )

    assert result is False
    engine.fix_issue.assert_not_called()
