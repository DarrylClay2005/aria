from pathlib import Path
from types import SimpleNamespace
import sys
import time

import pytest

from core.ai_service import AIService
from core.autonomy import AutonomousEngine


def test_ai_service_prefers_prefixed_api_key(monkeypatch):
    monkeypatch.setenv("ARIA_GEMINI_API_KEY", "prefixed-key")
    monkeypatch.setenv("GEMINI_API_KEY", "legacy-key")
    service = AIService()
    assert service.api_key == "prefixed-key"


def test_ai_service_falls_back_to_legacy_api_key(monkeypatch):
    monkeypatch.delenv("ARIA_GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "legacy-key")
    service = AIService()
    assert service.api_key == "legacy-key"


def test_ai_service_prefers_prefixed_groq_api_key(monkeypatch):
    monkeypatch.setenv("ARIA_GROQ_API_KEY", "prefixed-groq-key")
    monkeypatch.setenv("GROQ_API_KEY", "legacy-groq-key")
    service = AIService()
    assert service.groq_api_key == "prefixed-groq-key"
    assert service.enable_groq_fallback is True


def test_ai_service_falls_back_to_legacy_groq_api_key(monkeypatch):
    monkeypatch.delenv("ARIA_GROQ_API_KEY", raising=False)
    monkeypatch.setenv("GROQ_API_KEY", "legacy-groq-key")
    service = AIService()
    assert service.groq_api_key == "legacy-groq-key"
    assert service.enable_groq_fallback is True


def test_repo_no_longer_contains_hardcoded_gemini_fallback_key():
    sentinel = "AIza" + "SyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k"
    offenders = []
    for path in Path('.').rglob('*.py'):
        if path.name == 'test_runtime_safety.py':
            continue
        text = path.read_text(encoding='utf-8', errors='ignore')
        if sentinel in text:
            offenders.append(str(path))
    assert offenders == []


def test_refactored_cogs_no_longer_define_local_db_config():
    targets = [
        Path('cogs/bookclub.py'),
        Path('cogs/confessions.py'),
        Path('cogs/court.py'),
        Path('cogs/pacts.py'),
        Path('cogs/ultimatum.py'),
    ]
    for path in targets:
        text = path.read_text(encoding='utf-8', errors='ignore')
        assert 'DB_CONFIG =' not in text, path
        assert 'create_pool(**DB_CONFIG)' not in text, path
        assert 'db_cursor(' in text, path


@pytest.mark.asyncio
async def test_infra_planned_command_redacts_sensitive_values(monkeypatch):
    monkeypatch.setenv("ARIA_ENABLE_INFRA_CONTROL", "1")
    monkeypatch.setenv("ARIA_ALLOW_INFRA_EXEC", "0")
    monkeypatch.setenv("ARIA_AUTO_ESCALATE_INFRA_REPAIRS", "1")
    engine = AutonomousEngine(SimpleNamespace())

    success, mode, result = await engine._execute_infra_task(
        {"command_text": "docker login --password super-secret TOKEN=abc123"}
    )

    assert success is False
    assert mode == "planned"
    assert "super-secret" not in result
    assert "abc123" not in result
    assert "[REDACTED]" in result


@pytest.mark.asyncio
async def test_infra_execution_rejects_disallowed_executable(monkeypatch):
    monkeypatch.setenv("ARIA_ENABLE_INFRA_CONTROL", "1")
    monkeypatch.setenv("ARIA_ALLOW_INFRA_EXEC", "1")
    monkeypatch.setenv("ARIA_AUTO_ESCALATE_INFRA_REPAIRS", "1")
    monkeypatch.setenv("ARIA_INFRA_ALLOWED_EXECUTABLES", "systemctl")
    engine = AutonomousEngine(SimpleNamespace())

    success, mode, result = await engine._execute_infra_task(
        {"command_text": f"{sys.executable} --version"}
    )

    assert success is False
    assert mode == "rejected"
    assert "not allowlisted" in result


@pytest.mark.asyncio
async def test_infra_execution_checks_missing_executable(monkeypatch):
    monkeypatch.setenv("ARIA_ENABLE_INFRA_CONTROL", "1")
    monkeypatch.setenv("ARIA_ALLOW_INFRA_EXEC", "1")
    monkeypatch.setenv("ARIA_AUTO_ESCALATE_INFRA_REPAIRS", "1")
    monkeypatch.setenv("ARIA_INFRA_ALLOWED_EXECUTABLES", "definitely-not-real-aria-command")
    engine = AutonomousEngine(SimpleNamespace())

    success, mode, result = await engine._execute_infra_task(
        {"command_text": "definitely-not-real-aria-command restart"}
    )

    assert success is False
    assert mode == "rejected"
    assert "was not found" in result


@pytest.mark.asyncio
async def test_infra_execution_redacts_command_output(monkeypatch):
    monkeypatch.setenv("ARIA_ENABLE_INFRA_CONTROL", "1")
    monkeypatch.setenv("ARIA_ALLOW_INFRA_EXEC", "1")
    monkeypatch.setenv("ARIA_AUTO_ESCALATE_INFRA_REPAIRS", "1")
    monkeypatch.setenv("ARIA_INFRA_ALLOWED_EXECUTABLES", "echo")
    engine = AutonomousEngine(SimpleNamespace())

    success, mode, result = await engine._execute_infra_task(
        {"command_text": "echo TOKEN=abc123"}
    )

    assert success is True
    assert mode == "executed"
    assert "abc123" not in result
    assert "TOKEN=[REDACTED]" in result


@pytest.mark.asyncio
async def test_infra_timeout_kills_process(monkeypatch):
    monkeypatch.setenv("ARIA_ENABLE_INFRA_CONTROL", "1")
    monkeypatch.setenv("ARIA_ALLOW_INFRA_EXEC", "1")
    monkeypatch.setenv("ARIA_AUTO_ESCALATE_INFRA_REPAIRS", "1")
    monkeypatch.setenv("ARIA_INFRA_ALLOWED_EXECUTABLES", "sleep")
    engine = AutonomousEngine(SimpleNamespace())
    engine._infra_timeout_seconds = 0.01

    success, mode, result = await engine._execute_infra_task(
        {"command_text": "sleep 5"}
    )

    assert success is False
    assert mode == "executed"
    assert "timed out" in result


@pytest.mark.asyncio
async def test_auto_infra_restart_cooldown_blocks_recent_target(monkeypatch):
    monkeypatch.setenv("ARIA_AUTO_INFRA_RESTART_MIN_INTERVAL_SECONDS", "18000")
    engine = AutonomousEngine(SimpleNamespace())
    engine._infra_recent_restart_cache["nexus:restart"] = time.time()

    blocked, reason = await engine._auto_infra_restart_recently_requested("nexus", "restart")

    assert blocked is True
    assert "cooldown" in reason
