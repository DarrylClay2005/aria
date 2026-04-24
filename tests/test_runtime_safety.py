from pathlib import Path

from core.ai_service import AIService


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
