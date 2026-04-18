from pathlib import Path

from core.interaction_utils import require_guild


class _DummyResponse:
    def __init__(self, done=False):
        self._done = done
        self.calls = []

    def is_done(self):
        return self._done

    async def send_message(self, message, ephemeral=False):
        self.calls.append((message, ephemeral))


class _DummyFollowup:
    def __init__(self):
        self.calls = []

    async def send(self, message, ephemeral=False):
        self.calls.append((message, ephemeral))


class _DummyInteraction:
    def __init__(self, guild=None, done=False):
        self.guild = guild
        self.response = _DummyResponse(done=done)
        self.followup = _DummyFollowup()


def test_require_guild_uses_initial_response_when_not_done():
    import asyncio
    interaction = _DummyInteraction(guild=None, done=False)
    guild = asyncio.run(require_guild(interaction))
    assert guild is None
    assert interaction.response.calls == [(
        "That command only works inside a server. DMs are beneath this workflow.",
        True,
    )]
    assert interaction.followup.calls == []


def test_require_guild_uses_followup_when_response_already_done():
    import asyncio
    interaction = _DummyInteraction(guild=None, done=True)
    guild = asyncio.run(require_guild(interaction))
    assert guild is None
    assert interaction.response.calls == []
    assert interaction.followup.calls == [(
        "That command only works inside a server. DMs are beneath this workflow.",
        True,
    )]


def test_pvp_duel_hint_matches_actual_group_name():
    content = Path("cogs/pvp.py").read_text(encoding="utf-8")
    assert "/duel accept" in content
    assert "/battle accept" not in content


def test_legacy_hardcoded_db_config_removed_from_high_risk_command_cogs():
    for rel in ["cogs/affinity.py", "cogs/casino.py", "cogs/pvp.py", "cogs/vault.py"]:
        content = Path(rel).read_text(encoding="utf-8")
        assert "DB_CONFIG =" not in content
        assert "aiomysql.create_pool" not in content


def test_high_risk_command_cogs_use_guild_guard():
    for rel in ["cogs/casino.py", "cogs/pvp.py", "cogs/vault.py"]:
        content = Path(rel).read_text(encoding="utf-8")
        assert "require_guild" in content
