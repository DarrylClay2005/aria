from __future__ import annotations

from typing import Any

try:  # pragma: no cover - optional in lightweight local test shells
    import discord
except ImportError:  # pragma: no cover
    discord = Any


async def require_guild(interaction: Any):
    guild = getattr(interaction, "guild", None)
    if guild is not None:
        return guild

    message = "That command only works inside a server. DMs are beneath this workflow."
    response = getattr(interaction, "response", None)
    if response is not None and hasattr(response, "is_done") and response.is_done():
        followup = getattr(interaction, "followup", None)
        if followup is not None:
            await followup.send(message, ephemeral=True)
    elif response is not None:
        await response.send_message(message, ephemeral=True)
    return None
