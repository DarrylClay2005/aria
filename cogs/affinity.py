import logging

import discord
from discord import app_commands
from discord.ext import commands

from core.db_helpers import db_cursor

logger = logging.getLogger("discord")


class Affinity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        async with db_cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aria_affinity (
                    user_id BIGINT PRIMARY KEY,
                    score INT DEFAULT 0
                )
                """
            )

    async def get_affinity(self, user_id: int) -> int:
        async with db_cursor() as cur:
            await cur.execute("INSERT IGNORE INTO aria_affinity (user_id, score) VALUES (%s, 0)", (user_id,))
            await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
            result = await cur.fetchone()
            return result[0] if result else 0

    @app_commands.command(name="affinity", description="See how much Aria currently likes or tolerates you.")
    async def check_affinity(self, interaction: discord.Interaction):
        score = await self.get_affinity(interaction.user.id)

        if score <= -50:
            status = "Despised"
            msg = f"Your tolerance score is **{score}/100**. I actively despise you. Please stop talking to me."
        elif score < 0:
            status = "Annoying"
            msg = f"Your tolerance score is **{score}/100**. You are currently a nuisance. Try to be less exhausting."
        elif score == 0:
            status = "Neutral"
            msg = f"Your tolerance score is **{score}/100**. I feel absolutely nothing toward you. You are entirely forgettable."
        elif score < 50:
            status = "Tolerated"
            msg = f"Your tolerance score is **{score}/100**. You're surprisingly somewhat tolerable today. Don't ruin it."
        else:
            status = "Favored"
            msg = f"Your tolerance score is **{score}/100**. You are actually one of the few humans I don't want to throw into the sun. Congratulations, I guess."

        embed = discord.Embed(title=f"Affinity Status: {status}", description=msg, color=discord.Color.dark_purple())
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="set_affinity", description="[OWNER] Manually set a member's affinity score.")
    @app_commands.default_permissions(administrator=True)
    async def set_affinity(self, interaction: discord.Interaction, target: discord.Member, amount: int):
        amount = max(-100, min(100, amount))
        async with db_cursor() as cur:
            await cur.execute(
                "INSERT INTO aria_affinity (user_id, score) VALUES (%s, %s) ON DUPLICATE KEY UPDATE score = %s",
                (target.id, amount, amount),
            )

        await interaction.response.send_message(f"Adjusted {target.display_name}'s affinity to {amount}.", ephemeral=True)


async def setup(bot):
    await bot.add_cog(Affinity(bot))
