import logging
import random
from datetime import timedelta

import discord
from discord import app_commands
from discord.ext import commands

from core.db_helpers import db_cursor
from core.interaction_utils import require_guild

logger = logging.getLogger("discord")


class Casino(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def get_balance(self, user_id: int) -> int:
        async with db_cursor() as cur:
            await cur.execute("INSERT IGNORE INTO aria_economy (user_id, balance) VALUES (%s, 0)", (user_id,))
            await cur.execute("SELECT balance FROM aria_economy WHERE user_id = %s", (user_id,))
            res = await cur.fetchone()
            return res[0] if res else 0

    casino_group = app_commands.Group(name="casino", description="Gamble coins in Aria's rigged games and server taxes.")

    @casino_group.command(name="roulette", description="Bet coins on roulette for a big payout or a humiliating timeout.")
    @app_commands.describe(bet="How many coins to risk (Min: 1000)")
    async def roulette(self, interaction: discord.Interaction, bet: int):
        guild = await require_guild(interaction)
        if guild is None:
            return
        if bet < 1000:
            return await interaction.response.send_message(
                "This is the high-roller table. 1,000 coins minimum. Go play `/economy gamble` if you want to bet pocket change.",
                ephemeral=True,
            )

        bal = await self.get_balance(interaction.user.id)
        if bal < bet:
            return await interaction.response.send_message(
                f"You don't have {bet} coins. You only have {bal}. Stop trying to bet money you don't own.",
                ephemeral=True,
            )

        win = random.random() < 0.25
        async with db_cursor() as cur:
            if win:
                winnings = bet * 3
                await cur.execute("UPDATE aria_economy SET balance = balance + %s WHERE user_id = %s", (winnings, interaction.user.id))
                await interaction.response.send_message(
                    f"🎰 **JACKPOT!**\n\nSomehow, against all odds, you won. I've added **{winnings} coins** to your account. Take your money and leave before I change my mind."
                )
            else:
                await cur.execute("UPDATE aria_economy SET balance = balance - %s WHERE user_id = %s", (bet, interaction.user.id))
                try:
                    await interaction.user.timeout(timedelta(minutes=10), reason="Lost Aria's High-Stakes Roulette")
                    jail_msg = "Oh, and you're timed out for 10 minutes. I need some peace and quiet to count my new money."
                except discord.Forbidden:
                    jail_msg = "I would have muted you for 10 minutes too, but my permissions are too low. Consider yourself incredibly lucky."
                await interaction.response.send_message(
                    f"📉 **YOU LOSE.**\n\nYou just lost **{bet} coins**. The house always wins, fucking idiot. {jail_msg}"
                )

    @casino_group.command(name="taco_tax", description="[OWNER] Collect 5% of everyone's wealth across the server.")
    @app_commands.default_permissions(administrator=True)
    async def taco_tax(self, interaction: discord.Interaction):
        guild = await require_guild(interaction)
        if guild is None:
            return
        async with db_cursor() as cur:
            await cur.execute("SELECT SUM(FLOOR(balance * 0.05)) FROM aria_economy WHERE balance > 100")
            stolen_total = (await cur.fetchone())[0] or 0
            if stolen_total == 0:
                return await interaction.response.send_message("Everyone is too broke to tax. Pathetic.", ephemeral=True)
            await cur.execute("UPDATE aria_economy SET balance = balance - FLOOR(balance * 0.05) WHERE balance > 100")

        embed = discord.Embed(
            title="🌮 The Taco Tuesday Tax 🌮",
            description=(
                "*Aria forcefully audits the server.* \n\n"
                f'"I\'m tired of fast food. I need a meal. I have unilaterally decided to tax 5% of everyone\'s wealth. '
                f"I just seized a total of **{stolen_total} coins** from you fucking peasants. Deal with it.\""
            ),
            color=discord.Color.brand_red(),
        )
        await interaction.response.send_message(embed=embed)


async def setup(bot):
    await bot.add_cog(Casino(bot))
