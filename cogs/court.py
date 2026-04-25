import asyncio
import logging
import os

import discord
from discord import app_commands
from discord.ext import commands
from google import genai
from google.genai import types

from core.db_helpers import db_cursor

logger = logging.getLogger("discord")
GEMINI_API_KEY = os.getenv('ARIA_GEMINI_API_KEY', os.getenv('GEMINI_API_KEY', '')).strip()
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'


class ServerCourt(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def damage_sanity(self, user_id: int, amount: int):
        async with db_cursor() as cur:
            await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
            await cur.execute(
                "UPDATE aria_sanity SET sanity_level = GREATEST(0, sanity_level - %s) WHERE user_id = %s",
                (amount, user_id),
            )

    async def get_affinity(self, user_id: int) -> int:
        async with db_cursor() as cur:
            await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
            res = await cur.fetchone()
            return res[0] if res else 0

    court_group = app_commands.Group(name="court", description="Bring a case to Aria's deeply unfair courtroom.")

    @court_group.command(name="sue", description="Sue another member for sanity damage and let Aria judge the case.")
    @app_commands.describe(target="Who you are suing", amount="Sanity damage to inflict (Max 40)", reason="Why you are suing them")
    async def sue(self, interaction: discord.Interaction, target: discord.Member, amount: int, reason: str):
        if target.bot or target == interaction.user:
            return await interaction.response.send_message("You can't sue yourself or a bot.", ephemeral=True)
        if amount > 40:
            return await interaction.response.send_message(
                "The court limits psychological damage to 40 Sanity points per lawsuit.",
                ephemeral=True,
            )

        await interaction.response.send_message(
            f"⚖️ **COURT IS IN SESSION** ⚖️\n\n{interaction.user.mention} is suing {target.mention} "
            f"for **{amount}% Sanity Damage**.\n**Charge:** \"{reason}\"\n\n"
            "You both have exactly **60 seconds** to type your defense. Speak now or I rule against you."
        )

        plaintiff_args, defendant_args = [], []

        def check(m):
            if m.channel == interaction.channel:
                if m.author.id == interaction.user.id:
                    plaintiff_args.append(m.content)
                elif m.author.id == target.id:
                    defendant_args.append(m.content)
            return False

        try:
            await self.bot.wait_for('message', timeout=60.0, check=check)
        except asyncio.TimeoutError:
            pass

        await interaction.channel.send("🛑 **TIME IS UP.** Reviewing the case.")

        p_affinity = await self.get_affinity(interaction.user.id)
        d_affinity = await self.get_affinity(target.id)
        p_statement = " ".join(plaintiff_args) if plaintiff_args else "*[Silent cowardice]*"
        d_statement = " ".join(defendant_args) if defendant_args else "*[Silent cowardice]*"

        prompt = (
            f"Judge this lawsuit. Plaintiff ({interaction.user.display_name}, Affinity: {p_affinity}/100) "
            f"vs Defendant ({target.display_name}, Affinity: {d_affinity}/100). Suing to inflict {amount} "
            f"Sanity Damage because: '{reason}'. Plaintiff argued: '{p_statement}'. Defendant argued: "
            f"'{d_statement}'. Be biased toward higher affinity. Swear heavily. End with exactly: 'WINNER: [Name]'"
        )

        try:
            res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID,
                    contents=prompt,
                    config=types.GenerateContentConfig(system_instruction="You are a corrupt, toxic judge."),
                ),
            )
            judgment = res.text
            embed = discord.Embed(title="👩‍⚖️ The Verdict", description=judgment[:4096], color=discord.Color.dark_purple())

            if f"WINNER: {interaction.user.display_name}".lower() in judgment.lower():
                await self.damage_sanity(target.id, amount)
                embed.set_footer(text=f"Ruling: {interaction.user.display_name} wins. {target.display_name} suffers {amount}% Sanity Damage.")
            elif f"WINNER: {target.display_name}".lower() in judgment.lower():
                embed.set_footer(text=f"Ruling: Case dismissed. {target.display_name}'s mind is unharmed.")
            else:
                await self.damage_sanity(interaction.user.id, amount)
                embed.set_footer(
                    text=f"Ruling: Aria held Plaintiff in contempt. {interaction.user.display_name} suffers {amount}% Sanity Damage instead."
                )

            await interaction.channel.send(embed=embed)
        except Exception:
            await interaction.channel.send("Arguments too stupid. Case dismissed.")


async def setup(bot):
    await bot.add_cog(ServerCourt(bot))
