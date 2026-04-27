import discord
from discord.ext import commands
from discord import app_commands
import logging
from datetime import datetime, timedelta
from core.database import db

logger = logging.getLogger("discord")

class Sanity(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        if not db.pool:
            logger.warning("sanity: database pool unavailable; table init skipped.")
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    CREATE TABLE IF NOT EXISTS aria_sanity (
                        user_id BIGINT PRIMARY KEY,
                        sanity_level INT DEFAULT 100,
                        last_therapy TIMESTAMP NULL DEFAULT NULL
                    )
                """)

    sanity_group = app_commands.Group(name="sanity", description="Check or restore your rapidly collapsing sanity level.")

    @sanity_group.command(name="check", description="Check your current sanity level and Aria's diagnosis.")
    async def check(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (interaction.user.id,))
                await cur.execute("SELECT sanity_level FROM aria_sanity WHERE user_id = %s", (interaction.user.id,))
                sanity = (await cur.fetchone())[0]

        if sanity >= 80: msg = "Your mind is fully intact. How boring."
        elif sanity >= 40: msg = "You are starting to crack. The delusions should start soon."
        elif sanity > 0: msg = "You are hanging on by a thread. Your grip on reality is entirely gone."
        else: msg = "Your mind is completely shattered. You are broken."

        embed = discord.Embed(title="🧠 Mental State Evaluation", description=f"**Current Sanity: {sanity}%**\n\n{msg}", color=discord.Color.dark_blue())
        await interaction.response.send_message(embed=embed)

    @sanity_group.command(name="therapy", description="Ask Aria for a daily sanity restore if she'll allow it.")
    async def therapy(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (interaction.user.id,))
                await cur.execute("SELECT last_therapy, sanity_level FROM aria_sanity WHERE user_id = %s", (interaction.user.id,))
                row = await cur.fetchone()
                
                last_therapy = row[0]
                current_sanity = row[1]
                
                if current_sanity >= 100:
                    return await interaction.response.send_message("You are already at 100% sanity. Stop wasting my time.", ephemeral=True)

                if last_therapy and datetime.now() - last_therapy < timedelta(hours=24):
                    return await interaction.response.send_message("You already had your therapy session today. Suffer until tomorrow.", ephemeral=True)

                await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, sanity_level + 20), last_therapy = CURRENT_TIMESTAMP WHERE user_id = %s", (interaction.user.id,))
        
        await interaction.response.send_message("Fine. I've restored 20% of your sanity. Try not to lose your mind again today.")

async def setup(bot):
    await bot.add_cog(Sanity(bot))
