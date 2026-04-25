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


class Pacts(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        async with db_cursor() as cur:
            await cur.execute(
                """
                CREATE TABLE IF NOT EXISTS aria_pacts (
                    user_id BIGINT PRIMARY KEY,
                    pact_active BOOLEAN DEFAULT TRUE,
                    date_signed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        async with db_cursor() as cur:
            await cur.execute("SELECT pact_active FROM aria_pacts WHERE user_id = %s", (message.author.id,))
            res = await cur.fetchone()
            if res and res[0]:
                await cur.execute(
                    "UPDATE aria_sanity SET sanity_level = GREATEST(0, sanity_level - 1) WHERE user_id = %s",
                    (message.author.id,),
                )
                await cur.execute("SELECT sanity_level FROM aria_sanity WHERE user_id = %s", (message.author.id,))
                sanity = (await cur.fetchone())[0]

                if sanity == 0:
                    await cur.execute("UPDATE aria_pacts SET pact_active = FALSE WHERE user_id = %s", (message.author.id,))
                    await cur.execute("UPDATE aria_affinity SET score = -100 WHERE user_id = %s", (message.author.id,))
                    await message.channel.send(
                        f"⚠️ **PSYCHOLOGICAL BREAK.** {message.author.mention}'s sanity has hit 0%. "
                        "The pact has devoured their mind. I am revoking my favor and placing them in an asylum (timeout)."
                    )
                    try:
                        import datetime as dt

                        await message.author.timeout(dt.timedelta(hours=1), reason="Sanity hit 0%")
                    except discord.Forbidden:
                        pass

    pact_group = app_commands.Group(name="pact", description="Enter or break dangerous bargains with Aria.")

    @pact_group.command(name="sign", description="Sign a dangerous pact for max affinity at a serious long-term cost.")
    async def sign_pact(self, interaction: discord.Interaction):
        async with db_cursor() as cur:
            await cur.execute("SELECT pact_active FROM aria_pacts WHERE user_id = %s", (interaction.user.id,))
            res = await cur.fetchone()
            if res and res[0]:
                return await interaction.response.send_message(
                    "You already sold your mind to me. You can't sell it twice.",
                    ephemeral=True,
                )

            await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (interaction.user.id,))
            await cur.execute("UPDATE aria_sanity SET sanity_level = 100 WHERE user_id = %s", (interaction.user.id,))
            await cur.execute("INSERT IGNORE INTO aria_affinity (user_id, score) VALUES (%s, 0)", (interaction.user.id,))
            await cur.execute("UPDATE aria_affinity SET score = 100 WHERE user_id = %s", (interaction.user.id,))
            await cur.execute(
                "INSERT INTO aria_pacts (user_id, pact_active) VALUES (%s, TRUE) ON DUPLICATE KEY UPDATE pact_active = TRUE",
                (interaction.user.id,),
            )

        embed = discord.Embed(
            title="📜 The Pact is Sealed",
            description=(
                f"{interaction.user.mention} just made a terrible mistake.\n\n"
                "**The Boon:** I have magically decided I love you (Affinity maxed). Your sanity is reset to 100%.\n"
                "**The Curse:** Every single time you type a message in this server, I drain 1% of your Sanity. When it hits 0, you break."
            ),
            color=discord.Color.dark_red(),
        )
        await interaction.response.send_message(embed=embed)

    @pact_group.command(name="break", description="Try to escape your pact by solving Aria's challenge.")
    async def break_pact(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        async with db_cursor() as cur:
            await cur.execute("SELECT pact_active FROM aria_pacts WHERE user_id = %s", (interaction.user.id,))
            res = await cur.fetchone()
            if not res or not res[0]:
                return await interaction.followup.send("You don't even have a pact. Stop wasting my time.")

        try:
            res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID,
                    contents="Generate a dark, extremely difficult riddle. Output a secret marker [ANSWER] followed by the exact one-word answer.",
                    config=types.GenerateContentConfig(system_instruction="You are Aria Blaze."),
                ),
            )
            riddle_part, answer_part = res.text.split("[ANSWER]")
            riddle, correct_answer = riddle_part.strip(), answer_part.strip().lower()
        except Exception as e:
            logger.error(f"Riddle Generation Error: {e}")
            return await interaction.followup.send("My riddle engine broke. Consider your mind lucky. Try again.")

        await interaction.followup.send(
            f"Want out? Answer this riddle in 30 seconds. Fail, and I instantly drain 50% of your Sanity.\n\n📜 **The Riddle:**\n*{riddle}*"
        )

        def check(m):
            return m.channel == interaction.channel and m.author.id == interaction.user.id

        try:
            msg = await self.bot.wait_for('message', timeout=30.0, check=check)
            if correct_answer in msg.content.lower():
                async with db_cursor() as cur:
                    await cur.execute("UPDATE aria_pacts SET pact_active = FALSE WHERE user_id = %s", (interaction.user.id,))
                await interaction.channel.send(f"You guessed it. The answer was **{correct_answer}**. Your mind is yours again.")
            else:
                raise asyncio.TimeoutError
        except asyncio.TimeoutError:
            async with db_cursor() as cur:
                await cur.execute(
                    "UPDATE aria_sanity SET sanity_level = GREATEST(0, sanity_level - 50) WHERE user_id = %s",
                    (interaction.user.id,),
                )
            await interaction.channel.send(
                f"Wrong. The answer was **{correct_answer}**. I just ripped 50% of your sanity away, and I still own your mind."
            )


async def setup(bot):
    await bot.add_cog(Pacts(bot))
