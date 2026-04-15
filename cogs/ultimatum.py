import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
import asyncio
from datetime import timedelta

logger = logging.getLogger("discord")
import os
DB_CONFIG = {
    'host': os.getenv('ARIA_DB_HOST', '127.0.0.1'),
    'user': os.getenv('ARIA_DB_USER', 'botuser'),
    'password': os.getenv('ARIA_DB_PASSWORD', 'swarmpanel'),
    'db': os.getenv('ARIA_DB_NAME', 'discord_aria'),
    'autocommit': True
}
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k')
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

class DilemmaView(discord.ui.View):
    def __init__(self, user_a: discord.Member, user_b: discord.Member):
        super().__init__(timeout=300)
        self.user_a = user_a
        self.user_b = user_b
        self.choices = {user_a.id: None, user_b.id: None}

    async def handle_choice(self, interaction: discord.Interaction, choice: str):
        if interaction.user.id not in self.choices: return await interaction.response.send_message("Mind your own business.", ephemeral=True)
        if self.choices[interaction.user.id] is not None: return await interaction.response.send_message("Choice locked.", ephemeral=True)
        self.choices[interaction.user.id] = choice
        await interaction.response.send_message(f"You chose to **{choice}**.", ephemeral=True)
        if self.choices[self.user_a.id] is not None and self.choices[self.user_b.id] is not None: self.stop()

    @discord.ui.button(label="Cooperate (Trust)", style=discord.ButtonStyle.green, emoji="🤝")
    async def btn_cooperate(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_choice(interaction, "cooperate")

    @discord.ui.button(label="Betray (Damage Mind)", style=discord.ButtonStyle.red, emoji="🔪")
    async def btn_betray(self, interaction: discord.Interaction, button: discord.ui.Button): await self.handle_choice(interaction, "betray")

class Ultimatum(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def alter_sanity(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s", (amount, user_id))

    event_group = app_commands.Group(name="event", description="Aria's server-wide psychological experiments")

    @event_group.command(name="trolley_problem", description="[ADMIN] Force two users into a Prisoner's Dilemma for their Sanity")
    @app_commands.default_permissions(administrator=True)
    async def trolley_problem(self, interaction: discord.Interaction, user_a: discord.Member, user_b: discord.Member):
        await interaction.response.defer(ephemeral=False)
        prompt = f"Forcing {user_a.display_name} and {user_b.display_name} into a Prisoner's Dilemma. Coop/Coop = +10 Sanity. Betray = Steal 30 Sanity, Victim loses 50 Sanity. Double Betray = Both lose 40 Sanity. Introduce this game cynically."
        try:
            res = client.models.generate_content(model=MODEL_ID, contents=prompt, config=types.GenerateContentConfig(system_instruction="You are Aria Blaze."))
            intro_text = res.text
        except: intro_text = f"{user_a.mention} {user_b.mention}. Betray or Cooperate. Your minds are on the line."

        view = DilemmaView(user_a, user_b)
        msg = await interaction.followup.send(content=f"{user_a.mention} {user_b.mention}\n\n{intro_text}", view=view)
        await view.wait()
        for child in view.children: child.disabled = True
        await msg.edit(view=view)

        c_a, c_b = view.choices[user_a.id], view.choices[user_b.id]
        if c_a is None or c_b is None: return await interaction.channel.send("Experiment canceled. Someone didn't answer.")

        if c_a == "cooperate" and c_b == "cooperate":
            await self.alter_sanity(user_a.id, 10)
            await self.alter_sanity(user_b.id, 10)
            await interaction.channel.send("You both cooperated? Boring. You both restore 10% Sanity.")
        elif c_a == "betray" and c_b == "betray":
            await self.alter_sanity(user_a.id, -40)
            await self.alter_sanity(user_b.id, -40)
            try:
                await user_a.timeout(timedelta(minutes=30))
                await user_b.timeout(timedelta(minutes=30))
            except: pass
            await interaction.channel.send("🩸 **DOUBLE BETRAYAL.** You both lose 40% Sanity and are timed out for 30 minutes. Beautiful.")
        else:
            betrayer = user_a if c_a == "betray" else user_b
            victim = user_b if c_a == "betray" else user_a
            await self.alter_sanity(betrayer.id, 30)
            await self.alter_sanity(victim.id, -50)
            try: await victim.timeout(timedelta(minutes=30))
            except: pass
            await interaction.channel.send(f"🔪 **BLOODBATH.** {betrayer.display_name} restores 30% Sanity. {victim.mention} loses 50% Sanity and goes to timeout.")

async def setup(bot):
    await bot.add_cog(Ultimatum(bot))
