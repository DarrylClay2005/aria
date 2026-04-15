import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
import asyncio

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

class Confessions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def alter_sanity(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s", (amount, user_id))

    async def get_sanity(self, user_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("SELECT sanity_level FROM aria_sanity WHERE user_id = %s", (user_id,))
                    res = await cur.fetchone()
                    return res[0] if res else 100

    drama_group = app_commands.Group(name="drama", description="Trade sanity for secrets, confessions, and server gossip.")

    @drama_group.command(name="confess", description="Submit a secret to Aria and recover a little sanity in return.")
    async def confess(self, interaction: discord.Interaction, secret: str):
        if len(secret) < 10: return await interaction.response.send_message("That secret is too short and boring.", ephemeral=True)
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO aria_secrets (user_id, secret) VALUES (%s, %s)", (interaction.user.id, secret))
        
        await self.alter_sanity(interaction.user.id, 10)
        await interaction.response.send_message("Secret secured. Feeding me drama appeases me, so I have restored 10% of your Sanity.", ephemeral=True)

    @drama_group.command(name="buy_leak", description="Spend sanity to force Aria to reveal a random confession.")
    async def buy_leak(self, interaction: discord.Interaction):
        sanity = await self.get_sanity(interaction.user.id)
        if sanity < 30:
            return await interaction.response.send_message("You need at least 30% Sanity to stare into my vault without losing your mind completely.", ephemeral=True)

        await interaction.response.defer(ephemeral=False)
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT id, secret FROM aria_secrets WHERE exposed = FALSE ORDER BY RAND() LIMIT 1")
                    secret_row = await cur.fetchone()
                    if not secret_row: return await interaction.followup.send("My vault is currently empty.")
                    
                    await cur.execute("UPDATE aria_secrets SET exposed = TRUE WHERE id = %s", (secret_row['id'],))
                    
        await self.alter_sanity(interaction.user.id, -30)
        raw_secret = secret_row['secret']
        prompt = f"Rewrite this leaked secret dramatically: '{raw_secret}'. Mock whoever submitted it."
        
        try:
            res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: client.models.generate_content(
                    model=MODEL_ID,
                    contents=prompt,
                    config=types.GenerateContentConfig(system_instruction="You are Aria Blaze.")
                )
            )
            embed = discord.Embed(title="👁️ THE VAULT HAS OPENED", description=res.text[:4000], color=discord.Color.magenta())
            embed.set_footer(text=f"Leak funded by {interaction.user.display_name}. They suffered 30% Sanity Damage for this.")
            await interaction.followup.send(embed=embed)
        except:
            await interaction.followup.send(f"Raw leak: \"{raw_secret}\"")

async def setup(bot):
    await bot.add_cog(Confessions(bot))
