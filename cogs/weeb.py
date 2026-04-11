import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import aiohttp
import logging

logger = logging.getLogger("discord")
DB_CONFIG = {'host': '127.0.0.1', 'user': 'botuser', 'password': 'botpassword', 'db': 'discord_aria', 'autocommit': True}
GEMINI_API_KEY = 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k'
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

class Weeb(commands.Cog):
    def __init__(self, bot): self.bot = bot

    async def alter_sanity(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s", (amount, user_id))

    async def get_affinity(self, user_id: int) -> int:
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                    res = await cur.fetchone()
                    return res[0] if res else 0

    weeb_group = app_commands.Group(name="weeb", description="Aria ruthlessly judges your anime tastes")

    @weeb_group.command(name="degenerate", description="Random explicit anime + Sanity Damage")
    async def degenerate(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.jikan.moe/v4/random/anime?rating=rx") as resp:
                data = (await resp.json())['data']

        score = await self.get_affinity(interaction.user.id)
        sys_inst = f"You are Aria Blaze. Talk to {interaction.user.display_name}. Affinity: {score}. Roast their degenerate taste."
        res = client.models.generate_content(model=MODEL_ID, contents=f"Plot: {data.get('synopsis', 'None')}", config=types.GenerateContentConfig(system_instruction=sys_inst))
        
        embed = discord.Embed(title=f"🔞 {data.get('title')}", url=data.get('url'), description=res.text[:4096], color=discord.Color.brand_red())
        if 'images' in data: embed.set_image(url=data['images']['jpg']['large_image_url'])
        await interaction.followup.send(embed=embed)
        
        if score < 0:
            await self.alter_sanity(interaction.user.id, -10)
            await interaction.channel.send(f"*(Aria inflicted 10% Sanity Damage on {interaction.user.mention} for being gross)*")

async def setup(bot): await bot.add_cog(Weeb(bot))
