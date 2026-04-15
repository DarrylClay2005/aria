import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import aiohttp
import logging
import random

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

class BookClub(commands.Cog):
    def __init__(self, bot): self.bot = bot

    async def alter_sanity(self, user_id: int, amount: int):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,))
                    await cur.execute("UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s", (amount, user_id))

    bookclub_group = app_commands.Group(name="bookclub", description="Aria's mandatory cultural enrichment program")

    @bookclub_group.command(name="start", description="[ADMIN] Force the server to read a critically acclaimed manga")
    @app_commands.default_permissions(administrator=True)
    async def start_club(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        async with aiohttp.ClientSession() as session:
            async with session.get("https://api.jikan.moe/v4/top/manga") as resp:
                data = await resp.json()
                manga = random.choice(data['data'][:50])

        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("DELETE FROM aria_bookclub")
                    await cur.execute("DELETE FROM aria_bookclub_reviews")
                    await cur.execute("INSERT INTO aria_bookclub (title, mal_url, image_url) VALUES (%s, %s, %s)", (manga['title'], manga['url'], manga['images']['jpg']['large_image_url']))

        embed = discord.Embed(title=f"📖 MANDATORY READING: {manga['title']}", url=manga['url'], description=f"You people are culturally bankrupt.\n\n**ASSIGNMENT:** Submit a review using `/bookclub review`. Failure results in massive Sanity damage.", color=discord.Color.dark_magenta())
        embed.set_image(url=manga['images']['jpg']['large_image_url'])
        await interaction.followup.send(embed=embed)

    @bookclub_group.command(name="review", description="Submit your mandatory review to Aria")
    async def review(self, interaction: discord.Interaction, your_review: str):
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT title FROM aria_bookclub WHERE active = TRUE")
                    if not await cur.fetchone(): return await interaction.response.send_message("No active book club.", ephemeral=True)
                    await cur.execute("INSERT INTO aria_bookclub_reviews (user_id, review) VALUES (%s, %s) ON DUPLICATE KEY UPDATE review = %s", (interaction.user.id, your_review, your_review))
        await interaction.response.send_message("Review submitted.", ephemeral=True)

    @bookclub_group.command(name="conclude", description="[ADMIN] End book club, judge reviews, punish slackers")
    @app_commands.default_permissions(administrator=True)
    async def conclude_club(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=False)
        async with aiomysql.create_pool(**DB_CONFIG) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    await cur.execute("SELECT title FROM aria_bookclub WHERE active = TRUE")
                    book = await cur.fetchone()
                    if not book: return await interaction.followup.send("No active book club.")
                    
                    await cur.execute("SELECT user_id, review FROM aria_bookclub_reviews")
                    reviews = await cur.fetchall()
                    await cur.execute("SELECT user_id FROM aria_sanity")
                    all_users = [row['user_id'] for row in await cur.fetchall()]
                    slackers = [u for u in all_users if u not in [r['user_id'] for r in reviews]]
                    
                    for slacker in slackers: await self.alter_sanity(slacker, -30)
                    await cur.execute("UPDATE aria_bookclub SET active = FALSE")

        if not reviews: return await interaction.followup.send("Nobody submitted a review. Everyone loses 30% Sanity.")

        prompt = f"Manga: '{book['title']}'. Reviews: {reviews}. Pick the best review. Output EXACTLY: 'WINNER_ID: [ID]'"
        try:
            res = client.models.generate_content(model=MODEL_ID, contents=prompt, config=types.GenerateContentConfig(system_instruction="You are Aria Blaze. Be harsh."))
            winner_id = next((int(line.split(":")[1].strip()) for line in res.text.split("\n") if "WINNER_ID:" in line), None)
            
            if winner_id:
                await self.alter_sanity(winner_id, 50)
                reward_text = f"\n\n🏆 <@{winner_id}> had a tolerable take. I've restored 50% of their Sanity."
            else: reward_text = "\n\nAll takes were terrible."

            embed = discord.Embed(title=f"📚 Book Club Conclusion: {book['title']}", description=res.text[:4000] + reward_text, color=discord.Color.red())
            if slackers: embed.add_field(name="🗑️ Uncultured Swine (Suffered 30% Sanity Damage)", value=" ".join([f"<@{s}>" for s in slackers[:15]]), inline=False)
            await interaction.followup.send(embed=embed)
        except: await interaction.followup.send("Your reviews crashed my AI.")

async def setup(bot): await bot.add_cog(BookClub(bot))
