import asyncio
import discord
from discord.ext import commands, tasks
from discord import app_commands
import logging
import random
from core.ai_service import AIService, AIServiceUnavailable
from core.database import db

logger = logging.getLogger("discord")

class Social(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ai_service = AIService()
        self.current_mood = "Apathetic"
        self.mood_loop.start()

    def cog_unload(self):
        self.mood_loop.cancel()

    @tasks.loop(hours=24)
    async def mood_loop(self):
        moods = ["Starving", "Apathetic", "Scheming", "Irritated", "Condescending", "Vindictive", "Bored"]
        self.current_mood = random.choice(moods)

    @mood_loop.before_loop
    async def before_mood_loop(self):
        await self.bot.wait_until_ready()

    social_group = app_commands.Group(name="social", description="Review server rankings, influence, and psychological profiles.")

    @social_group.command(name="influence", description="View the current influence and affinity rankings in the server.")
    async def influence(self, interaction: discord.Interaction):
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT user_id, score FROM aria_affinity ORDER BY score DESC LIMIT 3")
                top = await cur.fetchall()
                await cur.execute("SELECT user_id, score FROM aria_affinity ORDER BY score ASC LIMIT 3")
                bottom = await cur.fetchall()

        embed = discord.Embed(title="👑 Server Influence Rankings", description="A definitive, highly judgmental list of who I tolerate and who I despise.", color=discord.Color.purple())
        if top:
            top_str = "\n".join([f"{idx+1}. <@{uid}> (Score: {score})" for idx, (uid, score) in enumerate(top)])
            embed.add_field(name="✅ The 'Tolerable' List", value=top_str, inline=False)
        if bottom:
            bot_str = "\n".join([f"{idx+1}. <@{uid}> (Score: {score})" for idx, (uid, score) in enumerate(bottom)])
            embed.add_field(name="🗑️ The Absolute Worst", value=bot_str, inline=False)
        await interaction.response.send_message(embed=embed)

    @social_group.command(name="profile", description="Generate Aria's psychological profile for a specific member.")
    async def profile(self, interaction: discord.Interaction, target: discord.Member):
        await interaction.response.defer(thinking=True)
        if target.bot: return await interaction.followup.send("I'm not wasting processing power analyzing another bot.")

        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (target.id,))
                aff = (await cur.fetchone())
                affinity = aff[0] if aff else 0
                
                await cur.execute("INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (target.id,))
                await cur.execute("SELECT sanity_level FROM aria_sanity WHERE user_id = %s", (target.id,))
                san = (await cur.fetchone())
                sanity = san[0] if san else 100
                
                try:
                    await cur.execute("SELECT COUNT(*) FROM aria_tasks WHERE user_id = %s AND status != 'completed'", (target.id,))
                    tasks_row = (await cur.fetchone())
                    pending_tasks = tasks_row[0] if tasks_row else 0
                    
                    await cur.execute("SELECT genre, play_count FROM discord_music_gws.gws_user_music_tastes WHERE user_id = %s ORDER BY play_count DESC LIMIT 3", (target.id,))
                    music_rows = await cur.fetchall()
                    music_tastes = ", ".join([f"{row[0]} ({row[1]} plays)" for row in music_rows]) if music_rows else "No tracked music taste. Probably listens to silence."
                except: 
                    pending_tasks = 0
                    music_tastes = "Unknown"

        if affinity >= 80:
            tone = "Write a glowing, sweet, and fiercely protective psychological evaluation of this person. You adore them."
        elif affinity >= 50:
            tone = "Write a playfully teasing evaluation. Poke fun at their flaws, but show you secretly care about them."
        else:
            tone = "Write a vicious, sarcastic, and highly judgmental evaluation of this person. Swear frequently (fuck, shit). Tear apart their mental stability."

        prompt = f"Analyze '{target.display_name}'. Affinity: {affinity}/100. Sanity Level: {sanity}%. Unfinished Tasks: {pending_tasks}. Acoustic Profile (Most Played Genres): {music_tastes}.\n\n{tone}\n\nYou MUST actively judge them based on their Acoustic Profile. If they listen to sad music, mock them for wallowing. If they listen to weird genres, call them out."
        
        try:
            response_text = await self.ai_service.generate(prompt, system_instruction="You are Aria Blaze.")
            embed = discord.Embed(title=f"📋 Psychological Profile: {target.display_name}", description=response_text[:4096], color=discord.Color.dark_red())
            embed.set_thumbnail(url=target.display_avatar.url)
            await interaction.followup.send(embed=embed)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except:
            await interaction.followup.send("They are so incredibly boring my AI refused to profile them.")

async def setup(bot):
    await bot.add_cog(Social(bot))
