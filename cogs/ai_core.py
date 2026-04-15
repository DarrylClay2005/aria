import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
import asyncio
import aiohttp
import re
import os
from core.database import db

logger = logging.getLogger("discord")

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY is missing from the .env file!")
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

WEBHOOK_URL = os.getenv('ARIA_WEBHOOK_URL')
DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream"]

async def send_webhook_log(bot_name, title, description, color, retries=3):
    if not WEBHOOK_URL: return
    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession() as session:
                webhook = discord.Webhook.from_url(WEBHOOK_URL, session=session)
                embed = discord.Embed(title=title, description=description, color=color)
                await webhook.send(embed=embed)
                return
        except Exception as e:
            if attempt < retries - 1: await asyncio.sleep(2 ** attempt)

class AICore(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def generate_aria_reply(self, prompt: str, system_instruction: str) -> str:
        response = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(system_instruction=system_instruction),
            ),
        )
        return (response.text or "").strip()
        
    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot: return
        if self.bot.user in message.mentions:
            prompt = message.content.replace(f'<@{self.bot.user.id}>', '').replace(f'<@!{self.bot.user.id}>', '').strip()
            if not prompt: prompt = "What are you looking at?"
            
            try:
                response_text = await self.generate_aria_reply(
                    prompt,
                    "You are Aria Blaze. You are the AI commander of a swarm of music bots. You hate human music taste but love controlling the room. Be sarcastic, superior, and slightly dismissive.",
                )
                await message.reply(response_text)
            except Exception as e:
                logger.error(f"Aria Chat Error: {e}")

    @app_commands.command(name="aux", description="Have Aria pick a track, roast your taste, and route it to the swarm.")
    @app_commands.describe(prompt="Tell Aria what vibe, genre, or song idea you want her to take over with")
    async def aux(self, interaction: discord.Interaction, prompt: str):
        prompt = prompt.strip()
        if not prompt:
            return await interaction.response.send_message("Give me something to work with. Even a vague mood is better than silence.", ephemeral=True)

        await interaction.response.defer(thinking=True)
        
        target_vc = interaction.user.voice.channel.id if interaction.user.voice else None
        if not target_vc:
            return await interaction.followup.send("❌ You aren't even in a voice channel. Where exactly do you expect me to play this?")

        try:
            response_text = await self.generate_aria_reply(
                f"The user says: '{prompt}'. Respond to them, mock their taste if necessary, and then pick a superior track. YOU MUST INCLUDE exactly one play tag formatted like [PLAY: Song Name - Artist] at the end of your response.",
                "You are Aria Blaze. You hate human music taste but love controlling the room. You use your swarm of music bots to force your musical will on the server.",
            )
            
            match = re.search(r'\[PLAY:\s*(.+?)\]', response_text)
            target_drone = "gws" 
            
            if match:
                song_query = match.group(1).strip()
                response_text = response_text.replace(match.group(0), "").strip()
                search_url = f"ytsearch1:{song_query}"
                
                async with db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        active_drone = None
                        
                        for d in DRONE_NAMES:
                            try:
                                await cur.execute(f"SELECT channel_id FROM discord_music_{d}.{d}_playback_state WHERE guild_id = %s AND is_playing = TRUE LIMIT 1", (interaction.guild_id,))
                                res_db = await cur.fetchone()
                                if res_db:
                                    active_drone = d
                                    target_vc = res_db['channel_id']
                                    break
                            except: pass
                        
                        if not active_drone:
                            for d in DRONE_NAMES:
                                try:
                                    await cur.execute(f"SELECT home_vc_id FROM discord_music_{d}.{d}_bot_home_channels WHERE guild_id = %s", (interaction.guild_id,))
                                    res_db = await cur.fetchone()
                                    if res_db and res_db['home_vc_id'] == target_vc:
                                        active_drone = d
                                        break
                                except: pass
                                
                        target_drone = active_drone or "gws"
                        
                        await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{target_drone}.{target_drone}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)")
                        await cur.execute(f"INSERT INTO discord_music_{target_drone}.{target_drone}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)", 
                            (target_drone, interaction.guild_id, target_vc, interaction.channel_id, "PLAY", search_url))
                        
            embed = discord.Embed(title="🎙️ Aria Takes the Aux", description=response_text[:4096], color=discord.Color.brand_red())
            if match:
                embed.set_footer(text=f"Swarm Link: Routed through node '{target_drone.capitalize()}'")
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Aux Error: {e}")
            await interaction.followup.send("My central matrix glitched out trying to process your request. Figure it out yourselves.")


    @app_commands.command(name="aria", description="Chat with Aria directly without mentioning her in the channel.")
    @app_commands.describe(prompt="What you want to say or ask Aria")
    async def aria(self, interaction: discord.Interaction, prompt: str):
        prompt = prompt.strip()
        if not prompt:
            return await interaction.response.send_message("Use your words. `/aria` needs an actual prompt.", ephemeral=True)

        await interaction.response.defer(thinking=True)
        try:
            reply = await self.generate_aria_reply(
                prompt,
                "You are Aria Blaze. You are the AI commander of a swarm of music bots. You hate human music taste but love controlling the room. Be sarcastic, superior, and slightly dismissive.",
            )
            embed = discord.Embed(title="Aria Blaze", description=reply[:4096], color=discord.Color.dark_purple())
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Aria Command Error: {e}")
            await interaction.followup.send("My neural net is currently refusing to process your garbage. Try again later.")

async def setup(bot):
    await bot.add_cog(AICore(bot))
