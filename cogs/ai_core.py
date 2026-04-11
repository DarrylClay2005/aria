import discord
from discord.ext import commands
from discord import app_commands
from google import genai
from google.genai import types
import aiomysql
import logging
from datetime import datetime
import asyncio
import aiohttp
import re

logger = logging.getLogger("discord")
DB_CONFIG = {'host': '127.0.0.1', 'user': 'botuser', 'password': 'botpassword', 'db': 'discord_aria', 'autocommit': True}
GEMINI_API_KEY = 'AIzaSyBe-PsYYalYB4Tum-vCmqj-N9m6MsfTL2k'
client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_ID = 'gemini-2.5-flash'

# --- SWARM MONITORING WEBHOOK ---
WEBHOOK_URL = 'https://discord.com/api/webhooks/1489706270775705611/cS6KKuSThfFFMnobsFmyIL-3hRPdRhKe7eFyJ7pkeWDNVt_bAmZ6JUiiQHRt8QPx5yCH'
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
        
    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot: return
        if self.bot.user in message.mentions:
            prompt = message.clean_content.replace(f'@{self.bot.user.name}', '').strip()
            if not prompt: prompt = "What are you looking at?"
            
            try:
                res = await asyncio.get_event_loop().run_in_executor(None, lambda: client.models.generate_content(
                    model=MODEL_ID,
                    contents=prompt, 
                    config=types.GenerateContentConfig(system_instruction="You are Aria Blaze. You are the AI commander of a swarm of music bots. You hate human music taste but love controlling the room. Be sarcastic, superior, and slightly dismissive.")
                ))
                response_text = res.text
                await message.reply(response_text)
            except Exception as e:
                logger.error(f"Aria Chat Error: {e}")

    @app_commands.command(name="aux", description="Ask Aria to hijack the speakers and pick a song for you.")
    async def aux(self, interaction: discord.Interaction, prompt: str):
        await interaction.response.defer()
        
        target_vc = interaction.user.voice.channel.id if interaction.user.voice else None
        if not target_vc:
            return await interaction.followup.send("❌ You aren't even in a voice channel. Where exactly do you expect me to play this?")

        try:
            # Generate the response and enforce the [PLAY: song] format
            res = await asyncio.get_event_loop().run_in_executor(None, lambda: client.models.generate_content(
                model=MODEL_ID,
                contents=f"The user says: '{prompt}'. Respond to them, mock their taste if necessary, and then pick a superior track. YOU MUST INCLUDE exactly one play tag formatted like [PLAY: Song Name - Artist] at the end of your response.", 
                config=types.GenerateContentConfig(system_instruction="You are Aria Blaze. You hate human music taste but love controlling the room. You use your swarm of music bots to force your musical will on the server.")
            ))
            response_text = res.text
            
            # Extract the embedded PLAY order
            match = re.search(r'\[PLAY:\s*(.+?)\]', response_text)
            target_drone = "gws" # Default fallback
            
            if match:
                song_query = match.group(1).strip()
                response_text = response_text.replace(match.group(0), "").strip()
                search_url = f"ytsearch1:{song_query}"
                
                # --- DYNAMIC CROSS-DATABASE ROUTER ---
                async with aiomysql.create_pool(host='127.0.0.1', user='botuser', password='botpassword', autocommit=True) as pool:
                    async with pool.acquire() as conn:
                        async with conn.cursor(aiomysql.DictCursor) as cur:
                            active_drone = None
                            
                            # Phase 1: Check if any bot is already playing in the server
                            for d in DRONE_NAMES:
                                try:
                                    await cur.execute(f"SELECT channel_id FROM discord_music_{d}.{d}_playback_state WHERE guild_id = %s AND is_playing = TRUE LIMIT 1", (interaction.guild_id,))
                                    res_db = await cur.fetchone()
                                    if res_db:
                                        active_drone = d
                                        target_vc = res_db['channel_id']
                                        break
                                except: pass
                            
                            # Phase 2: If no bot is playing, check who owns the home channel
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
                            
                            # Inject the order directly into the targeted bot's quarantine table
                            await cur.execute(f"CREATE TABLE IF NOT EXISTS discord_music_{target_drone}.{target_drone}_swarm_direct_orders (id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)")
                            await cur.execute(f"INSERT INTO discord_music_{target_drone}.{target_drone}_swarm_direct_orders (bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)", 
                                (target_drone, interaction.guild_id, target_vc, interaction.channel_id, "PLAY", search_url))
                # -------------------------------------
                            
            embed = discord.Embed(title="🎙️ Aria Takes the Aux", description=response_text[:4096], color=discord.Color.brand_red())
            if match:
                embed.set_footer(text=f"Swarm Link: Routed through node '{target_drone.capitalize()}'")
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            logger.error(f"Aux Error: {e}")
            await interaction.followup.send("My central matrix glitched out trying to process your request. Figure it out yourselves.")

async def setup(bot):
    await bot.add_cog(AICore(bot))