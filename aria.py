import discord
from discord.ext import commands
import os
import logging

# --- LOGGING SETUP ---
file_handler = logging.FileHandler(filename="aria_core.log", encoding="utf-8", mode="a")
discord.utils.setup_logging(handler=file_handler, level=logging.INFO)
logger = logging.getLogger("discord")

# --- CONFIGURATION ---
BOT_ENV_PREFIX = "ARIA"
TOKEN = os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_TOKEN", "").strip()

class AriaBot(commands.Bot):
    def __init__(self):
        super().__init__(
            assume_unsync_clock=False, 
            max_ratelimit_timeout=60.0, 
            chunk_guilds_at_startup=False, 
            command_prefix="a!", 
            intents=discord.Intents.all() # Aria needs ALL intents to monitor the Swarm environment
        )

    async def setup_hook(self):
        # Dynamically load the Swarm Admin router and AI Core matrices from the cogs folder
        if not os.path.exists('./cogs'):
            os.makedirs('./cogs')
            logger.warning("Created missing './cogs' directory.")

        for filename in os.listdir('./cogs'):
            if filename.endswith('.py'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    logger.info(f"🟢 Successfully loaded core module: {filename}")
                except Exception as e:
                    logger.error(f"🔴 Failed to load module {filename}: {e}")
        
        # Sync slash commands to Discord
        await self.tree.sync()
        logger.info("📡 Aria's global slash commands have been synced to the Discord matrix!")

bot = AriaBot()

@bot.event
async def on_ready():
    logger.info(f'🤖 Aria Intelligence Core is online and operating as {bot.user}')
    # Set Aria's status to show she is commanding the network
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching, 
            name="the 8-Node Swarm Network"
        ),
        status=discord.Status.dnd
    )

if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError(f"Missing required environment variable: {BOT_ENV_PREFIX}_DISCORD_TOKEN")
    bot.run(TOKEN)