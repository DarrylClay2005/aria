import discord
from discord.ext import commands
import os
import logging
import asyncio
from pathlib import Path
from dotenv import load_dotenv
from discord import app_commands

# 🟢 LOAD LOCAL .ENV FILES FIRST
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")
load_dotenv(BASE_DIR.parent / ".env")

# ================================
# 🧠 ARIA SYSTEM IMPORTS
# ================================
from aria.aria_core import AriaCore
from aria.aria_monitor import Monitor
from core.override import override_manager
from core.database import db  

# --- LOGGING SETUP ---
file_handler = logging.FileHandler(filename="aria_core.log", encoding="utf-8", mode="a")
discord.utils.setup_logging(handler=file_handler, level=logging.INFO)
logger = logging.getLogger("discord")

# --- CONFIGURATION ---
BOT_ENV_PREFIX = "ARIA"
TOKEN = os.getenv(f"{BOT_ENV_PREFIX}_DISCORD_TOKEN")
OVERRIDE_USER_ID = os.getenv("ARIA_OVERRIDE_USER_ID", "1304564041863266347")

# ================================
# 🤖 BOT CLASS
# ================================
class AriaBot(commands.Bot):
    def __init__(self):
        super().__init__(
            assume_unsync_clock=False,
            max_ratelimit_timeout=60.0,
            chunk_guilds_at_startup=False,
            command_prefix="a!",
            intents=discord.Intents.all()
        )
        self.monitor_task = None

    async def setup_hook(self):
        # 🟢 Boot the global DB pool BEFORE cogs load
        await db.connect()
        db.patch_legacy_create_pool()

        if not os.path.exists('./cogs'):
            os.makedirs('./cogs')
            logger.warning("Created missing './cogs' directory.")

        failed_extensions = []
        for filename in sorted(os.listdir('./cogs')):
            if filename.endswith('.py'):
                try:
                    await self.load_extension(f'cogs.{filename[:-3]}')
                    logger.info(f"🟢 Successfully loaded core module: {filename}")
                except Exception as e:
                    failed_extensions.append(filename)
                    logger.error(f"🔴 Failed to load module {filename}: {e}")

        await self.tree.sync()
        logger.info("📡 Aria's global slash commands have been synced!")
        if failed_extensions:
            logger.warning("⚠️ Extensions with load failures: %s", ", ".join(failed_extensions))

        try:
            override_manager.enable_override(int(OVERRIDE_USER_ID))
        except ValueError:
            logger.warning("Invalid ARIA_OVERRIDE_USER_ID value: %s", OVERRIDE_USER_ID)

    async def close(self):
        # 🟢 Cleanly close the DB pool when the bot shuts down
        await db.close()
        await super().close()

# ================================
# 🚀 BOT INIT
# ================================
bot = AriaBot()

# ================================
# 🧠 ARIA INTELLIGENCE LAYER
# ================================
aria = AriaCore()
monitor = Monitor(bot)

# ================================
# 🚀 ON READY
# ================================
@bot.event
async def on_ready():
    logger.info(f'🤖 Aria Intelligence Core is online and operating as {bot.user}')

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="My show. Fuck Off"
        ),
        status=discord.Status.dnd
    )

    # 🧠 Start autonomous system once
    if bot.monitor_task is None or bot.monitor_task.done():
        bot.monitor_task = asyncio.create_task(monitor.start())


def should_run_aria_core(message: discord.Message) -> bool:
    content = message.content.strip().lower()
    if not content:
        return False

    if content.startswith(bot.command_prefix):
        return False

    legacy_triggers = (
        "aria disable auto",
        "aria enable auto",
        "aria play",
        "aria pause",
        "aria skip",
        "aria stop",
        "aria fix",
    )
    return content.startswith(legacy_triggers)

# ================================
# 🧠 MAIN MESSAGE HANDLER
# ================================
@bot.event
async def on_message(message):
    if message.author.bot:
        return

    await bot.process_commands(message)

    if not should_run_aria_core(message):
        return

    # 🧠 ARIA PROCESSING (SAFE LAYER)
    try:
        response = await aria.handle(message, message.content)

        if response:
            await message.channel.send(response)

    except Exception as e:
        logger.exception("[ARIA ERROR] %s", e)


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    original = getattr(error, "original", error)
    logger.exception("Slash command failed: %s", original)

    message = "That command crashed before it finished. I've logged the error so it can actually be fixed."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    logger.exception("Prefix command failed: %s", error)
    try:
        await ctx.send("That command died mid-flight. Check the logs and fix the stack trace.")
    except discord.HTTPException:
        pass

# ================================
# 🔑 START BOT
# ================================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError(f"Missing required environment variable: {BOT_ENV_PREFIX}_DISCORD_TOKEN")

    bot.run(TOKEN)
