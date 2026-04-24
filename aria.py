import discord
from discord.ext import commands
import logging
import asyncio
from discord import app_commands

# ================================
# 🧠 ARIA SYSTEM IMPORTS
# ================================
from aria.aria_core import AriaCore
from aria.aria_monitor import Monitor
from core.override import override_manager
from core.database import db
from core.settings import BOT_ENV_PREFIX, COGS_DIR, OVERRIDE_USER_ID, TOKEN
from core.webhooks import install_error_reporting, install_loop_exception_handler, send_error_webhook_log, send_ops_webhook_log, send_webhook_log
from core.event_bus import EventBus

# --- LOGGING SETUP ---
file_handler = logging.FileHandler(filename="aria_core.log", encoding="utf-8", mode="a")
discord.utils.setup_logging(handler=file_handler, level=logging.INFO)
logger = logging.getLogger("discord")

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
        self.aria_core = AriaCore()
        self.event_bus = EventBus(self)
        self.monitor = Monitor(self, self.event_bus)
        self.monitor_task = None

    async def setup_hook(self):
        # 🟢 Boot the global DB pool BEFORE cogs load
        await db.connect()
        db.patch_legacy_create_pool()
        await self.aria_core.initialize()
        await self.event_bus.initialize()

        if not COGS_DIR.exists():
            COGS_DIR.mkdir(parents=True, exist_ok=True)
            logger.warning("Created missing cogs directory.")

        failed_extensions = []
        loaded_extensions = []
        cog_files = [path for path in sorted(COGS_DIR.glob("*.py")) if not path.name.startswith("_")]
        for cog_file in cog_files:
            extension = f"cogs.{cog_file.stem}"
            try:
                await self.load_extension(extension)
                loaded_extensions.append(cog_file.name)
                logger.info("🟢 Successfully loaded core module: %s", cog_file.name)
            except Exception:
                failed_extensions.append(cog_file.name)
                logger.exception("🔴 Failed to load module %s", cog_file.name)

        try:
            await self.tree.sync()
            logger.info("📡 Aria's global slash commands have been synced!")
        except Exception:
            logger.exception("Slash command sync failed during setup_hook; continuing startup.")
        logger.info("🧩 Loaded %s/%s cog modules.", len(loaded_extensions), len(cog_files))
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
@bot.event
async def on_ready():
    install_error_reporting()
    install_loop_exception_handler()
    logger.info(f'🤖 Aria Intelligence Core is online and operating as {bot.user}')
    await send_webhook_log(
        "Aria Online",
        f"Aria Intelligence Core is online and operating as {bot.user}.",
        color=discord.Color.brand_green(),
    )

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="My show. Fuck Off"
        ),
        status=discord.Status.dnd
    )

    # 🧠 Start/recreate autonomous monitor.
    # Discord may fire on_ready again after a reconnect; keep a live task, but
    # recreate it if the old one was cancelled/finished during the disconnect.
    task = bot.monitor_task
    if task is not None and not task.done():
        logger.info("Aria monitor task is already running; not spawning a duplicate.")
    else:
        if task is not None and task.cancelled():
            logger.warning("Aria monitor task was cancelled; recreating it after reconnect.")
        elif task is not None:
            exc = task.exception() if not task.cancelled() else None
            if exc:
                logger.warning("Aria monitor task exited with error; recreating it: %r", exc)
            else:
                logger.info("Aria monitor task finished; recreating it after reconnect.")
        bot.monitor_task = asyncio.create_task(bot.monitor.start(), name="aria-monitor")


def should_run_aria_core(message: discord.Message) -> bool:
    content = message.content.strip().lower()
    if not content:
        return False

    if content.startswith(bot.command_prefix):
        return False
    return content.startswith("aria ")

# ================================
# 🧠 MAIN MESSAGE HANDLER
# ================================
@bot.event
async def on_message(message):
    if message.author.bot:
        return

    # FIX: process_commands first so prefix commands are not also fed to aria_core
    await bot.process_commands(message)
    await bot.aria_core.observe_message(message)

    if not should_run_aria_core(message):
        return

    # 🧠 ARIA PROCESSING (SAFE LAYER)
    try:
        response = await bot.aria_core.handle(message, message.content)

        if response:
            await message.channel.send(response)

    except Exception as e:
        logger.exception("[ARIA ERROR] %s", e)
        await send_error_webhook_log("Aria Message Handler Error", str(e), traceback_text="".join(__import__("traceback").format_exception(type(e), e, e.__traceback__)))


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    original = getattr(error, "original", error)
    logger.exception("Slash command failed: %s", original)
    await send_error_webhook_log("Aria Slash Command Error", str(original), traceback_text="".join(__import__("traceback").format_exception(type(original), original, original.__traceback__)))

    msg = "That command crashed before it finished. I've logged the error so it can actually be fixed."
    try:
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
    except discord.HTTPException:
        pass


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    # FIX: don't log CommandNotFound — it's noisy and expected
    if isinstance(error, commands.CommandNotFound):
        return
    logger.exception("Prefix command failed: %s", error)
    await send_error_webhook_log("Aria Prefix Command Error", str(error), traceback_text="".join(__import__("traceback").format_exception(type(error), error, error.__traceback__)))
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
