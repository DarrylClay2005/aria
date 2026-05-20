import discord
from discord.ext import commands
import logging
import asyncio
import os
import time
from types import SimpleNamespace
from discord import app_commands

# ================================
# 🧠 ARIA SYSTEM IMPORTS
# ================================
from aria.aria_core import AriaCore, DISCORD_CHAT_SYSTEM_INSTRUCTION, TELEGRAM_CHAT_SYSTEM_INSTRUCTION
from aria.aria_monitor import Monitor
from core.override import override_manager
from core.database import db
from core.chat_attachments import MAX_CHAT_ATTACHMENTS, build_chat_upload_prompt, prepare_chat_uploads
from core.settings import BOT_ENV_PREFIX, COGS_DIR, OVERRIDE_USER_ID, TELEGRAM_ALLOWED_CHAT_IDS, TELEGRAM_BOT_TOKEN, TELEGRAM_POLLING_ENABLED, TOKEN
from core.telegram_bridge import TelegramBridge
from core.webhooks import close_http_session, install_error_reporting, install_loop_exception_handler, send_error_webhook_log, send_webhook_log
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
        self.heartbeat_task = None
        self.health_watch_task = None
        self.telegram_bridge = None
        self.aria_chat_semaphore = asyncio.Semaphore(max(1, int(os.getenv("ARIA_CHAT_CONCURRENCY", "3") or "3")))
        self._last_ready_webhook_at = 0.0
        self._telegram_alert_last: dict[str, float] = {}

    async def setup_hook(self):
        # 🟢 Boot the global DB pool BEFORE cogs load
        await db.connect()
        db.patch_legacy_create_pool()
        await self.aria_core.initialize()
        await self.event_bus.initialize()
        await self.ensure_heartbeat_schema()

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

        if TELEGRAM_POLLING_ENABLED and TELEGRAM_BOT_TOKEN:
            self.telegram_bridge = TelegramBridge(
                token=TELEGRAM_BOT_TOKEN,
                name="aria",
                handler=self.handle_telegram_update,
                allowed_chat_ids=TELEGRAM_ALLOWED_CHAT_IDS,
                commands=[
                    ("chat",      "Chat with Aria directly"),
                    ("play",      "Play a track: /play <song or query>"),
                    ("pause",     "Pause playback"),
                    ("resume",    "Resume playback"),
                    ("skip",      "Skip the current track"),
                    ("stop",      "Stop playback and clear the queue"),
                    ("queue",     "Show the current queue"),
                    ("shuffle",   "Shuffle the queue"),
                    ("broadcast", "Broadcast a track to all nodes: /broadcast <query>"),
                    ("radar",     "Show live swarm node status"),
                    ("recommend", "Get a smart track recommendation"),
                    ("heal",      "Run Aria self-diagnostics"),
                    ("status",    "Show Aria runtime status"),
                    ("id",        "Show this Telegram chat id"),
                    ("help",      "Show available commands"),
                ],
            )
            await self.telegram_bridge.start()
            await self.notify_telegram_operator(
                "Aria online",
                "Telegram bridge, heartbeat, and health watcher are available.",
                key="startup",
                cooldown=0.0,
            )

    async def close(self):
        # 🟢 Cleanly close long-lived resources when the bot shuts down.
        if self.telegram_bridge:
            await self.telegram_bridge.close()
        if self.heartbeat_task and not self.heartbeat_task.done():
            self.heartbeat_task.cancel()
            try:
                await self.heartbeat_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Aria heartbeat task raised while shutting down.")
        if self.health_watch_task and not self.health_watch_task.done():
            self.health_watch_task.cancel()
            try:
                await self.health_watch_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Aria health watch task raised while shutting down.")
        try:
            await self.write_heartbeat("offline")
        except Exception:
            logger.exception("Failed to mark Aria heartbeat offline during shutdown.")
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Aria monitor task raised while shutting down.")
        await close_http_session()
        await db.close()
        await super().close()

    async def ensure_heartbeat_schema(self):
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS swarm_health (
                        bot_name VARCHAR(50) PRIMARY KEY,
                        status VARCHAR(50) NOT NULL DEFAULT 'online',
                        last_pulse TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            ON UPDATE CURRENT_TIMESTAMP
                    )
                    """
                )

    async def write_heartbeat(self, status: str = "online"):
        if not db.pool:
            return
        await self.ensure_heartbeat_schema()
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO swarm_health (bot_name, status, last_pulse)
                    VALUES ('aria', %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        status = VALUES(status),
                        last_pulse = VALUES(last_pulse)
                    """,
                    (status,),
                )

    async def heartbeat_loop(self):
        interval = max(10.0, float(os.getenv("ARIA_HEARTBEAT_INTERVAL_SECONDS", "30") or "30"))
        while not self.is_closed():
            try:
                await self.write_heartbeat("online")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Aria heartbeat update failed.")
            await asyncio.sleep(interval)

    async def health_watch_loop(self):
        interval = max(60.0, float(os.getenv("ARIA_HEALTH_WATCH_INTERVAL_SECONDS", "300") or "300"))
        cooldown = max(300.0, float(os.getenv("ARIA_HEALTH_ALERT_COOLDOWN_SECONDS", "900") or "900"))
        last_alerts: dict[str, float] = {}
        await asyncio.sleep(20)
        while not self.is_closed():
            try:
                if not db.pool:
                    raise RuntimeError("Aria database pool is not connected")
                async with db.pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                        await cur.fetchone()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now = time.monotonic()
                if now - last_alerts.get("db", 0.0) >= cooldown:
                    last_alerts["db"] = now
                    await send_error_webhook_log("Aria Database Health Alert", str(exc), traceback_text=None)
                    await self.notify_telegram_operator(
                        "Aria database problem",
                        str(exc),
                        key="db-health",
                        cooldown=cooldown,
                    )
                try:
                    await db.connect()
                except Exception:
                    logger.exception("Aria database reconnect failed after health watch alert.")
            await asyncio.sleep(interval)

    async def notify_telegram_operator(self, title: str, detail: str, *, key: str, cooldown: float | None = None) -> None:
        if not self.telegram_bridge or not TELEGRAM_ALLOWED_CHAT_IDS:
            return
        interval = cooldown if cooldown is not None else max(300.0, float(os.getenv("ARIA_TELEGRAM_ALERT_COOLDOWN_SECONDS", "900") or "900"))
        now = time.monotonic()
        if now - self._telegram_alert_last.get(key, 0.0) < interval:
            return
        self._telegram_alert_last[key] = now
        message = f"{title}\n{str(detail or '').strip()[:1200]}"
        for chat_id in sorted(TELEGRAM_ALLOWED_CHAT_IDS):
            try:
                await self.telegram_bridge.send_message(chat_id, message)
            except Exception:
                logger.debug("Aria Telegram operator alert failed for chat %s.", chat_id, exc_info=True)

    # ARIA LIVE DATA FIX: Telegram needs a Discord guild context to read queues.
    def _resolve_telegram_guild(self, chat_id: int | None = None):
        env_keys = (
            "ARIA_TELEGRAM_DEFAULT_GUILD_ID",
            "TELEGRAM_DEFAULT_GUILD_ID",
            "ARIA_DEFAULT_GUILD_ID",
            "DISCORD_GUILD_ID",
        )
        for key in env_keys:
            raw = str(os.getenv(key, "") or "").strip()
            if not raw:
                continue
            try:
                guild_id = int(raw)
            except ValueError:
                continue
            guild = self.get_guild(guild_id)
            return guild, guild_id

        guilds = list(getattr(self, "guilds", []) or [])
        if len(guilds) == 1:
            return guilds[0], int(guilds[0].id)
        return (guilds[0], int(guilds[0].id)) if guilds else (None, None)

    def _telegram_chat_is_trusted(self, chat_id: int | None) -> bool:
        try:
            normalized = int(chat_id or 0)
        except (TypeError, ValueError):
            normalized = 0
        bridge_allowed = set(getattr(self.telegram_bridge, "allowed_chat_ids", set()) or set()) if self.telegram_bridge else set()
        if bridge_allowed:
            return normalized in bridge_allowed
        raw_admins = str(os.getenv("ARIA_TELEGRAM_ADMIN_CHAT_IDS", "") or "")
        if raw_admins:
            allowed = set()
            for chunk in raw_admins.replace(";", ",").split(","):
                chunk = chunk.strip()
                if not chunk:
                    continue
                try:
                    allowed.add(int(chunk))
                except ValueError as exc:
                    logger.debug("Ignoring invalid ARIA_TELEGRAM_ADMIN_CHAT_IDS entry %r: %s", chunk, exc)
            return normalized in allowed
        return str(os.getenv("ARIA_TELEGRAM_TRUST_ALL_CHATS", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}

    def _telegram_fake_context(self, chat_id: int, user_id: int, user_name: str):
        guild, guild_id = self._resolve_telegram_guild(chat_id)
        channel_id_raw = str(os.getenv("ARIA_TELEGRAM_DEFAULT_CHANNEL_ID", "") or "").strip()
        vc_id_raw = str(os.getenv("ARIA_TELEGRAM_DEFAULT_VC_ID", "") or "").strip()
        try:
            channel_id = int(channel_id_raw) if channel_id_raw else None
        except ValueError:
            channel_id = None
        try:
            vc_id = int(vc_id_raw) if vc_id_raw else None
        except ValueError:
            vc_id = None

        voice_channel = SimpleNamespace(id=vc_id) if vc_id else None
        voice_state = SimpleNamespace(channel=voice_channel) if voice_channel else None
        fake_actor = SimpleNamespace(
            id=int(user_id or 0),
            display_name=user_name,
            name=user_name,
            bot=False,
            voice=voice_state,
            guild_permissions=SimpleNamespace(administrator=self._telegram_chat_is_trusted(chat_id)),
        )
        fake_channel = SimpleNamespace(id=channel_id) if channel_id else None
        return SimpleNamespace(
            author=fake_actor,
            user=fake_actor,
            guild=guild,
            guild_id=guild_id,
            channel=fake_channel,
            channel_id=channel_id,
            bot=self,
            client=self,
        )

    async def handle_telegram_update(self, event: dict) -> str | None:
        text = str(event.get("text") or "").strip()
        if not text:
            return None
        chat_id = int(event.get("chat_id") or 0)
        message = event.get("message") or {}
        sender = message.get("from") or {}
        user_id = int(sender.get("id") or chat_id or 0)
        user_name = str(sender.get("username") or sender.get("first_name") or f"telegram-{user_id}")
        command = text.split(maxsplit=1)[0].split("@", 1)[0].lower()

        if command in {"/start", "/help"}:
            return (
                "Aria's on Telegram. Here's what I respond to:\n"
                "/play <query> — queue a track\n"
                "/pause · /resume · /skip · /stop — playback controls\n"
                "/queue — see what's queued\n"
                "/shuffle — shuffle the queue\n"
                "/broadcast <query> — blast a track to all nodes\n"
                "/radar — live swarm node status\n"
                "/recommend — smart track suggestion\n"
                "/heal — run self-diagnostics\n"
                "/status — bot runtime status\n"
                "/id — your Telegram chat id\n"
                "/chat <prompt> — talk to Aria directly\n"
                "Or just send me text and I'll figure it out."
            )

        if command == "/id":
            return f"Telegram chat id: {chat_id}"

        if command == "/status":
            monitor_live = bool(self.monitor_task and not self.monitor_task.done())
            telegram_status = self.telegram_bridge.status if self.telegram_bridge else None
            username = telegram_status.bot_username if telegram_status else ""
            return (
                "Aria runtime status:\n"
                f"Discord: {'online' if self.is_ready() else 'starting'}\n"
                f"Database: {'connected' if db.is_connected else 'not connected'}\n"
                f"Autonomous monitor: {'running' if monitor_live else 'stopped'}\n"
                f"Telegram bot: @{username or 'unknown'}"
            )

        # ── Swarm playback commands ──────────────────────────────────────────
        _arg = text.split(maxsplit=1)[1].strip() if len(text.split(maxsplit=1)) > 1 else ""

        if command == "/play":
            if not _arg:
                return "Give me something to play. /play <song or query>"
            prompt = f"play {_arg}"
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or f"Queued: {_arg}"
            finally:
                self.aria_chat_semaphore.release()

        if command in {"/pause", "/resume", "/skip", "/stop"}:
            action = command.lstrip("/")
            prompt = f"{action} {_arg}".strip() if _arg else action
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or f"{action.capitalize()} sent to the swarm."
            finally:
                self.aria_chat_semaphore.release()

        if command == "/queue":
            prompt = f"queue {_arg}".strip() if _arg else "queue"
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or "Queue is empty or the node didn't respond."
            finally:
                self.aria_chat_semaphore.release()

        if command == "/shuffle":
            prompt = f"shuffle {_arg}".strip() if _arg else "shuffle"
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or "Shuffled."
            finally:
                self.aria_chat_semaphore.release()

        if command == "/broadcast":
            if not _arg:
                return "Tell me what to broadcast. /broadcast <query>"
            prompt = f"broadcast {_arg}"
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or f"Broadcast queued: {_arg}"
            finally:
                self.aria_chat_semaphore.release()

        if command == "/radar":
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, "radar")
                return routed or "Radar returned nothing. The swarm might be quiet."
            finally:
                self.aria_chat_semaphore.release()

        if command == "/recommend":
            prompt = f"recommend {_arg}".strip() if _arg else "recommend"
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, prompt)
                return routed or "No recommendation ready yet. Play more music first."
            finally:
                self.aria_chat_semaphore.release()

        if command == "/heal":
            fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
            try:
                await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
            except TimeoutError:
                return "Handling another request right now. Try again in a moment."
            try:
                routed = await self.aria_core.handle(fake_ctx, "fix")
                return routed or "Diagnostics ran. No critical issues found."
            finally:
                self.aria_chat_semaphore.release()

        # ── /chat explicit or plain text fallthrough ─────────────────────────
        prompt = text
        if command == "/chat":
            prompt = _arg
        if not prompt:
            return "Give me a prompt after /chat."
        fake_ctx = self._telegram_fake_context(chat_id, user_id, user_name)
        maybe_command = prompt[5:].strip() if prompt.lower().startswith("aria ") else prompt

        try:
            await asyncio.wait_for(self.aria_chat_semaphore.acquire(), timeout=0.25)
        except TimeoutError:
            return "I am already handling a few Aria requests. Try again in a moment."

        try:
            routed = await self.aria_core.handle(fake_ctx, maybe_command)
            if routed:
                return routed
            return await self.aria_core.chat(
                maybe_command,
                system_instruction=TELEGRAM_CHAT_SYSTEM_INSTRUCTION,
                user_id=user_id,
                guild_id=getattr(fake_ctx, "guild_id", None),
                user_name=user_name,
                source_kind="telegram_chat",
                response_style="telegram_chat",
            )
        finally:
            self.aria_chat_semaphore.release()

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
    now = time.monotonic()
    ready_webhook_cooldown = max(30.0, float(os.getenv("ARIA_READY_WEBHOOK_COOLDOWN_SECONDS", "300") or "300"))
    if now - bot._last_ready_webhook_at >= ready_webhook_cooldown:
        bot._last_ready_webhook_at = now
        await send_webhook_log(
            "Aria Online",
            f"Aria Intelligence Core is online and operating as {bot.user}.",
            color=discord.Color.brand_green(),
        )

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Over your dumbasses"
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
            try:
                exc = task.exception() if not task.cancelled() else None
            except asyncio.CancelledError:
                exc = None
            if exc:
                logger.warning("Aria monitor task exited with error; recreating it: %r", exc)
            else:
                logger.info("Aria monitor task finished; recreating it after reconnect.")
        bot.monitor_task = asyncio.create_task(bot.monitor.start(), name="aria-monitor")

    heartbeat_task = bot.heartbeat_task
    if heartbeat_task is None or heartbeat_task.done():
        bot.heartbeat_task = asyncio.create_task(bot.heartbeat_loop(), name="aria-heartbeat")
    health_watch_task = bot.health_watch_task
    if health_watch_task is None or health_watch_task.done():
        bot.health_watch_task = asyncio.create_task(bot.health_watch_loop(), name="aria-health-watch")


async def send_discord_safely(channel, text: str, *, limit: int = 1900):
    payload = str(text or "").strip()
    if not payload:
        return
    chunks = []
    while payload:
        chunks.append(payload[:limit])
        payload = payload[limit:]
    for chunk in chunks[:6]:
        await channel.send(chunk)


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
        prompt = message.content.strip()
        if prompt.lower().startswith("aria "):
            prompt = prompt[5:].strip()
        if not prompt:
            prompt = "What?"

        try:
            await asyncio.wait_for(bot.aria_chat_semaphore.acquire(), timeout=0.25)
        except TimeoutError:
            await send_discord_safely(message.channel, "I’m already handling a few Aria requests. Try again in a moment.")
            return

        try:
            response = await bot.aria_core.handle(message, prompt)
        finally:
            bot.aria_chat_semaphore.release()

        if response:
            await send_discord_safely(message.channel, response)
            return

        selected_uploads = list(message.attachments or [])
        fresh_uploads = await prepare_chat_uploads(selected_uploads) if selected_uploads else []
        active_uploads = []
        try:
            if fresh_uploads:
                await bot.aria_core.learning.store_chat_uploads(
                    user_id=int(message.author.id),
                    guild_id=message.guild.id if message.guild else None,
                    channel_id=message.channel.id if message.channel else None,
                    message_id=message.id,
                    uploads=fresh_uploads,
                    ttl_seconds=300,
                )
            active_uploads = await bot.aria_core.learning.active_chat_uploads(
                user_id=int(message.author.id),
                guild_id=message.guild.id if message.guild else None,
                channel_id=message.channel.id if message.channel else None,
                limit=MAX_CHAT_ATTACHMENTS,
            )
        except Exception:
            logger.exception("Failed to store/load Aria prefix upload context; using current attachments only.")
            active_uploads = fresh_uploads

        contextual_prompt, attachment_context_note, direct_attachment = build_chat_upload_prompt(prompt, active_uploads)
        direct_attachment = direct_attachment or {}
        try:
            await asyncio.wait_for(bot.aria_chat_semaphore.acquire(), timeout=0.25)
        except TimeoutError:
            await send_discord_safely(message.channel, "I’m already handling a few Aria requests. Try again in a moment.")
            return

        try:
            reply = await bot.aria_core.chat(
            contextual_prompt,
            system_instruction=DISCORD_CHAT_SYSTEM_INSTRUCTION,
            user_id=message.author.id,
            guild_id=message.guild.id if message.guild else None,
            channel_id=message.channel.id if message.channel else None,
            user_name=message.author.display_name,
            source_kind="prefix_chat",
            response_style="prefix_chat",
            attachment_bytes=direct_attachment.get("attachment_bytes"),
            attachment_name=direct_attachment.get("attachment_name"),
            attachment_mime_type=direct_attachment.get("attachment_mime_type"),
            attachment_context_note=attachment_context_note,
            )
        finally:
            bot.aria_chat_semaphore.release()
        await send_discord_safely(message.channel, reply)

    except ValueError as e:
        await send_discord_safely(message.channel, str(e))
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
    except discord.HTTPException as exc:
        logger.debug("Could not send slash-command failure response: %s", exc)


@bot.event
async def on_command_error(ctx: commands.Context, error: commands.CommandError):
    # FIX: don't log CommandNotFound — it's noisy and expected
    if isinstance(error, commands.CommandNotFound):
        return
    logger.exception("Prefix command failed: %s", error)
    await send_error_webhook_log("Aria Prefix Command Error", str(error), traceback_text="".join(__import__("traceback").format_exception(type(error), error, error.__traceback__)))
    try:
        await ctx.send("That command died mid-flight. Check the logs and fix the stack trace.")
    except discord.HTTPException as exc:
        logger.debug("Could not send prefix-command failure response: %s", exc)

# ================================
# 🔑 START BOT
# ================================
if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError(f"Missing required environment variable: {BOT_ENV_PREFIX}_DISCORD_TOKEN")

    bot.run(TOKEN)
