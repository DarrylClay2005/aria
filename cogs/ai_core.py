import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import re
import io
import os
from aria.aria_core import AriaCore, DEFAULT_CHAT_SYSTEM_INSTRUCTION
from core.ai_service import AIServiceUnavailable
from core.database import db
from core.webhooks import send_error_webhook_log

logger = logging.getLogger("discord")

DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream", "alucard", "sapphire"]
FILE_REQUEST_RE = re.compile(r"\b(updated|fixed|corrected|patched)\s+(file|code)\b|\b(send|upload|return|give)\b.*\b(file|code|it)\b", re.IGNORECASE)
AFFIRM_ONLY_RE = re.compile(r"^(yes|yeah|yep|sure|please do|do it|send it|upload it|return it|i would|yes please)\b", re.IGNORECASE)
TEXT_MIME_PREFIXES = ("text/",)
TEXT_MIME_EXACT = {
    "application/json",
    "application/javascript",
    "application/xml",
    "application/yaml",
    "application/x-yaml",
}
MAX_CHAT_ATTACHMENT_BYTES = max(100_000, int(os.getenv("ARIA_CHAT_ATTACHMENT_MAX_BYTES", "6000000") or "6000000"))

class AICore(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.aria_core = getattr(bot, "aria_core", AriaCore())

    @staticmethod
    def _split_text(text: str, limit: int = 1990) -> list[str]:
        text = (text or "").strip()
        if not text:
            return ["I had a response ready, but the model returned nothing useful."]

        chunks = []
        remaining = text
        while len(remaining) > limit:
            split_at = remaining.rfind("\n", 0, limit)
            if split_at <= 0:
                split_at = remaining.rfind(" ", 0, limit)
            if split_at <= 0:
                split_at = limit
            chunks.append(remaining[:split_at].strip())
            remaining = remaining[split_at:].strip()
        if remaining:
            chunks.append(remaining)
        return chunks

    async def _reply_chunked(self, message: discord.Message, text: str):
        chunks = self._split_text(text)
        first_message = None
        for index, chunk in enumerate(chunks):
            if index == 0:
                first_message = await message.reply(chunk)
            else:
                await message.channel.send(chunk, reference=first_message or message)

    async def _send_followup_chunked(
        self,
        interaction: discord.Interaction,
        text: str,
        *,
        title: str = "Aria Blaze",
        color: discord.Color | None = None,
    ):
        color = color or discord.Color.dark_purple()
        chunks = self._split_text(text, limit=4096)
        for index, chunk in enumerate(chunks):
            if index == 0:
                embed = discord.Embed(title=title, description=chunk, color=color)
            else:
                embed = discord.Embed(description=chunk, color=color)
            await interaction.followup.send(embed=embed)

    @staticmethod
    def _looks_text_like(attachment: discord.Attachment) -> bool:
        content_type = (attachment.content_type or "").lower()
        if any(content_type.startswith(prefix) for prefix in TEXT_MIME_PREFIXES):
            return True
        if content_type in TEXT_MIME_EXACT:
            return True
        filename = (attachment.filename or "").lower()
        return filename.endswith(
            (
                ".txt",
                ".md",
                ".py",
                ".js",
                ".ts",
                ".tsx",
                ".jsx",
                ".json",
                ".yaml",
                ".yml",
                ".toml",
                ".ini",
                ".cfg",
                ".conf",
                ".log",
                ".csv",
                ".html",
                ".css",
                ".xml",
            )
        )

    async def _prepare_chat_attachment(
        self,
        attachment: discord.Attachment | None,
    ) -> tuple[bytes | None, str | None, str | None, str | None]:
        if attachment is None:
            return None, None, None, None
        if attachment.size > MAX_CHAT_ATTACHMENT_BYTES:
            raise ValueError(
                f"`{attachment.filename}` is too large for `/aria` context here. Keep it under {MAX_CHAT_ATTACHMENT_BYTES // 1000} KB."
            )
        payload = await attachment.read()
        if not payload:
            raise ValueError(f"`{attachment.filename}` was empty.")

        content_type = (attachment.content_type or "").strip().lower()
        if content_type.startswith("image/"):
            note = f"image attachment `{attachment.filename}`"
            return payload, content_type, attachment.filename, note

        if self._looks_text_like(attachment):
            try:
                text = payload.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    text = payload.decode("latin-1")
                except UnicodeDecodeError as exc:
                    raise ValueError(f"`{attachment.filename}` could not be decoded as readable text.") from exc
            cleaned = text.strip()
            if not cleaned:
                raise ValueError(f"`{attachment.filename}` was empty.")
            if len(cleaned) > 12000:
                cleaned = cleaned[:11950].rstrip() + "\n\n[Attachment truncated for chat context]"
            note = f"text attachment `{attachment.filename}`"
            return cleaned.encode("utf-8"), "text/plain", attachment.filename, note

        raise ValueError(
            f"`{attachment.filename}` is not a supported `/aria` context file yet. Send an image or a readable text file."
        )

    async def _maybe_deliver_pending_code_file(self, ctx, prompt: str):
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        if not actor:
            return False
        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        artifact = await self.aria_core.learning.latest_file_artifact(
            user_id=int(actor.id),
            guild_id=guild_id,
            require_pending=True,
        )
        if not artifact:
            return False

        lowered = (prompt or "").strip().lower()
        wants_file = bool(FILE_REQUEST_RE.search(prompt or "")) or bool(AFFIRM_ONLY_RE.search(lowered))
        if not wants_file:
            return False

        filename = artifact.get("filename") or "updated_file.txt"
        payload = io.BytesIO(str(artifact.get("current_code") or "").encode("utf-8"))
        discord_file = discord.File(payload, filename=f"fixed_{filename}")
        message = "Fine. Here's the corrected file you asked for. Try not to break it again immediately."

        if isinstance(ctx, discord.Message):
            await ctx.reply(message, file=discord_file)
        else:
            embed = discord.Embed(title="Aria Blaze", description=message, color=discord.Color.dark_purple())
            await ctx.followup.send(embed=embed, file=discord_file)

        artifact_id = int(artifact.get("id") or 0)
        if artifact_id:
            await self.aria_core.learning.consume_file_offer(artifact_id=artifact_id)
        return True

    async def generate_aria_reply(
        self,
        prompt: str,
        system_instruction: str | None,
        *,
        ctx=None,
        source_kind: str = "chat",
        attachment_bytes: bytes | None = None,
        attachment_name: str | None = None,
        attachment_mime_type: str | None = None,
        attachment_context_note: str | None = None,
    ) -> str:
        actor = getattr(ctx, "author", None) or getattr(ctx, "user", None)
        guild = getattr(ctx, "guild", None)
        guild_id = guild.id if guild else getattr(ctx, "guild_id", None)
        user_name = getattr(actor, "display_name", None) if actor else None
        user_id = getattr(actor, "id", None) if actor else None
        return await self.aria_core.chat(
            prompt,
            system_instruction=system_instruction,
            user_id=user_id,
            guild_id=guild_id,
            user_name=user_name,
            source_kind=source_kind,
            response_style=source_kind,
            attachment_bytes=attachment_bytes,
            attachment_name=attachment_name,
            attachment_mime_type=attachment_mime_type,
            attachment_context_note=attachment_context_note,
        )

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        if self.bot.user not in message.mentions:
            return

        prompt = (
            message.content
            .replace(f'<@{self.bot.user.id}>', '')
            .replace(f'<@!{self.bot.user.id}>', '')
            .strip()
        )
        if not prompt:
            prompt = "What are you looking at?"

        try:
            if await self._maybe_deliver_pending_code_file(message, prompt):
                return

            control_result = await self.aria_core.handle(message, prompt)
            if control_result:
                await self._reply_chunked(message, control_result)
                return

            response_text = await self.generate_aria_reply(
                prompt,
                DEFAULT_CHAT_SYSTEM_INSTRUCTION,
                ctx=message,
                source_kind="mention_chat",
            )
            await self._reply_chunked(
                message,
                response_text or "I had a response ready, but the model returned nothing useful.",
            )
        except AIServiceUnavailable as exc:
            logger.warning("Aria mention reply unavailable: %s", exc)
            await self._reply_chunked(message, exc.public_message)
        except Exception as e:
            logger.exception("Aria Chat Error: %s", e)
            await send_error_webhook_log("Aria Chat Error", str(e), traceback_text="".join(__import__("traceback").format_exception(type(e), e, e.__traceback__)))

    @app_commands.command(name="aux", description="Have Aria pick a track, roast your taste, and route it to the swarm.")
    @app_commands.describe(prompt="Tell Aria what vibe, genre, or song idea you want her to take over with")
    async def aux(self, interaction: discord.Interaction, prompt: str):
        prompt = prompt.strip()
        if not prompt:
            return await interaction.response.send_message(
                "Give me something to work with. Even a vague mood is better than silence.", ephemeral=True
            )

        await interaction.response.defer(thinking=True)

        # FIX: guard against voice being set but channel being None (e.g. stage channels)
        voice_state = interaction.user.voice
        target_vc = voice_state.channel.id if (voice_state and voice_state.channel) else None
        if not target_vc:
            return await interaction.followup.send(
                "❌ You aren't even in a voice channel. Where exactly do you expect me to play this?"
            )

        try:
            response_text = await self.generate_aria_reply(
                f"The user says: '{prompt}'. Respond to them, mock their taste if necessary, and then pick a superior track. "
                "YOU MUST INCLUDE exactly one play tag formatted like [PLAY: Song Name - Artist] at the end of your response.",
                DEFAULT_CHAT_SYSTEM_INSTRUCTION
                + " When taking the aux, stay focused on music selection and include exactly one [PLAY: Song Name - Artist] tag.",
                ctx=interaction,
                source_kind="aux_chat",
            )

            match = re.search(r'\[PLAY:\s*(.+?)\]', response_text)
            target_drone = "gws"

            if match:
                song_query = match.group(1).strip()
                response_text = response_text.replace(match.group(0), "").strip()
                search_url = f"ytmsearch:{song_query}"

                async with db.pool.acquire() as conn:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        active_drone = None

                        for d in DRONE_NAMES:
                            try:
                                await cur.execute(
                                    f"SELECT channel_id FROM discord_music_{d}.{d}_playback_state "
                                    f"WHERE guild_id = %s AND is_playing = TRUE LIMIT 1",
                                    (interaction.guild_id,),
                                )
                                res_db = await cur.fetchone()
                                if res_db:
                                    active_drone = d
                                    target_vc = res_db['channel_id']
                                    break
                            except Exception as e:
                                logger.debug("aux: playback_state lookup failed for drone %s — %s", d, e)

                        if not active_drone:
                            for d in DRONE_NAMES:
                                try:
                                    await cur.execute(
                                        f"SELECT home_vc_id FROM discord_music_{d}.{d}_bot_home_channels "
                                        f"WHERE guild_id = %s",
                                        (interaction.guild_id,),
                                    )
                                    res_db = await cur.fetchone()
                                    if res_db and res_db['home_vc_id'] == target_vc:
                                        active_drone = d
                                        break
                                except Exception as e:
                                    logger.debug("aux: home_channels lookup failed for drone %s — %s", d, e)

                        target_drone = active_drone or "gws"

                        await cur.execute(
                            f"CREATE TABLE IF NOT EXISTS discord_music_{target_drone}.{target_drone}_swarm_direct_orders "
                            f"(id INT AUTO_INCREMENT PRIMARY KEY, bot_name VARCHAR(50), guild_id BIGINT, "
                            f"vc_id BIGINT, text_channel_id BIGINT, command VARCHAR(50), data TEXT)"
                        )
                        await cur.execute(
                            f"INSERT INTO discord_music_{target_drone}.{target_drone}_swarm_direct_orders "
                            f"(bot_name, guild_id, vc_id, text_channel_id, command, data) VALUES (%s, %s, %s, %s, %s, %s)",
                            (target_drone, interaction.guild_id, target_vc, interaction.channel_id, "PLAY", search_url),
                        )

            embed = discord.Embed(
                title="🎙️ Aria Takes the Aux",
                description=response_text[:4096],
                color=discord.Color.brand_red(),
            )
            if match:
                embed.set_footer(text=f"Swarm Link: Routed through node '{target_drone.capitalize()}'")
            await interaction.followup.send(embed=embed)

        except AIServiceUnavailable as exc:
            logger.warning("Aux unavailable: %s", exc)
            await interaction.followup.send(exc.public_message)
        except Exception as e:
            logger.exception("Aux Error: %s", e)
            await send_error_webhook_log("Aria Aux Error", str(e), traceback_text="".join(__import__("traceback").format_exception(type(e), e, e.__traceback__)))
            await interaction.followup.send(
                "My central matrix glitched out trying to process your request. Figure it out yourselves."
            )

    @app_commands.command(name="aria", description="Chat with Aria directly without mentioning her in the channel.")
    @app_commands.describe(
        prompt="What you want to say or ask Aria",
        file="Optional screenshot or readable file for extra context",
    )
    async def aria(self, interaction: discord.Interaction, prompt: str, file: discord.Attachment | None = None):
        prompt = prompt.strip()
        if not prompt:
            return await interaction.response.send_message(
                "Use your words. `/aria` needs an actual prompt.", ephemeral=True
            )

        await interaction.response.defer(thinking=True)
        try:
            if await self._maybe_deliver_pending_code_file(interaction, prompt):
                return

            control_result = await self.aria_core.handle(interaction, prompt)
            if control_result:
                await self._send_followup_chunked(interaction, control_result)
                return

            attachment_bytes = None
            attachment_mime_type = None
            attachment_name = None
            attachment_context_note = None
            if file is not None:
                (
                    attachment_bytes,
                    attachment_mime_type,
                    attachment_name,
                    attachment_context_note,
                ) = await self._prepare_chat_attachment(file)

            reply = await self.generate_aria_reply(
                prompt,
                DEFAULT_CHAT_SYSTEM_INSTRUCTION,
                ctx=interaction,
                source_kind="slash_chat",
                attachment_bytes=attachment_bytes,
                attachment_name=attachment_name,
                attachment_mime_type=attachment_mime_type,
                attachment_context_note=attachment_context_note,
            )
            await self._send_followup_chunked(interaction, reply)
        except ValueError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
        except AIServiceUnavailable as exc:
            logger.warning("Aria command unavailable: %s", exc)
            await interaction.followup.send(exc.public_message)
        except Exception as e:
            logger.exception("Aria Command Error: %s", e)
            await interaction.followup.send(
                "My neural net is currently refusing to process your garbage. Try again later."
            )


async def setup(bot):
    await bot.add_cog(AICore(bot))
