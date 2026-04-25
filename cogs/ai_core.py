import discord
from discord.ext import commands
from discord import app_commands
import aiomysql
import logging
import asyncio
import re
from aria.aria_core import AriaCore
from core.ai_service import AIServiceUnavailable
from core.database import db
from core.webhooks import send_webhook_log, send_error_webhook_log

logger = logging.getLogger("discord")

DRONE_NAMES = ["gws", "harmonic", "maestro", "melodic", "nexus", "rhythm", "symphony", "tunestream"]

class AICore(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.aria_core = getattr(bot, "aria_core", AriaCore())

    async def generate_aria_reply(self, prompt: str, system_instruction: str, *, ctx=None) -> str:
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
            control_result = await self.aria_core.handle(message, prompt)
            if control_result:
                await message.reply(control_result)
                return

            response_text = await self.generate_aria_reply(
                prompt,
                "You are Aria Blaze. You are the AI commander of a swarm of music bots. "
                "You hate human music taste but love controlling the room. Be sarcastic, superior, and slightly dismissive.",
                ctx=message,
            )
            await message.reply(response_text or "I had a response ready, but the model returned nothing useful.")
        except AIServiceUnavailable as exc:
            logger.warning("Aria mention reply unavailable: %s", exc)
            await message.reply(exc.public_message)
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
                "You are Aria Blaze. You hate human music taste but love controlling the room. "
                "You use your swarm of music bots to force your musical will on the server.",
                ctx=interaction,
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
    @app_commands.describe(prompt="What you want to say or ask Aria")
    async def aria(self, interaction: discord.Interaction, prompt: str):
        prompt = prompt.strip()
        if not prompt:
            return await interaction.response.send_message(
                "Use your words. `/aria` needs an actual prompt.", ephemeral=True
            )

        await interaction.response.defer(thinking=True)
        try:
            control_result = await self.aria_core.handle(interaction, prompt)
            if control_result:
                embed = discord.Embed(
                    title="Aria Blaze", description=control_result[:4096], color=discord.Color.dark_purple()
                )
                await interaction.followup.send(embed=embed)
                return

            reply = await self.generate_aria_reply(
                prompt,
                "You are Aria Blaze. You are the AI commander of a swarm of music bots. "
                "You hate human music taste but love controlling the room. Be sarcastic, superior, and slightly dismissive.",
                ctx=interaction,
            )
            embed = discord.Embed(
                title="Aria Blaze", description=reply[:4096], color=discord.Color.dark_purple()
            )
            await interaction.followup.send(embed=embed)
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
