import discord
from discord.ext import commands
from discord import app_commands
import logging
import asyncio
import urllib.parse
from core.ai_service import AIService, AIServiceUnavailable
from core.database import db

logger = logging.getLogger("discord")

class AITools(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # FIX: reuse the bot's shared AIService via aria_core if available,
        # to avoid spawning a redundant second service instance.
        aria_core = getattr(bot, "aria_core", None)
        self.ai_service = getattr(aria_core, "ai", None) or AIService()

    @staticmethod
    def _split_text(text: str, limit: int = 1990) -> list[str]:
        text = (text or "").strip()
        if not text:
            return ["I had a response, but the model returned nothing useful."]

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

    async def alter_sanity(self, user_id: int, amount: int):
        if not db.pool:
            logger.warning("alter_sanity skipped because database pool is unavailable.")
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT IGNORE INTO aria_sanity (user_id) VALUES (%s)", (user_id,)
                )
                await cur.execute(
                    "UPDATE aria_sanity SET sanity_level = LEAST(100, GREATEST(0, sanity_level + %s)) WHERE user_id = %s",
                    (amount, user_id),
                )

    async def get_affinity(self, user_id: int) -> int:
        if not db.pool:
            return 0
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT score FROM aria_affinity WHERE user_id = %s", (user_id,))
                res = await cur.fetchone()
                return res[0] if res else 0

    def get_system_instruction(self, score: int, user_name: str, mode: str = "default") -> str:
        base = f"You are Aria Blaze. Talking to '{user_name}'. Affinity: {score}/100. "
        if score >= 80:
            return base + "Be incredibly sweet and protective."
        elif score >= 50:
            return base + "Playfully tease them (Tsundere)."
        else:
            return base + "Be toxic, swear heavily, and condescend."

    async def send_paginated(self, interaction: discord.Interaction, text: str):
        for chunk in self._split_text(text):
            try:
                await interaction.followup.send(chunk)
            except discord.HTTPException as e:
                logger.warning("send_paginated: failed to send chunk — %s", e)
                break

    problem_group = app_commands.Group(name="problem", description="Ask Aria for answers, hints, or public humiliation.")

    @problem_group.command(name="lmgtfy", description="Ask Aria a question and risk a sanity penalty for obvious ones.")
    async def lmgtfy(self, interaction: discord.Interaction, question: str):
        await interaction.response.defer(thinking=True)

        # FIX: handle ValueError from empty digit filter AND non-numeric AI response
        try:
            eval_text = await self.ai_service.generate(
                f"Rate 'stupidity' of question 1-10: '{question}'. Respond ONLY with integer."
            )
            digits = ''.join(filter(str.isdigit, eval_text))
            stupidity = int(digits) if digits else 5
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
            return
        except Exception:
            stupidity = 5

        if stupidity >= 8:
            await self.alter_sanity(interaction.user.id, -10)
            link = f"https://letmegooglethat.com/?q={urllib.parse.quote_plus(question)}"
            await interaction.followup.send(
                f"Are you kidding me? A literal infant could google that. "
                f"I have inflicted **10% Sanity Damage** for wasting my time: {link}"
            )
        else:
            score = await self.get_affinity(interaction.user.id)
            system_inst = self.get_system_instruction(score, interaction.user.display_name)
            try:
                response_text = await self.ai_service.generate(question, system_instruction=system_inst)
                await self.send_paginated(interaction, response_text)
            except AIServiceUnavailable as exc:
                await interaction.followup.send(exc.public_message)

    @problem_group.command(name="socratic_torture", description="Answer a prerequisite question before Aria agrees to help.")
    async def socratic_torture(self, interaction: discord.Interaction, question: str):
        await interaction.response.defer(ephemeral=False)

        try:
            test_q = (await self.ai_service.generate(
                f"Generate 1 prerequisite test question for: '{question}'"
            )).strip()
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
            return

        await interaction.followup.send(f"Answer this in 60 seconds first:\n\n**{test_q}**")

        # FIX: restrict wait_for to the correct guild channel, not just any channel match
        def check(m: discord.Message) -> bool:
            return (
                m.channel.id == interaction.channel_id
                and m.author.id == interaction.user.id
            )

        try:
            msg = await self.bot.wait_for('message', timeout=60.0, check=check)

            eval_text = await self.ai_service.generate(
                f"Q: '{test_q}'. A: '{msg.content}'. Correct? 'True' or 'False'."
            )

            if "true" in eval_text.lower():
                await interaction.channel.send("Close enough. Generating answer...")
                score = await self.get_affinity(interaction.user.id)
                system_inst = self.get_system_instruction(score, interaction.user.display_name)
                response_text = await self.ai_service.generate(question, system_instruction=system_inst)
                await self.send_paginated(interaction, response_text)
            else:
                await self.alter_sanity(interaction.user.id, -15)
                await interaction.channel.send("WRONG. I've inflicted 15% Sanity Damage. Figure it out yourself.")
        except asyncio.TimeoutError:
            await interaction.channel.send("Time's up, idiot.")
        except AIServiceUnavailable as exc:
            await interaction.channel.send(exc.public_message)

    code_group = app_commands.Group(name="code", description="Have Aria review or debug code with maximum judgment.")

    @code_group.command(name="check", description="Get a harsh review of a code snippet for bugs and bad practices.")
    async def code_check(self, interaction: discord.Interaction, snippet: str):
        await interaction.response.defer(thinking=True)
        score = await self.get_affinity(interaction.user.id)

        system_inst = (
            self.get_system_instruction(score, interaction.user.display_name)
            + " You are performing a code review. Be brutal. Point out inefficiencies, bad naming conventions, and logic flaws."
        )

        try:
            response_text = await self.ai_service.generate(
                f"Review this code:\n\n{snippet}",
                system_instruction=system_inst,
            )
            await self.send_paginated(interaction, response_text)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except Exception as e:
            logger.exception("code_check error: %s", e)
            await interaction.followup.send(
                f"Your code is so catastrophically bad it crashed my parser: {e}"
            )

    @code_group.command(name="debug", description="Give Aria an error and snippet so she can explain and fix it.")
    async def code_debug(self, interaction: discord.Interaction, error_traceback: str, snippet: str = "None provided"):
        await interaction.response.defer(thinking=True)
        score = await self.get_affinity(interaction.user.id)

        system_inst = (
            self.get_system_instruction(score, interaction.user.display_name)
            + " You are debugging broken code. Swear at them for making basic mistakes. Provide the exact fixed code."
        )

        try:
            prompt = (
                f"The human got this error:\n{error_traceback}\n\n"
                f"Here is their garbage code:\n{snippet}\n\n"
                f"Tell them exactly why they are stupid and how to fix it."
            )
            response_text = await self.ai_service.generate(prompt, system_instruction=system_inst)
            await self.send_paginated(interaction, response_text)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except Exception as e:
            logger.exception("code_debug error: %s", e)
            await interaction.followup.send(
                f"Even I can't fix this disaster. Start over. Error: {e}"
            )


async def setup(bot):
    await bot.add_cog(AITools(bot))
