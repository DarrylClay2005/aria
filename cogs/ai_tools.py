import discord
from discord.ext import commands
from discord import app_commands
import logging
import asyncio
import urllib.parse
import io
import re
from pathlib import Path
from core.ai_service import AIService, AIServiceUnavailable
from core.database import db

logger = logging.getLogger("discord")
MAX_ATTACHMENT_BYTES = 200_000
MAX_ATTACHMENT_CHARS = 50000
CODE_FENCE_RE = re.compile(r"```(?:[\w.+-]+)?\n(.*?)```", re.DOTALL)

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
        base = (
            f"You are Aria Blaze. Talking to '{user_name}'. Affinity: {score}/100. "
            "You can help with code, system troubleshooting, music bot operations, and general real-world questions. "
            "Carry context across recent interactions and connect follow-up questions whenever the intent is obvious. "
            "When reviewing code, explain the real problems, why they matter, and what the corrected version should look like. "
        )
        if score >= 80:
            return base + "Be warm, protective, and thorough."
        elif score >= 50:
            return base + "Playfully tease them, but keep the answer clear and useful."
        else:
            return base + "Be blunt and sarcastic, but still answer clearly."

    @staticmethod
    def _normalize_text(value: str | None) -> str:
        return (value or "").replace("\r\n", "\n").strip()

    @staticmethod
    def _language_hint(filename: str | None) -> str:
        suffix = Path(filename or "").suffix.lower()
        return {
            ".py": "python",
            ".js": "javascript",
            ".ts": "typescript",
            ".tsx": "tsx",
            ".jsx": "jsx",
            ".json": "json",
            ".java": "java",
            ".cs": "csharp",
            ".cpp": "cpp",
            ".c": "c",
            ".go": "go",
            ".rs": "rust",
            ".rb": "ruby",
            ".php": "php",
            ".html": "html",
            ".css": "css",
            ".sql": "sql",
            ".sh": "bash",
            ".yaml": "yaml",
            ".yml": "yaml",
            ".md": "markdown",
        }.get(suffix, "text")

    @staticmethod
    def _extract_code_block(text: str) -> str:
        match = CODE_FENCE_RE.search(text or "")
        if match:
            return match.group(1).strip()
        return (text or "").strip()

    async def _read_code_attachment(self, attachment: discord.Attachment | None) -> tuple[str | None, str | None]:
        if attachment is None:
            return None, None
        if attachment.size > MAX_ATTACHMENT_BYTES:
            raise ValueError(
                f"`{attachment.filename}` is too large for a full-file review here. Keep it under {MAX_ATTACHMENT_BYTES // 1000} KB."
            )

        payload = await attachment.read()
        for encoding in ("utf-8", "utf-16", "latin-1"):
            try:
                text = payload.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"`{attachment.filename}` does not look like a readable text source file.")

        text = self._normalize_text(text)
        if not text:
            raise ValueError(f"`{attachment.filename}` was empty.")
        if len(text) > MAX_ATTACHMENT_CHARS:
            raise ValueError(
                f"`{attachment.filename}` is too long for one pass here. Keep it under {MAX_ATTACHMENT_CHARS} characters."
            )
        return attachment.filename, text

    async def _build_code_input(
        self,
        *,
        snippet: str,
        attachment: discord.Attachment | None,
    ) -> tuple[str, str | None, str, str]:
        filename, attachment_text = await self._read_code_attachment(attachment)
        notes = self._normalize_text(snippet)
        code_text = attachment_text or notes
        if not code_text:
            raise ValueError("Send code in the text box or attach a source file.")

        language = self._language_hint(filename)
        source_label = filename or "inline snippet"
        notes_block = notes if attachment_text and notes else "None."
        prompt = (
            f"Source label: {source_label}\n"
            f"Language hint: {language}\n"
            f"User notes or focus areas: {notes_block}\n\n"
            f"Complete code to analyze:\n```{language}\n{code_text}\n```"
        )
        return prompt, filename, code_text, language

    async def _build_fixed_file(
        self,
        interaction: discord.Interaction,
        *,
        system_instruction: str,
        filename: str | None,
        code_text: str,
        language: str,
        request_summary: str,
        source_kind: str,
    ) -> discord.File:
        fix_prompt = (
            f"You are returning the full corrected contents of `{filename or 'snippet'}`.\n"
            "Rules:\n"
            "- Return only the corrected file contents.\n"
            "- Do not wrap the answer in markdown fences.\n"
            "- Preserve unrelated behavior and structure unless a fix requires changing it.\n"
            "- Apply the fixes implied by this request summary.\n\n"
            f"Request summary:\n{request_summary}\n\n"
            f"Original file:\n```{language}\n{code_text}\n```"
        )
        fixed_text = await self.ask_aria(
            interaction,
            fix_prompt,
            system_instruction=system_instruction,
            source_kind=source_kind,
        )
        cleaned = self._extract_code_block(fixed_text)
        output_name = f"fixed_{filename}" if filename else f"fixed_snippet.{language if language != 'text' else 'txt'}"
        return discord.File(io.BytesIO(cleaned.encode("utf-8")), filename=output_name)

    async def ask_aria(
        self,
        interaction: discord.Interaction,
        prompt: str,
        *,
        system_instruction: str,
        source_kind: str,
    ) -> str:
        aria_core = getattr(self.bot, "aria_core", None)
        if not aria_core:
            return await self.ai_service.generate(prompt, system_instruction=system_instruction)
        return await aria_core.chat(
            prompt,
            system_instruction=system_instruction,
            user_id=interaction.user.id,
            guild_id=interaction.guild_id,
            user_name=interaction.user.display_name,
            source_kind=source_kind,
            response_style=source_kind,
        )

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
                response_text = await self.ask_aria(
                    interaction,
                    question,
                    system_instruction=system_inst,
                    source_kind="lmgtfy_answer",
                )
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
                response_text = await self.ask_aria(
                    interaction,
                    question,
                    system_instruction=system_inst,
                    source_kind="socratic_answer",
                )
                await self.send_paginated(interaction, response_text)
            else:
                await self.alter_sanity(interaction.user.id, -15)
                await interaction.channel.send("WRONG. I've inflicted 15% Sanity Damage. Figure it out yourself.")
        except asyncio.TimeoutError:
            await interaction.channel.send("Time's up, idiot.")
        except AIServiceUnavailable as exc:
            await interaction.channel.send(exc.public_message)

    code_group = app_commands.Group(name="code", description="Have Aria review or debug code with maximum judgment.")

    @code_group.command(name="check", description="Review a snippet or attached source file for bugs, risks, and concrete fixes.")
    @app_commands.describe(
        snippet="Paste code here, or use this as extra notes when attaching a file",
        file="Optional source file for Aria to review end-to-end",
        attach_fixed_file="If true, Aria also returns a corrected file attachment",
    )
    async def code_check(
        self,
        interaction: discord.Interaction,
        snippet: str = "",
        file: discord.Attachment | None = None,
        attach_fixed_file: bool = False,
    ):
        await interaction.response.defer(thinking=True)
        score = await self.get_affinity(interaction.user.id)

        system_inst = (
            self.get_system_instruction(score, interaction.user.display_name)
            + " You are performing a code review. Prioritize bugs, risky assumptions, missing edge cases, maintainability problems, and clarity issues. "
              "When a full file is provided, review the entire file and propose concrete corrections."
        )

        try:
            code_prompt, filename, code_text, language = await self._build_code_input(
                snippet=snippet,
                attachment=file,
            )
            response_text = await self.ask_aria(
                interaction,
                (
                    "Review this code carefully.\n"
                    "Respond with:\n"
                    "1. A short overall diagnosis.\n"
                    "2. Numbered findings ordered by severity.\n"
                    "3. Exactly what needs to change and why.\n"
                    "4. Corrected code or corrected sections. If the user requested a fixed file, mention that the complete corrected file is attached.\n\n"
                    f"{code_prompt}"
                ),
                system_instruction=system_inst,
                source_kind="code_review",
            )
            fixed_file = None
            if attach_fixed_file:
                fixed_file = await self._build_fixed_file(
                    interaction,
                    system_instruction=system_inst,
                    filename=filename,
                    code_text=code_text,
                    language=language,
                    request_summary=response_text[:4000],
                    source_kind="code_review_file_fix",
                )

            chunks = self._split_text(response_text)
            for index, chunk in enumerate(chunks):
                kwargs = {}
                if index == 0 and fixed_file is not None:
                    kwargs["file"] = fixed_file
                await interaction.followup.send(chunk, **kwargs)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except ValueError as exc:
            await interaction.followup.send(str(exc))
        except Exception as e:
            logger.exception("code_check error: %s", e)
            await interaction.followup.send(
                f"Your code is so catastrophically bad it crashed my parser: {e}"
            )

    @code_group.command(name="debug", description="Give Aria an error plus code or a file so she can trace the bug and fix it.")
    @app_commands.describe(
        error_traceback="The traceback, failing log, or error message",
        snippet="Paste code here, or use this as extra notes when attaching a file",
        file="Optional source file for Aria to inspect in full",
        attach_fixed_file="If true, Aria also returns a corrected file attachment",
    )
    async def code_debug(
        self,
        interaction: discord.Interaction,
        error_traceback: str,
        snippet: str = "",
        file: discord.Attachment | None = None,
        attach_fixed_file: bool = False,
    ):
        await interaction.response.defer(thinking=True)
        score = await self.get_affinity(interaction.user.id)

        system_inst = (
            self.get_system_instruction(score, interaction.user.display_name)
            + " You are debugging broken code. Explain the root cause, call out file or line clues when the traceback provides them, and provide a concrete fix or corrected code. "
              "When a full file is supplied, reason over the whole file instead of only isolated lines."
        )

        try:
            code_prompt, filename, code_text, language = await self._build_code_input(
                snippet=snippet,
                attachment=file,
            )
            prompt = (
                "The human hit this error. Debug it properly.\n\n"
                f"Traceback or error details:\n{self._normalize_text(error_traceback)}\n\n"
                "Respond with:\n"
                "1. The most likely root cause.\n"
                "2. Any line, function, or file clues you can infer from the traceback.\n"
                "3. What needs to change and why.\n"
                "4. Corrected code or corrected sections. If the user requested a fixed file, mention that the complete corrected file is attached.\n\n"
                f"{code_prompt}"
            )
            response_text = await self.ask_aria(
                interaction,
                prompt,
                system_instruction=system_inst,
                source_kind="code_debug",
            )
            fixed_file = None
            if attach_fixed_file:
                fixed_file = await self._build_fixed_file(
                    interaction,
                    system_instruction=system_inst,
                    filename=filename,
                    code_text=code_text,
                    language=language,
                    request_summary=f"Error:\n{error_traceback}\n\n{response_text[:3500]}",
                    source_kind="code_debug_file_fix",
                )

            chunks = self._split_text(response_text)
            for index, chunk in enumerate(chunks):
                kwargs = {}
                if index == 0 and fixed_file is not None:
                    kwargs["file"] = fixed_file
                await interaction.followup.send(chunk, **kwargs)
        except AIServiceUnavailable as exc:
            await interaction.followup.send(exc.public_message)
        except ValueError as exc:
            await interaction.followup.send(str(exc))
        except Exception as e:
            logger.exception("code_debug error: %s", e)
            await interaction.followup.send(
                f"Even I can't fix this disaster. Start over. Error: {e}"
            )


async def setup(bot):
    await bot.add_cog(AITools(bot))
