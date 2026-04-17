from __future__ import annotations

import asyncio
import io
import logging
import random
import urllib.parse

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from core.database import db
from core.image_pipeline import (
    ImageCandidate,
    ImagePipelineError,
    RenderedImage,
    dedupe_candidates,
    extract_bing_image_candidates,
    render_delivery_image,
    render_forge_image,
    render_upscale_fallback,
    upscale_with_realesrgan,
)
from core.settings import REAL_ESRGAN_BINARY, REAL_ESRGAN_MODEL_DIR

logger = logging.getLogger("discord")


class HTTPSessionManager:
    _session: aiohttp.ClientSession | None = None

    @classmethod
    async def get_session(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            timeout = aiohttp.ClientTimeout(total=35, sock_connect=10, sock_read=25)
            cls._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "Aria/visual-suite"},
            )
        return cls._session

    @classmethod
    async def close(cls) -> None:
        if cls._session and not cls._session.closed:
            await cls._session.close()
        cls._session = None


class VaultTagModal(discord.ui.Modal, title="Encrypt Image to Vault"):
    keyword = discord.ui.TextInput(
        label="Enter a keyword or tag",
        placeholder="e.g., funny cat, server logo, reaction meme",
        max_length=100,
    )

    def __init__(self, image_url: str):
        super().__init__()
        self.image_url = image_url

    async def on_submit(self, interaction: discord.Interaction) -> None:
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS aria_visual_vault (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            keyword VARCHAR(100),
                            image_url TEXT,
                            added_by BIGINT,
                            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    await cur.execute(
                        "INSERT INTO aria_visual_vault (keyword, image_url, added_by) VALUES (%s, %s, %s)",
                        (self.keyword.value, self.image_url, interaction.user.id),
                    )
            await interaction.response.send_message(
                f"💾 **Encrypted & Stored** inside the Visual Vault under the tag `{self.keyword.value}`!",
                ephemeral=True,
            )
        except Exception as exc:
            logger.exception("Vault save failed: %s", exc)
            await interaction.response.send_message(
                f"❌ Database Write Error: {exc}",
                ephemeral=True,
            )


class ImageCarousel(discord.ui.View):
    def __init__(self, cog: "ImageOps", requester_id: int, query: str, results: list[ImageCandidate]):
        super().__init__(timeout=300)
        self.cog = cog
        self.requester_id = requester_id
        self.query = query
        self.results = results
        self.index = 0
        self.nav_step = 1
        self.failed_indexes: set[int] = set()
        self.message: discord.Message | None = None
        button_ns = f"aria:image:{requester_id}:{id(self)}"
        self.prev_btn = discord.ui.Button(
            label="◀ Prev",
            style=discord.ButtonStyle.blurple,
            row=0,
            custom_id=f"{button_ns}:prev",
        )
        self.next_btn = discord.ui.Button(
            label="Next ▶",
            style=discord.ButtonStyle.blurple,
            row=0,
            custom_id=f"{button_ns}:next",
        )
        self.upscale_btn = discord.ui.Button(
            label="AI Upscale",
            style=discord.ButtonStyle.green,
            row=1,
            custom_id=f"{button_ns}:upscale",
        )
        self.save_btn = discord.ui.Button(
            label="Save to Vault",
            style=discord.ButtonStyle.green,
            row=1,
            custom_id=f"{button_ns}:save",
        )
        self.prev_btn.callback = self._handle_prev
        self.next_btn.callback = self._handle_next
        self.upscale_btn.callback = self._handle_upscale
        self.save_btn.callback = self._handle_save
        self.add_item(self.prev_btn)
        self.add_item(self.next_btn)
        self.add_item(self.upscale_btn)
        self.add_item(self.save_btn)
        self._sync_button_state()

    @property
    def current(self) -> ImageCandidate:
        return self.results[self.index]

    def _sync_button_state(self) -> None:
        disable_nav = len(self.results) <= 1
        self.prev_btn.disabled = disable_nav
        self.next_btn.disabled = disable_nav

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.requester_id:
            return True
        await interaction.response.send_message(
            "Only the person who opened this image carousel can drive it.",
            ephemeral=True,
        )
        return False

    async def on_timeout(self) -> None:
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass

    async def _handle_prev(self, interaction: discord.Interaction) -> None:
        self.nav_step = -1
        self.index = (self.index - 1) % len(self.results)
        self._sync_button_state()
        await self.cog.refresh_carousel(interaction, self)

    async def _handle_next(self, interaction: discord.Interaction) -> None:
        self.nav_step = 1
        self.index = (self.index + 1) % len(self.results)
        self._sync_button_state()
        await self.cog.refresh_carousel(interaction, self)

    async def _handle_upscale(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(thinking=True)
        try:
            rendered = await self.cog.build_upscaled_image(self.query, self.current)
            file = self.cog.rendered_to_file(rendered, f"aria_upscaled_{self.index + 1}")

            embed = discord.Embed(title="✨ AI Enhancement Complete", color=discord.Color.green())
            embed.description = self.cog.build_source_value(self.current)
            embed.set_image(url=f"attachment://{file.filename}")
            embed.set_footer(text=f"Result {self.index + 1} of {len(self.results)}")

            await interaction.followup.send(embed=embed, file=file)
        except Exception as exc:
            logger.exception("Upscale failed: %s", exc)
            await interaction.followup.send(
                "❌ I couldn't upscale that image cleanly. The source server may be blocking downloads, or the local model failed.",
                ephemeral=True,
            )

    async def _handle_save(self, interaction: discord.Interaction) -> None:
        await interaction.response.send_modal(VaultTagModal(self.current.source_url))


class ImageOps(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.ctx_vault = app_commands.ContextMenu(name="💾 Save to Vault", callback=self.save_to_vault_context)
        self.ctx_ocr = app_commands.ContextMenu(name="🔤 Extract Text (OCR)", callback=self.extract_text_context)
        self.ctx_source = app_commands.ContextMenu(name="🔍 Find Anime Source", callback=self.find_source_context)

        self.bot.tree.add_command(self.ctx_vault)
        self.bot.tree.add_command(self.ctx_ocr)
        self.bot.tree.add_command(self.ctx_source)

    def cog_unload(self) -> None:
        self.bot.tree.remove_command(self.ctx_vault.name, type=self.ctx_vault.type)
        self.bot.tree.remove_command(self.ctx_ocr.name, type=self.ctx_ocr.type)
        self.bot.tree.remove_command(self.ctx_source.name, type=self.ctx_source.type)
        try:
            asyncio.get_running_loop().create_task(HTTPSessionManager.close())
        except RuntimeError:
            pass

    @staticmethod
    def rendered_to_file(rendered: RenderedImage, stem: str) -> discord.File:
        filename = f"{stem}.{rendered.extension}"
        payload = io.BytesIO(rendered.data)
        payload.seek(0)
        return discord.File(fp=payload, filename=filename)

    @staticmethod
    def build_source_value(candidate: ImageCandidate) -> str:
        lines = [f"[Open original]({candidate.source_url})"]
        if candidate.page_url and candidate.page_url != candidate.source_url:
            lines.append(f"[Open source page]({candidate.page_url})")
        return "\n".join(lines)

    async def _fetch_bytes(self, url: str) -> bytes:
        session = await HTTPSessionManager.get_session()
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                async with session.get(url, allow_redirects=True) as response:
                    response.raise_for_status()
                    payload = await response.read()
                    if not payload:
                        raise ImagePipelineError("Image host returned an empty response.")
                    return payload
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(0.35 * (attempt + 1))
        if last_error is None:
            raise ImagePipelineError("Image host returned no usable payload.")
        raise last_error

    async def _get_best_render(self, candidate: ImageCandidate) -> RenderedImage:
        last_error: Exception | None = None
        for url in candidate.download_urls():
            try:
                payload = await self._fetch_bytes(url)
                return render_delivery_image(payload)
            except Exception as exc:
                last_error = exc
        if last_error is None:
            raise ImagePipelineError("The image candidate had no usable URLs.")
        raise last_error

    async def _search_with_duckduckgo(self, query: str) -> list[ImageCandidate]:
        try:
            from duckduckgo_search import DDGS
        except ImportError:
            return []

        def _run_search() -> list[ImageCandidate]:
            results: list[ImageCandidate] = []
            with DDGS() as ddgs:
                for row in ddgs.images(query, max_results=40):
                    source_url = (row.get("image") or row.get("thumbnail") or "").strip()
                    preview_url = (row.get("thumbnail") or "").strip() or None
                    page_url = (row.get("url") or "").strip() or None
                    title = (row.get("title") or "").strip() or None
                    if source_url:
                        results.append(ImageCandidate(source_url, preview_url, page_url, title))
            return results

        try:
            return dedupe_candidates(await asyncio.to_thread(_run_search), limit=32)
        except Exception as exc:
            logger.warning("DuckDuckGo image search failed: %s", exc)
            return []

    async def _search_with_bing(self, query: str) -> list[ImageCandidate]:
        query_safe = urllib.parse.quote_plus(query)
        offsets = (1, 36, 71, 106)
        headers = {"User-Agent": "Mozilla/5.0"}
        session = await HTTPSessionManager.get_session()
        urls = [
            "https://www.bing.com/images/search"
            f"?q={query_safe}&qft=+filterui:imagesize-large&form=HDRSC2&first={offset}"
            for offset in offsets
        ]

        async def _fetch_page(url: str) -> str | None:
            try:
                async with session.get(url, headers=headers, allow_redirects=True) as response:
                    if response.status == 200:
                        return await response.text()
            except Exception as exc:
                logger.warning("Bing image search page fetch failed: %s", exc)
            return None

        html_pages = [page for page in await asyncio.gather(*(_fetch_page(url) for url in urls)) if page]

        candidates: list[ImageCandidate] = []
        for page in html_pages:
            candidates.extend(extract_bing_image_candidates(page, limit=24))
        return dedupe_candidates(candidates, limit=32)

    async def search_image_candidates(self, query: str) -> list[ImageCandidate]:
        sources = await asyncio.gather(
            self._search_with_duckduckgo(query),
            self._search_with_bing(query),
            return_exceptions=True,
        )

        candidates: list[ImageCandidate] = []
        for source in sources:
            if isinstance(source, Exception):
                logger.warning("Image source aggregation failed for %r: %s", query, source)
                continue
            candidates.extend(source)

        candidates = dedupe_candidates(candidates, limit=48)
        random.shuffle(candidates)
        return candidates

    def _pick_upscale_model(self, query: str, candidate: ImageCandidate) -> str:
        probe = " ".join(filter(None, [query, candidate.title, candidate.page_url or ""])).lower()
        anime_markers = ("anime", "manga", "waifu", "genshin", "naruto", "bleach", "cosplay")
        if any(marker in probe for marker in anime_markers):
            return "realesrgan-x4plus-anime"
        return "realesrgan-x4plus"

    async def build_search_embed(
        self,
        query: str,
        candidate: ImageCandidate,
        index: int,
        total: int,
    ) -> tuple[discord.Embed, discord.File]:
        rendered = await self._get_best_render(candidate)
        file = self.rendered_to_file(rendered, f"aria_search_{index + 1}")

        embed = discord.Embed(title=f"🔍 Omni-Lens: {query}", color=discord.Color.teal())
        embed.description = self.build_source_value(candidate)
        embed.set_image(url=f"attachment://{file.filename}")
        embed.set_footer(text=f"Result {index + 1} of {total} | {rendered.width}x{rendered.height}")
        return embed, file

    async def _build_first_renderable_embed(self, view: ImageCarousel) -> tuple[discord.Embed, discord.File]:
        total = len(view.results)
        step = view.nav_step if view.nav_step in (-1, 1) else 1
        last_error: Exception | None = None
        candidate_indexes = [(view.index + (offset * step)) % total for offset in range(total)]
        if view.failed_indexes:
            healthy_indexes = [index for index in candidate_indexes if index not in view.failed_indexes]
            if healthy_indexes:
                candidate_indexes = healthy_indexes + [index for index in candidate_indexes if index in view.failed_indexes]

        for candidate_index in candidate_indexes:
            candidate = view.results[candidate_index]
            try:
                embed, file = await self.build_search_embed(view.query, candidate, candidate_index, total)
                view.index = candidate_index
                view.failed_indexes.discard(candidate_index)
                view._sync_button_state()
                return embed, file
            except Exception as exc:
                last_error = exc
                view.failed_indexes.add(candidate_index)
                logger.warning(
                    "Skipping non-renderable carousel candidate %s for query %r: %s",
                    candidate_index,
                    view.query,
                    exc,
                )

        if last_error is None:
            raise ImagePipelineError("The image carousel has no renderable results.")
        raise last_error

    async def refresh_carousel(self, interaction: discord.Interaction, view: ImageCarousel) -> None:
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            embed, file = await self._build_first_renderable_embed(view)
            target_message = view.message or interaction.message
            if target_message:
                await target_message.edit(embed=embed, attachments=[file], view=view)
            else:
                await interaction.edit_original_response(embed=embed, attachments=[file], view=view)
        except Exception as exc:
            logger.exception("Carousel refresh failed: %s", exc)
            message = "❌ I couldn't refresh that result. The image host probably blocked the request."
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)

    async def build_upscaled_image(self, query: str, candidate: ImageCandidate) -> RenderedImage:
        last_error: Exception | None = None
        model_name = self._pick_upscale_model(query, candidate)
        for url in candidate.download_urls():
            try:
                payload = await self._fetch_bytes(url)
                try:
                    return await asyncio.to_thread(
                        upscale_with_realesrgan,
                        payload,
                        binary_path=REAL_ESRGAN_BINARY,
                        model_dir=REAL_ESRGAN_MODEL_DIR,
                        model_name=model_name,
                    )
                except Exception as exc:
                    logger.warning("Real-ESRGAN unavailable, using fallback upscale: %s", exc)
                    return render_upscale_fallback(payload)
            except Exception as exc:
                last_error = exc
        if last_error is None:
            raise ImagePipelineError("The image candidate had no usable URLs for upscale.")
        raise last_error

    async def _get_image_url(self, message: discord.Message) -> str | None:
        if message.attachments:
            for attachment in message.attachments:
                if attachment.content_type and attachment.content_type.startswith("image/"):
                    return attachment.url
        if message.embeds:
            for embed in message.embeds:
                if embed.image and embed.image.url:
                    return embed.image.url
                if embed.thumbnail and embed.thumbnail.url:
                    return embed.thumbnail.url
        return None

    async def save_to_vault_context(self, interaction: discord.Interaction, message: discord.Message) -> None:
        url = await self._get_image_url(message)
        if not url:
            await interaction.response.send_message(
                "❌ The Omni-Lens could not detect a valid image in that message.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(VaultTagModal(url))

    async def extract_text_context(self, interaction: discord.Interaction, message: discord.Message) -> None:
        url = await self._get_image_url(message)
        if not url:
            await interaction.response.send_message("❌ No valid image found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)
        api_url = f"https://api.ocr.space/parse/imageurl?apikey=helloworld&url={url}"

        try:
            session = await HTTPSessionManager.get_session()
            async with session.get(api_url) as response:
                data = await response.json()

            if data.get("IsErroredOnProcessing"):
                await interaction.followup.send("❌ OCR Processing Failed.")
                return

            parsed_results = data.get("ParsedResults")
            if not parsed_results or not parsed_results[0].get("ParsedText", "").strip():
                await interaction.followup.send("📭 No readable text found in the image.")
                return

            text = parsed_results[0]["ParsedText"]
            fence = chr(96) * 3
            desc = f"{fence}\n{text[:4000]}\n{fence}"
            embed = discord.Embed(
                title="🔤 Optical Character Recognition",
                description=desc,
                color=discord.Color.blue(),
            )
            await interaction.followup.send(embed=embed)
        except Exception as exc:
            logger.exception("OCR context action failed: %s", exc)
            await interaction.followup.send(f"❌ Network Error: {exc}")

    async def find_source_context(self, interaction: discord.Interaction, message: discord.Message) -> None:
        url = await self._get_image_url(message)
        if not url:
            await interaction.response.send_message("❌ No valid image found.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=False)
        api_url = f"https://api.trace.moe/search?anilistInfo&url={url}"

        try:
            session = await HTTPSessionManager.get_session()
            async with session.get(api_url) as response:
                data = await response.json()

            if data.get("error"):
                await interaction.followup.send(f"❌ Trace.moe Error: {data['error']}")
                return

            result = data["result"][0]
            similarity = result["similarity"] * 100
            if similarity < 70:
                await interaction.followup.send(
                    f"⚠️ No confident matches found. Best guess was {similarity:.1f}% accurate."
                )
                return

            anime_title = result["anilist"]["title"].get("romaji") or result["anilist"]["title"].get("english")
            episode = result.get("episode", "Movie/OVA")
            minutes, seconds = divmod(int(result["from"]), 60)

            embed = discord.Embed(title="🔍 Anime Source Detective", color=discord.Color.purple())
            embed.add_field(name="Anime", value=f"**{anime_title}**", inline=False)
            embed.add_field(name="Episode", value=str(episode), inline=True)
            embed.add_field(name="Timestamp", value=f"{minutes}:{seconds:02d}", inline=True)
            embed.add_field(name="Confidence", value=f"{similarity:.1f}%", inline=True)
            embed.set_image(url=result["image"])

            await interaction.followup.send(embed=embed)
        except Exception as exc:
            logger.exception("Anime source lookup failed: %s", exc)
            await interaction.followup.send(f"❌ Network Error: {exc}")

    image_group = app_commands.Group(name="image", description="Search, save, and manipulate images with Aria's visual tools.")

    @image_group.command(name="search", description="Search for images and browse the results in Aria's carousel.")
    async def search(self, interaction: discord.Interaction, query: str) -> None:
        await interaction.response.defer(thinking=True)

        try:
            candidates = await self.search_image_candidates(query)
            if not candidates:
                await interaction.followup.send(f"❌ The Omni-Lens found zero matches for `{query}`.")
                return

            view = ImageCarousel(self, interaction.user.id, query, candidates)
            embed, file = await self._build_first_renderable_embed(view)
            view.message = await interaction.followup.send(embed=embed, file=file, view=view, wait=True)
        except Exception as exc:
            logger.exception("Image search failed: %s", exc)
            await interaction.followup.send(f"❌ Network Fetch Error: {exc}")

    @image_group.command(name="vault", description="Retrieve an image previously saved in the Visual Vault.")
    async def vault_get(self, interaction: discord.Interaction, keyword: str) -> None:
        await interaction.response.defer()
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS aria_visual_vault (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            keyword VARCHAR(100),
                            image_url TEXT,
                            added_by BIGINT,
                            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                        """
                    )
                    await cur.execute(
                        "SELECT image_url, added_by FROM aria_visual_vault WHERE keyword LIKE %s ORDER BY RAND() LIMIT 1",
                        (f"%{keyword}%",),
                    )
                    result = await cur.fetchone()

            if not result:
                await interaction.followup.send(f"📭 The Visual Vault contains no records for `{keyword}`.")
                return

            embed = discord.Embed(title=f"🔐 Vault Record: {keyword}", color=discord.Color.gold())
            embed.set_image(url=result[0])
            embed.set_footer(text=f"Archived by User ID: {result[1]}")
            await interaction.followup.send(embed=embed)
        except Exception as exc:
            logger.exception("Vault fetch failed: %s", exc)
            await interaction.followup.send(f"❌ Vault Fetch Error: {exc}")

    @image_group.command(name="forge", description="Generate a simple image with custom text burned onto it.")
    async def forge(self, interaction: discord.Interaction, text: str, color: str = "black") -> None:
        await interaction.response.defer()
        try:
            rendered = render_forge_image(text, background_color=color)
            file = self.rendered_to_file(rendered, "forge_output")
            embed = discord.Embed(title="⚒️ The Image Forge", color=discord.Color.dark_theme())
            embed.set_image(url=f"attachment://{file.filename}")
            await interaction.followup.send(embed=embed, file=file)
        except Exception as exc:
            logger.exception("Forge render failed: %s", exc)
            await interaction.followup.send(f"❌ Forge Error: {exc}")


async def setup(bot):
    await bot.add_cog(ImageOps(bot))
