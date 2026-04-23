from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from urllib.parse import urlparse

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands

from core.database import db
from core.swarm_control import SwarmController
from core.video_pipeline import (
    SUPPORTED_PUBLIC_VIDEO_HINTS,
    VideoCandidate,
    build_bing_video_search_url,
    extract_bing_video_candidates,
)

logger = logging.getLogger("discord")
SEARCH_CACHE_TTL = 600
MAX_CACHE_KEYS_PER_GUILD = 25
VAULT_LIMIT = 50
PLAYBACK_HINTS = tuple(SUPPORTED_PUBLIC_VIDEO_HINTS)
PROVIDER_SCORE_MAP = {
    'youtube.com': 0.95,
    'youtu.be': 0.95,
    'vimeo.com': 0.9,
    'dailymotion.com': 0.8,
    'archive.org': 0.85,
    'rumble.com': 0.82,
    'twitch.tv': 0.78,
    'clips.twitch.tv': 0.8,
    'tiktok.com': 0.76,
    'facebook.com': 0.68,
    'fb.watch': 0.68,
    'x.com': 0.62,
    'twitter.com': 0.62,
    'instagram.com': 0.58,
    'bilibili.com': 0.7,
    'rutube.ru': 0.65,
    'odysee.com': 0.7,
    'streamable.com': 0.75,
    'reddit.com': 0.6,
    'veoh.com': 0.6,
    'vk.com': 0.55,
    'ok.ru': 0.55,
}


def _duration_seconds(value: str | None) -> int:
    if not value:
        return -1
    try:
        parts = [int(p) for p in value.split(":")]
    except Exception:
        return -1
    if len(parts) == 2:
        return parts[0] * 60 + parts[1]
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2]
    return -1


def _provider_key(candidate: VideoCandidate) -> str:
    provider = (candidate.provider or "").strip().lower()
    if provider:
        return provider
    host = urlparse(candidate.page_url).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def _provider_score(candidate: VideoCandidate) -> float:
    host = _provider_key(candidate)
    if not host:
        return 0.3
    for hint, score in PROVIDER_SCORE_MAP.items():
        if hint in host:
            return score
    return 0.3

def _is_playback_supported(candidate: VideoCandidate) -> bool:
    return _provider_score(candidate) >= 0.5


class VideoHTTPSession:
    _session: aiohttp.ClientSession | None = None

    @classmethod
    async def get(cls) -> aiohttp.ClientSession:
        if cls._session is None or cls._session.closed:
            timeout = aiohttp.ClientTimeout(total=25, sock_connect=8, sock_read=20)
            cls._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; AriaVideo/1.0)",
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
        return cls._session

    @classmethod
    async def close(cls) -> None:
        if cls._session and not cls._session.closed:
            await cls._session.close()
        cls._session = None


class VideoResultView(discord.ui.View):
    def __init__(self, cog: "VideoOps", requester_id: int, query: str, results: list[VideoCandidate], *, vault_keyword: str | None = None, provider_filter: str | None = None, playable_only: bool = False, sort_by: str = 'relevance'):
        super().__init__(timeout=300)
        self.cog = cog
        self.requester_id = requester_id
        self.query = query
        self.results = results
        self.vault_keyword = vault_keyword
        self.provider_filter = provider_filter
        self.playable_only = playable_only
        self.sort_by = sort_by
        self.index = 0
        self.message: discord.Message | None = None
        self.prev_btn = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.blurple, row=0)
        self.next_btn = discord.ui.Button(label="Next ▶", style=discord.ButtonStyle.blurple, row=0)
        self.open_btn = discord.ui.Button(label="Open Page", style=discord.ButtonStyle.link, row=1, url=self.current.page_url)
        self.media_btn = discord.ui.Button(label="Open Media", style=discord.ButtonStyle.link, row=1, url=self.current.best_link())
        self.play_btn = discord.ui.Button(label="Queue to Swarm", style=discord.ButtonStyle.green, row=2)
        self.share_btn = discord.ui.Button(label="Post Link", style=discord.ButtonStyle.gray, row=2)
        self.save_btn = discord.ui.Button(label="Save", style=discord.ButtonStyle.gray, row=2)
        self.shuffle_btn = discord.ui.Button(label="Shuffle", style=discord.ButtonStyle.gray, row=2)
        self.prev_btn.callback = self._prev
        self.next_btn.callback = self._next
        self.play_btn.callback = self._play
        self.share_btn.callback = self._share
        self.save_btn.callback = self._save
        self.shuffle_btn.callback = self._shuffle
        self.add_item(self.prev_btn)
        self.add_item(self.next_btn)
        self.add_item(self.open_btn)
        self.add_item(self.media_btn)
        self.add_item(self.play_btn)
        self.add_item(self.share_btn)
        self.add_item(self.save_btn)
        self.add_item(self.shuffle_btn)
        self._sync_state()

    @property
    def current(self) -> VideoCandidate:
        return self.results[self.index]

    def _sync_state(self) -> None:
        disable_nav = len(self.results) <= 1
        self.prev_btn.disabled = disable_nav
        self.next_btn.disabled = disable_nav
        self.open_btn.url = self.current.page_url
        self.media_btn.url = self.current.best_link()
        self.media_btn.disabled = not bool(self.current.best_link())
        self.save_btn.disabled = bool(self.vault_keyword)
        score = _provider_score(self.current)
        self.play_btn.disabled = score < 0.5
        self.play_btn.label = 'Queue to Swarm' if score >= 0.8 else 'Queue (Low Confidence)'

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.requester_id:
            return True
        await interaction.response.send_message("Only the person who opened this video browser can drive it.", ephemeral=True)
        return False

    async def on_timeout(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.style != discord.ButtonStyle.link:
                child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except (discord.HTTPException, discord.NotFound):
                pass

    async def _prev(self, interaction: discord.Interaction) -> None:
        self.index = (self.index - 1) % len(self.results)
        self._sync_state()
        await self.cog.refresh_results(interaction, self)

    async def _next(self, interaction: discord.Interaction) -> None:
        self.index = (self.index + 1) % len(self.results)
        self._sync_state()
        await self.cog.refresh_results(interaction, self)

    async def _play(self, interaction: discord.Interaction) -> None:
        status = await self.cog.queue_video_candidate(interaction, self.current)
        if not interaction.response.is_done():
            await interaction.response.send_message(status, ephemeral=True)
        else:
            await interaction.followup.send(status, ephemeral=True)

    async def _share(self, interaction: discord.Interaction) -> None:
        candidate = self.current
        content = candidate.best_link()
        if not content:
            await interaction.response.send_message("No shareable link exists for this result.", ephemeral=True)
            return
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content)
            else:
                await interaction.followup.send(content)
        except discord.HTTPException:
            pass

    async def _shuffle(self, interaction: discord.Interaction) -> None:
        import random
        current = self.current.page_url
        random.shuffle(self.results)
        for i, item in enumerate(self.results):
            if item.page_url == current:
                self.index = i
                break
        self._sync_state()
        await self.cog.refresh_results(interaction, self)

    async def _save(self, interaction: discord.Interaction) -> None:
        keyword = self.query.strip().lower()[:100]
        saved = await self.cog.save_video_candidate(keyword, self.current, interaction.user.id)
        if saved:
            await interaction.response.send_message(f"Saved this result to the video vault under **{keyword}**.", ephemeral=True)
        else:
            await interaction.response.send_message("I could not save that video result right now.", ephemeral=True)


class VideoOps(commands.Cog):
    video_group = app_commands.Group(name="video", description="Search and store public video results.")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.swarm = SwarmController()
        self.search_cache: dict[tuple[int, str], tuple[float, list[VideoCandidate]]] = {}
        self.cache_order: defaultdict[int, list[str]] = defaultdict(list)
        self.session_memory: dict[tuple[int, int], dict] = {}

    async def cog_load(self) -> None:
        await self.ensure_video_tables()

    def cog_unload(self) -> None:
        try:
            asyncio.get_running_loop().create_task(VideoHTTPSession.close())
        except RuntimeError:
            pass

    async def ensure_video_tables(self) -> None:
        if not db.pool:
            return
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS aria_video_vault (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        keyword VARCHAR(100) NOT NULL,
                        title TEXT NOT NULL,
                        page_url TEXT NOT NULL,
                        thumbnail_url TEXT NULL,
                        duration_text VARCHAR(32) NULL,
                        source_url TEXT NULL,
                        provider VARCHAR(255) NULL,
                        description TEXT NULL,
                        added_by BIGINT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        INDEX idx_keyword (keyword(100))
                    )
                    """
                )
            await conn.commit()

    async def _fetch_html(self, url: str) -> str:
        session = await VideoHTTPSession.get()
        async with session.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            return await resp.text(errors="ignore")

    async def _search_with_bing(self, query: str, *, limit: int = 10) -> list[VideoCandidate]:
        html = await self._fetch_html(build_bing_video_search_url(query))
        return extract_bing_video_candidates(html, limit=limit)

    def _cache_key(self, query: str, provider_filter: str | None, playable_only: bool, sort_by: str) -> str:
        return f"{query.lower()}|{(provider_filter or '').lower()}|{int(playable_only)}|{sort_by.lower()}"

    def _cache_get(self, guild_id: int, cache_key: str) -> list[VideoCandidate] | None:
        entry = self.search_cache.get((guild_id, cache_key))
        if not entry:
            return None
        ts, results = entry
        if (time.time() - ts) > SEARCH_CACHE_TTL:
            self.search_cache.pop((guild_id, cache_key), None)
            return None
        return results

    def _cache_put(self, guild_id: int, cache_key: str, results: list[VideoCandidate]) -> None:
        self.search_cache[(guild_id, cache_key)] = (time.time(), results)
        order = self.cache_order[guild_id]
        if cache_key in order:
            order.remove(cache_key)
        order.append(cache_key)
        while len(order) > MAX_CACHE_KEYS_PER_GUILD:
            old = order.pop(0)
            self.search_cache.pop((guild_id, old), None)

    def _apply_filters(self, results: list[VideoCandidate], *, provider_filter: str | None, playable_only: bool, sort_by: str) -> list[VideoCandidate]:
        filtered = results
        if provider_filter:
            needle = provider_filter.lower().strip()
            filtered = [r for r in filtered if needle in _provider_key(r)]
        if playable_only:
            filtered = [r for r in filtered if _is_playback_supported(r)]

        if sort_by == "provider":
            filtered = sorted(filtered, key=lambda r: (_provider_key(r), r.title.lower()))
        elif sort_by == "duration_desc":
            filtered = sorted(filtered, key=lambda r: _duration_seconds(r.duration_text), reverse=True)
        elif sort_by == "duration_asc":
            filtered = sorted(filtered, key=lambda r: (_duration_seconds(r.duration_text) if _duration_seconds(r.duration_text) >= 0 else 10**9))
        return filtered

    async def search_public_videos(self, guild_id: int, query: str, *, limit: int = 10, provider_filter: str | None = None, playable_only: bool = False, sort_by: str = "relevance") -> list[VideoCandidate]:
        query = query.strip()
        cache_key = self._cache_key(query, provider_filter, playable_only, sort_by)
        cached = self._cache_get(guild_id, cache_key)
        if cached:
            return cached[:limit]
        base = await self._search_with_bing(query, limit=max(limit * 2, 12))
        results = self._apply_filters(base, provider_filter=provider_filter, playable_only=playable_only, sort_by=sort_by)[:limit]
        self._cache_put(guild_id, cache_key, results)
        return results

    async def save_video_candidate(self, keyword: str, candidate: VideoCandidate, added_by: int | None) -> bool:
        await self.ensure_video_tables()
        if not db.pool:
            return False
        try:
            async with db.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        INSERT INTO aria_video_vault
                        (keyword, title, page_url, thumbnail_url, duration_text, source_url, provider, description, added_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            keyword[:100], candidate.title[:500], candidate.page_url, candidate.thumbnail_url,
                            candidate.duration_text, candidate.source_url, candidate.provider, candidate.description, added_by
                        ),
                    )
                await conn.commit()
            return True
        except Exception:
            logger.exception("Video vault save failed")
            return False

    async def fetch_vault_results(self, keyword: str, *, limit: int = VAULT_LIMIT, provider_filter: str | None = None, playable_only: bool = False, sort_by: str = "relevance") -> list[VideoCandidate]:
        await self.ensure_video_tables()
        if not db.pool:
            return []
        results: list[VideoCandidate] = []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT title, page_url, thumbnail_url, duration_text, source_url, provider, description
                    FROM aria_video_vault
                    WHERE keyword = %s OR keyword LIKE %s
                    ORDER BY (keyword = %s) DESC, created_at DESC
                    LIMIT %s
                    """,
                    (keyword[:100], f"%{keyword[:100]}%", keyword[:100], limit),
                )
                rows = await cur.fetchall()
        for row in rows:
            results.append(VideoCandidate(
                title=row[0], page_url=row[1], thumbnail_url=row[2], duration_text=row[3],
                source_url=row[4], provider=row[5], description=row[6]
            ))
        return self._apply_filters(results, provider_filter=provider_filter, playable_only=playable_only, sort_by=sort_by)

    async def delete_vault_keyword(self, keyword: str) -> int:
        await self.ensure_video_tables()
        if not db.pool:
            return 0
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("DELETE FROM aria_video_vault WHERE keyword = %s", (keyword[:100],))
                count = cur.rowcount or 0
            await conn.commit()
        return count

    async def list_vault_keywords(self, *, limit: int = 20) -> list[tuple[str, int, str]]:
        await self.ensure_video_tables()
        if not db.pool:
            return []
        async with db.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT keyword, COUNT(*), MAX(created_at)
                    FROM aria_video_vault
                    GROUP BY keyword
                    ORDER BY MAX(created_at) DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                rows = await cur.fetchall()
        return [(str(r[0]), int(r[1]), str(r[2])) for r in rows]

    def build_result_embed(self, query: str, results: list[VideoCandidate], index: int, *, vault_keyword: str | None = None) -> discord.Embed:
        candidate = results[index]
        embed = discord.Embed(
            title=candidate.title[:256],
            url=candidate.page_url,
            color=discord.Color.red(),
            description=(candidate.description or "Public video result. Open the page or post the link to let Discord preview/embed it where supported.")[:4000],
        )
        provider = candidate.provider or "unknown source"
        duration = candidate.duration_text or "unknown length"
        score = _provider_score(candidate)
        embed.add_field(name="Provider", value=provider[:1024], inline=True)
        embed.add_field(name="Duration", value=duration[:1024], inline=True)
        embed.add_field(name="Playback", value=("Supported for swarm handoff" if _is_playback_supported(candidate) else "Search/share only"), inline=True)
        embed.add_field(name="Best Link", value=f"[Open result]({candidate.best_link()})", inline=False)
        if 0.5 <= score < 0.8:
            embed.add_field(name='⚠ Playback Warning', value='This source may not fully support swarm playback. You can still try the handoff.', inline=False)
        if candidate.thumbnail_url:
            embed.set_thumbnail(url=candidate.thumbnail_url)
        source = f"Vault • {vault_keyword}" if vault_keyword else f"Query: {query[:100]}"
        embed.set_footer(text=f"Result {index + 1} of {len(results)} • {source}")
        return embed

    async def queue_video_candidate(self, interaction: discord.Interaction, candidate: VideoCandidate) -> str:
        if not interaction.guild_id:
            return "Video swarm handoff only works inside a server."
        score = _provider_score(candidate)
        if score < 0.5:
            return 'This site/result is searchable, but I am not confident it is supported for swarm playback handoff yet.'
        query = candidate.best_link() or candidate.page_url
        if not query:
            return "That result does not have a usable link for playback handoff."
        try:
            status = await self.swarm.play(interaction, query)
        except Exception:
            logger.exception("Video playback handoff failed")
            return "I could not hand that video result off to the swarm right now."
        if score < 0.8:
            return f'Playback handoff (low confidence): {status}'
        return f'Playback handoff: {status}'

    async def refresh_results(self, interaction: discord.Interaction, view: VideoResultView) -> None:
        self.session_memory[(interaction.guild_id or 0, interaction.user.id)] = {
            'query': view.query, 'provider_filter': view.provider_filter, 'playable_only': view.playable_only, 'sort_by': view.sort_by, 'index': view.index, 'vault_keyword': view.vault_keyword
        }
        embed = self.build_result_embed(view.query, view.results, view.index, vault_keyword=view.vault_keyword)
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=embed, view=view)
                return
        except (discord.HTTPException, discord.NotFound):
            pass
        try:
            if view.message:
                await view.message.edit(embed=embed, view=view)
                return
        except (discord.HTTPException, discord.NotFound):
            pass
        if interaction.channel:
            msg = await interaction.channel.send(embed=embed, view=view)
            view.message = msg

    @video_group.command(name="search", description="Search public video pages and browse the results.")
    @app_commands.describe(
        query="What video to search for",
        limit="How many results to fetch (1-10)",
        provider_filter="Optional provider/domain filter like youtube, vimeo, archive",
        playable_only="Only show results likely to hand off to the swarm",
        sort_by="How to sort the returned results",
    )
    @app_commands.choices(sort_by=[
        app_commands.Choice(name="Relevance", value="relevance"),
        app_commands.Choice(name="Provider", value="provider"),
        app_commands.Choice(name="Duration longest first", value="duration_desc"),
        app_commands.Choice(name="Duration shortest first", value="duration_asc"),
    ])
    async def videosearch(
        self,
        interaction: discord.Interaction,
        query: str,
        limit: app_commands.Range[int, 1, 10] = 8,
        provider_filter: str | None = None,
        playable_only: bool = False,
        sort_by: str = "relevance",
    ):
        await interaction.response.defer(thinking=True)
        try:
            guild_id = interaction.guild_id or 0
            results = await self.search_public_videos(
                guild_id, query, limit=limit, provider_filter=provider_filter,
                playable_only=playable_only, sort_by=sort_by,
            )
        except Exception as exc:
            logger.exception("Video search failed: %s", exc)
            await interaction.followup.send("I could not complete that public video search right now.", ephemeral=True)
            return

        if not results:
            await interaction.followup.send("No public video results came back for that query with those filters.", ephemeral=True)
            return

        view = VideoResultView(self, interaction.user.id, query, results, provider_filter=provider_filter, playable_only=playable_only, sort_by=sort_by)
        self.session_memory[(interaction.guild_id or 0, interaction.user.id)] = {
            'query': query, 'provider_filter': provider_filter, 'playable_only': playable_only, 'sort_by': sort_by, 'index': 0, 'vault_keyword': None
        }
        embed = self.build_result_embed(query, results, 0)
        guidance = "\n".join([
            "This is the careful public video browser.",
            "Use **Open Page** for the source page, **Open Media** where a direct media link exists, **Post Link** to let Discord preview/embed supported sites, **Save** to keep a result in Aria's video vault, and **Queue to Swarm** for supported public sites.",
            f"Best supported public playback hints usually include: {', '.join(SUPPORTED_PUBLIC_VIDEO_HINTS[:6])}...",
        ])
        embed.add_field(name="How to use this", value=guidance[:1024], inline=False)
        sent = await interaction.followup.send(embed=embed, view=view, wait=True)
        view.message = sent

    @video_group.command(name="vault", description="Browse all saved video results for a keyword.")
    @app_commands.describe(
        keyword="Vault keyword to browse",
        provider_filter="Optional provider/domain filter like youtube, vimeo, archive",
        playable_only="Only show results likely to hand off to the swarm",
        sort_by="How to sort the returned results",
    )
    @app_commands.choices(sort_by=[
        app_commands.Choice(name="Relevance", value="relevance"),
        app_commands.Choice(name="Provider", value="provider"),
        app_commands.Choice(name="Duration longest first", value="duration_desc"),
        app_commands.Choice(name="Duration shortest first", value="duration_asc"),
    ])
    async def videovault(
        self,
        interaction: discord.Interaction,
        keyword: str,
        provider_filter: str | None = None,
        playable_only: bool = False,
        sort_by: str = "relevance",
    ):
        await interaction.response.defer(thinking=True)
        clean_keyword = keyword.strip().lower()
        try:
            results = await self.fetch_vault_results(clean_keyword, provider_filter=provider_filter, playable_only=playable_only, sort_by=sort_by)
        except Exception as exc:
            logger.exception("Video vault load failed: %s", exc)
            await interaction.followup.send("I could not load that video vault keyword right now.", ephemeral=True)
            return
        if not results:
            await interaction.followup.send("I do not have any saved public videos for that keyword with those filters yet.", ephemeral=True)
            return
        view = VideoResultView(self, interaction.user.id, clean_keyword, results, vault_keyword=clean_keyword, provider_filter=provider_filter, playable_only=playable_only, sort_by=sort_by)
        self.session_memory[(interaction.guild_id or 0, interaction.user.id)] = {
            'query': clean_keyword, 'provider_filter': provider_filter, 'playable_only': playable_only, 'sort_by': sort_by, 'index': 0, 'vault_keyword': clean_keyword
        }
        embed = self.build_result_embed(keyword, results, 0, vault_keyword=clean_keyword)
        embed.add_field(name="Vault", value="This is a saved set from Aria's video vault. Browse with Prev/Next or queue a supported result to the swarm.", inline=False)
        sent = await interaction.followup.send(embed=embed, view=view, wait=True)
        view.message = sent

    @video_group.command(name="vaultlist", description="List recent keywords stored in Aria's video vault.")
    async def videovault_list(self, interaction: discord.Interaction):
        await interaction.response.defer(thinking=True)
        rows = await self.list_vault_keywords()
        if not rows:
            await interaction.followup.send("Aria's video vault is empty right now.", ephemeral=True)
            return
        embed = discord.Embed(title="Aria Video Vault", color=discord.Color.red())
        lines = [f"**{keyword}** — {count} item(s)" for keyword, count, _ in rows[:20]]
        embed.description = "\n".join(lines)
        await interaction.followup.send(embed=embed)

    @video_group.command(name="vaultdelete", description="Delete all saved video results for a vault keyword.")
    @app_commands.describe(keyword="Exact vault keyword to delete")
    async def videovault_delete(self, interaction: discord.Interaction, keyword: str):
        await interaction.response.defer(thinking=True, ephemeral=True)
        removed = await self.delete_vault_keyword(keyword.strip().lower())
        if removed:
            await interaction.followup.send(f"Deleted {removed} saved video result(s) from **{keyword.strip().lower()}**.", ephemeral=True)
        else:
            await interaction.followup.send("I did not find any saved video results under that exact keyword.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(VideoOps(bot))
