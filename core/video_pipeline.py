from __future__ import annotations

import base64
from dataclasses import dataclass
from html import unescape
import json
import re
from urllib.parse import parse_qs, quote_plus, unquote, urlparse


@dataclass(slots=True)
class VideoCandidate:
    title: str
    page_url: str
    thumbnail_url: str | None = None
    duration_text: str | None = None
    source_url: str | None = None
    provider: str | None = None
    description: str | None = None

    def best_link(self) -> str:
        return self.source_url or self.page_url


_BING_ANCHOR_RE = re.compile(
    r'<a(?P<attrs>[^>]*\bclass="[^"]*mc_vtvc_link[^"]*"[^>]*)>(?P<body>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
_BING_THUMB_RE = re.compile(r'i\.(?:bp|ytimg)\.(?P<kind>[^"\']+)', re.IGNORECASE)
_BING_DURATION_RE = re.compile(r'\b(?P<duration>\d{1,2}:\d{2}(?::\d{2})?)\b')
_TAG_RE = re.compile(r'<[^>]+>')
_BING_MEDIA_RE = re.compile(r'\bm="(?P<meta>[^"]+)"', re.IGNORECASE)


def _strip_tags(text: str) -> str:
    return unescape(_TAG_RE.sub('', text or '')).strip()


def _html_attr(html_text: str, name: str) -> str | None:
    match = re.search(rf'\b{re.escape(name)}="(?P<value>[^"]*)"', html_text or '', re.IGNORECASE)
    if not match:
        return None
    return unescape(match.group('value')).strip() or None


def _clean_url(url: str | None) -> str:
    if not url:
        return ''
    value = unescape(url).replace('&amp;', '&').strip()
    if value.startswith('//'):
        value = f'https:{value}'
    return value


def _unwrap_bing_redirect(url: str) -> str:
    url = _clean_url(url)
    parsed = urlparse(url)
    if parsed.path.startswith('/videos/') and parsed.query:
        qs = parse_qs(parsed.query)
        for key in ('churl', 'ru', 'url', 'u'):
            if key in qs and qs[key]:
                return _clean_url(qs[key][0])
        if not parsed.netloc:
            return _clean_url(f"https://www.bing.com{url}")
    if parsed.netloc.endswith('bing.com') and parsed.path == '/ck/a':
        qs = parse_qs(parsed.query)
        for key in ('u', 'url'):
            if key in qs and qs[key]:
                candidate = qs[key][0]
                if candidate.startswith('a1'):
                    candidate = candidate[2:]
                    padding = '=' * (-len(candidate) % 4)
                    try:
                        decoded = base64.urlsafe_b64decode(f"{candidate}{padding}").decode("utf-8")
                        if decoded:
                            candidate = decoded
                    except (ValueError, UnicodeDecodeError):
                        pass
                return _clean_url(unquote(candidate))
    return _clean_url(url)


def _guess_provider(url: str) -> str | None:
    host = urlparse(url).netloc.lower()
    if host.startswith('www.'):
        host = host[4:]
    return host or None


def extract_bing_video_candidates(html_text: str, *, limit: int = 10) -> list[VideoCandidate]:
    candidates: list[VideoCandidate] = []
    seen: set[str] = set()

    media_meta: list[dict] = []
    for match in _BING_MEDIA_RE.finditer(html_text):
        raw = match.group('meta')
        try:
            decoded = raw.replace('&quot;', '"').replace('\\u002f', '/')
            media_meta.append(json.loads(decoded))
        except Exception:
            continue

    meta_iter = iter(media_meta)
    for match in _BING_ANCHOR_RE.finditer(html_text):
        attrs = match.group('attrs')
        body = match.group('body')
        anchor_html = match.group(0)
        href = _html_attr(attrs, 'href')
        direct_url = _html_attr(anchor_html, 'ourl')
        href = _unwrap_bing_redirect(direct_url or href or '')
        if not href or href in seen:
            continue
        label = _html_attr(attrs, 'aria-label') or ''
        title = (label.split(' from ', 1)[0].split(' · ', 1)[0]).strip()
        title = title or _strip_tags(body) or 'Untitled video'
        provider = _guess_provider(href)
        thumbnail_url = _clean_url(_html_attr(anchor_html, 'data-src-hq') or _html_attr(anchor_html, 'src')) or None
        duration_text = None
        source_url = None
        description = None

        try:
            meta = next(meta_iter)
        except StopIteration:
            meta = {}

        if isinstance(meta, dict):
            meta_thumbnail_url = _clean_url(meta.get('turl') or meta.get('imgurl') or meta.get('thumbnailUrl')) or None
            thumbnail_url = meta_thumbnail_url or thumbnail_url
            source_url = _clean_url(meta.get('murl') or meta.get('mediaUrl') or meta.get('vurl')) or None
            duration_text = str(meta.get('dur') or meta.get('duration') or '').strip() or None
            description = str(meta.get('desc') or meta.get('snippet') or '').strip() or None
            provider = provider or str(meta.get('surl') or meta.get('source') or '').strip() or None

        if not duration_text:
            tail = html_text[match.end(): match.end() + 500]
            duration_match = _BING_DURATION_RE.search(tail)
            if duration_match:
                duration_text = duration_match.group('duration')

        candidates.append(
            VideoCandidate(
                title=title,
                page_url=href,
                thumbnail_url=thumbnail_url,
                duration_text=duration_text,
                source_url=source_url,
                provider=provider,
                description=description,
            )
        )
        seen.add(href)
        if len(candidates) >= limit:
            break

    return candidates


SUPPORTED_PUBLIC_VIDEO_HINTS = (
    'youtube.com',
    'youtu.be',
    'vimeo.com',
    'dailymotion.com',
    'archive.org',
    'bilibili.com',
    'rutube.ru',
    'odysee.com',
    'streamable.com',
)


def build_bing_video_search_url(query: str) -> str:
    return f'https://www.bing.com/videos/search?q={quote_plus(query)}'
