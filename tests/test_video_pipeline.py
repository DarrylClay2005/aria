from __future__ import annotations

import base64
from html import escape
import json

from core.video_pipeline import build_bing_video_search_url, extract_bing_video_candidates


def _bing_encoded_url(url: str) -> str:
    encoded = base64.urlsafe_b64encode(url.encode("utf-8")).decode("ascii").rstrip("=")
    return f"https://www.bing.com/ck/a?u=a1{encoded}"


def test_extract_bing_video_candidates_decodes_bing_redirect_url():
    html = (
        f'<a class="mc_vtvc_link" href="{_bing_encoded_url("https://www.youtube.com/watch?v=abc123")}">'
        "A clean result title"
        "</a>"
    )

    results = extract_bing_video_candidates(html)

    assert len(results) == 1
    assert results[0].page_url == "https://www.youtube.com/watch?v=abc123"
    assert results[0].provider == "youtube.com"
    assert results[0].title == "A clean result title"


def test_extract_bing_video_candidates_unwraps_relative_riverview_url():
    html = (
        '<a class="mc_vtvc_link" '
        'href="/videos/riverview/relatedvideo?q=lofi&churl=https%3A%2F%2Fwww.youtube.com%2Fwatch%3Fv%3Dxyz789">'
        "Relative Bing result"
        "</a>"
    )

    results = extract_bing_video_candidates(html)

    assert len(results) == 1
    assert results[0].page_url == "https://www.youtube.com/watch?v=xyz789"
    assert results[0].provider == "youtube.com"


def test_extract_bing_video_candidates_prefers_direct_ourl_and_clean_title():
    html = (
        '<a aria-label="Clean Video Title from YouTube · Duration: 3 minutes" '
        'class="mc_vtvc_link" '
        'href="/videos/riverview/relatedvideo?q=lofi&churl=https%3A%2F%2Fwww.youtube.com%2Fchannel%2Fabc">'
        '<div ourl="https://www.youtube.com/watch?v=direct123">'
        '<img data-src-hq="https://thumb.example/direct.jpg">'
        "</div>"
        "</a>"
    )

    results = extract_bing_video_candidates(html)

    assert len(results) == 1
    assert results[0].page_url == "https://www.youtube.com/watch?v=direct123"
    assert results[0].provider == "youtube.com"
    assert results[0].title == "Clean Video Title"
    assert results[0].thumbnail_url == "https://thumb.example/direct.jpg"


def test_extract_bing_video_candidates_pairs_metadata_with_results():
    metadata = escape(
        json.dumps(
            {
                "turl": "https://thumb.example/video.jpg",
                "murl": "https://media.example/video.mp4",
                "dur": "1:02:03",
                "desc": "Preview description",
            }
        ),
        quote=True,
    )
    html = (
        f'<div m="{metadata}"></div>'
        '<a class="mc_vtvc_link" href="https://vimeo.com/123">Vimeo result</a>'
    )

    results = extract_bing_video_candidates(html)

    assert len(results) == 1
    assert results[0].thumbnail_url == "https://thumb.example/video.jpg"
    assert results[0].source_url == "https://media.example/video.mp4"
    assert results[0].duration_text == "1:02:03"
    assert results[0].description == "Preview description"


def test_build_bing_video_search_url_quotes_query():
    assert build_bing_video_search_url("aria music videos") == "https://www.bing.com/videos/search?q=aria+music+videos"
