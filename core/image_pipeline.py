from __future__ import annotations

from dataclasses import dataclass
from html import unescape
import io
import json
from pathlib import Path
import re
import subprocess
import tempfile
from typing import Iterable

from PIL import Image, ImageDraw, ImageEnhance, ImageFont, ImageOps, UnidentifiedImageError


RESAMPLING = Image.Resampling if hasattr(Image, "Resampling") else Image
DEFAULT_FONT_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    "/usr/share/fonts/TTF/DejaVuSans-Bold.ttf",
    "/Library/Fonts/Arial Bold.ttf",
    "C:/Windows/Fonts/arialbd.ttf",
)
_BING_METADATA_RE = re.compile(r'\bm="(?P<meta>[^"]+)"')
_BING_URL_RE = re.compile(
    r"murl&quot;:&quot;(?P<source>.*?)&quot;.*?(?:turl&quot;:&quot;(?P<preview>.*?)&quot;)?"
    r".*?(?:purl&quot;:&quot;(?:\\u002f\\u002f)?(?P<page>.*?)&quot;)?",
    re.IGNORECASE | re.DOTALL,
)


class ImagePipelineError(RuntimeError):
    pass


@dataclass(slots=True)
class ImageCandidate:
    source_url: str
    preview_url: str | None = None
    page_url: str | None = None
    title: str | None = None

    def download_urls(self) -> tuple[str, ...]:
        seen = set()
        urls = []
        for url in (self.source_url, self.preview_url):
            cleaned = _clean_url(url)
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                urls.append(cleaned)
        return tuple(urls)


@dataclass(slots=True)
class RenderedImage:
    data: bytes
    extension: str
    width: int
    height: int
    mime_type: str


def _clean_url(url: str | None) -> str:
    if not url:
        return ""
    cleaned = unescape(url).replace("\\/", "/").replace("\\u002f", "/").strip()
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    return cleaned


def _rendered_image(image: Image.Image, *, extension: str = "png") -> RenderedImage:
    output = io.BytesIO()
    save_kwargs = {"format": extension.upper()}
    if extension.lower() in {"jpg", "jpeg"}:
        save_kwargs.update({"quality": 95, "optimize": True})
    else:
        save_kwargs.update({"optimize": True})
    image.save(output, **save_kwargs)
    return RenderedImage(
        data=output.getvalue(),
        extension="jpg" if extension.lower() == "jpeg" else extension.lower(),
        width=image.width,
        height=image.height,
        mime_type=f"image/{'jpeg' if extension.lower() in {'jpg', 'jpeg'} else extension.lower()}",
    )


def _open_image(source: bytes) -> Image.Image:
    try:
        image = Image.open(io.BytesIO(source))
        image.load()
    except (UnidentifiedImageError, OSError) as exc:
        raise ImagePipelineError("Image data could not be decoded.") from exc
    return ImageOps.exif_transpose(image)


def _calculate_scaled_size(size: tuple[int, int], *, min_edge: int, max_edge: int) -> tuple[int, int]:
    width, height = size
    longest_edge = max(width, height)
    scale = 1.0

    if longest_edge < min_edge:
        scale = min_edge / longest_edge

    if longest_edge * scale > max_edge:
        scale = max_edge / longest_edge

    if scale == 1.0:
        return width, height

    return max(1, round(width * scale)), max(1, round(height * scale))


def _enhance_image(image: Image.Image, *, sharpness: float = 1.22, contrast: float = 1.05) -> Image.Image:
    image = ImageEnhance.Sharpness(image).enhance(sharpness)
    image = ImageEnhance.Contrast(image).enhance(contrast)
    return image


def render_delivery_image(
    source: bytes,
    *,
    min_edge: int = 1280,
    max_edge: int = 2048,
    sharpness: float = 1.22,
    contrast: float = 1.05,
) -> RenderedImage:
    image = _open_image(source)
    has_alpha = "A" in image.getbands()
    working = image.convert("RGBA" if has_alpha else "RGB")
    target_size = _calculate_scaled_size(working.size, min_edge=min_edge, max_edge=max_edge)
    if target_size != working.size:
        working = working.resize(target_size, RESAMPLING.LANCZOS)
    working = _enhance_image(working, sharpness=sharpness, contrast=contrast)
    return _rendered_image(working, extension="png")


def render_upscale_fallback(source: bytes, *, min_edge: int = 2048, max_edge: int = 3072) -> RenderedImage:
    return render_delivery_image(
        source,
        min_edge=min_edge,
        max_edge=max_edge,
        sharpness=1.32,
        contrast=1.08,
    )


def _resolve_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in DEFAULT_FONT_PATHS:
        font_path = Path(candidate)
        if font_path.is_file():
            try:
                return ImageFont.truetype(str(font_path), size)
            except OSError:
                continue
    return ImageFont.load_default()


def _wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = text.split()
    if not words:
        return [""]

    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        trial = f"{current} {word}"
        bbox = draw.textbbox((0, 0), trial, font=font)
        if bbox[2] - bbox[0] <= max_width:
            current = trial
            continue
        lines.append(current)
        current = word
    lines.append(current)
    return lines


def render_forge_image(text: str, *, background_color: str = "black") -> RenderedImage:
    canvas_color = background_color.lower() if background_color.isalpha() else "#1E1E2E"
    image = Image.new("RGB", (1600, 900), color=canvas_color)
    draw = ImageDraw.Draw(image)
    font = _resolve_font(110)
    lines = _wrap_text(draw, text.strip() or "Aria", font, int(image.width * 0.82))
    line_height = draw.textbbox((0, 0), "Ag", font=font)[3] + 18
    total_height = line_height * len(lines)
    y = (image.height - total_height) / 2

    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        text_width = bbox[2] - bbox[0]
        x = (image.width - text_width) / 2
        draw.text((x + 6, y + 6), line, font=font, fill="black")
        draw.text((x, y), line, font=font, fill="white")
        y += line_height

    return _rendered_image(image, extension="png")


def upscale_with_realesrgan(
    source: bytes,
    *,
    binary_path: str | Path,
    model_dir: str | Path,
    model_name: str = "realesrgan-x4plus",
    scale: int = 4,
    blend_alpha: float = 0.72,
) -> RenderedImage:
    binary = Path(binary_path)
    models = Path(model_dir)

    if not binary.is_file():
        raise FileNotFoundError("Bundled Real-ESRGAN executable is missing.")
    if not models.is_dir():
        raise FileNotFoundError("Bundled Real-ESRGAN model directory is missing.")

    base = _open_image(source).convert("RGB")

    with tempfile.TemporaryDirectory(prefix="aria-upscale-") as tmpdir:
        temp_dir = Path(tmpdir)
        input_path = temp_dir / "input.png"
        output_path = temp_dir / "output.png"
        base.save(input_path, format="PNG")

        completed = subprocess.run(
            [
                str(binary),
                "-i",
                str(input_path),
                "-o",
                str(output_path),
                "-s",
                str(scale),
                "-t",
                "256",
                "-n",
                model_name,
                "-m",
                str(models),
            ],
            capture_output=True,
            text=True,
        )
        if completed.returncode != 0:
            stderr = completed.stderr.strip() or completed.stdout.strip()
            raise ImagePipelineError(stderr or "Real-ESRGAN failed to produce an output image.")

        ai_image = _open_image(output_path.read_bytes()).convert("RGBA")
        resized_base = base.resize(ai_image.size, RESAMPLING.LANCZOS).convert("RGBA")
        blended = Image.blend(resized_base, ai_image, alpha=blend_alpha)
        blended = _enhance_image(blended)
        return _rendered_image(blended, extension="png")


def dedupe_candidates(candidates: Iterable[ImageCandidate], *, limit: int = 20) -> list[ImageCandidate]:
    unique: list[ImageCandidate] = []
    seen = set()
    for candidate in candidates:
        key = _clean_url(candidate.source_url)
        if not key or key in seen:
            continue
        seen.add(key)
        unique.append(
            ImageCandidate(
                source_url=key,
                preview_url=_clean_url(candidate.preview_url) or None,
                page_url=_clean_url(candidate.page_url) or None,
                title=(candidate.title or "").strip() or None,
            )
        )
        if len(unique) >= limit:
            break
    return unique


def extract_bing_image_candidates(html_text: str, *, limit: int = 20) -> list[ImageCandidate]:
    candidates: list[ImageCandidate] = []

    for match in _BING_METADATA_RE.finditer(html_text):
        metadata = unescape(match.group("meta"))
        try:
            payload = json.loads(metadata)
        except json.JSONDecodeError:
            continue

        source_url = _clean_url(payload.get("murl") or payload.get("imgurl"))
        preview_url = _clean_url(payload.get("turl"))
        page_url = _clean_url(payload.get("purl"))
        title = (payload.get("t") or payload.get("title") or "").strip() or None
        if source_url:
            candidates.append(ImageCandidate(source_url, preview_url or None, page_url or None, title))

    if candidates:
        return dedupe_candidates(candidates, limit=limit)

    for match in _BING_URL_RE.finditer(html_text):
        source_url = _clean_url(match.group("source"))
        preview_url = _clean_url(match.group("preview"))
        page_url = _clean_url(match.group("page"))
        if source_url:
            candidates.append(ImageCandidate(source_url, preview_url or None, page_url or None, None))

    return dedupe_candidates(candidates, limit=limit)
