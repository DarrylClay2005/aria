from __future__ import annotations

from html import escape
import io
import json
import unittest

from PIL import Image

from core.image_pipeline import (
    extract_bing_image_candidates,
    render_delivery_image,
    render_forge_image,
    render_upscale_fallback,
)


class ImagePipelineTests(unittest.TestCase):
    @staticmethod
    def _png_bytes(size: tuple[int, int], color: str = "navy") -> bytes:
        image = Image.new("RGB", size, color=color)
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue()

    def test_render_delivery_image_upscales_small_images(self) -> None:
        rendered = render_delivery_image(self._png_bytes((120, 60)), min_edge=512, max_edge=1024)

        self.assertEqual(rendered.extension, "png")
        self.assertEqual((rendered.width, rendered.height), (512, 256))

        decoded = Image.open(io.BytesIO(rendered.data))
        decoded.load()
        self.assertEqual(decoded.size, (512, 256))

    def test_render_forge_image_outputs_valid_png(self) -> None:
        rendered = render_forge_image("Aria quality test", background_color="#223344")

        self.assertEqual(rendered.extension, "png")
        self.assertGreater(rendered.width, 0)
        self.assertGreater(rendered.height, 0)

        decoded = Image.open(io.BytesIO(rendered.data))
        decoded.load()
        self.assertEqual(decoded.format, "PNG")

    def test_render_upscale_fallback_pushes_to_larger_target(self) -> None:
        rendered = render_upscale_fallback(self._png_bytes((160, 90)))

        self.assertEqual(rendered.extension, "png")
        self.assertEqual((rendered.width, rendered.height), (2048, 1152))

        decoded = Image.open(io.BytesIO(rendered.data))
        decoded.load()
        self.assertEqual(decoded.size, (2048, 1152))

    def test_extract_bing_image_candidates_uses_full_resolution_urls(self) -> None:
        metadata = escape(
            json.dumps(
                {
                    "murl": "https://images.example.com/full.png",
                    "turl": "https://th.bing.com/thumb.png?w=320&h=180",
                    "purl": "https://example.com/page",
                    "t": "Sample Result",
                }
            ),
            quote=True,
        )
        html = f'<a class="iusc" m="{metadata}"></a>'

        results = extract_bing_image_candidates(html)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].source_url, "https://images.example.com/full.png")
        self.assertEqual(results[0].preview_url, "https://th.bing.com/thumb.png?w=320&h=180")
        self.assertEqual(results[0].page_url, "https://example.com/page")


if __name__ == "__main__":
    unittest.main()
