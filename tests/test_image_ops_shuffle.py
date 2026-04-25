import sys
import types


def _install_discord_stub() -> None:
    if "discord" in sys.modules:
        return

    discord = types.ModuleType("discord")

    class _DummyColor:
        @staticmethod
        def teal(): return 0
        @staticmethod
        def green(): return 0
        @staticmethod
        def blue(): return 0
        @staticmethod
        def purple(): return 0
        @staticmethod
        def gold(): return 0
        @staticmethod
        def dark_theme(): return 0

    class _DummyEmbed:
        def __init__(self, *args, **kwargs):
            pass
        def set_image(self, *args, **kwargs):
            pass
        def set_footer(self, *args, **kwargs):
            pass
        def add_field(self, *args, **kwargs):
            pass

    class _DummyFile:
        def __init__(self, fp=None, filename="file"):
            self.fp = fp
            self.filename = filename

    class _DummyInteraction: ...
    class _DummyMessage: ...
    class _DummyHTTPException(Exception): ...
    class _DummyNotFound(Exception): ...

    class _DummyTextInput:
        def __init__(self, *args, **kwargs):
            self.value = ""

    class _DummyModal:
        def __init_subclass__(cls, **kwargs):
            return super().__init_subclass__()
        def __init__(self, *args, **kwargs):
            pass

    class _DummyButton:
        def __init__(self, *args, **kwargs):
            self.disabled = False
            self.callback = None

    class _DummyView:
        def __init__(self, *args, **kwargs):
            self.children = []
        def add_item(self, item):
            self.children.append(item)

    discord.Color = _DummyColor
    discord.Embed = _DummyEmbed
    discord.File = _DummyFile
    discord.Interaction = _DummyInteraction
    discord.Message = _DummyMessage
    discord.HTTPException = _DummyHTTPException
    discord.NotFound = _DummyNotFound
    discord.ButtonStyle = types.SimpleNamespace(blurple=1, green=2)
    discord.ui = types.SimpleNamespace(Modal=_DummyModal, TextInput=_DummyTextInput, View=_DummyView, Button=_DummyButton)

    app_commands = types.ModuleType("discord.app_commands")
    class _DummyGroup:
        def __init__(self, *args, **kwargs):
            pass
        def command(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    class _DummyContextMenu:
        def __init__(self, name=None, callback=None):
            self.name = name
            self.callback = callback
            self.type = "context"
    app_commands.Group = _DummyGroup
    app_commands.ContextMenu = _DummyContextMenu

    ext = types.ModuleType("discord.ext")
    commands = types.ModuleType("discord.ext.commands")
    class _DummyCog: ...
    commands.Cog = _DummyCog
    ext.commands = commands

    discord.app_commands = app_commands
    discord.ext = ext

    sys.modules["discord"] = discord
    sys.modules["discord.app_commands"] = app_commands
    sys.modules["discord.ext"] = ext
    sys.modules["discord.ext.commands"] = commands


def _install_db_stub() -> None:
    if "core.database" in sys.modules:
        return
    database = types.ModuleType("core.database")
    database.db = types.SimpleNamespace(pool=None)
    sys.modules["core.database"] = database


def _install_settings_stub() -> None:
    if "core.settings" in sys.modules:
        return
    settings = types.ModuleType("core.settings")
    settings.REAL_ESRGAN_BINARY = "realesrgan"
    settings.REAL_ESRGAN_MODEL_DIR = "/tmp"
    settings.IMAGE_FILTER_LEVEL = "relaxed"
    sys.modules["core.settings"] = settings


_install_discord_stub()
_install_db_stub()
_install_settings_stub()

from cogs.image_ops import ImageOps
from core.image_pipeline import ImageCandidate


class _DummyTree:
    def add_command(self, command):
        return None

    def remove_command(self, name, type=None):
        return None


class _DummyBot:
    def __init__(self):
        self.tree = _DummyTree()


def _candidate(idx: int) -> ImageCandidate:
    return ImageCandidate(
        source_url=f"https://example.com/image-{idx}.jpg",
        preview_url=None,
        page_url=f"https://example.com/page-{idx}",
        title=f"Image {idx}",
    )


def test_same_query_is_reordered_across_repeated_searches_for_same_user():
    cog = ImageOps(_DummyBot())
    candidates = [_candidate(i) for i in range(12)]

    first = cog._shuffle_candidates_for_request(111, "cats", candidates)
    second = cog._shuffle_candidates_for_request(111, "cats", candidates)

    assert [item.source_url for item in first] != [item.source_url for item in second]
    assert first[0].source_url != second[0].source_url


def test_query_shuffle_history_is_scoped_per_user_and_query():
    cog = ImageOps(_DummyBot())
    candidates = [_candidate(i) for i in range(8)]

    cats_first = cog._shuffle_candidates_for_request(111, "cats", candidates)
    dogs_first = cog._shuffle_candidates_for_request(111, "dogs", candidates)
    other_user_cats = cog._shuffle_candidates_for_request(222, "cats", candidates)

    assert cats_first
    assert dogs_first
    assert other_user_cats
    assert cog._normalize_query_key("  CaTs   ") == "cats"


def test_relaxed_filter_level_maps_to_looser_provider_settings():
    cog = ImageOps(_DummyBot())

    assert cog.image_filter_level == "relaxed"
    assert cog._duckduckgo_safesearch() == "off"
    assert cog._bing_adult_filter() == "off"


def test_source_diversity_rotates_domains_before_repeating():
    cog = ImageOps(_DummyBot())
    candidates = [
        ImageCandidate(source_url="https://cdn.alpha.example/a1.jpg", page_url="https://alpha.example/1", title="a1"),
        ImageCandidate(source_url="https://cdn.alpha.example/a2.jpg", page_url="https://alpha.example/2", title="a2"),
        ImageCandidate(source_url="https://cdn.beta.example/b1.jpg", page_url="https://beta.example/1", title="b1"),
        ImageCandidate(source_url="https://cdn.beta.example/b2.jpg", page_url="https://beta.example/2", title="b2"),
        ImageCandidate(source_url="https://cdn.gamma.example/c1.jpg", page_url="https://gamma.example/1", title="c1"),
    ]

    diversified = cog._apply_source_diversity(candidates, limit=5)
    first_three_domains = [cog._candidate_domain(candidate) for candidate in diversified[:3]]

    assert len(set(first_three_domains)) == 3
