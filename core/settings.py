import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some local shells
    def load_dotenv(*args, **kwargs):
        return False


BOT_ENV_PREFIX = "ARIA"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COGS_DIR = PROJECT_ROOT / "cogs"
REAL_ESRGAN_BINARY = PROJECT_ROOT / "realesrgan-ncnn-vulkan"
REAL_ESRGAN_MODEL_DIR = PROJECT_ROOT / "models"

_DEFAULT_SHARED_ENV_DIR = "Music"
_DEFAULT_OVERRIDE_USER_ID = "1304564041863266347"
_loaded_env_file = None


def _build_external_env_path() -> Path:
    shared_env_dir = os.getenv(f"{BOT_ENV_PREFIX}_SHARED_ENV_DIR", _DEFAULT_SHARED_ENV_DIR).strip() or _DEFAULT_SHARED_ENV_DIR
    return PROJECT_ROOT.parent / shared_env_dir / ".env"


def load_external_env() -> Path | None:
    global _loaded_env_file

    if _loaded_env_file is not None:
        return _loaded_env_file

    explicit_env_file = os.getenv(f"{BOT_ENV_PREFIX}_ENV_FILE", "").strip()
    candidates = []

    if explicit_env_file:
        candidates.append(Path(explicit_env_file).expanduser())

    candidates.append(_build_external_env_path())

    for candidate in candidates:
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            _loaded_env_file = candidate
            return _loaded_env_file

    return None


load_external_env()


def prefixed_env(name: str, default: str = "") -> str:
    return os.getenv(f"{BOT_ENV_PREFIX}_{name}", default)


TOKEN = prefixed_env("DISCORD_TOKEN", "").strip()
OVERRIDE_USER_ID = prefixed_env("OVERRIDE_USER_ID", _DEFAULT_OVERRIDE_USER_ID).strip()
GEMINI_MODEL_ID = prefixed_env("GEMINI_MODEL", os.getenv("GEMINI_MODEL", "gemini-2.5-flash")).strip() or "gemini-2.5-flash"
DB_CONFIG = {
    "host": prefixed_env("DB_HOST", "127.0.0.1"),
    "user": prefixed_env("DB_USER", "botuser"),
    "password": prefixed_env("DB_PASSWORD", ""),
    "db": prefixed_env("DB_NAME", "discord_aria"),
    "autocommit": True,
}
