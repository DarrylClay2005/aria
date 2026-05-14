import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency in some local shells
    def load_dotenv(path, override=False, **_kwargs):
        env_path = Path(path)
        if not env_path.is_file():
            return False
        for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if not key or (not override and key in os.environ):
                continue
            value = value.strip().strip('"').strip("'")
            os.environ[key] = value
        return True


BOT_ENV_PREFIX = "ARIA"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COGS_DIR = PROJECT_ROOT / "cogs"
REAL_ESRGAN_BINARY = Path(os.getenv("ARIA_REAL_ESRGAN_BINARY", os.getenv("REAL_ESRGAN_BINARY", str(PROJECT_ROOT / "realesrgan-ncnn-vulkan")))).expanduser()
REAL_ESRGAN_MODEL_DIR = Path(os.getenv("ARIA_REAL_ESRGAN_MODEL_DIR", os.getenv("REAL_ESRGAN_MODEL_DIR", str(PROJECT_ROOT / "models")))).expanduser()

_DEFAULT_SHARED_ENV_DIR = "Music"
_DEFAULT_OVERRIDE_USER_ID = "1304564041863266347"
_ENV_STATE = {"loaded_file": None}


def _build_external_env_path() -> Path:
    shared_env_dir = os.getenv(f"{BOT_ENV_PREFIX}_SHARED_ENV_DIR", _DEFAULT_SHARED_ENV_DIR).strip() or _DEFAULT_SHARED_ENV_DIR
    return PROJECT_ROOT.parent / shared_env_dir / ".env"


def load_external_env() -> Path | None:
    if _ENV_STATE["loaded_file"] is not None:
        return _ENV_STATE["loaded_file"]

    explicit_env_file = os.getenv(f"{BOT_ENV_PREFIX}_ENV_FILE", "").strip()
    candidates = []

    if explicit_env_file:
        candidates.append(Path(explicit_env_file).expanduser())

    candidates.append(PROJECT_ROOT / ".env")
    candidates.append(_build_external_env_path())

    loaded_file = None
    for candidate in candidates:
        if candidate.is_file():
            load_dotenv(candidate, override=False)
            loaded_file = loaded_file or candidate

    _ENV_STATE["loaded_file"] = loaded_file
    if loaded_file:
        return loaded_file
    return None


load_external_env()


def prefixed_env(name: str, default: str = "") -> str:
    return os.getenv(f"{BOT_ENV_PREFIX}_{name}", default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = prefixed_env(name, os.getenv(name, ""))
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int_set(name: str) -> set[int]:
    raw = prefixed_env(name, os.getenv(name, ""))
    values: set[int] = set()
    for item in str(raw or "").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.add(int(item))
        except ValueError:
            continue
    return values


TOKEN = prefixed_env("DISCORD_TOKEN", "").strip()
OVERRIDE_USER_ID = prefixed_env("OVERRIDE_USER_ID", _DEFAULT_OVERRIDE_USER_ID).strip()
TELEGRAM_BOT_TOKEN = (prefixed_env("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")).strip()
TELEGRAM_ALLOWED_CHAT_IDS = _env_int_set("TELEGRAM_ALLOWED_CHAT_IDS")
TELEGRAM_POLLING_ENABLED = _env_bool("TELEGRAM_POLLING_ENABLED", bool(TELEGRAM_BOT_TOKEN))
GEMINI_MODEL_ID = prefixed_env("GEMINI_MODEL", os.getenv("GEMINI_MODEL", "gemini-2.5-flash-lite")).strip() or "gemini-2.5-flash-lite"
GEMINI_FALLBACK_MODELS = [
    value.strip()
    for value in prefixed_env("GEMINI_FALLBACK_MODELS", os.getenv("GEMINI_FALLBACK_MODELS", "gemini-2.5-flash")).split(",")
    if value.strip()
]
GROK_MODEL_ID = prefixed_env("GROK_MODEL", os.getenv("GROK_MODEL", "grok-4.3")).strip() or "grok-4.3"
GROK_FALLBACK_MODELS = [
    value.strip()
    for value in prefixed_env("GROK_FALLBACK_MODELS", os.getenv("GROK_FALLBACK_MODELS", "")).split(",")
    if value.strip()
]
GROQ_MODEL_ID = (
    prefixed_env(
        "GROQ_MODEL",
        os.getenv("GROQ_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct"),
    ).strip()
    or "meta-llama/llama-4-scout-17b-16e-instruct"
)
GROQ_FALLBACK_MODELS = [
    value.strip()
    for value in prefixed_env("GROQ_FALLBACK_MODELS", os.getenv("GROQ_FALLBACK_MODELS", "")).split(",")
    if value.strip()
]
OPENAI_MODEL_ID = prefixed_env("OPENAI_MODEL", os.getenv("OPENAI_MODEL", "gpt-4.1-mini")).strip() or "gpt-4.1-mini"
OPENAI_FALLBACK_MODELS = [
    value.strip()
    for value in prefixed_env("OPENAI_FALLBACK_MODELS", os.getenv("OPENAI_FALLBACK_MODELS", "")).split(",")
    if value.strip()
]
DB_CONFIG = {
    "host": prefixed_env("DB_HOST") or os.getenv("DB_HOST") or os.getenv("MYSQL_HOST") or "host.docker.internal",
    "port": int(prefixed_env("DB_PORT") or os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or "3306"),
    "user": prefixed_env("DB_USER") or os.getenv("DB_USER") or os.getenv("MYSQL_USER") or "botuser",
    "password": prefixed_env("DB_PASSWORD") or os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD") or "",
    "db": prefixed_env("DB_NAME") or os.getenv("ARIA_DB_NAME") or "discord_aria",
    "autocommit": True,
}


IMAGE_FILTER_LEVEL = prefixed_env("IMAGE_FILTER_LEVEL", "relaxed").strip().lower() or "relaxed"
