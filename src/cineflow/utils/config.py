from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_OVERRIDE_VAR = "CINEFLOW_ENV_FILE"
_INSIDE_CONTAINER = Path("/.dockerenv").exists()
_DEFAULT_ENV_CHAIN: tuple[tuple[Path, bool], ...] = (
    (Path(".env"), False),
) + (  # only load host overrides when *not* inside Docker/CI
    ((Path(".env.local"), True),) if not _INSIDE_CONTAINER else ()
)


def _bootstrap_env() -> None:
    """Load layered .env files so local/dev setups don't fight each other."""
    custom_env = os.getenv(_ENV_OVERRIDE_VAR)
    if custom_env:
        load_dotenv(custom_env, override=True)
        return

    for env_path, should_override in _DEFAULT_ENV_CHAIN:
        if env_path.exists():
            load_dotenv(env_path, override=should_override)


_bootstrap_env()


class Settings(BaseSettings):
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "cineflow"
    POSTGRES_USER: str = "cineflow"
    POSTGRES_PASSWORD: str = "cin3flow"
    MONGO_URI: str = "mongodb://cineflow:cineflow@localhost:27017"
    MONGO_DB: str = "cineflow"

    model_config = SettingsConfigDict(
        extra="ignore",  # ignore env vars only used in other contexts (e.g., Docker)
    )


settings = Settings()
