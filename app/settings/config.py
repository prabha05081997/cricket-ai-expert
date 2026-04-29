from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    cricsheet_data_dir: Path = Field(alias="CRICSHEET_DATA_DIR")
    players_data_dir: Path | None = Field(default=None, alias="CRICKET_AI_PLAYERS_DATA_DIR")
    storage_dir: Path = Field(default=Path("./storage"), alias="CRICKET_AI_STORAGE_DIR")
    embedding_model: str = Field(
        default="sentence-transformers/all-MiniLM-L6-v2",
        alias="CRICKET_AI_EMBEDDING_MODEL",
    )
    chroma_collection: str = Field(default="cricsheet_chunks", alias="CRICKET_AI_CHROMA_COLLECTION")
    ollama_model: str = Field(default="llama3.2:3b", alias="CRICKET_AI_OLLAMA_MODEL")
    ollama_base_url: str = Field(default="http://localhost:11434", alias="CRICKET_AI_OLLAMA_BASE_URL")
    top_k: int = 6

    @field_validator("storage_dir", "cricsheet_data_dir", "players_data_dir", mode="before")
    @classmethod
    def _expand_path(cls, value: str | Path | None) -> Path | None:
        if value in (None, ""):
            return None
        return Path(value).expanduser().resolve()

    @property
    def registry_db_path(self) -> Path:
        return self.storage_dir / "registry.sqlite3"

    @property
    def chroma_dir(self) -> Path:
        return self.storage_dir / "chroma"

    def ensure_storage(self) -> None:
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.chroma_dir.mkdir(parents=True, exist_ok=True)

    def validate_data_dir(self) -> None:
        if not self.cricsheet_data_dir.exists() or not self.cricsheet_data_dir.is_dir():
            raise FileNotFoundError(
                "CRICSHEET_DATA_DIR does not exist or is not a directory: "
                f"{self.cricsheet_data_dir}"
            )

    def validate_players_data_dir(self) -> None:
        if self.players_data_dir is None:
            return
        if not self.players_data_dir.exists() or not self.players_data_dir.is_dir():
            raise FileNotFoundError(
                "CRICKET_AI_PLAYERS_DATA_DIR does not exist or is not a directory: "
                f"{self.players_data_dir}"
            )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_storage()
    return settings
