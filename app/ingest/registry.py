from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path


class Registry:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS sources (
                    source_file_path TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    last_indexed_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT
                );

                CREATE TABLE IF NOT EXISTS documents (
                    doc_id TEXT PRIMARY KEY,
                    match_id TEXT NOT NULL,
                    document_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    doc_id TEXT NOT NULL,
                    match_id TEXT NOT NULL,
                    chunk_index INTEGER NOT NULL,
                    content TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_sources_match_id ON sources(match_id);
                CREATE INDEX IF NOT EXISTS idx_documents_match_id ON documents(match_id);
                CREATE INDEX IF NOT EXISTS idx_chunks_match_id ON chunks(match_id);

                CREATE TABLE IF NOT EXISTS players (
                    player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    canonical_name TEXT NOT NULL,
                    normalized_name TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS player_aliases (
                    player_id INTEGER NOT NULL,
                    alias TEXT NOT NULL,
                    normalized_alias TEXT NOT NULL,
                    alias_type TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (player_id, normalized_alias),
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                );

                CREATE TABLE IF NOT EXISTS match_players (
                    match_id TEXT NOT NULL,
                    player_id INTEGER NOT NULL,
                    team_name TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (match_id, player_id),
                    FOREIGN KEY (player_id) REFERENCES players(player_id)
                );

                CREATE INDEX IF NOT EXISTS idx_players_normalized_name ON players(normalized_name);
                CREATE INDEX IF NOT EXISTS idx_player_aliases_normalized_alias ON player_aliases(normalized_alias);
                CREATE INDEX IF NOT EXISTS idx_match_players_player_id ON match_players(player_id);
                """
            )

    def get_source(self, source_file_path: str) -> sqlite3.Row | None:
        with self.connect() as connection:
            return connection.execute(
                "SELECT * FROM sources WHERE source_file_path = ?",
                (source_file_path,),
            ).fetchone()
