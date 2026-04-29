from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from app.analytics.players import sync_external_player_directory, sync_match_players
from app.domain import Document, MatchRecord
from app.ingest.documents import build_documents
from app.ingest.parser import compute_file_hash, parse_match_file
from app.ingest.registry import Registry
from app.rag.chunking import chunk_document
from app.rag.index import LocalIndex
from app.settings import Settings


class IngestionPipeline:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.registry = Registry(settings.registry_db_path)
        self.index = LocalIndex(
            registry_db_path=settings.registry_db_path,
            chroma_dir=settings.chroma_dir,
            collection_name=settings.chroma_collection,
            embedding_model_name=settings.embedding_model,
        )

    def update(self) -> dict[str, int]:
        self.settings.validate_data_dir()
        self.settings.validate_players_data_dir()
        self._sync_external_player_data()
        seen = 0
        indexed = 0
        skipped = 0
        failed = 0

        for path in sorted(self.settings.cricsheet_data_dir.rglob("*.json")):
            seen += 1
            file_hash = compute_file_hash(path)
            source_row = self.registry.get_source(str(path))
            if source_row and source_row["file_hash"] == file_hash and source_row["status"] == "indexed":
                skipped += 1
                continue

            try:
                match = parse_match_file(path)
                documents = build_documents(match)
                self._replace_match(match.match_id, documents, match=match)
                self._record_source(path, match.match_id, file_hash, "indexed", None)
                indexed += 1
            except Exception as exc:
                self._record_source(path, path.stem, file_hash, "failed", str(exc))
                failed += 1

        return {"seen": seen, "indexed": indexed, "skipped": skipped, "failed": failed}

    def rebuild(self) -> dict[str, int]:
        if self.settings.registry_db_path.exists():
            self.settings.registry_db_path.unlink()
        if self.settings.chroma_dir.exists():
            for child in self.settings.chroma_dir.iterdir():
                if child.is_dir():
                    for nested in child.rglob("*"):
                        if nested.is_file():
                            nested.unlink()
                    for nested in sorted(child.rglob("*"), reverse=True):
                        if nested.is_dir():
                            nested.rmdir()
                    child.rmdir()
                else:
                    child.unlink()
        self.registry = Registry(self.settings.registry_db_path)
        self.index = LocalIndex(
            registry_db_path=self.settings.registry_db_path,
            chroma_dir=self.settings.chroma_dir,
            collection_name=self.settings.chroma_collection,
            embedding_model_name=self.settings.embedding_model,
        )
        return self.update()

    def _replace_match(
        self,
        match_id: str,
        documents: list[Document],
        match: MatchRecord | None = None,
    ) -> None:
        self.index.delete_match(match_id)
        if match is not None:
            self._persist_player_identities(match)
        self._persist_documents(documents)
        all_chunks = []
        for document in documents:
            all_chunks.extend(chunk_document(document))
        self.index.upsert_chunks(all_chunks)

    def _persist_documents(self, documents: list[Document]) -> None:
        with sqlite3.connect(self.settings.registry_db_path) as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO documents (doc_id, match_id, document_type, title, content, metadata_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        document.doc_id,
                        document.match_id,
                        document.document_type,
                        document.title,
                        document.content,
                        json.dumps(document.metadata),
                        _timestamp(),
                    )
                    for document in documents
                ],
            )
            connection.commit()

    def _persist_player_identities(self, match: MatchRecord) -> None:
        with sqlite3.connect(self.settings.registry_db_path) as connection:
            connection.row_factory = sqlite3.Row
            sync_match_players(connection, match)
            connection.commit()

    def _sync_external_player_data(self) -> None:
        with sqlite3.connect(self.settings.registry_db_path) as connection:
            connection.row_factory = sqlite3.Row
            sync_external_player_directory(connection, self.settings.players_data_dir)
            connection.commit()

    def _record_source(
        self,
        path: Path,
        match_id: str,
        file_hash: str,
        status: str,
        error_message: str | None,
    ) -> None:
        with sqlite3.connect(self.settings.registry_db_path) as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO sources (
                    source_file_path, match_id, file_hash, last_indexed_at, status, error_message
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(path), match_id, file_hash, _timestamp(), status, error_message),
            )
            connection.commit()


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()
