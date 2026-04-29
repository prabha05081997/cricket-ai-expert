from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from app.domain import Chunk, RetrievedChunk


class LocalIndex:
    def __init__(self, registry_db_path: Path, chroma_dir: Path, collection_name: str, embedding_model_name: str) -> None:
        self.registry_db_path = registry_db_path
        self.chroma_dir = chroma_dir
        self.collection_name = collection_name
        self.embedding_model_name = embedding_model_name
        self._chroma_collection = None
        self._embedder = None

    def upsert_chunks(self, chunks: list[Chunk]) -> None:
        if not chunks:
            return

        self._persist_sqlite_chunks(chunks)
        collection = self._get_chroma_collection()
        if collection is None:
            return

        ids = [chunk.chunk_id for chunk in chunks]
        documents = [chunk.text for chunk in chunks]
        metadatas = [_sanitize_metadata(chunk.metadata) for chunk in chunks]
        embeddings = self._embed(documents)
        collection.upsert(ids=ids, documents=documents, metadatas=metadatas, embeddings=embeddings)

    def delete_match(self, match_id: str) -> None:
        with sqlite3.connect(self.registry_db_path) as connection:
            connection.execute("DELETE FROM documents WHERE match_id = ?", (match_id,))
            connection.execute("DELETE FROM chunks WHERE match_id = ?", (match_id,))
            connection.commit()

        collection = self._get_chroma_collection()
        if collection is not None:
            collection.delete(where={"match_id": match_id})

    def retrieve(self, query: str, top_k: int = 6) -> list[RetrievedChunk]:
        combined: dict[str, RetrievedChunk] = {}

        for chunk in self._keyword_search(query, top_k=top_k):
            combined[chunk.chunk_id] = chunk

        collection = self._get_chroma_collection()
        if collection is not None:
            query_embedding = self._embed([query])[0]
            results = collection.query(query_embeddings=[query_embedding], n_results=top_k)
            ids = results.get("ids", [[]])[0]
            docs = results.get("documents", [[]])[0]
            metadatas = results.get("metadatas", [[]])[0]
            distances = results.get("distances", [[]])[0]

            for chunk_id, text, metadata, distance in zip(ids, docs, metadatas, distances):
                score = 1.0 / (1.0 + float(distance))
                existing = combined.get(chunk_id)
                if existing is None or score > existing.score:
                    combined[chunk_id] = RetrievedChunk(
                        chunk_id=chunk_id,
                        text=text,
                        score=score,
                        metadata=metadata,
                    )

        return sorted(combined.values(), key=lambda item: item.score, reverse=True)[:top_k]

    def _persist_sqlite_chunks(self, chunks: list[Chunk]) -> None:
        with sqlite3.connect(self.registry_db_path) as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO chunks (chunk_id, doc_id, match_id, chunk_index, content, metadata_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                [
                    (
                        chunk.chunk_id,
                        chunk.doc_id,
                        chunk.match_id,
                        chunk.chunk_index,
                        chunk.text,
                        json.dumps(chunk.metadata),
                    )
                    for chunk in chunks
                ],
            )
            connection.commit()

    def _keyword_search(self, query: str, top_k: int) -> list[RetrievedChunk]:
        tokens = _normalize_query_tokens(query)
        if not tokens:
            return []

        with sqlite3.connect(self.registry_db_path) as connection:
            connection.row_factory = sqlite3.Row
            where_clauses = []
            params: list[str] = []
            for token in tokens[:8]:
                where_clauses.append("lower(content) LIKE ?")
                params.append(f"%{token}%")

            if not where_clauses:
                return []

            sql = (
                "SELECT chunk_id, content, metadata_json "
                "FROM chunks "
                f"WHERE {' OR '.join(where_clauses)} "
                "LIMIT 5000"
            )
            rows = connection.execute(sql, params).fetchall()

        scored: list[RetrievedChunk] = []
        for row in rows:
            haystack = row["content"].lower()
            metadata = json.loads(row["metadata_json"])
            score = _score_keyword_match(query, tokens, haystack, metadata)
            if score == 0:
                continue
            scored.append(
                RetrievedChunk(
                    chunk_id=row["chunk_id"],
                    text=row["content"],
                    score=float(score),
                    metadata=metadata,
                )
            )
        return sorted(scored, key=lambda item: item.score, reverse=True)[:top_k]

    def _get_chroma_collection(self):
        if self._chroma_collection is not None:
            return self._chroma_collection
        try:
            import chromadb
        except ImportError:
            return None

        client = chromadb.PersistentClient(path=str(self.chroma_dir))
        self._chroma_collection = client.get_or_create_collection(name=self.collection_name)
        return self._chroma_collection

    def _embed(self, texts: list[str]) -> list[list[float]]:
        if self._embedder is None:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError as exc:
                raise RuntimeError(
                    "sentence-transformers is required for vector indexing. Install project dependencies first."
                ) from exc
            self._embedder = SentenceTransformer(self.embedding_model_name)
        vectors = self._embedder.encode(texts, normalize_embeddings=True)
        return [vector.tolist() for vector in vectors]


def _sanitize_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    sanitized: dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, list):
            sanitized[key] = ", ".join(str(item) for item in value)
        elif value is None:
            sanitized[key] = ""
        else:
            sanitized[key] = value
    return sanitized


def _normalize_query_tokens(query: str) -> list[str]:
    raw_tokens = re.findall(r"[a-zA-Z0-9]+", query.lower())
    stopwords = {
        "the",
        "is",
        "in",
        "a",
        "an",
        "of",
        "what",
        "who",
        "it",
        "match",
        "score",
        "scores",
        "scored",
        "individual",
    }
    return [token for token in raw_tokens if token not in stopwords and len(token) > 1]


def _score_keyword_match(
    query: str,
    tokens: list[str],
    haystack: str,
    metadata: dict[str, Any],
) -> float:
    score = 0.0

    for token in tokens:
        token_hits = haystack.count(token)
        if token_hits:
            score += token_hits

        match_type = str(metadata.get("match_type", "")).lower()
        event_name = str(metadata.get("event_name", "")).lower()
        teams = str(metadata.get("teams", "")).lower()
        title = str(metadata.get("title", "")).lower()
        player_name = str(metadata.get("player_name", "")).lower()
        metadata_blob = " ".join([match_type, event_name, teams, title, player_name])
        if token in metadata_blob:
            score += 3.0

    lowered_query = query.lower()
    match_type = str(metadata.get("match_type", "")).lower()
    event_name = str(metadata.get("event_name", "")).lower()
    document_type = str(metadata.get("document_type", "")).lower()

    if "odi" in lowered_query and match_type in {"odi", "odm"}:
        score += 8.0
    if "t20" in lowered_query and "t20" in match_type:
        score += 8.0
    if "international" in lowered_query and "international" in event_name:
        score += 6.0
    if document_type == "player_performance":
        score += 2.0

    return score
