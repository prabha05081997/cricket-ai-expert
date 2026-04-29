from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class MatchRecord:
    match_id: str
    source_file: str
    date: str | None
    teams: list[str]
    gender: str | None
    match_type: str | None
    event_name: str | None
    venue: str | None
    city: str | None
    toss_winner: str | None
    toss_decision: str | None
    outcome: str | None
    player_of_match: list[str]
    innings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class Document:
    doc_id: str
    match_id: str
    title: str
    document_type: str
    content: str
    metadata: dict[str, Any]


@dataclass(slots=True)
class Chunk:
    chunk_id: str
    doc_id: str
    match_id: str
    text: str
    chunk_index: int
    metadata: dict[str, Any]


@dataclass(slots=True)
class RetrievedChunk:
    chunk_id: str
    text: str
    score: float
    metadata: dict[str, Any]

