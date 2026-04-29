from __future__ import annotations

import csv
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.domain import MatchRecord


@dataclass(slots=True)
class PlayerCandidate:
    player_id: int
    canonical_name: str
    matched_alias: str
    alias_type: str
    score: float


@dataclass(slots=True)
class PlayerImportStats:
    players_imported: int = 0
    aliases_imported: int = 0


def sync_match_players(connection: sqlite3.Connection, match: MatchRecord) -> None:
    connection.execute("DELETE FROM match_players WHERE match_id = ?", (match.match_id,))

    players_by_name = extract_player_entries(match)
    for canonical_name, team_name in sorted(players_by_name.items()):
        player_id = _upsert_player(connection, canonical_name)
        _upsert_aliases(connection, player_id, canonical_name)
        connection.execute(
            """
            INSERT OR REPLACE INTO match_players (match_id, player_id, team_name, updated_at)
            VALUES (?, ?, ?, datetime('now'))
            """,
            (match.match_id, player_id, team_name),
        )


def sync_external_player_directory(
    connection: sqlite3.Connection,
    players_data_dir: Path | None,
) -> PlayerImportStats:
    if players_data_dir is None:
        return PlayerImportStats()

    people_path = players_data_dir / "people.csv"
    names_path = players_data_dir / "names.csv"
    if not people_path.exists() or not names_path.exists():
        return PlayerImportStats()

    alias_rows = _load_alias_rows(names_path)
    stats = PlayerImportStats()

    with people_path.open(newline="", encoding="utf-8") as file_obj:
        for row in csv.DictReader(file_obj):
            canonical_name = (row.get("unique_name") or row.get("name") or "").strip()
            if not canonical_name:
                continue

            player_id = _upsert_player(connection, canonical_name)
            stats.players_imported += 1
            _upsert_aliases(connection, player_id, canonical_name)

            external_aliases = alias_rows.get((row.get("identifier") or "").strip(), [])
            for alias in external_aliases:
                if _upsert_alias(connection, player_id, alias, "external_csv"):
                    stats.aliases_imported += 1

    return stats


def extract_player_entries(match: MatchRecord) -> dict[str, str | None]:
    players: dict[str, str | None] = {}

    for innings in match.innings:
        team_name = innings.get("team")
        for batting in innings.get("batting", []):
            player_name = str(batting.get("player", "")).strip()
            if player_name:
                players[player_name] = team_name
        for bowling in innings.get("bowling", []):
            player_name = str(bowling.get("player", "")).strip()
            if player_name:
                players[player_name] = players.get(player_name, team_name)

    for player_name in match.player_of_match:
        cleaned = str(player_name).strip()
        if cleaned:
            players.setdefault(cleaned, None)

    return players


def resolve_player_name(
    connection: sqlite3.Connection,
    query_text: str,
    limit: int = 5,
) -> list[PlayerCandidate]:
    normalized_query = normalize_person_name(query_text)
    if not normalized_query:
        return []

    rows = connection.execute(
        """
        SELECT
            p.player_id,
            p.canonical_name,
            a.alias,
            a.alias_type,
            a.normalized_alias
        FROM player_aliases a
        JOIN players p ON p.player_id = a.player_id
        WHERE a.normalized_alias = ?
           OR a.normalized_alias LIKE ?
           OR ? LIKE '%' || a.normalized_alias || '%'
        """,
        (normalized_query, f"%{normalized_query}%", normalized_query),
    ).fetchall()

    candidates: list[PlayerCandidate] = []
    for row in rows:
        alias = str(row["alias"])
        alias_type = str(row["alias_type"])
        normalized_alias = str(row["normalized_alias"])
        score = _score_alias_match(normalized_query, normalized_alias, alias_type)
        candidates.append(
            PlayerCandidate(
                player_id=int(row["player_id"]),
                canonical_name=str(row["canonical_name"]),
                matched_alias=alias,
                alias_type=alias_type,
                score=score,
            )
        )

    deduped: dict[int, PlayerCandidate] = {}
    for candidate in sorted(candidates, key=lambda item: item.score, reverse=True):
        existing = deduped.get(candidate.player_id)
        if existing is None or candidate.score > existing.score:
            deduped[candidate.player_id] = candidate

    return sorted(deduped.values(), key=lambda item: item.score, reverse=True)[:limit]


def normalize_person_name(name: str) -> str:
    lowered = name.lower().strip()
    collapsed = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", collapsed).strip()


def generate_player_aliases(canonical_name: str) -> list[tuple[str, str]]:
    normalized_full_name = normalize_person_name(canonical_name)
    if not normalized_full_name:
        return []

    parts = normalized_full_name.split()
    aliases: list[tuple[str, str]] = [("canonical", normalized_full_name)]

    if len(parts) >= 2:
        first_name = parts[0]
        last_name = parts[-1]
        initials = "".join(part[0] for part in parts[:-1] if part)

        if len(first_name) >= 3:
            aliases.append(("first_name", first_name))
        if len(last_name) >= 3:
            aliases.append(("last_name", last_name))
        if initials:
            aliases.append(("initials_last_name", f"{initials} {last_name}"))
            aliases.append(("initials_compact", f"{initials}{last_name}"))

    seen: set[tuple[str, str]] = set()
    ordered_aliases: list[tuple[str, str]] = []
    for alias_type, alias in aliases:
        cleaned_alias = normalize_person_name(alias)
        if not cleaned_alias:
            continue
        item = (alias_type, cleaned_alias)
        if item in seen:
            continue
        seen.add(item)
        ordered_aliases.append(item)
    return ordered_aliases


def _upsert_player(connection: sqlite3.Connection, canonical_name: str) -> int:
    normalized_name = normalize_person_name(canonical_name)
    row = connection.execute(
        "SELECT player_id FROM players WHERE normalized_name = ?",
        (normalized_name,),
    ).fetchone()
    if row is not None:
        connection.execute(
            """
            UPDATE players
            SET canonical_name = ?, updated_at = datetime('now')
            WHERE player_id = ?
            """,
            (canonical_name, row["player_id"]),
        )
        return int(row["player_id"])

    cursor = connection.execute(
        """
        INSERT INTO players (canonical_name, normalized_name, created_at, updated_at)
        VALUES (?, ?, datetime('now'), datetime('now'))
        """,
        (canonical_name, normalized_name),
    )
    return int(cursor.lastrowid)


def _upsert_aliases(connection: sqlite3.Connection, player_id: int, canonical_name: str) -> None:
    for alias_type, alias in generate_player_aliases(canonical_name):
        _upsert_alias(connection, player_id, alias, alias_type)


def _load_alias_rows(names_path: Path) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    with names_path.open(newline="", encoding="utf-8") as file_obj:
        for row in csv.DictReader(file_obj):
            identifier = (row.get("identifier") or "").strip()
            alias = (row.get("name") or "").strip()
            if not identifier or not alias:
                continue
            aliases.setdefault(identifier, []).append(alias)
    return aliases


def _upsert_alias(connection: sqlite3.Connection, player_id: int, alias: str, alias_type: str) -> bool:
    normalized_alias = normalize_person_name(alias)
    if not normalized_alias:
        return False
    connection.execute(
        """
        INSERT OR REPLACE INTO player_aliases (
            player_id, alias, normalized_alias, alias_type, updated_at
        ) VALUES (?, ?, ?, ?, datetime('now'))
        """,
        (player_id, alias, normalized_alias, alias_type),
    )
    return True


def _score_alias_match(query: str, alias: str, alias_type: str) -> float:
    if query == alias:
        base = 100.0
    elif alias.startswith(query) or query.startswith(alias):
        base = 70.0
    else:
        base = 40.0

    alias_weights = {
        "canonical": 10.0,
        "initials_last_name": 8.0,
        "last_name": 6.0,
        "first_name": 4.0,
        "initials_compact": 3.0,
    }
    return base + alias_weights.get(alias_type, 0.0)
