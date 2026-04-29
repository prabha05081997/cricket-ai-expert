from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from app.analytics.players import upsert_player_identity
from app.domain import MatchRecord


@dataclass(slots=True)
class AggregateQuery:
    metric: str
    match_type: str | None = None
    year: int | None = None
    venue: str | None = None
    international_only: bool = False


class AnalyticsQueryService:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    def answer(self, question: str) -> dict[str, object] | None:
        parsed = parse_aggregate_question(question)
        if parsed is None:
            return None

        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row
            if parsed.metric == "highest_individual_score":
                row = _query_highest_individual_score(connection, parsed)
                if row is None:
                    return None
                answer = (
                    f"The highest individual score"
                    f"{_format_filter_suffix(parsed)} is {row['runs']} by {row['player_name']} "
                    f"for {row['innings_team']} against {row['opposition_team']} on {row['date']}."
                )
                source_text = (
                    f"{row['player_name']} scored {row['runs']} off {row['balls']} balls "
                    f"for {row['innings_team']} against {row['opposition_team']} in "
                    f"{row['match_type']} on {row['date']} at {row['venue'] or 'unknown venue'}."
                )
                return {
                    "answer": answer,
                    "sources": [
                        {
                            "chunk_id": f"analytics:{row['match_id']}:highest_individual_score",
                            "score": 1.0,
                            "text": source_text,
                            "title": "Analytics record result",
                            "match_id": row["match_id"],
                            "date": row["date"],
                            "match_type": row["match_type"],
                            "event_name": row["event_name"],
                            "venue": row["venue"],
                            "document_type": "analytics_result",
                        }
                    ],
                }

            if parsed.metric == "most_runs":
                row = _query_most_runs(connection, parsed)
                if row is None:
                    return None
                answer = (
                    f"The player with the most runs{_format_filter_suffix(parsed)} is "
                    f"{row['player_name']} with {row['total_runs']} runs."
                )
                source_text = (
                    f"{row['player_name']} has {row['total_runs']} runs across {row['innings_count']} innings"
                    f"{_format_filter_suffix(parsed)}."
                )
                return {
                    "answer": answer,
                    "sources": [
                        {
                            "chunk_id": f"analytics:player:{row['player_id']}:most_runs",
                            "score": 1.0,
                            "text": source_text,
                            "title": "Analytics aggregate result",
                            "match_id": "",
                            "date": "",
                            "match_type": parsed.match_type or "",
                            "event_name": "",
                            "venue": parsed.venue or "",
                            "document_type": "analytics_result",
                        }
                    ],
                }

            if parsed.metric == "most_wickets":
                row = _query_most_wickets(connection, parsed)
                if row is None:
                    return None
                answer = (
                    f"The player with the most wickets{_format_filter_suffix(parsed)} is "
                    f"{row['player_name']} with {row['total_wickets']} wickets."
                )
                source_text = (
                    f"{row['player_name']} has {row['total_wickets']} wickets in {row['match_count']} matches"
                    f"{_format_filter_suffix(parsed)}."
                )
                return {
                    "answer": answer,
                    "sources": [
                        {
                            "chunk_id": f"analytics:player:{row['player_id']}:most_wickets",
                            "score": 1.0,
                            "text": source_text,
                            "title": "Analytics aggregate result",
                            "match_id": "",
                            "date": "",
                            "match_type": parsed.match_type or "",
                            "event_name": "",
                            "venue": parsed.venue or "",
                            "document_type": "analytics_result",
                        }
                    ],
                }

        return None


def sync_match_analytics(connection: sqlite3.Connection, match: MatchRecord) -> None:
    connection.execute("DELETE FROM batting_performances WHERE match_id = ?", (match.match_id,))
    connection.execute("DELETE FROM bowling_performances WHERE match_id = ?", (match.match_id,))
    connection.execute("DELETE FROM analytics_innings WHERE match_id = ?", (match.match_id,))
    connection.execute("DELETE FROM analytics_matches WHERE match_id = ?", (match.match_id,))

    connection.execute(
        """
        INSERT INTO analytics_matches (
            match_id, date, teams_csv, gender, match_type, event_name, venue, city,
            toss_winner, toss_decision, outcome, player_of_match_csv, source_file, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            match.match_id,
            match.date,
            " vs ".join(match.teams),
            match.gender,
            match.match_type,
            match.event_name,
            match.venue,
            match.city,
            match.toss_winner,
            match.toss_decision,
            match.outcome,
            ", ".join(match.player_of_match),
            match.source_file,
        ),
    )

    for innings_index, innings in enumerate(match.innings, start=1):
        team_name = str(innings.get("team") or "")
        opposition_team = next((team for team in match.teams if team != team_name), None)
        connection.execute(
            """
            INSERT INTO analytics_innings (
                match_id, innings_number, team_name, opposition_team, runs, wickets, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
            """,
            (
                match.match_id,
                innings_index,
                team_name,
                opposition_team,
                int(innings.get("runs", 0)),
                int(innings.get("wickets", 0)),
            ),
        )

        for batting in innings.get("batting", []):
            player_name = str(batting.get("player", "")).strip()
            if not player_name:
                continue
            player_id = upsert_player_identity(connection, player_name)
            runs = int(batting.get("runs", 0))
            balls = int(batting.get("balls", 0))
            fours = int(batting.get("fours", 0))
            sixes = int(batting.get("sixes", 0))
            strike_rate = round((runs * 100.0 / balls), 2) if balls else None
            connection.execute(
                """
                INSERT INTO batting_performances (
                    match_id, innings_number, player_id, player_name, innings_team, opposition_team,
                    runs, balls, fours, sixes, strike_rate, match_type, date, venue, event_name, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    match.match_id,
                    innings_index,
                    player_id,
                    player_name,
                    team_name,
                    opposition_team,
                    runs,
                    balls,
                    fours,
                    sixes,
                    strike_rate,
                    match.match_type,
                    match.date,
                    match.venue,
                    match.event_name,
                ),
            )

        for bowling in innings.get("bowling", []):
            player_name = str(bowling.get("player", "")).strip()
            if not player_name:
                continue
            player_id = upsert_player_identity(connection, player_name)
            wickets = int(bowling.get("wickets", 0))
            runs_conceded = int(bowling.get("runs_conceded", 0))
            balls_bowled = int(bowling.get("balls", 0))
            economy = round((runs_conceded * 6.0 / balls_bowled), 2) if balls_bowled else None
            connection.execute(
                """
                INSERT INTO bowling_performances (
                    match_id, innings_number, player_id, player_name, bowling_team, opposition_team,
                    wickets, runs_conceded, balls_bowled, economy, match_type, date, venue, event_name, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    match.match_id,
                    innings_index,
                    player_id,
                    player_name,
                    opposition_team,
                    team_name,
                    wickets,
                    runs_conceded,
                    balls_bowled,
                    economy,
                    match.match_type,
                    match.date,
                    match.venue,
                    match.event_name,
                ),
            )


def parse_aggregate_question(question: str) -> AggregateQuery | None:
    lowered = question.lower()

    metric: str | None = None
    if "highest individual score" in lowered or ("highest" in lowered and "score" in lowered and "individual" in lowered):
        metric = "highest_individual_score"
    elif "most wickets" in lowered:
        metric = "most_wickets"
    elif "most runs" in lowered:
        metric = "most_runs"

    if metric is None:
        return None

    match_type = _extract_match_type(lowered)
    year_match = re.search(r"\b(19|20)\d{2}\b", lowered)
    venue_match = re.search(r"\bat\s+([a-z0-9' .-]+?)(?:\?|$|\sin\s|\sfor\s)", lowered)
    venue = venue_match.group(1).strip() if venue_match else None
    international_only = "international" in lowered or "icc" in lowered

    return AggregateQuery(
        metric=metric,
        match_type=match_type,
        year=int(year_match.group(0)) if year_match else None,
        venue=venue,
        international_only=international_only,
    )


def _extract_match_type(question: str) -> str | None:
    if "t20i" in question or "t20 international" in question or "international t20" in question:
        return "T20I"
    if re.search(r"\bodi\b", question):
        return "ODI"
    if re.search(r"\btest\b", question):
        return "Test"
    if re.search(r"\bt20\b", question):
        return "T20"
    return None


def _query_highest_individual_score(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.match_id,
            bp.player_id,
            bp.player_name,
            bp.innings_team,
            bp.opposition_team,
            bp.runs,
            bp.balls,
            bp.match_type,
            bp.date,
            bp.venue,
            bp.event_name
        FROM batting_performances bp
        WHERE {' AND '.join(clauses)}
        ORDER BY bp.runs DESC, bp.balls ASC, bp.date ASC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_runs(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.player_id,
            bp.player_name,
            SUM(bp.runs) AS total_runs,
            COUNT(*) AS innings_count
        FROM batting_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY bp.player_id, bp.player_name
        ORDER BY total_runs DESC, innings_count ASC, bp.player_name ASC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_wickets(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.player_id,
            bp.player_name,
            SUM(bp.wickets) AS total_wickets,
            COUNT(DISTINCT bp.match_id) AS match_count
        FROM bowling_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY bp.player_id, bp.player_name
        ORDER BY total_wickets DESC, match_count ASC, bp.player_name ASC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _apply_common_filters(
    clauses: list[str],
    params: list[object],
    parsed: AggregateQuery,
    table_alias: str,
) -> None:
    if parsed.match_type:
        allowed_types = _match_type_filters(parsed.match_type)
        placeholders = ", ".join("?" for _ in allowed_types)
        clauses.append(f"{table_alias}.match_type IN ({placeholders})")
        params.extend(allowed_types)
    if parsed.year:
        clauses.append(f"substr({table_alias}.date, 1, 4) = ?")
        params.append(str(parsed.year))
    if parsed.venue:
        clauses.append(f"lower({table_alias}.venue) LIKE ?")
        params.append(f"%{parsed.venue.lower()}%")
    if parsed.international_only and not parsed.match_type:
        allowed_types = ["ODI", "Test", "IT20", "T20I"]
        placeholders = ", ".join("?" for _ in allowed_types)
        clauses.append(f"{table_alias}.match_type IN ({placeholders})")
        params.extend(allowed_types)


def _format_filter_suffix(parsed: AggregateQuery) -> str:
    parts: list[str] = []
    if parsed.match_type:
        parts.append(parsed.match_type)
    if parsed.year:
        parts.append(str(parsed.year))
    if parsed.international_only and not parsed.match_type:
        parts.append("international cricket")
    if parsed.venue:
        parts.append(f"at {parsed.venue}")
    if not parts:
        return ""
    if parsed.venue and not parsed.match_type and not parsed.year:
        return f" at {parsed.venue}"
    return f" in {' '.join(parts)}"


def _match_type_filters(match_type: str) -> list[str]:
    normalized = match_type.upper()
    if normalized == "T20I":
        return ["IT20", "T20I"]
    if normalized == "T20":
        return ["T20", "IT20", "T20I"]
    return [match_type]
