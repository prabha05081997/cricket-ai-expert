from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from app.analytics.players import get_preferred_player_display_name, upsert_player_identity
from app.domain import MatchRecord

if TYPE_CHECKING:
    from app.rag.intent import IntentResult


@dataclass(slots=True)
class AggregateQuery:
    metric: str
    match_type: str | None = None
    year: int | None = None
    venue: str | None = None
    international_only: bool = False


@dataclass(slots=True)
class PlayerMatchQuery:
    player_name: str
    match_type: str | None = None
    event_name: str | None = None
    year: int | None = None
    team_terms: list[str] | None = None
    match_id: str | None = None  # when set, all other filters are ignored


@dataclass(slots=True)
class DismissalQuery:
    """Query for who dismissed a specific batter (optionally in a specific match context)."""
    batter_name: str
    match_id: str | None = None       # pin to a specific match when context is available
    event_name: str | None = None
    year: int | None = None
    match_type: str | None = None


class AnalyticsQueryService:
    def __init__(self, db_path: Path, ollama_base_url: str = "", ollama_model: str = "") -> None:
        self.db_path = db_path
        self._ollama_base_url = ollama_base_url
        self._ollama_model = ollama_model

    def answer(self, question: str, intent: IntentResult | None = None) -> dict[str, object] | None:
        """Answer a question using structured analytics."""
        with sqlite3.connect(self.db_path) as connection:
            connection.row_factory = sqlite3.Row

            if intent is not None:
                if intent.intent == "player_performance" and intent.player:
                    return self._answer_player_performance(connection, question, intent)

                if intent.intent == "player_dismissal" and intent.player:
                    return self._answer_player_dismissal(connection, question, intent)

                if intent.intent == "aggregate_stats":
                    parsed = _intent_to_aggregate_query(intent)
                    if parsed is not None:
                        return self._answer_aggregate(connection, parsed)
                    return None

                return None

            # When intent is None the LLM classifier was unavailable.
            # Return None and let the caller fall back to RAG.
            return None

    def _answer_player_performance(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        from app.analytics.players import resolve_player_name

        # If we have a pinned match_id from context, use it directly
        if intent.pinned_match_id:
            pmq = PlayerMatchQuery(
                player_name=intent.player,
                match_id=intent.pinned_match_id,
            )
            return _answer_player_match_question(connection, pmq)

        # 1. Resolve player to all known aliases
        candidates = resolve_player_name(connection, intent.player, limit=1)
        player_names: set[str] = {intent.player}
        player_id: int | None = None
        if candidates:
            player_id = candidates[0].player_id
            alias_rows = connection.execute(
                "SELECT alias FROM player_aliases WHERE player_id = ?", (player_id,)
            ).fetchall()
            for r in alias_rows:
                player_names.add(str(r["alias"]).strip())

        # 2. Find all candidate matches for this player with the given filters
        match_candidates = _find_player_match_candidates(
            connection, player_names, intent
        )

        if not match_candidates:
            return None

        # 3. If only one candidate, use it directly. Otherwise ask the LLM to pick.
        chosen_match_id: str | None = None
        if len(match_candidates) == 1:
            chosen_match_id = str(match_candidates[0]["match_id"])
        elif self._ollama_base_url:
            from app.rag.intent import select_match
            chosen_match_id = select_match(
                question, match_candidates,
                self._ollama_base_url, self._ollama_model,
            )

        # 4. Fall back to most recent match if LLM selection failed
        if not chosen_match_id:
            chosen_match_id = str(match_candidates[-1]["match_id"])  # last = most recent

        # 5. Query the actual performance for the chosen match
        pmq = PlayerMatchQuery(
            player_name=intent.player,
            match_type=None,
            event_name=None,
            year=None,
            team_terms=None,
            match_id=chosen_match_id,  # pin to exact match
        )
        return _answer_player_match_question(connection, pmq)

    def _answer_player_dismissal(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        from app.analytics.players import resolve_player_name

        candidates = resolve_player_name(connection, intent.player, limit=1)
        batter_names: set[str] = {intent.player}
        if candidates:
            alias_rows = connection.execute(
                "SELECT alias FROM player_aliases WHERE player_id = ?",
                (candidates[0].player_id,),
            ).fetchall()
            for r in alias_rows:
                batter_names.add(str(r["alias"]).strip())

        # If the intent carries a pinned match_id (from last_match_id in context),
        # use it directly — no candidate search needed, no LLM selection needed.
        pinned_match_id = getattr(intent, "pinned_match_id", None)
        if pinned_match_id:
            dq = DismissalQuery(batter_name=intent.player, match_id=pinned_match_id)
            return _answer_dismissal_question(connection, dq)

        # Otherwise find candidates and let the LLM pick
        dismissal_candidates = _find_dismissal_candidates(connection, batter_names, intent)
        if not dismissal_candidates:
            return None

        chosen_match_id: str | None = None
        if len(dismissal_candidates) == 1:
            chosen_match_id = str(dismissal_candidates[0]["match_id"])
        elif self._ollama_base_url:
            from app.rag.intent import select_match
            chosen_match_id = select_match(
                question, dismissal_candidates,
                self._ollama_base_url, self._ollama_model,
            )

        if not chosen_match_id:
            chosen_match_id = str(dismissal_candidates[-1]["match_id"])

        dq = DismissalQuery(batter_name=intent.player, match_id=chosen_match_id)
        return _answer_dismissal_question(connection, dq)

    def _answer_aggregate(
        self, connection: sqlite3.Connection, parsed: "AggregateQuery"
    ) -> dict[str, object] | None:
        if parsed.metric == "highest_individual_score":
            row = _query_highest_individual_score(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            answer = (
                f"The highest individual score"
                f"{_format_filter_suffix(parsed)} is {row['runs']} by {display_name} "
                f"for {row['innings_team']} against {row['opposition_team']} on {row['date']}."
            )
            source_text = (
                f"{display_name} scored {row['runs']} off {row['balls']} balls "
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
                        "player_name": row["player_name"],
                        "display_name": display_name,
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
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            answer = (
                f"The player with the most runs{_format_filter_suffix(parsed)} is "
                f"{display_name} with {row['total_runs']} runs."
            )
            source_text = (
                f"{display_name} has {row['total_runs']} runs across {row['innings_count']} innings"
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
                        "player_name": row["player_name"],
                        "display_name": display_name,
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
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            answer = (
                f"The player with the most wickets{_format_filter_suffix(parsed)} is "
                f"{display_name} with {row['total_wickets']} wickets."
            )
            source_text = (
                f"{display_name} has {row['total_wickets']} wickets in {row['match_count']} matches"
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
                        "player_name": row["player_name"],
                        "display_name": display_name,
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
    connection.execute("DELETE FROM dismissals WHERE match_id = ?", (match.match_id,))

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

        # Persist structured dismissal records
        for dismissal in innings.get("dismissals", []):
            batter_name = str(dismissal.get("batter", "")).strip()
            if not batter_name:
                continue
            batter_player_id = upsert_player_identity(connection, batter_name)
            bowler_name = str(dismissal.get("bowler", "")).strip() or None
            bowler_player_id = upsert_player_identity(connection, bowler_name) if bowler_name else None
            connection.execute(
                """
                INSERT OR REPLACE INTO dismissals (
                    match_id, innings_number, batter_name, batter_player_id,
                    bowler_name, bowler_player_id, dismissal_kind, over_number,
                    match_type, date, event_name, batting_team, bowling_team, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    match.match_id,
                    innings_index,
                    batter_name,
                    batter_player_id,
                    bowler_name,
                    bowler_player_id,
                    str(dismissal.get("kind", "unknown")),
                    dismissal.get("over"),
                    match.match_type,
                    match.date,
                    match.event_name,
                    team_name,
                    opposition_team,
                ),
            )


def parse_aggregate_question(question: str) -> AggregateQuery | None:
    """Offline fallback: parse aggregate stats questions without the LLM.

    Only used by tests. In production, _intent_to_aggregate_query() is used
    with the LLM-classified IntentResult.
    """
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
    venue_match = re.search(r"\bat\s+([a-z0-9' .-]+?)(?:\?|$|\sin\s|\sfor\s)", question, flags=re.IGNORECASE)
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
    """Offline fallback: extract match type from question text."""
    if "t20i" in question or "t20 international" in question or "international t20" in question:
        return "T20I"
    if re.search(r"\bodi\b", question):
        return "ODI"
    if re.search(r"\btest\b", question):
        return "Test"
    if re.search(r"\bt20\b", question):
        return "T20"
    return None


def _answer_dismissal_question(
    connection: sqlite3.Connection,
    query: "DismissalQuery",
) -> dict[str, object] | None:
    from app.analytics.players import get_preferred_player_display_name, resolve_player_name

    # Resolve batter to all known aliases for the DB lookup
    candidates = resolve_player_name(connection, query.batter_name, limit=1)
    batter_names: set[str] = {query.batter_name}
    batter_player_id: int | None = None
    if candidates:
        batter_player_id = candidates[0].player_id
        alias_rows = connection.execute(
            "SELECT alias FROM player_aliases WHERE player_id = ?",
            (batter_player_id,),
        ).fetchall()
        for row in alias_rows:
            alias = str(row["alias"]).strip()
            if alias:
                batter_names.add(alias)

    placeholders = ", ".join("?" for _ in batter_names)
    clauses = [f"lower(d.batter_name) IN ({placeholders})"]
    params: list[object] = [n.lower() for n in batter_names]

    if query.match_id:
        clauses.append("d.match_id = ?")
        params.append(query.match_id)
    if query.event_name:
        clauses.append(
            "(lower(d.event_name) LIKE ? OR ? LIKE '%' || lower(d.event_name) || '%')"
        )
        params.append(f"%{query.event_name.lower()}%")
        params.append(query.event_name.lower())
    if query.year:
        clauses.append("substr(d.date, 1, 4) = ?")
        params.append(str(query.year))
    if query.match_type:
        allowed = _match_type_filters(query.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"d.match_type IN ({ph})")
        params.extend(allowed)

    sql = f"""
        SELECT
            d.match_id,
            d.batter_name,
            d.batter_player_id,
            d.bowler_name,
            d.bowler_player_id,
            d.dismissal_kind,
            d.over_number,
            d.date,
            d.event_name,
            d.match_type,
            d.batting_team,
            d.bowling_team,
            am.teams_csv
        FROM dismissals d
        LEFT JOIN analytics_matches am ON am.match_id = d.match_id
        WHERE {' AND '.join(clauses)}
        ORDER BY
            CASE WHEN lower(d.event_name) LIKE '%world cup%' THEN 0 ELSE 1 END,
            d.date DESC
        LIMIT 1
    """
    row = connection.execute(sql, params).fetchone()
    if row is None:
        return None

    # Resolve display names — always use the player_id from the row itself,
    # not from the candidate resolution, to avoid wrong-player display name bugs.
    batter_display = query.batter_name
    row_batter_player_id = row["batter_player_id"]
    if row_batter_player_id:
        batter_display = get_preferred_player_display_name(
            connection, int(row_batter_player_id), query.batter_name
        )

    bowler_name = row["bowler_name"]
    bowler_display = bowler_name or "unknown"
    if row["bowler_player_id"]:
        bowler_display = get_preferred_player_display_name(
            connection, int(row["bowler_player_id"]), bowler_name or "unknown"
        )

    kind = str(row["dismissal_kind"])
    over = row["over_number"]
    match_label = str(row["teams_csv"] or f"{row['batting_team']} vs {row['bowling_team']}")
    date = str(row["date"] or "")

    # Build a natural-sounding answer
    over_str = f" in over {over}" if over is not None else ""
    if kind in {"caught", "caught and bowled"}:
        if kind == "caught and bowled":
            answer = f"{batter_display} was caught and bowled by {bowler_display}{over_str}"
        else:
            answer = f"{batter_display} was caught off the bowling of {bowler_display}{over_str}"
    elif kind == "bowled":
        answer = f"{batter_display} was bowled by {bowler_display}{over_str}"
    elif kind == "lbw":
        answer = f"{batter_display} was given out LBW to {bowler_display}{over_str}"
    elif kind == "run out":
        answer = f"{batter_display} was run out{over_str}"
    elif kind == "stumped":
        answer = f"{batter_display} was stumped off {bowler_display}{over_str}"
    else:
        answer = f"{batter_display} was dismissed ({kind}){over_str}"
        if bowler_display != "unknown":
            answer += f" — bowler credited: {bowler_display}"

    answer += f" ({match_label}"
    if date:
        answer += f", {date}"
    answer += ")."

    source_text = (
        f"{batter_display} dismissed: {kind}"
        + (f" b {bowler_display}" if bowler_display != "unknown" else "")
        + f" | {match_label} | {date}"
    )
    return {
        "answer": answer,
        "sources": [
            {
                "chunk_id": f"analytics:{row['match_id']}:dismissal:{row['batter_name']}",
                "score": 1.0,
                "text": source_text,
                "title": "Dismissal record",
                "player_name": row["batter_name"],
                "display_name": batter_display,
                "teams": match_label,
                "match_id": row["match_id"],
                "date": date,
                "match_type": row["match_type"] or "",
                "event_name": row["event_name"] or "",
                "venue": "",
                "document_type": "analytics_result",
            }
        ],
    }

def _intent_to_aggregate_query(intent: IntentResult) -> AggregateQuery | None:
    """Convert an IntentResult with aggregate_stats intent into an AggregateQuery."""
    metric_map = {
        "highest_score": "highest_individual_score",
        "runs": "most_runs",
        "wickets": "most_wickets",
    }
    # Also try to infer metric from the rewritten question text as fallback
    metric = metric_map.get(intent.metric or "")
    if not metric:
        lowered = (intent.rewritten_question or "").lower()
        if "highest" in lowered and ("score" in lowered or "run" in lowered):
            metric = "highest_individual_score"
        elif "most runs" in lowered or "most run" in lowered:
            metric = "most_runs"
        elif "most wicket" in lowered:
            metric = "most_wickets"
    if not metric:
        return None
    return AggregateQuery(
        metric=metric,
        match_type=intent.match_type,
        year=intent.year,
        venue=intent.venue,
        international_only=bool(intent.event and "international" in (intent.event or "").lower()),
    )


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
    if parsed.venue and not parsed.match_type and not parsed.year and not parsed.international_only:
        return f" across all recorded matches at {parsed.venue}"

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


def _normalize_event_name(event: str | None) -> str | None:
    """Strip year prefixes and stage suffixes the LLM adds to event names.

    The DB stores short names like "ICC Cricket World Cup".
    The LLM often returns "2011 ICC Cricket World Cup Final" or
    "ICC Cricket World Cup 2011 Final".  We strip those extras so the
    LIKE filter has a chance of matching.
    """
    if not event:
        return None
    cleaned = event.strip()
    # Remove leading 4-digit year
    cleaned = re.sub(r"^\d{4}\s+", "", cleaned).strip()
    # Remove trailing stage words (before or after stripping year)
    cleaned = re.sub(
        r"\s+(final|semi.final|quarter.final|group stage|qualifier[s]?)$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    # Remove trailing 4-digit year (may appear after stage word removal)
    cleaned = re.sub(r"\s+\d{4}$", "", cleaned).strip()
    # One more pass for stage words that were after the year
    cleaned = re.sub(
        r"\s+(final|semi.final|quarter.final|group stage|qualifier[s]?)$",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    return cleaned or None

def _find_player_match_candidates(
    connection: sqlite3.Connection,
    player_names: set[str],
    intent: "IntentResult",
) -> list[dict[str, str]]:
    """Return candidate matches for a player given intent filters."""
    placeholders = ", ".join("?" for _ in player_names)
    clauses = [f"lower(bp.player_name) IN ({placeholders})"]
    params: list[object] = [n.lower() for n in player_names]

    if intent.match_type:
        allowed = _match_type_filters(intent.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"bp.match_type IN ({ph})")
        params.extend(allowed)
    if intent.year:
        clauses.append("substr(bp.date, 1, 4) = ?")
        params.append(str(intent.year))
    if intent.event:
        normalized = _normalize_event_name(intent.event)
        if normalized:
            clauses.append(
                "(lower(bp.event_name) LIKE ? OR ? LIKE '%' || lower(bp.event_name) || '%')"
            )
            params.append(f"%{normalized.lower()}%")
            params.append(normalized.lower())
    if intent.team:
        clauses.append(
            "(lower(am.teams_csv) LIKE ? OR lower(bp.innings_team) LIKE ? OR lower(bp.opposition_team) LIKE ?)"
        )
        params.extend([f"%{intent.team.lower()}%"] * 3)

    sql = f"""
        SELECT DISTINCT bp.match_id, bp.date, am.teams_csv, bp.event_name
        FROM batting_performances bp
        JOIN analytics_matches am ON am.match_id = bp.match_id
        WHERE {' AND '.join(clauses)}
        ORDER BY bp.date ASC
        LIMIT 20
    """
    rows = connection.execute(sql, params).fetchall()
    return [
        {
            "match_id": str(r["match_id"]),
            "date": str(r["date"] or ""),
            "teams": str(r["teams_csv"] or ""),
            "event": str(r["event_name"] or ""),
        }
        for r in rows
    ]


def _find_dismissal_candidates(
    connection: sqlite3.Connection,
    batter_names: set[str],
    intent: "IntentResult",
) -> list[dict[str, str]]:
    """Return candidate dismissal matches for a batter given intent filters."""
    placeholders = ", ".join("?" for _ in batter_names)
    clauses = [f"lower(d.batter_name) IN ({placeholders})"]
    params: list[object] = [n.lower() for n in batter_names]

    if intent.year:
        clauses.append("substr(d.date, 1, 4) = ?")
        params.append(str(intent.year))
    if intent.event:
        normalized = _normalize_event_name(intent.event)
        if normalized:
            clauses.append(
                "(lower(d.event_name) LIKE ? OR ? LIKE '%' || lower(d.event_name) || '%')"
            )
            params.append(f"%{normalized.lower()}%")
            params.append(normalized.lower())

    sql = f"""
        SELECT DISTINCT d.match_id, d.date, am.teams_csv, d.event_name
        FROM dismissals d
        LEFT JOIN analytics_matches am ON am.match_id = d.match_id
        WHERE {' AND '.join(clauses)}
        ORDER BY d.date ASC
        LIMIT 20
    """
    rows = connection.execute(sql, params).fetchall()
    return [
        {
            "match_id": str(r["match_id"]),
            "date": str(r["date"] or ""),
            "teams": str(r["teams_csv"] or ""),
            "event": str(r["event_name"] or ""),
        }
        for r in rows
    ]


def _answer_player_match_question(connection: sqlite3.Connection, query: PlayerMatchQuery) -> dict[str, object] | None:
    from app.analytics.players import get_preferred_player_display_name, normalize_person_name, resolve_player_name

    candidates = resolve_player_name(connection, query.player_name, limit=3)
    player_names = {str(query.player_name)}
    player_id: int | None = None
    if candidates:
        player_id = candidates[0].player_id
        alias_rows = connection.execute(
            "SELECT alias, normalized_alias FROM player_aliases WHERE player_id = ?",
            (player_id,),
        ).fetchall()
        for row in alias_rows:
            alias = str(row["alias"]).strip()
            if alias:
                player_names.add(alias)
    last_name = query.player_name.strip().split()[-1] if query.player_name.strip() else ""
    if len(last_name) >= 3:
        player_names.add(last_name)

    placeholders = ", ".join("?" for _ in player_names)
    clauses = [f"lower(bp.player_name) IN ({placeholders})"]
    params: list[object] = [name.lower() for name in player_names]

    # When match_id is set, pin directly to that match — ignore all other filters
    if query.match_id:
        clauses.append("bp.match_id = ?")
        params.append(query.match_id)
    else:
        if query.match_type:
            allowed = _match_type_filters(query.match_type)
            placeholders = ", ".join("?" for _ in allowed)
            clauses.append(f"bp.match_type IN ({placeholders})")
            params.extend(allowed)
        if query.year:
            clauses.append("substr(bp.date, 1, 4) = ?")
            params.append(str(query.year))
        if query.event_name:
            clauses.append(
                "(lower(bp.event_name) LIKE ? OR ? LIKE '%' || lower(bp.event_name) || '%')"
            )
            params.append(f"%{query.event_name.lower()}%")
            params.append(query.event_name.lower())
        if query.team_terms:
            for term in query.team_terms:
                clauses.append("(lower(am.teams_csv) LIKE ? OR lower(bp.innings_team) LIKE ? OR lower(bp.opposition_team) LIKE ?)")
                params.extend([f"%{term}%", f"%{term}%", f"%{term}%"])

    sql = f"""
        SELECT
            bp.match_id,
            bp.player_id,
            bp.player_name,
            bp.innings_team,
            bp.opposition_team,
            bp.runs,
            bp.balls,
            bp.fours,
            bp.sixes,
            bp.match_type,
            bp.date,
            bp.venue,
            bp.event_name,
            am.teams_csv,
            bw.wickets AS bowling_wickets,
            bw.runs_conceded,
            bw.balls_bowled
        FROM batting_performances bp
        JOIN analytics_matches am ON am.match_id = bp.match_id
        LEFT JOIN bowling_performances bw
          ON bw.match_id = bp.match_id
         AND bw.player_id = bp.player_id
        WHERE {' AND '.join(clauses)}
        ORDER BY bp.date DESC
        LIMIT 1
    """
    row = connection.execute(sql, params).fetchone()
    if row is None:
        return None

    resolved_player_id = int(row["player_id"])  # always trust the actual row's player_id
    display_name = get_preferred_player_display_name(connection, resolved_player_id, str(row["player_name"]))
    if len(normalize_person_name(query.player_name)) > len(normalize_person_name(display_name)):
        display_name = query.player_name
    batting_part = (
        f"{display_name} scored {row['runs']} runs off {row['balls']} balls with "
        f"{row['fours']} fours and {row['sixes']} sixes"
    )
    bowling_part = ""
    if row["bowling_wickets"] is not None and int(row["balls_bowled"] or 0) > 0:
        bowling_part = (
            f" He also bowled {row['balls_bowled']} balls, taking {row['bowling_wickets']} wickets "
            f"for {row['runs_conceded']} runs."
        )
    answer = (
        f"In {row['teams_csv']} on {row['date']}, {batting_part} for {row['innings_team']} "
        f"against {row['opposition_team']}.{bowling_part}"
    )
    source_text = (
        f"{display_name}: {row['runs']}({row['balls']}) for {row['innings_team']} "
        f"against {row['opposition_team']} in {row['match_type']} on {row['date']}."
    )
    return {
        "answer": answer,
        "sources": [
            {
                "chunk_id": f"analytics:{row['match_id']}:player_match:{row['player_id']}",
                "score": 1.0,
                "text": source_text,
                "title": "Analytics player match result",
                "player_name": row["player_name"],
                "display_name": display_name,
                "teams": row["teams_csv"],
                "match_id": row["match_id"],
                "date": row["date"],
                "match_type": row["match_type"],
                "event_name": row["event_name"],
                "venue": row["venue"],
                "document_type": "analytics_result",
            }
        ],
    }
