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
    # For top-N queries
    limit: int = 1
    # For ranked record queries such as "second highest score".
    rank: int = 1
    # For follow-up aggregate queries pinned to one match, e.g. "top scorer".
    match_id: str | None = None


@dataclass(slots=True)
class PlayerCareerQuery:
    """Career stats for a specific player across all or filtered matches."""
    player_name: str
    match_type: str | None = None
    year: int | None = None
    event_name: str | None = None


@dataclass(slots=True)
class HeadToHeadQuery:
    """Win/loss record between two teams."""
    team1: str
    team2: str
    match_type: str | None = None
    year: int | None = None


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

                if intent.intent == "match_narrative":
                    return self._answer_match_narrative(connection, intent)

                if intent.intent == "mixed":
                    mixed = self._answer_mixed_question(connection, question, intent)
                    if mixed is not None:
                        return mixed

                if intent.intent == "aggregate_stats":
                    # Career stats for a specific player — check before leaderboards
                    if intent.player:
                        special = _answer_player_specific_aggregate(connection, intent)
                        if special is not None:
                            return special

                        career_q = _intent_to_career_query(intent)
                        if career_q is not None:
                            result = _answer_career_query(connection, career_q)
                            if result is not None:
                                # Always let the LLM synthesise the answer from the
                                # career fact sheet. This handles any question phrasing
                                # without keyword matching — "how many centuries?",
                                # "what's the average?", "overseas record?", etc.
                                if self._ollama_base_url:
                                    result["answer"] = self._synthesise_career_answer(
                                        intent.rewritten_question, result
                                    )
                                return result

                    # Head-to-head between two teams — extract both from rewritten question
                    h2h = _intent_to_head_to_head(intent)
                    if h2h is not None:
                        result = _answer_head_to_head(connection, h2h)
                        if result is not None:
                            return result

                    # Leaderboard / record queries
                    parsed = _intent_to_aggregate_query(intent)
                    if parsed is not None:
                        return self._answer_aggregate(connection, parsed)
                    return None

                return None

            # When intent is None the LLM classifier was unavailable.
            # Return None and let the caller fall back to RAG.
            return None

    def _synthesise_career_answer(
        self,
        question: str,
        career_result: dict[str, object],
    ) -> str:
        """Use the LLM to answer a specific question from the career stats fact sheet.

        The career fact sheet contains all computed stats. The LLM reads it and
        answers the specific question — "how many centuries?", "what's the average?",
        "overseas record?" — without any keyword matching on our side.

        If the fact sheet doesn't contain the data needed (e.g. home/away splits),
        the LLM will say so honestly.
        """
        import httpx

        sources = career_result.get("sources") or []
        if not sources:
            return str(career_result.get("answer", ""))

        fact_sheet = str(sources[0].get("text", ""))
        prompt = (
            "You are a cricket stats assistant. Answer the question using ONLY the "
            "career stats fact sheet below.\n"
            "If the question asks for 'career stats' or 'career record', include: "
            "matches, innings, runs, highest score, average, centuries, fifties.\n"
            "If the question asks for a specific stat (e.g. average, centuries), answer that directly.\n"
            "If the fact sheet does not contain the information needed "
            "(e.g. home/away splits, overseas records, venue-specific stats), "
            "say exactly: \"I don't have that breakdown in the current dataset.\"\n"
            "Do not invent numbers. Do not add information not in the fact sheet.\n\n"
            f"Career stats fact sheet:\n{fact_sheet}\n\n"
            f"Question: {question}\n"
            "Answer:"
        )
        try:
            response = httpx.post(
                f"{self._ollama_base_url.rstrip('/')}/api/generate",
                json={
                    "model": self._ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0},
                },
                timeout=30.0,
            )
            response.raise_for_status()
            synthesised = str(response.json().get("response", "")).strip()
            return synthesised if synthesised else str(career_result.get("answer", ""))
        except Exception:
            return str(career_result.get("answer", ""))

    def _answer_mixed_question(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        if intent.player and intent.player2:
            return self._answer_player_comparison(connection, question, intent)
        return None

    def _answer_player_comparison(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        left_query = PlayerCareerQuery(
            player_name=intent.player,
            match_type=intent.match_type,
            year=intent.year,
            event_name=intent.event,
        )
        right_query = PlayerCareerQuery(
            player_name=intent.player2,
            match_type=intent.match_type,
            year=intent.year,
            event_name=intent.event,
        )

        left_result = _answer_career_query(connection, left_query)
        right_result = _answer_career_query(connection, right_query)

        if left_result is None or right_result is None:
            return None

        answer = self._synthesise_comparison_answer(
            question,
            left_result,
            right_result,
        )

        return {
            "answer": answer,
            "sources": left_result.get("sources", []) + right_result.get("sources", []),
        }

    def _synthesise_comparison_answer(
        self,
        question: str,
        left_result: dict[str, object],
        right_result: dict[str, object],
    ) -> str:
        import httpx

        left_text = str(left_result.get("sources", [])[0].get("text", ""))
        right_text = str(right_result.get("sources", [])[0].get("text", ""))

        prompt = (
            "You are a cricket analyst. Compare the two fact sheets below and answer the question directly.\n"
            "Focus on the comparison request and mention both players by name.\n"
            "Do not invent numbers or conclusions not supported by the fact sheets.\n"
            "If the answer cannot be determined from the provided data, say:\n"
            "\"I don't have enough information in the current context to answer that.\"\n\n"
            f"Question: {question}\n\n"
            f"Player 1 fact sheet:\n{left_text}\n\n"
            f"Player 2 fact sheet:\n{right_text}\n"
        )

        if not self._ollama_base_url:
            return f"{left_result.get('answer', '')}\n\n{right_result.get('answer', '')}"

        try:
            response = httpx.post(
                f"{self._ollama_base_url.rstrip('/')}/api/generate",
                json={
                    "model": self._ollama_model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0},
                },
                timeout=30.0,
            )
            response.raise_for_status()
            return str(response.json().get("response", "")).strip() or f"{left_result.get('answer', '')}\n\n{right_result.get('answer', '')}"
        except Exception:
            return f"{left_result.get('answer', '')}\n\n{right_result.get('answer', '')}"

    def _answer_match_narrative(
        self,
        connection: sqlite3.Connection,
        intent: "IntentResult",
    ) -> dict[str, object] | None:
        """Answer 'who won X?' questions from the analytics_matches table."""
        lowered = intent.rewritten_question.lower()

        # Build filters from intent
        clauses: list[str] = ["1=1"]
        params: list[object] = []

        if intent.match_type:
            allowed = _match_type_filters(intent.match_type)
            ph = ", ".join("?" for _ in allowed)
            clauses.append(f"match_type IN ({ph})")
            params.extend(allowed)
        if intent.year:
            clauses.append("substr(date, 1, 4) = ?")
            params.append(str(intent.year))
        if intent.event:
            normalized = _normalize_event_name(intent.event)
            if normalized:
                clauses.append(
                    "(lower(event_name) LIKE ? OR ? LIKE '%' || lower(event_name) || '%')"
                )
                params.append(f"%{normalized.lower()}%")
                params.append(normalized.lower())
        if intent.team:
            clauses.append(
                "(lower(teams_csv) LIKE ?)"
            )
            params.append(f"%{intent.team.lower()}%")

        # For "final" questions, take the last match by date in the event
        is_final = "final" in lowered and "semi" not in lowered and "quarter" not in lowered
        order = "date DESC" if is_final else "date DESC"

        sql = f"""
            SELECT match_id, teams_csv, outcome, date, event_name, match_type
            FROM analytics_matches
            WHERE {' AND '.join(clauses)}
            ORDER BY {order}
            LIMIT 1
        """
        row = connection.execute(sql, params).fetchone()
        if row is None:
            row = _find_match_result_fallback(connection, intent.rewritten_question)
        if row is None or not row["outcome"]:
            return None

        answer = (
            f"{row['teams_csv']} — {row['outcome']} "
            f"({row['event_name'] or row['match_type']}, {row['date']})."
        )
        source_text = answer
        return {
            "answer": answer,
            "sources": [{
                "chunk_id": f"analytics:{row['match_id']}:narrative",
                "score": 1.0,
                "text": source_text,
                "title": f"Match result: {row['teams_csv']}",
                "player_name": "",
                "display_name": "",
                "teams": row["teams_csv"],
                "match_id": row["match_id"],
                "date": row["date"],
                "match_type": row["match_type"] or "",
                "event_name": row["event_name"] or "",
                "venue": "",
                "document_type": "analytics_result",
            }],
        }

    def _answer_player_performance(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        from app.analytics.players import resolve_player_name
        import re as _re

        # If we have a pinned match_id from context, use it directly
        if intent.pinned_match_id:
            pmq = PlayerMatchQuery(
                player_name=intent.player,
                match_id=intent.pinned_match_id,
            )
            if "bowl" in intent.rewritten_question.lower() or "bowling" in intent.rewritten_question.lower():
                return _answer_player_bowling_match_question(connection, pmq)
            return _answer_player_match_question(connection, pmq)

        # Extract a specific date from the rewritten question if present
        # e.g. "on 2014-11-13" → use as exact date filter
        specific_date = None
        date_match = _re.search(r'\b(\d{4}-\d{2}-\d{2})\b', intent.rewritten_question)
        if date_match:
            specific_date = date_match.group(1)

        # 1. Resolve player to all known aliases
        candidates = resolve_player_name(connection, intent.player, limit=10)
        player_names: set[str] = {intent.player}
        player_id: int | None = None
        if candidates:
            # Pick candidate with most data
            best = max(candidates, key=lambda c: connection.execute(
                "SELECT COUNT(*) as n FROM batting_performances WHERE player_id=?", (c.player_id,)
            ).fetchone()["n"])
            player_id = best.player_id
            alias_rows = connection.execute(
                "SELECT alias FROM player_aliases WHERE player_id = ?", (player_id,)
            ).fetchall()
            for r in alias_rows:
                player_names.add(str(r["alias"]).strip())

        # If we have a specific date, query directly — no candidate search needed
        if specific_date:
            pmq = PlayerMatchQuery(
                player_name=intent.player,
                match_id=None,
                year=None,
                event_name=None,
                match_type=intent.match_type,
                team_terms=([intent.team] if intent.team else None),
            )
            # Override with date filter
            placeholders = ", ".join("?" for _ in player_names)
            clauses = [f"lower(bp.player_name) IN ({placeholders})", "bp.date = ?"]
            params: list[object] = [n.lower() for n in player_names] + [specific_date]
            if intent.match_type:
                allowed = _match_type_filters(intent.match_type)
                ph = ", ".join("?" for _ in allowed)
                clauses.append(f"bp.match_type IN ({ph})")
                params.extend(allowed)
            sql = f"""
                SELECT bp.match_id FROM batting_performances bp
                WHERE {' AND '.join(clauses)} LIMIT 1
            """
            row = connection.execute(sql, params).fetchone()
            if row:
                pmq = PlayerMatchQuery(player_name=intent.player, match_id=str(row["match_id"]))
                return _answer_player_match_question(connection, pmq)

        # 2. Find all candidate matches for this player with the given filters
        match_candidates = _find_player_match_candidates(connection, player_names, intent)

        if not match_candidates:
            return None

        # 3. Pick match: final → last by date; otherwise LLM selection
        chosen_match_id: str | None = None
        if len(match_candidates) == 1:
            chosen_match_id = str(match_candidates[0]["match_id"])
        else:
            rewritten_lower = intent.rewritten_question.lower()
            if "final" in rewritten_lower and "semi" not in rewritten_lower and "quarter" not in rewritten_lower:
                chosen_match_id = str(match_candidates[-1]["match_id"])
            elif self._ollama_base_url:
                from app.rag.intent import select_match
                llm_choice = select_match(
                    question, match_candidates,
                    self._ollama_base_url, self._ollama_model,
                )
                valid_ids = {m["match_id"] for m in match_candidates}
                if llm_choice and llm_choice in valid_ids:
                    chosen_match_id = llm_choice

        if not chosen_match_id:
            chosen_match_id = str(match_candidates[-1]["match_id"])

        pmq = PlayerMatchQuery(
            player_name=intent.player,
            match_id=chosen_match_id,
        )
        if "bowl" in intent.rewritten_question.lower() or "bowling" in intent.rewritten_question.lower():
            return _answer_player_bowling_match_question(connection, pmq)
        return _answer_player_match_question(connection, pmq)

    def _answer_player_dismissal(
        self,
        connection: sqlite3.Connection,
        question: str,
        intent: IntentResult,
    ) -> dict[str, object] | None:
        from app.analytics.players import resolve_player_name

        candidates = resolve_player_name(connection, intent.player, limit=10)
        # Pick the candidate with the most data
        best_pid = candidates[0].player_id if candidates else None
        best_n = 0
        for c in candidates:
            n = connection.execute(
                "SELECT COUNT(*) as n FROM dismissals WHERE batter_player_id=?", (c.player_id,)
            ).fetchone()["n"]
            if n > best_n:
                best_n = n
                best_pid = c.player_id

        batter_names: set[str] = {intent.player}
        if best_pid:
            alias_rows = connection.execute(
                "SELECT alias FROM player_aliases WHERE player_id = ?", (best_pid,)
            ).fetchall()
            for r in alias_rows:
                batter_names.add(str(r["alias"]).strip())

        # Detect aggregate dismissal questions (career stats, not a specific match)
        # Only treat as aggregate when there's no specific match context
        lowered = intent.rewritten_question.lower()
        has_match_context = bool(
            intent.event or intent.year or
            getattr(intent, "pinned_match_id", None) or
            any(w in lowered for w in ["final", "semi-final", "world cup", "champions trophy", "ashes"])
        )
        is_aggregate = not has_match_context and any(phrase in lowered for phrase in [
            "most times", "most often", "most common", "how many times",
            "mode of dismissal", "dismissed most", "dismissed by most",
            "who has dismissed", "most wickets against",
        ])

        if is_aggregate and best_pid:
            # "who dismissed X most" → bowler leaderboard
            if any(phrase in lowered for phrase in ["who dismissed", "who has dismissed", "most times dismissed by", "dismissed most"]):
                row = connection.execute("""
                    SELECT bowler_name, COUNT(*) as n
                    FROM dismissals WHERE batter_player_id=?
                    AND bowler_name IS NOT NULL AND bowler_name != 'None'
                    GROUP BY bowler_name ORDER BY n DESC LIMIT 1
                """, (best_pid,)).fetchone()
                if row:
                    from app.analytics.players import get_preferred_player_display_name
                    batter_display = get_preferred_player_display_name(connection, best_pid, intent.player)
                    answer = f"{row['bowler_name']} has dismissed {batter_display} the most times — {row['n']} times."
                    return {"answer": answer, "sources": [{"chunk_id": f"analytics:dismissal_stats:{best_pid}",
                        "score": 1.0, "text": answer, "title": "Dismissal stats",
                        "player_name": intent.player, "display_name": batter_display,
                        "match_id": "", "date": "", "match_type": "", "event_name": "",
                        "venue": "", "document_type": "analytics_result"}]}

            # "most common mode of dismissal"
            if any(phrase in lowered for phrase in ["mode of dismissal", "most common", "how is", "how was"]):
                rows = connection.execute("""
                    SELECT dismissal_kind, COUNT(*) as n
                    FROM dismissals WHERE batter_player_id=?
                    GROUP BY dismissal_kind ORDER BY n DESC LIMIT 3
                """, (best_pid,)).fetchall()
                if rows:
                    from app.analytics.players import get_preferred_player_display_name
                    batter_display = get_preferred_player_display_name(connection, best_pid, intent.player)
                    top = rows[0]
                    breakdown = ", ".join(f"{r['dismissal_kind']} ({r['n']})" for r in rows)
                    answer = (
                        f"{batter_display}'s most common mode of dismissal is {top['dismissal_kind']} "
                        f"({top['n']} times). Top 3: {breakdown}."
                    )
                    return {"answer": answer, "sources": [{"chunk_id": f"analytics:dismissal_mode:{best_pid}",
                        "score": 1.0, "text": answer, "title": "Dismissal stats",
                        "player_name": intent.player, "display_name": batter_display,
                        "match_id": "", "date": "", "match_type": "", "event_name": "",
                        "venue": "", "document_type": "analytics_result"}]}

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
        else:
            rewritten_lower = intent.rewritten_question.lower()
            if "final" in rewritten_lower and "semi" not in rewritten_lower and "quarter" not in rewritten_lower:
                chosen_match_id = str(dismissal_candidates[-1]["match_id"])
            elif self._ollama_base_url:
                from app.rag.intent import select_match
                llm_choice = select_match(
                    question, dismissal_candidates,
                    self._ollama_base_url, self._ollama_model,
                )
                valid_ids = {m["match_id"] for m in dismissal_candidates}
                if llm_choice and llm_choice in valid_ids:
                    chosen_match_id = llm_choice

        if not chosen_match_id:
            chosen_match_id = str(dismissal_candidates[-1]["match_id"])

        dq = DismissalQuery(batter_name=intent.player, match_id=chosen_match_id)
        return _answer_dismissal_question(connection, dq)

    def _answer_aggregate(
        self, connection: sqlite3.Connection, parsed: "AggregateQuery"
    ) -> dict[str, object] | None:
        if parsed.metric == "top_match_scorer" and parsed.match_id:
            row = _query_top_match_scorer(connection, parsed.match_id)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            answer = (
                f"The top scorer in that match was {display_name} with {row['runs']} runs "
                f"for {row['innings_team']} against {row['opposition_team']}."
            )
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:{parsed.match_id}:top_scorer",
                "score": 1.0, "text": answer, "title": "Match top scorer",
                "player_name": row["player_name"], "display_name": display_name,
                "match_id": parsed.match_id, "date": row["date"], "match_type": row["match_type"] or "",
                "event_name": row["event_name"] or "", "venue": row["venue"] or "",
                "document_type": "analytics_result"}]}

        if parsed.metric == "highest_individual_score":
            row = _query_highest_individual_score(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            rank_prefix = _rank_label(parsed.rank)
            answer = (
                f"The {rank_prefix} individual score"
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
            result = _query_most_runs(connection, parsed)
            if result is None:
                return None
            rows = result if isinstance(result, list) else [result]
            if parsed.limit == 1:
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                answer = (
                    f"The player with the most runs{_format_filter_suffix(parsed)} is "
                    f"{display_name} with {row['total_runs']} runs."
                )
                source_text = (
                    f"{display_name} has {row['total_runs']} runs across {row['innings_count']} innings"
                    f"{_format_filter_suffix(parsed)}."
                )
            else:
                suffix = _format_filter_suffix(parsed)
                lines = [f"Top {len(rows)} run scorers{suffix}:"]
                for i, row in enumerate(rows, 1):
                    dn = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                    lines.append(f"{i}. {dn} — {row['total_runs']} runs ({row['innings_count']} innings)")
                answer = "\n".join(lines)
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                source_text = f"Run scorers leaderboard{suffix}: {row['player_name']} leads with {row['total_runs']}"
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
            result = _query_most_wickets(connection, parsed)
            if result is None:
                return None
            rows = result if isinstance(result, list) else [result]
            if parsed.limit == 1:
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                answer = (
                    f"The player with the most wickets{_format_filter_suffix(parsed)} is "
                    f"{display_name} with {row['total_wickets']} wickets."
                )
                source_text = (
                    f"{display_name} has {row['total_wickets']} wickets in {row['match_count']} matches"
                    f"{_format_filter_suffix(parsed)}."
                )
            else:
                suffix = _format_filter_suffix(parsed)
                lines = [f"Top {len(rows)} wicket takers{suffix}:"]
                for i, row in enumerate(rows, 1):
                    dn = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                    lines.append(f"{i}. {dn} — {row['total_wickets']} wickets ({row['match_count']} matches)")
                answer = "\n".join(lines)
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                source_text = f"Wicket takers leaderboard{suffix}: {row['player_name']} leads with {row['total_wickets']}"
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

        if parsed.metric == "best_batting_average":
            rows = _query_best_batting_average(connection, parsed)
            if not rows:
                return None
            suffix = _format_filter_suffix(parsed)
            if parsed.limit == 1:
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                answer = (
                    f"The best batting average{suffix} is {row['average']} by {display_name} "
                    f"({row['total_runs']} runs in {row['innings']} innings, "
                    f"{row['dismissals']} dismissals)."
                )
            else:
                lines = [f"Top batting averages{suffix}:"]
                for i, row in enumerate(rows, 1):
                    dn = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
                    lines.append(f"{i}. {dn} — {row['average']} avg ({row['total_runs']} runs, {row['innings']} innings)")
                answer = "\n".join(lines)
                row = rows[0]
                display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            source_text = f"Batting average leaderboard{suffix}: {rows[0]['player_name']} leads with {rows[0]['average']}"
            return {
                "answer": answer,
                "sources": [{"chunk_id": f"analytics:batting_average{suffix}", "score": 1.0,
                             "text": source_text, "title": "Analytics aggregate result",
                             "player_name": rows[0]["player_name"], "display_name": display_name,
                             "match_id": "", "date": "", "match_type": parsed.match_type or "",
                             "event_name": "", "venue": parsed.venue or "", "document_type": "analytics_result"}],
            }

        if parsed.metric == "best_bowling_figures":
            row = _query_best_bowling_figures(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The best bowling figures{suffix} are {row['wickets']}/{row['runs_conceded']} "
                f"by {display_name} for {row['bowling_team']} against {row['opposition_team']} on {row['date']}."
            )
            source_text = (
                f"{display_name}: {row['wickets']}/{row['runs_conceded']} "
                f"for {row['bowling_team']} vs {row['opposition_team']} on {row['date']}."
            )
            return {
                "answer": answer,
                "sources": [{"chunk_id": f"analytics:{row['match_id']}:best_bowling", "score": 1.0,
                             "text": source_text, "title": "Analytics record result",
                             "player_name": row["player_name"], "display_name": display_name,
                             "match_id": row["match_id"], "date": row["date"],
                             "match_type": row["match_type"] or "", "event_name": row["event_name"] or "",
                             "venue": row["venue"] or "", "document_type": "analytics_result"}],
            }

        if parsed.metric == "best_economy":
            row = _query_best_economy(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The best bowling economy{suffix} is {row['economy']} by {display_name} "
                f"({row['total_wickets']} wickets in {row['match_count']} matches)."
            )
            source_text = f"{display_name}: economy {row['economy']}{suffix}"
            return {
                "answer": answer,
                "sources": [{"chunk_id": f"analytics:player:{row['player_id']}:economy", "score": 1.0,
                             "text": source_text, "title": "Analytics aggregate result",
                             "player_name": row["player_name"], "display_name": display_name,
                             "match_id": "", "date": "", "match_type": parsed.match_type or "",
                             "event_name": "", "venue": parsed.venue or "", "document_type": "analytics_result"}],
            }

        if parsed.metric == "most_runs_venue":
            row = _query_most_runs_venue(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The player with the most runs{suffix} is {display_name} "
                f"with {row['total_runs']} runs in {row['innings']} innings."
            )
            source_text = f"{display_name}: {row['total_runs']} runs in {row['innings']} innings{suffix}"
            return {
                "answer": answer,
                "sources": [{"chunk_id": f"analytics:player:{row['player_id']}:venue_runs", "score": 1.0,
                             "text": source_text, "title": "Analytics aggregate result",
                             "player_name": row["player_name"], "display_name": display_name,
                             "match_id": "", "date": "", "match_type": parsed.match_type or "",
                             "event_name": "", "venue": parsed.venue or "", "document_type": "analytics_result"}],
            }

        if parsed.metric == "most_wickets_venue":
            row = _query_most_wickets_venue(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The bowler with the most wickets{suffix} is {display_name} "
                f"with {row['total_wickets']} wickets in {row['match_count']} matches."
            )
            source_text = f"{display_name}: {row['total_wickets']} wickets{suffix}"
            return {
                "answer": answer,
                "sources": [{"chunk_id": f"analytics:player:{row['player_id']}:venue_wickets", "score": 1.0,
                             "text": source_text, "title": "Analytics aggregate result",
                             "player_name": row["player_name"], "display_name": display_name,
                             "match_id": "", "date": "", "match_type": parsed.match_type or "",
                             "event_name": "", "venue": parsed.venue or "", "document_type": "analytics_result"}],
            }

        if parsed.metric == "most_sixes":
            row = _query_most_sixes(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = f"The player with the most sixes{suffix} is {display_name} with {row['total_sixes']} sixes."
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:player:{row['player_id']}:sixes",
                "score": 1.0, "text": answer, "title": "Analytics aggregate result",
                "player_name": row["player_name"], "display_name": display_name,
                "match_id": "", "date": "", "match_type": parsed.match_type or "",
                "event_name": "", "venue": "", "document_type": "analytics_result"}]}

        if parsed.metric == "most_fours":
            row = _query_most_fours(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = f"The player with the most fours{suffix} is {display_name} with {row['total_fours']} fours."
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:player:{row['player_id']}:fours",
                "score": 1.0, "text": answer, "title": "Analytics aggregate result",
                "player_name": row["player_name"], "display_name": display_name,
                "match_id": "", "date": "", "match_type": parsed.match_type or "",
                "event_name": "", "venue": "", "document_type": "analytics_result"}]}

        if parsed.metric == "most_potm":
            row = _query_most_potm(connection, parsed)
            if row is None:
                return None
            suffix = _format_filter_suffix(parsed)
            answer = f"The player with the most Player of the Match awards{suffix} is {row['player']} with {row['count']} awards."
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:potm:{row['player']}",
                "score": 1.0, "text": answer, "title": "Analytics aggregate result",
                "player_name": row["player"], "display_name": row["player"],
                "match_id": "", "date": "", "match_type": parsed.match_type or "",
                "event_name": "", "venue": "", "document_type": "analytics_result"}]}

        if parsed.metric == "best_year_runs":
            row = _query_best_year_runs(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The most runs scored in a single calendar year{suffix} is {row['runs']} "
                f"by {display_name} in {row['year']} ({row['innings']} innings)."
            )
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:best_year:{row['player_id']}",
                "score": 1.0, "text": answer, "title": "Analytics aggregate result",
                "player_name": row["player_name"], "display_name": display_name,
                "match_id": "", "date": str(row["year"]), "match_type": parsed.match_type or "",
                "event_name": "", "venue": "", "document_type": "analytics_result"}]}

        if parsed.metric == "most_wickets_year":
            row = _query_most_wickets_year(connection, parsed)
            if row is None:
                return None
            display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
            suffix = _format_filter_suffix(parsed)
            answer = (
                f"The most wickets taken in a single calendar year{suffix} is {row['wickets']} "
                f"by {display_name} in {row['year']}."
            )
            return {"answer": answer, "sources": [{"chunk_id": f"analytics:best_year_wickets:{row['player_id']}",
                "score": 1.0, "text": answer, "title": "Analytics aggregate result",
                "player_name": row["player_name"], "display_name": display_name,
                "match_id": "", "date": str(row["year"]), "match_type": parsed.match_type or "",
                "event_name": "", "venue": "", "document_type": "analytics_result"}]}

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
    if re.search(r"\bodi\b", question) or "one day international" in question:
        return "ODI"
    if re.search(r"\btest\b", question):
        return "Test"
    if re.search(r"\bt20\b", question):
        return "T20"
    return None


def _extract_venue(question: str) -> str | None:
    """Extract obvious venue names when the intent LLM leaves venue empty."""
    question = question.replace("’", "'")
    if "lord" in question:
        return "Lord"
    known = [
        "lord's cricket ground",
        "lord's",
        "lords",
        "wankhede stadium",
        "trent bridge",
        "eden gardens",
        "melbourne cricket ground",
        "kennington oval",
    ]
    for venue in known:
        if venue in question:
            if venue in {"lord's cricket ground", "lord's", "lords"}:
                return "Lord"
            return venue.title()

    match = re.search(r"\bat\s+([a-z0-9' .-]+?)(?:\?|$|\sin\s|\sfor\s)", question, flags=re.IGNORECASE)
    if not match:
        return None
    candidate = match.group(1).strip()
    if candidate.lower().startswith("least "):
        return None
    return _normalize_venue(candidate)


def _normalize_venue(venue: str | None) -> str | None:
    if not venue:
        return None
    lowered = venue.lower().replace("’", "'")
    if "lord" in lowered:
        return "Lord"
    if "wankhede" in lowered:
        return "Wankhede Stadium"
    if "trent bridge" in lowered:
        return "Trent Bridge"
    if "eden gardens" in lowered:
        return "Eden Gardens"
    return venue.strip()


def _answer_dismissal_question(
    connection: sqlite3.Connection,
    query: "DismissalQuery",
) -> dict[str, object] | None:
    from app.analytics.players import get_preferred_player_display_name, resolve_player_name

    # Resolve batter to all known aliases for the DB lookup
    candidates = resolve_player_name(connection, query.batter_name, limit=10)
    batter_names: set[str] = {query.batter_name}
    batter_player_id: int | None = None
    if candidates:
        batter_player_id = _pick_player_with_most_data(connection, candidates)
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
        if query.match_id and batter_player_id:
            played = connection.execute(
                """
                SELECT bp.player_name, bp.player_id, am.teams_csv, bp.date, bp.match_type, bp.event_name
                FROM batting_performances bp
                JOIN analytics_matches am ON am.match_id = bp.match_id
                WHERE bp.match_id = ? AND bp.player_id = ?
                LIMIT 1
                """,
                (query.match_id, batter_player_id),
            ).fetchone()
            if played is not None:
                batter_display = get_preferred_player_display_name(
                    connection, batter_player_id, str(played["player_name"])
                )
                answer = f"{batter_display} was not out in {played['teams_csv']} on {played['date']}."
                return {"answer": answer, "sources": [{
                    "chunk_id": f"analytics:{query.match_id}:dismissal:not_out:{batter_player_id}",
                    "score": 1.0, "text": answer, "title": "Dismissal record",
                    "player_name": str(played["player_name"]), "display_name": batter_display,
                    "teams": str(played["teams_csv"] or ""), "match_id": query.match_id,
                    "date": str(played["date"] or ""), "match_type": str(played["match_type"] or ""),
                    "event_name": str(played["event_name"] or ""), "venue": "",
                    "document_type": "analytics_result",
                }]}
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
    lowered = (intent.rewritten_question or "").lower()
    metric = intent.metric or ""
    inferred_match_type = intent.match_type or _extract_match_type(lowered)
    inferred_venue = _normalize_venue(intent.venue) or _extract_venue(lowered)
    pinned_match_id = getattr(intent, "pinned_match_id", None)

    # Map LLM metric field values to internal metric names
    metric_map = {
        "highest_score": "highest_individual_score",
        "runs": "most_runs",
        "wickets": "most_wickets",
        "average": "best_batting_average",
        "batting_average": "best_batting_average",
        "economy": "best_economy",
        "economy_rate": "best_economy",
        "bowling_average": "best_economy",
        "strike_rate": "most_runs",
        "sixes": "most_sixes",
        "fours": "most_fours",
    }
    resolved = metric_map.get(metric)

    # Detect top-N intent first — overrides metric mapping for leaderboard questions
    import re as _re
    top_n_match = _re.search(r"\btop\s+(\d+)\b", lowered)
    # Also detect "second highest", "third highest" etc.
    nth_match = _re.search(r"\b(second|third|fourth|fifth|2nd|3rd|4th|5th)\s+highest\b", lowered)
    nth_map = {"second": 2, "2nd": 2, "third": 3, "3rd": 3, "fourth": 4, "4th": 4, "fifth": 5, "5th": 5}
    limit = int(top_n_match.group(1)) if top_n_match else 1
    if nth_match:
        nth_word = nth_match.group(1).lower()
        limit = nth_map.get(nth_word, 1)
    rank = limit if nth_match else 1

    # Textual cues should override a too-generic LLM metric.
    if "top scorer" in lowered and pinned_match_id:
        resolved = "top_match_scorer"
    elif re.search(r"\bbest\b.*\byear\b", lowered) or "most runs in a year" in lowered or "most runs in a single year" in lowered:
        resolved = "best_year_runs"
    elif re.search(r"\bmost\b.*\bwickets\b.*\byear\b", lowered):
        resolved = "most_wickets_year"
    elif "most sixes" in lowered or "sixes" in lowered:
        resolved = "most_sixes"
    elif "most fours" in lowered or "fours" in lowered:
        resolved = "most_fours"
    elif "player of the match" in lowered or "potm" in lowered or "man of the match" in lowered:
        resolved = "most_potm"
    elif ("most runs" in lowered or "most run" in lowered or "run scorer" in lowered) and inferred_venue:
        resolved = "most_runs_venue"
    elif ("most wickets" in lowered or "most wicket" in lowered) and inferred_venue:
        resolved = "most_wickets_venue"

    # When top-N is requested, "highest_score" likely means "most runs" leaderboard
    if (top_n_match or limit > 1) and resolved == "highest_individual_score" and not nth_match:
        if "run scorer" in lowered or "batting" in lowered:
            resolved = "most_runs"
        elif "wicket" in lowered or "bowler" in lowered:
            resolved = "most_wickets"

    # Infer from rewritten question text when metric field is missing or unmapped
    if not resolved:
        if "best bowling figures" in lowered or "best figures" in lowered:
            resolved = "best_bowling_figures"
        elif "batting average" in lowered or "best average" in lowered:
            resolved = "best_batting_average"
        elif "economy" in lowered and ("best" in lowered or "lowest" in lowered):
            resolved = "best_economy"
        elif "most sixes" in lowered or "sixes" in lowered:
            resolved = "most_sixes"
        elif "most fours" in lowered or "fours" in lowered:
            resolved = "most_fours"
        elif "player of the match" in lowered or "potm" in lowered or "man of the match" in lowered:
            resolved = "most_potm"
        elif ("most runs" in lowered or "most run" in lowered or "run scorer" in lowered) and inferred_venue:
            resolved = "most_runs_venue"
        elif ("most wickets" in lowered or "most wicket" in lowered) and inferred_venue:
            resolved = "most_wickets_venue"
        elif "most runs" in lowered or "most run" in lowered or "run scorer" in lowered:
            resolved = "most_runs"
        elif "most wickets" in lowered or "most wicket" in lowered:
            resolved = "most_wickets"
        elif "highest" in lowered and ("score" in lowered or "run" in lowered):
            resolved = "highest_individual_score"

    if not resolved:
        return None

    return AggregateQuery(
        metric=resolved,
        match_type=inferred_match_type,
        year=intent.year,
        venue=inferred_venue,
        international_only=bool(intent.event and "international" in (intent.event or "").lower()),
        limit=min(limit, 10),  # cap at 10
        rank=rank,
        match_id=pinned_match_id,
    )


def _find_match_result_fallback(
    connection: sqlite3.Connection,
    question: str,
) -> sqlite3.Row | None:
    lowered = question.lower()
    event: str | None = None
    if "champions trophy" in lowered:
        event = "champions trophy"
    elif "world cup" in lowered:
        event = "world cup"
    if not event:
        return None

    clauses = ["lower(event_name) LIKE ?"]
    params: list[object] = [f"%{event}%"]
    year_match = re.search(r"\b(19|20)\d{2}\b", lowered)
    if year_match:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(year_match.group(0))

    for team in _known_team_names():
        if team in lowered:
            clauses.append("lower(teams_csv) LIKE ?")
            params.append(f"%{team}%")

    return connection.execute(
        f"""
        SELECT match_id, teams_csv, outcome, date, event_name, match_type
        FROM analytics_matches
        WHERE {' AND '.join(clauses)}
        ORDER BY date DESC
        LIMIT 1
        """,
        params,
    ).fetchone()


def _known_team_names() -> list[str]:
    return [
        "india", "australia", "england", "pakistan", "sri lanka", "new zealand",
        "south africa", "west indies", "bangladesh", "zimbabwe", "afghanistan",
        "ireland", "scotland", "netherlands", "kenya", "canada", "uae",
    ]


def _intent_to_career_query(intent: IntentResult) -> "PlayerCareerQuery | None":
    """When a player is named in an aggregate_stats question, it's a career query.

    Any aggregate question about a specific player — "what's Kohli's average?",
    "how many centuries?", "what's his highest score?", "how many sixes?" — is
    answered from that player's career stats. No keyword matching needed.
    """
    if not intent.player:
        return None

    # Use match_type from intent, but also try to infer it from the rewritten
    # question when the LLM didn't extract it (e.g. "Test batting average" →
    # match_type should be "Test" even if intent.match_type is None).
    match_type = intent.match_type
    if not match_type:
        match_type = _extract_match_type((intent.rewritten_question or "").lower())

    return PlayerCareerQuery(
        player_name=intent.player,
        match_type=match_type,
        year=intent.year,
        event_name=intent.event,
    )


def _intent_to_head_to_head(intent: IntentResult) -> "HeadToHeadQuery | None":
    """Detect head-to-head questions: 'India vs Australia ODI record'."""
    lowered = (intent.rewritten_question or "").lower()
    h2h_signals = ["head to head", "head-to-head", "record against", " vs ", "versus", "win loss", "wins against"]
    if not any(s in lowered for s in h2h_signals):
        return None

    # Extract both teams from the rewritten question using known team names
    known_teams = [
        "india", "australia", "england", "pakistan", "sri lanka", "new zealand",
        "south africa", "west indies", "bangladesh", "zimbabwe", "afghanistan",
        "ireland", "scotland", "netherlands", "kenya", "canada", "uae",
    ]
    found_teams = [t for t in known_teams if t in lowered]
    if len(found_teams) < 2:
        # Fall back to intent fields
        team1 = intent.team
        team2 = intent.player2 or intent.player
        if not team1 or not team2:
            return None
    else:
        team1, team2 = found_teams[0], found_teams[1]

    return HeadToHeadQuery(
        team1=team1,
        team2=team2,
        match_type=intent.match_type,
        year=intent.year,
    )


def _answer_player_specific_aggregate(
    connection: sqlite3.Connection,
    intent: IntentResult,
) -> dict[str, object] | None:
    """Answer precise player aggregate questions deterministically."""
    from app.analytics.players import get_preferred_player_display_name, resolve_player_name

    lowered = (intent.rewritten_question or "").lower()
    candidates = resolve_player_name(connection, intent.player or "", limit=10)
    if not candidates:
        return None

    player_id = _pick_player_with_most_data(connection, candidates)
    display_name = get_preferred_player_display_name(connection, player_id, intent.player)
    match_type = intent.match_type or _extract_match_type(lowered)

    if "overseas" in lowered or "home" in lowered or "away" in lowered:
        answer = "I don't have that breakdown in the current dataset."
        return _simple_player_source(answer, player_id, display_name, match_type)

    if intent.event and ("how many runs" in lowered or "total runs" in lowered):
        row = _query_player_event_runs(connection, player_id, intent.event, intent.year, match_type)
        if row is not None:
            event_name = _normalize_event_name(intent.event) or intent.event
            answer = f"{display_name} scored {row['total_runs']} runs in the {intent.year or ''} {event_name}.".replace("  ", " ")
            return _simple_player_source(answer, player_id, display_name, match_type)

    if "player of the match" in lowered or "potm" in lowered or "man of the match" in lowered:
        count = _query_player_potm_count(connection, player_id, match_type, intent.year)
        if count is None:
            return None
        answer = f"{display_name} has won {count} Player of the Match awards{_format_simple_scope(match_type, intent.year)}."
        return _simple_player_source(answer, player_id, display_name, match_type)

    if "compare" in lowered and "kohli" in lowered and "average" in lowered:
        other_candidates = resolve_player_name(connection, "Virat Kohli", limit=10)
        if other_candidates:
            other_id = _pick_player_with_most_data(connection, other_candidates)
            row1 = _query_player_batting_summary(connection, player_id, match_type, intent.year)
            row2 = _query_player_batting_summary(connection, other_id, match_type, intent.year)
            if row1 is not None and row2 is not None:
                other_name = get_preferred_player_display_name(connection, other_id, "Virat Kohli")
                answer = (
                    f"{display_name}'s batting average{_format_simple_scope(match_type, intent.year)} "
                    f"is {row1['average']}; {other_name}'s is {row2['average']}."
                )
                return _simple_player_source(answer, player_id, display_name, match_type)

    if re.search(r"\bbest\b.*\byear\b", lowered) or "sabse acha" in lowered:
        row = _query_player_best_year_runs(connection, player_id, match_type)
        if row is None:
            return None
        answer = f"{display_name}'s best year by runs{_format_simple_scope(match_type, None)} was {row['year']} with {row['runs']} runs."
        return _simple_player_source(answer, player_id, display_name, match_type)

    if intent.year and ("average" in lowered or "batting average" in lowered):
        row = _query_player_batting_summary(connection, player_id, match_type, intent.year)
        if row is None:
            return None
        answer = (
            f"{display_name}'s batting average{_format_simple_scope(match_type, intent.year)} "
            f"was {row['average']} ({row['total_runs']} runs, {row['dismissals']} dismissals)."
        )
        return _simple_player_source(answer, player_id, display_name, match_type)

    if "wicket" in lowered:
        row = _query_player_bowling_summary(connection, player_id, match_type, intent.year)
        if row is None or row["total_wickets"] is None:
            return None
        answer = f"{display_name} has taken {row['total_wickets']} wickets{_format_simple_scope(match_type, intent.year)}."
        return _simple_player_source(answer, player_id, display_name, match_type)

    if "batting average" in lowered or "average" in lowered:
        row = _query_player_batting_summary(connection, player_id, match_type, intent.year)
        if row is None:
            return None
        answer = (
            f"{display_name}'s batting average{_format_simple_scope(match_type, intent.year)} "
            f"is {row['average']} ({row['total_runs']} runs)."
        )
        return _simple_player_source(answer, player_id, display_name, match_type)

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
        LIMIT 1 OFFSET {max(parsed.rank - 1, 0)}
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
        LIMIT {parsed.limit}
    """
    rows = connection.execute(sql, params).fetchall()
    if not rows:
        return None
    return rows[0] if parsed.limit == 1 else rows  # type: ignore[return-value]


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
        LIMIT {parsed.limit}
    """
    rows = connection.execute(sql, params).fetchall()
    if not rows:
        return None
    return rows[0] if parsed.limit == 1 else rows  # type: ignore[return-value]


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


# ---------------------------------------------------------------------------
# New query functions for expanded analytics
# ---------------------------------------------------------------------------


def _query_best_batting_average(
    connection: sqlite3.Connection, parsed: AggregateQuery
) -> list[sqlite3.Row]:
    """Batting average = total runs / dismissals (min 20 innings)."""
    clauses: list[str] = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    where = " AND ".join(clauses)
    sql = f"""
        SELECT
            bp.player_id,
            bp.player_name,
            SUM(bp.runs) AS total_runs,
            COUNT(*) AS innings,
            COUNT(d.batter_name) AS dismissals,
            ROUND(
                CAST(SUM(bp.runs) AS REAL) / NULLIF(COUNT(d.batter_name), 0),
                2
            ) AS average
        FROM batting_performances bp
        LEFT JOIN dismissals d
            ON d.match_id = bp.match_id
            AND d.innings_number = bp.innings_number
            AND lower(d.batter_name) = lower(bp.player_name)
        WHERE {where}
        GROUP BY bp.player_id, bp.player_name
        HAVING innings >= 30
        ORDER BY average DESC, total_runs DESC
        LIMIT {parsed.limit}
    """
    return connection.execute(sql, params).fetchall()


def _query_best_bowling_figures(
    connection: sqlite3.Connection, parsed: AggregateQuery
) -> sqlite3.Row | None:
    """Best single-innings bowling figures (most wickets, fewest runs)."""
    clauses: list[str] = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.match_id,
            bp.player_id,
            bp.player_name,
            bp.wickets,
            bp.runs_conceded,
            bp.bowling_team,
            bp.opposition_team,
            bp.match_type,
            bp.date,
            bp.venue,
            bp.event_name
        FROM bowling_performances bp
        WHERE {' AND '.join(clauses)}
        ORDER BY bp.wickets DESC, bp.runs_conceded ASC, bp.date ASC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_best_economy(
    connection: sqlite3.Connection, parsed: AggregateQuery
) -> sqlite3.Row | None:
    """Best bowling economy rate (min 300 balls bowled)."""
    clauses: list[str] = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.player_id,
            bp.player_name,
            ROUND(
                CAST(SUM(bp.runs_conceded) * 6.0 AS REAL) / NULLIF(SUM(bp.balls_bowled), 0),
                2
            ) AS economy,
            SUM(bp.wickets) AS total_wickets,
            COUNT(DISTINCT bp.match_id) AS match_count
        FROM bowling_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY bp.player_id, bp.player_name
        HAVING SUM(bp.balls_bowled) >= 300
        ORDER BY economy ASC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_runs_venue(
    connection: sqlite3.Connection, parsed: AggregateQuery
) -> sqlite3.Row | None:
    """Most runs at a specific venue."""
    if not parsed.venue:
        return None
    clauses: list[str] = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT
            bp.player_id,
            bp.player_name,
            SUM(bp.runs) AS total_runs,
            COUNT(*) AS innings
        FROM batting_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY bp.player_id, bp.player_name
        ORDER BY total_runs DESC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_wickets_venue(
    connection: sqlite3.Connection, parsed: AggregateQuery
) -> sqlite3.Row | None:
    """Most wickets at a specific venue."""
    if not parsed.venue:
        return None
    clauses: list[str] = ["1=1"]
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
        ORDER BY total_wickets DESC
        LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_sixes(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT player_id, player_name, SUM(sixes) AS total_sixes
        FROM batting_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY player_id, player_name ORDER BY total_sixes DESC LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_fours(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["1=1"]
    params: list[object] = []
    _apply_common_filters(clauses, params, parsed, table_alias="bp")
    sql = f"""
        SELECT player_id, player_name, SUM(fours) AS total_fours
        FROM batting_performances bp
        WHERE {' AND '.join(clauses)}
        GROUP BY player_id, player_name ORDER BY total_fours DESC LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_potm(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    clauses = ["player_of_match_csv != ''", "player_of_match_csv NOT LIKE '%,%'"]
    params: list[object] = []
    if parsed.match_type:
        allowed = _match_type_filters(parsed.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"match_type IN ({ph})")
        params.extend(allowed)
    if parsed.year:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(str(parsed.year))
    sql = f"""
        SELECT player_of_match_csv AS player, COUNT(*) AS count
        FROM analytics_matches
        WHERE {' AND '.join(clauses)}
        GROUP BY player_of_match_csv ORDER BY count DESC LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_best_year_runs(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    """Most runs by any player in a single calendar year."""
    clauses = ["1=1"]
    params: list[object] = []
    if parsed.match_type:
        allowed = _match_type_filters(parsed.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"match_type IN ({ph})")
        params.extend(allowed)
    sql = f"""
        SELECT player_id, player_name, substr(date, 1, 4) AS year,
               SUM(runs) AS runs, COUNT(*) AS innings
        FROM batting_performances
        WHERE {' AND '.join(clauses)}
        GROUP BY player_id, player_name, year
        ORDER BY runs DESC LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_most_wickets_year(connection: sqlite3.Connection, parsed: AggregateQuery) -> sqlite3.Row | None:
    """Most wickets by any bowler in a single calendar year."""
    clauses = ["1=1"]
    params: list[object] = []
    if parsed.match_type:
        allowed = _match_type_filters(parsed.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"match_type IN ({ph})")
        params.extend(allowed)
    sql = f"""
        SELECT player_id, player_name, substr(date, 1, 4) AS year,
               SUM(wickets) AS wickets
        FROM bowling_performances
        WHERE {' AND '.join(clauses)}
        GROUP BY player_id, player_name, year
        ORDER BY wickets DESC LIMIT 1
    """
    return connection.execute(sql, params).fetchone()


def _query_top_match_scorer(connection: sqlite3.Connection, match_id: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT player_id, player_name, innings_team, opposition_team, runs, balls,
               match_type, date, venue, event_name
        FROM batting_performances
        WHERE match_id = ?
        ORDER BY runs DESC, balls ASC
        LIMIT 1
        """,
        (match_id,),
    ).fetchone()


def _pick_player_with_most_data(connection: sqlite3.Connection, candidates: list[object]) -> int:
    best_player_id = int(getattr(candidates[0], "player_id"))
    best_total = -1
    for candidate in candidates:
        player_id = int(getattr(candidate, "player_id"))
        row = connection.execute(
            """
            SELECT
              (SELECT COUNT(*) FROM batting_performances WHERE player_id = ?) +
              (SELECT COUNT(*) FROM bowling_performances WHERE player_id = ?) AS n
            """,
            (player_id, player_id),
        ).fetchone()
        total = int(row["n"] or 0) if row else 0
        if total > best_total:
            best_total = total
            best_player_id = player_id
    return best_player_id


def _query_player_batting_summary(
    connection: sqlite3.Connection,
    player_id: int,
    match_type: str | None,
    year: int | None,
) -> sqlite3.Row | None:
    clauses = ["bp.player_id = ?"]
    params: list[object] = [player_id]
    if match_type:
        allowed = _match_type_filters(match_type)
        clauses.append(f"bp.match_type IN ({', '.join('?' for _ in allowed)})")
        params.extend(allowed)
    if year:
        clauses.append("substr(bp.date, 1, 4) = ?")
        params.append(str(year))
    row = connection.execute(
        f"""
        SELECT SUM(bp.runs) AS total_runs,
               COUNT(*) AS innings,
               COUNT(d.batter_player_id) AS dismissals,
               ROUND(CAST(SUM(bp.runs) AS REAL) / NULLIF(COUNT(d.batter_player_id), 0), 2) AS average
        FROM batting_performances bp
        LEFT JOIN dismissals d
          ON d.match_id = bp.match_id
         AND d.innings_number = bp.innings_number
         AND d.batter_player_id = bp.player_id
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchone()
    if row is None or row["innings"] is None or int(row["innings"]) == 0:
        return None
    return row


def _query_player_bowling_summary(
    connection: sqlite3.Connection,
    player_id: int,
    match_type: str | None,
    year: int | None,
) -> sqlite3.Row | None:
    clauses = ["player_id = ?"]
    params: list[object] = [player_id]
    if match_type:
        allowed = _match_type_filters(match_type)
        clauses.append(f"match_type IN ({', '.join('?' for _ in allowed)})")
        params.extend(allowed)
    if year:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(str(year))
    return connection.execute(
        f"SELECT SUM(wickets) AS total_wickets FROM bowling_performances WHERE {' AND '.join(clauses)}",
        params,
    ).fetchone()


def _query_player_best_year_runs(
    connection: sqlite3.Connection,
    player_id: int,
    match_type: str | None,
) -> sqlite3.Row | None:
    clauses = ["player_id = ?"]
    params: list[object] = [player_id]
    if match_type:
        allowed = _match_type_filters(match_type)
        clauses.append(f"match_type IN ({', '.join('?' for _ in allowed)})")
        params.extend(allowed)
    return connection.execute(
        f"""
        SELECT substr(date, 1, 4) AS year, SUM(runs) AS runs, COUNT(*) AS innings
        FROM batting_performances
        WHERE {' AND '.join(clauses)}
        GROUP BY year
        ORDER BY runs DESC
        LIMIT 1
        """,
        params,
    ).fetchone()


def _query_player_event_runs(
    connection: sqlite3.Connection,
    player_id: int,
    event_name: str,
    year: int | None,
    match_type: str | None,
) -> sqlite3.Row | None:
    normalized_event = _normalize_event_name(event_name)
    clauses = ["player_id = ?"]
    params: list[object] = [player_id]
    if normalized_event:
        clauses.append("(lower(event_name) LIKE ? OR ? LIKE '%' || lower(event_name) || '%')")
        params.append(f"%{normalized_event.lower()}%")
        params.append(normalized_event.lower())
    if year:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(str(year))
    if match_type:
        allowed = _match_type_filters(match_type)
        clauses.append(f"match_type IN ({', '.join('?' for _ in allowed)})")
        params.extend(allowed)
    row = connection.execute(
        f"""
        SELECT SUM(runs) AS total_runs, COUNT(*) AS innings
        FROM batting_performances
        WHERE {' AND '.join(clauses)}
        """,
        params,
    ).fetchone()
    if row is None or row["innings"] is None or int(row["innings"]) == 0:
        return None
    return row


def _query_player_potm_count(
    connection: sqlite3.Connection,
    player_id: int,
    match_type: str | None,
    year: int | None,
) -> int | None:
    aliases = [r["alias"].lower() for r in connection.execute(
        "SELECT alias FROM player_aliases WHERE player_id = ?", (player_id,)
    ).fetchall()]
    if not aliases:
        return None
    clauses = ["player_of_match_csv != ''"]
    params: list[object] = []
    if match_type:
        allowed = _match_type_filters(match_type)
        clauses.append(f"match_type IN ({', '.join('?' for _ in allowed)})")
        params.extend(allowed)
    if year:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(str(year))
    rows = connection.execute(
        f"SELECT player_of_match_csv FROM analytics_matches WHERE {' AND '.join(clauses)}",
        params,
    ).fetchall()
    count = 0
    alias_set = set(aliases)
    for row in rows:
        winners = [part.strip().lower() for part in str(row["player_of_match_csv"]).split(",")]
        if any(winner in alias_set for winner in winners):
            count += 1
    return count


def _format_simple_scope(match_type: str | None, year: int | None) -> str:
    bits = []
    if match_type:
        bits.append(match_type)
    if year:
        bits.append(str(year))
    return f" in {' '.join(bits)}" if bits else ""


def _simple_player_source(
    answer: str,
    player_id: int,
    display_name: str,
    match_type: str | None,
) -> dict[str, object]:
    return {"answer": answer, "sources": [{"chunk_id": f"analytics:player:{player_id}:specific",
        "score": 1.0, "text": answer, "title": "Player aggregate stat",
        "player_name": display_name, "display_name": display_name,
        "match_id": "", "date": "", "match_type": match_type or "",
        "event_name": "", "venue": "", "document_type": "analytics_result"}]}


def _rank_label(rank: int) -> str:
    if rank == 2:
        return "second highest"
    if rank == 3:
        return "third highest"
    if rank == 4:
        return "fourth highest"
    if rank == 5:
        return "fifth highest"
    return "highest"


def _answer_career_query(
    connection: sqlite3.Connection,
    query: PlayerCareerQuery,
) -> dict[str, object] | None:
    """Answer a player career stats question."""
    from app.analytics.players import get_preferred_player_display_name, resolve_player_name

    # Resolve player — try up to 10 candidates and pick the one with the most data.
    # For ambiguous last-name-only queries (e.g. "Kohli"), the player with the
    # most innings in the DB is almost certainly the famous one.
    candidates = resolve_player_name(connection, query.player_name, limit=10)
    if not candidates:
        return None

    # Pick the candidate with the most batting innings (most data = most likely the right player)
    best_player_id = candidates[0].player_id
    best_innings = 0
    for candidate in candidates:
        row = connection.execute(
            "SELECT COUNT(*) as n FROM batting_performances WHERE player_id = ?",
            (candidate.player_id,),
        ).fetchone()
        if row and int(row["n"]) > best_innings:
            best_innings = int(row["n"])
            best_player_id = candidate.player_id

    player_id = best_player_id
    display_name = get_preferred_player_display_name(connection, player_id, query.player_name)

    # Collect all aliases for the player
    alias_rows = connection.execute(
        "SELECT alias FROM player_aliases WHERE player_id = ?", (player_id,)
    ).fetchall()
    player_names = {query.player_name} | {str(r["alias"]).strip() for r in alias_rows}
    placeholders = ", ".join("?" for _ in player_names)
    params: list[object] = [n.lower() for n in player_names]

    # Build filters — use player_id directly for reliability
    bat_clauses = ["bp.player_id = ?"]
    bat_params: list[object] = [player_id]

    if query.match_type:
        allowed = _match_type_filters(query.match_type)
        ph = ", ".join("?" for _ in allowed)
        bat_clauses.append(f"bp.match_type IN ({ph})")
        bat_params.extend(allowed)
    if query.year:
        bat_clauses.append("substr(bp.date, 1, 4) = ?")
        bat_params.append(str(query.year))

    # Batting stats
    bat_row = connection.execute(f"""
        SELECT
            COUNT(DISTINCT bp.match_id) AS matches,
            COUNT(*) AS innings,
            SUM(bp.runs) AS total_runs,
            MAX(bp.runs) AS highest,
            ROUND(AVG(bp.strike_rate), 1) AS avg_sr,
            SUM(bp.fours) AS fours,
            SUM(bp.sixes) AS sixes,
            COUNT(d.batter_player_id) AS dismissals,
            ROUND(CAST(SUM(bp.runs) AS REAL) / NULLIF(COUNT(d.batter_player_id), 0), 2) AS average,
            SUM(CASE WHEN bp.runs >= 100 THEN 1 ELSE 0 END) AS centuries,
            SUM(CASE WHEN bp.runs >= 50 AND bp.runs < 100 THEN 1 ELSE 0 END) AS fifties
        FROM batting_performances bp
        LEFT JOIN dismissals d
            ON d.match_id = bp.match_id
            AND d.innings_number = bp.innings_number
            AND d.batter_player_id = bp.player_id
        WHERE {' AND '.join(bat_clauses)}
    """, bat_params).fetchone()

    # Bowling stats — query by player_id directly (more reliable than name matching)
    bowl_row = connection.execute(f"""
        SELECT
            SUM(bp.wickets) AS total_wickets,
            COUNT(DISTINCT bp.match_id) AS match_count,
            SUM(bp.runs_conceded) AS runs_conceded,
            SUM(bp.balls_bowled) AS balls_bowled,
            ROUND(CAST(SUM(bp.runs_conceded) * 6.0 AS REAL) / NULLIF(SUM(bp.balls_bowled), 0), 2) AS economy,
            MAX(bp.wickets) AS best_wickets,
            MIN(CASE WHEN bp.wickets = (SELECT MAX(wickets) FROM bowling_performances WHERE player_id = ?) THEN bp.runs_conceded ELSE NULL END) AS best_runs
        FROM bowling_performances bp
        WHERE bp.player_id = ?
        {("AND bp.match_type IN (" + ", ".join("?" for _ in _match_type_filters(query.match_type)) + ")") if query.match_type else ""}
        {("AND substr(bp.date, 1, 4) = ?") if query.year else ""}
    """, [player_id, player_id]
        + (_match_type_filters(query.match_type) if query.match_type else [])
        + ([str(query.year)] if query.year else [])
    ).fetchone()

    if bat_row is None or bat_row["matches"] == 0:
        # Try broader match type filter (T20I data may be stored as T20)
        if query.match_type and query.match_type.upper() in ("T20I", "IT20"):
            broader = PlayerCareerQuery(
                player_name=query.player_name,
                match_type="T20",
                year=query.year,
                event_name=query.event_name,
            )
            return _answer_career_query(connection, broader)
        return None

    fmt = query.match_type or "all formats"
    parts = [f"**{display_name}** — {fmt} career stats:"]

    # Batting
    if bat_row["innings"] and int(bat_row["innings"]) > 0:
        parts.append(
            f"Batting: {bat_row['matches']} matches, {bat_row['innings']} innings, "
            f"{bat_row['total_runs']} runs, highest {bat_row['highest']}, "
            f"average {bat_row['average'] or 'N/A'}, SR {bat_row['avg_sr'] or 'N/A'}, "
            f"{bat_row['centuries']} centuries, {bat_row['fifties']} fifties."
        )

    # Bowling
    if bowl_row and bowl_row["total_wickets"] and int(bowl_row["total_wickets"]) > 0:
        parts.append(
            f"Bowling: {bowl_row['total_wickets']} wickets, "
            f"economy {bowl_row['economy'] or 'N/A'}, "
            f"best figures {bowl_row['best_wickets']}/{bowl_row['best_runs'] or '?'}."
        )

    answer = "\n".join(parts)
    # Rich fact sheet — the LLM reads this to answer any specific question.
    # Include every computed stat so the LLM can answer "how many centuries?",
    # "what's the average?", "how many sixes?", etc. without keyword matching.
    # Explicitly note what's NOT available so the LLM answers honestly.
    source_text = (
        f"{display_name} {fmt} career (all matches in dataset, including domestic/IPL): "
        f"{bat_row['matches']} matches, {bat_row['innings']} innings, "
        f"{bat_row['total_runs']} runs, highest score {bat_row['highest']}, "
        f"batting average {bat_row['average'] or 'N/A'} (runs divided by dismissals), "
        f"strike rate {bat_row['avg_sr'] or 'N/A'}, "
        f"{bat_row['centuries']} centuries (100s, includes domestic/IPL centuries), "
        f"{bat_row['fifties']} fifties (50s), "
        f"{bat_row['fours']} fours, {bat_row['sixes']} sixes, "
        f"dismissed {bat_row['dismissals']} times, "
        f"{bat_row['innings'] - bat_row['dismissals']} not-outs."
    )
    if bowl_row and bowl_row["total_wickets"] and int(bowl_row["total_wickets"]) > 0:
        source_text += (
            f" Bowling: {bowl_row['total_wickets']} wickets in {bowl_row['match_count']} matches, "
            f"economy {bowl_row['economy'] or 'N/A'}, "
            f"best figures {bowl_row['best_wickets']}/{bowl_row['best_runs'] or '?'}."
        )
    source_text += (
        " Note: home/away splits, overseas records, and venue-specific breakdowns "
        "are not available in this dataset."
    )
    return {
        "answer": answer,
        "sources": [{
            "chunk_id": f"analytics:career:{player_id}:{fmt}",
            "score": 1.0,
            "text": source_text,
            "title": "Player career stats",
            "player_name": query.player_name,
            "display_name": display_name,
            "match_id": "",
            "date": "",
            "match_type": query.match_type or "",
            "event_name": "",
            "venue": "",
            "document_type": "analytics_result",
        }],
    }


def _answer_head_to_head(
    connection: sqlite3.Connection,
    query: HeadToHeadQuery,
) -> dict[str, object] | None:
    """Answer a head-to-head record question between two teams."""
    clauses = [
        "lower(teams_csv) LIKE ?",
        "lower(teams_csv) LIKE ?",
    ]
    params: list[object] = [f"%{query.team1.lower()}%", f"%{query.team2.lower()}%"]

    if query.match_type:
        allowed = _match_type_filters(query.match_type)
        ph = ", ".join("?" for _ in allowed)
        clauses.append(f"match_type IN ({ph})")
        params.extend(allowed)
    if query.year:
        clauses.append("substr(date, 1, 4) = ?")
        params.append(str(query.year))

    sql = f"""
        SELECT
            COUNT(*) AS total_matches,
            SUM(CASE WHEN lower(outcome) LIKE ? THEN 1 ELSE 0 END) AS team1_wins,
            SUM(CASE WHEN lower(outcome) LIKE ? THEN 1 ELSE 0 END) AS team2_wins,
            SUM(CASE WHEN lower(outcome) LIKE '%no result%'
                      OR lower(outcome) LIKE '%abandoned%'
                      OR lower(outcome) LIKE '%tied%' THEN 1 ELSE 0 END) AS other
        FROM analytics_matches
        WHERE {' AND '.join(clauses)}
    """
    # CASE WHEN params come first (they're in SELECT), then WHERE params
    all_params = [f"%{query.team1.lower()}%won%", f"%{query.team2.lower()}%won%"] + params
    row = connection.execute(sql, all_params).fetchone()

    if row is None or int(row["total_matches"]) == 0:
        return None

    fmt = query.match_type or "all formats"
    t1 = query.team1.title()
    t2 = query.team2.title()
    answer = (
        f"{t1} vs {t2} head-to-head in {fmt}: "
        f"{row['total_matches']} matches played. "
        f"{t1} won {row['team1_wins']}, {t2} won {row['team2_wins']}"
        + (f", {row['other']} no result/tied." if row["other"] else ".")
    )
    source_text = f"{t1} vs {t2} {fmt}: {row['total_matches']} matches, {t1} {row['team1_wins']} wins, {t2} {row['team2_wins']} wins"
    return {
        "answer": answer,
        "sources": [{
            "chunk_id": f"analytics:h2h:{query.team1}:{query.team2}:{fmt}",
            "score": 1.0,
            "text": source_text,
            "title": "Head-to-head record",
            "player_name": "",
            "display_name": "",
            "match_id": "",
            "date": "",
            "match_type": query.match_type or "",
            "event_name": "",
            "venue": "",
            "document_type": "analytics_result",
        }],
    }


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
    venue_filter = intent.venue or _extract_venue((intent.rewritten_question or "").lower())
    if venue_filter:
        clauses.append("lower(bp.venue) LIKE ?")
        params.append(f"%{venue_filter.lower()}%")

    # Use intent.team if available; otherwise try to extract opposition from rewritten question
    team_filter = intent.team
    if not team_filter:
        # Extract known team names from the rewritten question
        _KNOWN_TEAMS = [
            "india", "australia", "england", "pakistan", "sri lanka", "new zealand",
            "south africa", "west indies", "bangladesh", "zimbabwe", "afghanistan",
            "ireland", "scotland", "netherlands", "kenya", "canada", "uae",
        ]
        lowered_q = (intent.rewritten_question or "").lower()
        for team in _KNOWN_TEAMS:
            if team in lowered_q:
                team_filter = team
                break

    if team_filter:
        clauses.append(
            "(lower(am.teams_csv) LIKE ? OR lower(bp.innings_team) LIKE ? OR lower(bp.opposition_team) LIKE ?)"
        )
        params.extend([f"%{team_filter.lower()}%"] * 3)

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

    candidates = resolve_player_name(connection, query.player_name, limit=10)
    player_names = {str(query.player_name)}
    player_id: int | None = None
    if candidates:
        player_id = _pick_player_with_most_data(connection, candidates)
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
                lowered_term = term.lower()
                params.extend([f"%{lowered_term}%", f"%{lowered_term}%", f"%{lowered_term}%"])

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


def _answer_player_bowling_match_question(connection: sqlite3.Connection, query: PlayerMatchQuery) -> dict[str, object] | None:
    from app.analytics.players import get_preferred_player_display_name, resolve_player_name

    candidates = resolve_player_name(connection, query.player_name, limit=10)
    if not candidates:
        return None
    player_id = _pick_player_with_most_data(connection, candidates)

    clauses = ["bw.player_id = ?"]
    params: list[object] = [player_id]
    if query.match_id:
        clauses.append("bw.match_id = ?")
        params.append(query.match_id)
    else:
        if query.match_type:
            allowed = _match_type_filters(query.match_type)
            clauses.append(f"bw.match_type IN ({', '.join('?' for _ in allowed)})")
            params.extend(allowed)
        if query.year:
            clauses.append("substr(bw.date, 1, 4) = ?")
            params.append(str(query.year))
        if query.event_name:
            clauses.append("(lower(bw.event_name) LIKE ? OR ? LIKE '%' || lower(bw.event_name) || '%')")
            params.append(f"%{query.event_name.lower()}%")
            params.append(query.event_name.lower())
        if query.team_terms:
            for term in query.team_terms:
                lowered_term = term.lower()
                clauses.append("(lower(am.teams_csv) LIKE ? OR lower(bw.bowling_team) LIKE ? OR lower(bw.opposition_team) LIKE ?)")
                params.extend([f"%{lowered_term}%", f"%{lowered_term}%", f"%{lowered_term}%"])

    sql = f"""
        SELECT bw.match_id, bw.player_id, bw.player_name, bw.bowling_team, bw.opposition_team,
               bw.wickets, bw.runs_conceded, bw.balls_bowled, bw.match_type, bw.date,
               bw.venue, bw.event_name, am.teams_csv
        FROM bowling_performances bw
        JOIN analytics_matches am ON am.match_id = bw.match_id
        WHERE {' AND '.join(clauses)}
        ORDER BY bw.wickets DESC, bw.runs_conceded ASC, bw.date DESC
        LIMIT 1
    """
    row = connection.execute(sql, params).fetchone()
    if row is None:
        return None

    display_name = get_preferred_player_display_name(connection, int(row["player_id"]), str(row["player_name"]))
    answer = (
        f"In {row['teams_csv']} on {row['date']}, {display_name} took "
        f"{row['wickets']}/{row['runs_conceded']} from {row['balls_bowled']} balls "
        f"for {row['bowling_team']} against {row['opposition_team']}."
    )
    return {"answer": answer, "sources": [{
        "chunk_id": f"analytics:{row['match_id']}:player_bowling:{row['player_id']}",
        "score": 1.0, "text": answer, "title": "Analytics player bowling result",
        "player_name": row["player_name"], "display_name": display_name,
        "teams": row["teams_csv"], "match_id": row["match_id"], "date": row["date"],
        "match_type": row["match_type"] or "", "event_name": row["event_name"] or "",
        "venue": row["venue"] or "", "document_type": "analytics_result",
    }]}
