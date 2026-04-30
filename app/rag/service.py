from __future__ import annotations

import json
import sqlite3
import re

from app.analytics.stats import AnalyticsQueryService
from app.chat.disambiguation import (
    build_clarification_response,
    detect_player_ambiguity,
    resolve_pending_disambiguation,
)
from app.chat.memory import resolve_follow_up_question, update_conversation_state
from app.knowledge.service import KnowledgeService, looks_like_knowledge_question
from app.analytics.players import resolve_player_name
from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient


class ChatService:
    def __init__(
        self,
        index: LocalIndex,
        llm_client: OllamaClient,
        analytics_service: AnalyticsQueryService | None = None,
        knowledge_service: KnowledgeService | None = None,
    ) -> None:
        self.index = index
        self.llm_client = llm_client
        self.analytics_service = analytics_service
        self.knowledge_service = knowledge_service

    def answer(
        self,
        question: str,
        top_k: int = 6,
        conversation_state: dict[str, str] | None = None,
    ) -> dict[str, object]:
        # ------------------------------------------------------------------
        # Step 1: Check if the user is replying to a pending disambiguation.
        # ------------------------------------------------------------------
        pending_raw = (conversation_state or {}).get("pending_disambiguation")
        if pending_raw:
            resolved = self._try_resolve_disambiguation(question, pending_raw)
            if resolved is not None:
                # Re-run the original question with the resolved player name
                # substituted in, and clear the pending state.
                pending = json.loads(pending_raw)
                original_question = str(pending.get("original_question", question))
                player_fragment = str(pending.get("player_fragment", ""))
                resolved_question = _substitute_player(original_question, player_fragment, resolved)

                # Strip the pending disambiguation from state before recursing
                clean_state = {k: v for k, v in (conversation_state or {}).items() if k != "pending_disambiguation"}
                result = self.answer(resolved_question, top_k=top_k, conversation_state=clean_state)
                return result

        # ------------------------------------------------------------------
        # Step 2: Normal follow-up resolution (match/player context carry-over)
        # ------------------------------------------------------------------
        rewritten_question, explicit_player_name = resolve_follow_up_question(
            question,
            conversation_state,
            player_resolver=self._resolve_explicit_player_name,
        )

        # ------------------------------------------------------------------
        # Step 3: Player disambiguation check.
        # Only run when the question looks like it's asking about a specific
        # player and we don't already have a confident explicit player name.
        # ------------------------------------------------------------------
        if explicit_player_name is None and _looks_like_player_question(rewritten_question):
            disambiguation = self._check_player_disambiguation(rewritten_question)
            if disambiguation is not None:
                return build_clarification_response(disambiguation, conversation_state)

        # ------------------------------------------------------------------
        # Step 4: Knowledge base
        # ------------------------------------------------------------------
        if looks_like_knowledge_question(rewritten_question) and self.knowledge_service is not None:
            knowledge_answer = self.knowledge_service.answer(rewritten_question)
            if knowledge_answer is not None:
                knowledge_answer["conversation_state"] = update_conversation_state(
                    conversation_state,
                    rewritten_question,
                    knowledge_answer,
                    explicit_player_name=explicit_player_name,
                )
                return knowledge_answer

        # ------------------------------------------------------------------
        # Step 5: Structured analytics
        # ------------------------------------------------------------------
        if _looks_like_aggregate_stats_question(rewritten_question) and self.analytics_service is not None:
            analytics_answer = self.analytics_service.answer(rewritten_question)
            if analytics_answer is not None:
                analytics_answer["conversation_state"] = update_conversation_state(
                    conversation_state,
                    rewritten_question,
                    analytics_answer,
                    explicit_player_name=explicit_player_name,
                )
                return analytics_answer

        if _looks_like_aggregate_stats_question(rewritten_question):
            result = {
                "answer": (
                    "I can't answer that reliably yet from the current MVP RAG pipeline. "
                    "Questions like highest, most, best, or overall records across all matches "
                    "need a structured statistics layer rather than plain retrieval. "
                    "Right now this app is better at match-specific and player-in-a-match questions."
                ),
                "sources": [],
            }
            result["conversation_state"] = update_conversation_state(
                conversation_state,
                rewritten_question,
                result,
                explicit_player_name=explicit_player_name,
            )
            return result

        # ------------------------------------------------------------------
        # Step 6: RAG fallback
        # ------------------------------------------------------------------
        retrieved = self.index.retrieve(rewritten_question, top_k=top_k)
        sources = [
            {
                "chunk_id": chunk.chunk_id,
                "score": chunk.score,
                "text": chunk.text,
                **chunk.metadata,
            }
            for chunk in retrieved
        ]
        answer = self.llm_client.generate_answer(rewritten_question, sources) if sources else (
            "I could not find supporting CricSheet context for that question yet."
        )
        result = {"answer": answer, "sources": sources}
        result["conversation_state"] = update_conversation_state(
            conversation_state,
            rewritten_question,
            result,
            explicit_player_name=explicit_player_name,
        )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _resolve_explicit_player_name(self, question: str) -> str | None:
        """Return a confident single player name from the question, or None."""
        try:
            with sqlite3.connect(self.index.registry_db_path) as connection:
                connection.row_factory = sqlite3.Row
                candidates = resolve_player_name(connection, question, limit=2)
        except sqlite3.Error:
            return None

        if not candidates:
            return None
        top = candidates[0]
        if len(candidates) > 1 and abs(top.score - candidates[1].score) < 3:
            return None
        if top.score < 95:
            return None
        return top.matched_alias if " " in top.matched_alias else top.canonical_name

    def _check_player_disambiguation(self, question: str) -> object | None:
        """Return a DisambiguationRequest if the question is ambiguous, else None."""
        try:
            with sqlite3.connect(self.index.registry_db_path) as connection:
                connection.row_factory = sqlite3.Row
                return detect_player_ambiguity(question, connection)
        except sqlite3.Error:
            return None

    def _try_resolve_disambiguation(
        self,
        user_reply: str,
        pending_raw: str,
    ) -> str | None:
        """Try to resolve a pending disambiguation from the user's reply."""
        try:
            pending = json.loads(pending_raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return resolve_pending_disambiguation(user_reply, pending)


# ---------------------------------------------------------------------------
# Question-type helpers
# ---------------------------------------------------------------------------


def _looks_like_player_question(question: str) -> bool:
    """Return True if the question is likely asking about a specific player."""
    lowered = question.lower()
    player_markers = [
        "how did",
        "how has",
        "what did",
        "what was",
        "what about",
        "how about",
        "tell me about",
        "show me",
    ]
    return any(marker in lowered for marker in player_markers)


def _looks_like_aggregate_stats_question(question: str) -> bool:
    lowered = question.lower()
    aggregate_markers = [
        "highest",
        "most",
        "best",
        "lowest",
        "top ",
        "record",
        "overall",
        "across",
        "all matches",
    ]
    stat_markers = [
        "score",
        "runs",
        "wickets",
        "economy",
        "strike rate",
        "average",
    ]
    return any(marker in lowered for marker in aggregate_markers) and any(
        marker in lowered for marker in stat_markers
    )


def _substitute_player(question: str, fragment: str, resolved_name: str) -> str:
    """Replace the ambiguous player fragment in the question with the resolved name.

    Falls back to appending context if the fragment isn't found verbatim.
    """
    if fragment and fragment.lower() in question.lower():
        # Case-insensitive replacement preserving surrounding text
        pattern = re.compile(re.escape(fragment), re.IGNORECASE)
        return pattern.sub(resolved_name, question, count=1)
    # Fallback: prepend context
    return f"Regarding {resolved_name}: {question}"
