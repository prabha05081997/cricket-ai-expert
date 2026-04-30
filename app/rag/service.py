from __future__ import annotations

import sqlite3
import re

from app.analytics.stats import AnalyticsQueryService
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
        rewritten_question, explicit_player_name = resolve_follow_up_question(
            question,
            conversation_state,
            player_resolver=self._resolve_explicit_player_name,
        )

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

    def _resolve_explicit_player_name(self, question: str) -> str | None:
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
