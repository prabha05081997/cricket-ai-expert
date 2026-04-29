from __future__ import annotations

import re

from app.analytics.stats import AnalyticsQueryService
from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient


class ChatService:
    def __init__(
        self,
        index: LocalIndex,
        llm_client: OllamaClient,
        analytics_service: AnalyticsQueryService | None = None,
    ) -> None:
        self.index = index
        self.llm_client = llm_client
        self.analytics_service = analytics_service

    def answer(self, question: str, top_k: int = 6) -> dict[str, object]:
        if _looks_like_aggregate_stats_question(question) and self.analytics_service is not None:
            analytics_answer = self.analytics_service.answer(question)
            if analytics_answer is not None:
                return analytics_answer
        if _looks_like_aggregate_stats_question(question):
            return {
                "answer": (
                    "I can't answer that reliably yet from the current MVP RAG pipeline. "
                    "Questions like highest, most, best, or overall records across all matches "
                    "need a structured statistics layer rather than plain retrieval. "
                    "Right now this app is better at match-specific and player-in-a-match questions."
                ),
                "sources": [],
            }

        retrieved = self.index.retrieve(question, top_k=top_k)
        sources = [
            {
                "chunk_id": chunk.chunk_id,
                "score": chunk.score,
                "text": chunk.text,
                **chunk.metadata,
            }
            for chunk in retrieved
        ]
        answer = self.llm_client.generate_answer(question, sources) if sources else (
            "I could not find supporting CricSheet context for that question yet."
        )
        return {"answer": answer, "sources": sources}


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
