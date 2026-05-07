"""ChatService — the main entry point for answering user questions.

Routing strategy
----------------
1. The LLM classifies the intent of every question and resolves pronouns /
   follow-up references using conversation context.  This replaces the old
   brittle string-matching rule engine.

2. Based on the classified intent we dispatch to the right subsystem:
   - player_performance  → AnalyticsQueryService (player-in-match lookup)
   - player_dismissal    → AnalyticsQueryService (dismissal lookup)
   - aggregate_stats     → AnalyticsQueryService (leaderboard / record queries)
   - cricket_knowledge   → KnowledgeService
   - match_narrative     → RAG (vector + keyword retrieval)
   - general_chat        → RAG fallback

3. If the analytics / knowledge layer returns None (data not in DB), we fall
   back to RAG so the LLM can still attempt an answer from retrieved chunks.

4. Player disambiguation is handled by the LLM classifier — it will ask for
   clarification in the rewritten_question when the name is ambiguous.

Debug logging
-------------
Set the environment variable CRICKET_AI_DEBUG=1 to enable routing debug logs.
Each turn prints:
  [INTENT]  intent | player | event | year | match_type | rewritten
  [ROUTE]   which subsystem handled the question
  [RESULT]  analytics hit / RAG fallback / not found
"""

from __future__ import annotations

import logging
import os

from app.analytics.stats import AnalyticsQueryService
from app.chat.memory import update_conversation_state
from app.knowledge.service import KnowledgeService
from app.rag.index import LocalIndex
from app.rag.intent import IntentResult, classify_intent
from app.rag.llm import OllamaClient
from app.settings import get_settings

_DEBUG = os.environ.get("CRICKET_AI_DEBUG", "").strip() in {"1", "true", "yes"}
logger = logging.getLogger(__name__)


def _debug(msg: str) -> None:
    if _DEBUG:
        print(f"[DEBUG] {msg}", flush=True)


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
        settings = get_settings()
        self._ollama_base_url = settings.ollama_base_url
        self._ollama_model = settings.ollama_model
        self._ollama_intent_model = settings.ollama_intent_model

    def answer(
        self,
        question: str,
        top_k: int = 6,
        conversation_state: dict[str, str] | None = None,
    ) -> dict[str, object]:
        # ------------------------------------------------------------------
        # Step 1: LLM classifies intent and resolves all references.
        # ------------------------------------------------------------------
        intent = classify_intent(
            question,
            ollama_base_url=self._ollama_base_url,
            ollama_model=self._ollama_intent_model,
            conversation_state=conversation_state,
        )

        # If the conversation has a pinned match from the previous turn and this
        # is a follow-up about the same player, carry the match_id forward so
        # dismissal / performance queries don't need to search or guess.
        if conversation_state and conversation_state.get("last_match_id"):
            last_player = conversation_state.get("last_player_name", "")
            is_followup = (
                intent.intent in {"player_dismissal", "player_performance"}
                and intent.player
                and (
                    not intent.event
                    and not intent.year
                    or (last_player and intent.player.lower() in last_player.lower())
                    or (last_player and last_player.lower() in intent.player.lower())
                )
            )
            if is_followup:
                intent.pinned_match_id = conversation_state["last_match_id"]

        _debug(
            f"[INTENT] intent={intent.intent} | player={intent.player!r} | "
            f"event={intent.event!r} | year={intent.year} | "
            f"match_type={intent.match_type!r} | "
            f"pinned_match_id={intent.pinned_match_id!r} | "
            f"rewritten={intent.rewritten_question!r}"
        )

        resolved_question = intent.rewritten_question

        # ------------------------------------------------------------------
        # Step 2: Dispatch based on intent
        # ------------------------------------------------------------------
        result: dict[str, object] | None = None

        if intent.intent == "cricket_knowledge" and self.knowledge_service is not None:
            _debug("[ROUTE] → KnowledgeService")
            result = self.knowledge_service.answer(resolved_question)
            _debug(f"[RESULT] knowledge={'hit' if result else 'miss'}")

        elif intent.intent in {"player_performance", "player_dismissal", "aggregate_stats"}:
            if self.analytics_service is not None:
                _debug(f"[ROUTE] → AnalyticsQueryService ({intent.intent})")
                result = self.analytics_service.answer(resolved_question, intent=intent)
                _debug(f"[RESULT] analytics={'hit' if result else 'miss → RAG fallback'}")

        elif intent.intent == "match_narrative":
            _debug("[ROUTE] → RAG (match_narrative)")
            result = self._rag_answer(resolved_question, top_k)

        # ------------------------------------------------------------------
        # Step 3: Fallback to RAG if primary subsystem returned nothing
        # ------------------------------------------------------------------
        if result is None:
            _debug("[ROUTE] → RAG (fallback)")
            result = self._rag_answer(resolved_question, top_k)

        # ------------------------------------------------------------------
        # Step 4: Update conversation state and return
        # ------------------------------------------------------------------
        result["conversation_state"] = update_conversation_state(
            conversation_state,
            resolved_question,
            result,
        )
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _rag_answer(self, question: str, top_k: int) -> dict[str, object]:
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
        answer = (
            self.llm_client.generate_answer(question, sources)
            if sources
            else "I could not find supporting CricSheet context for that question yet."
        )
        return {"answer": answer, "sources": sources}
