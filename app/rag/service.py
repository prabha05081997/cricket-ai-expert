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
        original_lower = question.lower()
        if (
            "best batsman ever" in original_lower
            or "best batter ever" in original_lower
            or "greatest batsman ever" in original_lower
            or "greatest batter ever" in original_lower
        ):
            answer = (
                "There is no single objective best batsman ever; it is subjective and depends "
                "on format, era, conditions, and whether you value peak or longevity."
            )
            result: dict[str, object] = {"answer": answer, "sources": []}
            result["conversation_state"] = update_conversation_state(
                conversation_state,
                question,
                result,
            )
            return result

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

        # If the LLM resolved a pronoun in rewritten_question but didn't set
        # intent.player (can happen for aggregate follow-ups like "how many of
        # them?" or "what's his highest?"), inject the player from context.
        # The LLM already put the name in rewritten_question — we just need it
        # in intent.player so the career/aggregate dispatch can use it.
        if (
            intent.intent == "aggregate_stats"
            and not intent.player
            and conversation_state
            and conversation_state.get("last_player_name")
        ):
            last_player = conversation_state["last_player_name"]
            # Only inject if the rewritten question mentions the player name
            if last_player.lower() in intent.rewritten_question.lower():
                intent.player = last_player

        lowered_rewritten = intent.rewritten_question.lower()

        if "champions trophy" in lowered_rewritten:
            intent.event = "ICC Champions Trophy"
        elif "world cup" in lowered_rewritten and not intent.event:
            intent.event = "ICC Cricket World Cup"

        if not intent.match_type:
            if "one day international" in lowered_rewritten or "odi" in lowered_rewritten:
                intent.match_type = "ODI"
            elif "test" in lowered_rewritten:
                intent.match_type = "Test"
            elif "t20i" in lowered_rewritten:
                intent.match_type = "T20I"

        if intent.intent == "player_performance" and not intent.player:
            if any(word in lowered_rewritten for word in ["won", "result", "final", "perform"]):
                intent.intent = "match_narrative"

        # If the LLM classifies a career stat question as player_performance,
        # redirect it to aggregate_stats. Career metrics (average, centuries,
        # wickets, sixes, fours, best year) are aggregate stats, not match
        # performances. The LLM sometimes confuses these.
        _CAREER_METRICS = {
            "average", "batting_average", "bowling_average", "centuries",
            "wickets", "sixes", "fours", "economy", "strike_rate",
            "best_year", "most_runs", "most_wickets",
        }
        _CAREER_KEYWORDS = {
            "average", "batting average", "career", "centuries", "hundreds",
            "fifties", "best year", "most runs", "most wickets", "sixes",
            "fours", "strike rate", "economy", "statistics", "stats",
            "overseas", "home", "away",  # split stats → career, not match
        }
        if intent.intent == "player_performance" and intent.player and not intent.pinned_match_id:
            is_career = (
                intent.metric in _CAREER_METRICS
                or (
                    not intent.event
                    and any(kw in intent.rewritten_question.lower() for kw in _CAREER_KEYWORDS)
                )
                or ("best" in lowered_rewritten and "year" in lowered_rewritten)
                or "sabse acha" in lowered_rewritten
                or (
                    intent.event
                    and "final" not in lowered_rewritten
                    and any(phrase in lowered_rewritten for phrase in ["how many runs", "total runs"])
                )
            )
            if is_career:
                intent.intent = "aggregate_stats"
                _debug(f"[REDIRECT] player_performance → aggregate_stats (metric={intent.metric!r})")

        if (
            intent.intent == "aggregate_stats"
            and conversation_state
            and conversation_state.get("last_match_id")
            and any(phrase in lowered_rewritten for phrase in ["top scorer", "highest scorer", "most runs in that match"])
        ):
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
            if result is not None:
                result["answer_type"] = "knowledge"

        elif intent.intent in {"player_performance", "player_dismissal", "aggregate_stats"}:
            if self.analytics_service is not None:
                _debug(f"[ROUTE] → AnalyticsQueryService ({intent.intent})")
                result = self.analytics_service.answer(resolved_question, intent=intent)
                _debug(f"[RESULT] analytics={'hit' if result else 'miss → RAG fallback'}")
                if result is not None:
                    result["answer_type"] = "analytics"

        elif intent.intent == "match_narrative":
            # Try analytics first — match results are in the DB and more reliable
            # than RAG retrieval for "who won X final?" questions.
            if self.analytics_service is not None:
                _debug("[ROUTE] → AnalyticsQueryService (match_narrative)")
                result = self.analytics_service.answer(resolved_question, intent=intent)
                _debug(f"[RESULT] analytics={'hit' if result else 'miss → RAG fallback'}")
                if result is not None:
                    result["answer_type"] = "analytics"
            if result is None:
                _debug("[ROUTE] → RAG (match_narrative)")
                result = self._rag_answer(resolved_question, top_k, route="match_narrative")
                result["answer_type"] = "match_narrative_rag"

        # ------------------------------------------------------------------
        # Step 3: Fallback to RAG if primary subsystem returned nothing
        # ------------------------------------------------------------------
        if result is None:
            _debug("[ROUTE] → RAG (fallback)")
            result = self._rag_answer(resolved_question, top_k, route="rag_fallback")
            result["answer_type"] = "rag_fallback"

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

    def _rag_answer(self, question: str, top_k: int, route: str = "rag") -> dict[str, object]:
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
            self.llm_client.generate_answer(question, sources, route=route)
            if sources
            else "I could not find supporting CricSheet context for that question yet."
        )
        return {"answer": answer, "sources": sources}
