from __future__ import annotations

import os

import streamlit as st

from app.analytics.stats import AnalyticsQueryService
from app.knowledge.service import KnowledgeService
from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient
from app.rag.service import ChatService
from app.settings import get_settings

DEBUG_MODE = os.environ.get("CRICKET_AI_DEBUG", "").strip().lower() in {"1", "true", "yes"}


def _label_for_answer_type(answer_type: str) -> str:
    labels = {
        "analytics": "Analytics",
        "knowledge": "Knowledge",
        "match_narrative_rag": "Match Narrative",
        "mixed": "Mixed Intent",
        "mixed_rag": "Mixed Fallback",
        "rag_fallback": "Fallback Retrieval",
        "general": "General",
        "disambiguation": "Disambiguation",
    }
    return labels.get(answer_type, answer_type or "Unknown")


def _confidence_for_answer_type(answer_type: str) -> str:
    if answer_type in {"analytics", "knowledge", "mixed"}:
        return "High"
    if answer_type in {"match_narrative_rag", "mixed_rag"}:
        return "Medium"
    if answer_type == "rag_fallback":
        return "Low"
    return "Medium"


def _followup_suggestions(answer_type: str) -> list[str]:
    suggestions = {
        "analytics": [
            "Ask for another player's career stats.",
            "Try a top-5 leaderboard question for runs or wickets.",
        ],
        "knowledge": [
            "Ask about another cricket rule or tournament format.",
            "Ask for a definition of a common cricket term.",
        ],
        "match_narrative_rag": [
            "Ask about a different match or series outcome.",
            "Ask who was the key performer in a specific final.",
        ],
        "mixed": [
            "Compare another pair of players or records.",
            "Ask whether the result is strong for that format.",
        ],
        "mixed_rag": [
            "Try a more specific comparison question.",
            "Ask about another match result plus stats.",
        ],
        "rag_fallback": [
            "Try a more specific match or player question.",
            "Ask about a known event, tournament, or record.",
        ],
        "general": [
            "Ask about cricket history or famous players.",
            "Ask for an explanation of cricket strategy.",
        ],
    }
    return suggestions.get(answer_type, [])


def _render_message_metadata(message: dict) -> None:
    answer_type = _label_for_answer_type(message.get("answer_type", ""))
    confidence = message.get("confidence") or _confidence_for_answer_type(message.get("answer_type", ""))
    st.markdown(
        f"**Answer type:** `{answer_type}`  \n"
        f"**Confidence:** `{confidence}`"
    )
    if DEBUG_MODE and message.get("rewritten_question"):
        st.caption(f"Rewritten question: {message['rewritten_question']}")


def _render_assistant_metadata(result: dict) -> None:
    answer_type = _label_for_answer_type(result.get("answer_type", ""))
    confidence = result.get("confidence") or _confidence_for_answer_type(result.get("answer_type", ""))
    st.markdown(
        f"**Answer type:** `{answer_type}`  \n"
        f"**Confidence:** `{confidence}`"
    )
    if DEBUG_MODE and result.get("rewritten_question"):
        st.caption(f"Rewritten question: {result['rewritten_question']}")
    followups = _followup_suggestions(result.get("answer_type", ""))
    if followups:
        with st.expander("Suggested follow-up questions", expanded=False):
            for followup in followups:
                st.write(f"- {followup}")


@st.cache_resource
def get_chat_service() -> ChatService:
    settings = get_settings()
    index = LocalIndex(
        registry_db_path=settings.registry_db_path,
        chroma_dir=settings.chroma_dir,
        collection_name=settings.chroma_collection,
        embedding_model_name=settings.embedding_model,
    )
    llm = OllamaClient(settings.ollama_base_url, settings.ollama_model)
    analytics = AnalyticsQueryService(
        settings.registry_db_path,
        ollama_base_url=settings.ollama_base_url,
        ollama_model=settings.ollama_intent_model,
    )
    knowledge = KnowledgeService()
    return ChatService(index=index, llm_client=llm, analytics_service=analytics, knowledge_service=knowledge)


st.set_page_config(page_title="Cricket AI Expert", layout="wide")
st.title("Cricket AI Expert")
st.caption("Ask cricket questions grounded in your local CricSheet dataset.")

if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_state" not in st.session_state:
    st.session_state.conversation_state = {}

col1, col2 = st.columns([1, 5])
with col1:
    if st.button("Clear chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.conversation_state = {}
        st.rerun()

# ---------------------------------------------------------------------------
# Render existing messages
# ---------------------------------------------------------------------------
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        if message["role"] == "assistant":
            _render_message_metadata(message)
        st.markdown(message["content"])
        if message.get("sources"):
            with st.expander("Sources used"):
                for source in message["sources"]:
                    st.markdown(
                        f"**{source.get('title', source['chunk_id'])}**  \n"
                        f"Match: `{source.get('match_id', 'unknown')}` | "
                        f"Date: `{source.get('date', 'unknown')}` | "
                        f"Score: `{source.get('score', 0):.3f}`"
                    )
                    st.write(source["text"])

# ---------------------------------------------------------------------------
# Disambiguation quick-pick buttons
# If the last assistant message was a disambiguation prompt, show buttons so
# the user can click a candidate instead of typing.
# ---------------------------------------------------------------------------
_pending_candidates: list[str] = []
if st.session_state.messages:
    last_msg = st.session_state.messages[-1]
    if last_msg.get("role") == "assistant" and last_msg.get("answer_type") == "disambiguation":
        _pending_candidates = last_msg.get("disambiguation_candidates", [])

if _pending_candidates:
    st.markdown("**Pick a player:**")
    cols = st.columns(len(_pending_candidates))
    for col, candidate in zip(cols, _pending_candidates):
        if col.button(candidate, key=f"disambig_{candidate}"):
            # Treat the button click as a user message
            user_input = candidate
            st.session_state.messages.append({"role": "user", "content": user_input})
            with st.chat_message("assistant"):
                with st.spinner("Searching matches and preparing an answer..."):
                    result = get_chat_service().answer(
                        user_input,
                        conversation_state=st.session_state.conversation_state,
                    )
                _render_assistant_result(result)  # type: ignore[name-defined]  # defined below
            st.session_state.conversation_state = result.get(
                "conversation_state", st.session_state.conversation_state
            )
            st.session_state.messages.append(_make_message_record(result))  # type: ignore[name-defined]
            st.rerun()

# ---------------------------------------------------------------------------
# Helper functions (defined after first use site to keep render flow readable)
# ---------------------------------------------------------------------------


def _render_assistant_result(result: dict) -> None:
    """Render an assistant result inside the current chat_message context."""
    _render_assistant_metadata(result)
    st.markdown(result["answer"])
    sources = result.get("sources") or []
    if sources:
        with st.expander("Sources used", expanded=False):
            for source in sources:
                st.markdown(
                    f"**{source.get('title', source['chunk_id'])}**  \n"
                    f"Match: `{source.get('match_id', 'unknown')}` | "
                    f"Date: `{source.get('date', 'unknown')}` | "
                    f"Score: `{source.get('score', 0):.3f}`"
                )
                st.write(source["text"])


def _make_message_record(result: dict) -> dict:
    return {
        "role": "assistant",
        "content": result["answer"],
        "sources": result.get("sources") or [],
        "answer_type": result.get("answer_type", ""),
        "confidence": result.get("confidence", ""),
        "rewritten_question": result.get("rewritten_question", ""),
        "disambiguation_candidates": result.get("disambiguation_candidates", []),
    }


# ---------------------------------------------------------------------------
# Chat input
# ---------------------------------------------------------------------------
question = st.chat_input("Ask a cricket question")
if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Searching matches and preparing an answer..."):
            result = get_chat_service().answer(
                question,
                conversation_state=st.session_state.conversation_state,
            )
        _render_assistant_result(result)

    st.session_state.conversation_state = result.get(
        "conversation_state", st.session_state.conversation_state
    )
    st.session_state.messages.append(_make_message_record(result))
