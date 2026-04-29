from __future__ import annotations

import streamlit as st

from app.analytics.stats import AnalyticsQueryService
from app.knowledge.service import KnowledgeService
from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient
from app.rag.service import ChatService
from app.settings import get_settings


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
    analytics = AnalyticsQueryService(settings.registry_db_path)
    knowledge = KnowledgeService()
    return ChatService(index=index, llm_client=llm, analytics_service=analytics, knowledge_service=knowledge)


st.set_page_config(page_title="Cricket AI Expert", layout="wide")
st.title("Cricket AI Expert")
st.caption("Ask cricket questions grounded in your local CricSheet dataset.")

if "messages" not in st.session_state:
    st.session_state.messages = []

col1, col2 = st.columns([1, 5])
with col1:
    if st.button("Clear chat", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
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

question = st.chat_input("Ask a cricket question")
if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    with st.chat_message("assistant"):
        with st.spinner("Searching matches and preparing an answer..."):
            result = get_chat_service().answer(question)
        st.markdown(result["answer"])
        with st.expander("Sources used", expanded=False):
            for source in result["sources"]:
                st.markdown(
                    f"**{source.get('title', source['chunk_id'])}**  \n"
                    f"Match: `{source.get('match_id', 'unknown')}` | "
                    f"Date: `{source.get('date', 'unknown')}` | "
                    f"Score: `{source.get('score', 0):.3f}`"
                )
                st.write(source["text"])

    st.session_state.messages.append(
        {"role": "assistant", "content": result["answer"], "sources": result["sources"]}
    )
