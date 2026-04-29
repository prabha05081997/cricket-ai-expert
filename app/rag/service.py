from __future__ import annotations

from app.rag.index import LocalIndex
from app.rag.llm import OllamaClient


class ChatService:
    def __init__(self, index: LocalIndex, llm_client: OllamaClient) -> None:
        self.index = index
        self.llm_client = llm_client

    def answer(self, question: str, top_k: int = 6) -> dict[str, object]:
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

