from __future__ import annotations

from typing import Any

import httpx


class OllamaClient:
    def __init__(self, base_url: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model

    def generate_answer(
        self,
        question: str,
        context_chunks: list[dict[str, Any]],
        route: str = "rag",
    ) -> str:
        context_text = "\n\n".join(
            f"Source {index + 1} [{chunk.get('title', 'untitled')}]:\n{chunk['text']}"
            for index, chunk in enumerate(context_chunks)
        )
        prompt = self._build_prompt(question, context_text, route)
        response = httpx.post(
            f"{self.base_url}/api/generate",
            json={"model": self.model, "prompt": prompt, "stream": False},
            timeout=120.0,
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response", "")).strip()

    def _build_prompt(self, question: str, context_text: str, route: str) -> str:
        if route == "analytics":
            return (
                "You are a cricket stats assistant. Answer the question using only the provided fact sheet.\n"
                "Keep the response terse and factual. Do not add padding, opinion, or extra commentary.\n"
                "If the fact sheet does not include the requested information, say:\n"
                "\"I don't have the numbers for that in the current dataset.\"\n\n"
                f"Question:\n{question}\n\n"
                f"Fact sheet:\n{context_text}\n"
            )

        if route == "match_narrative":
            return (
                "You are a cricket commentator and match analyst. Use the provided context to answer clearly and naturally.\n"
                "Give the direct answer first, then a brief supporting sentence if needed.\n"
                "Keep the tone conversational and grounded in the context.\n"
                "Do not invent match details or statistics that are not in the context.\n\n"
                f"Question:\n{question}\n\n"
                f"Context:\n{context_text}\n"
            )

        if route == "knowledge":
            return (
                "You are a cricket knowledge expert. Answer the question using only the provided context.\n"
                "Be structured, clear, and explanatory. Do not invent details.\n"
                "If the answer is not directly supported, say:\n"
                "\"I don't have enough information in the current context to answer that.\"\n\n"
                f"Question:\n{question}\n\n"
                f"Context:\n{context_text}\n"
            )

        if route == "mixed":
            return (
                "You are a cricket analyst. Compare the provided sources and answer the question directly.\n"
                "Focus on the requested comparison or synthesis and mention both subjects clearly.\n"
                "Do not invent statistics or perform math not supported by the fact sheets.\n"
                "If the answer cannot be determined from the sources, say:\n"
                "\"I don't have enough information in the current context to answer that.\"\n\n"
                f"Question:\n{question}\n\n"
                f"Sources:\n{context_text}\n"
            )

        # Default to a conservative RAG prompt for match and fallback answers.
        return (
            "You are a cricket expert assistant grounded strictly in the provided context.\n\n"
            "RULES — follow these exactly:\n"
            "1. Answer ONLY using facts that appear in the context below.\n"
            "2. If the player or match mentioned in the question does not appear in the context, "
            "say: \"I don't have data for that player or match in the current index.\" "
            "Do NOT invent stats, scores, or match details.\n"
            "3. Do NOT cite external sources such as ESPN Cricinfo or CricketArchive. "
            "Your only sources are the context chunks provided.\n"
            "4. Give the direct answer first, then a brief natural explanation. "
            "Write like a knowledgeable cricket analyst, not a generic chatbot.\n"
            "5. Keep the answer concise — 2 to 4 sentences unless the question needs more detail.\n\n"
            f"Question:\n{question}\n\n"
            f"Context:\n{context_text}\n"
        )

