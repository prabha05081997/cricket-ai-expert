from __future__ import annotations

from typing import Any

import httpx


class OllamaClient:
    def __init__(self, base_url: str, model: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.model = model

    def generate_answer(self, question: str, context_chunks: list[dict[str, Any]]) -> str:
        context_text = "\n\n".join(
            f"Source {index + 1} [{chunk.get('title', 'untitled')}]:\n{chunk['text']}"
            for index, chunk in enumerate(context_chunks)
        )
        prompt = (
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
        response = httpx.post(
            f"{self.base_url}/api/generate",
            json={"model": self.model, "prompt": prompt, "stream": False},
            timeout=120.0,
        )
        response.raise_for_status()
        payload = response.json()
        return str(payload.get("response", "")).strip()

