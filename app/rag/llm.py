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
            "You are a cricket expert assistant. Answer only from the provided context. "
            "If the context is insufficient, say so clearly. Give the direct answer first, "
            "then a short explanation, then a short Sources section.\n\n"
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

