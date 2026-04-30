from __future__ import annotations

import re

from app.knowledge.data import KNOWLEDGE_ITEMS


class KnowledgeService:
    def answer(self, question: str) -> dict[str, object] | None:
        item = self.retrieve(question)
        if item is None:
            return None
        return {
            "answer": item["content"],
            "sources": [
                {
                    "chunk_id": f"knowledge:{item['id']}",
                    "score": 1.0,
                    "text": item["content"],
                    "title": item["title"],
                    "knowledge_id": item["id"],
                    "document_type": "knowledge_entry",
                    "match_id": "",
                    "date": "",
                    "match_type": "",
                    "event_name": "",
                    "venue": "",
                }
            ],
        }

    def retrieve(self, question: str) -> dict[str, str] | None:
        query_tokens = _normalize_tokens(question)
        if not query_tokens:
            return None

        best_item = None
        best_score = 0
        for item in KNOWLEDGE_ITEMS:
            score = 0
            haystacks = [
                item["title"].lower(),
                item["content"].lower(),
                " ".join(item["topics"]).lower(),
            ]
            for token in query_tokens:
                for haystack in haystacks:
                    if token in haystack:
                        score += 2
            if item["id"].startswith("powerplay") and "powerplay" in query_tokens:
                if "odi" in query_tokens and item["id"] == "powerplay_odi":
                    score += 5
                if ("t20" in query_tokens or "t20i" in query_tokens) and item["id"] == "powerplay_t20":
                    score += 5
            if score > best_score:
                best_item = item
                best_score = score

        return best_item if best_score > 0 else None


def looks_like_knowledge_question(question: str) -> bool:
    lowered = question.lower()
    markers = [
        "what is",
        "what does",
        "difference between",
        "how does",
        "explain",
        "meaning of",
        "what's a",
    ]
    cricket_terms = [
        "powerplay",
        "dls",
        "duckworth",
        "no-ball",
        "no ball",
        "wide",
        "lbw",
        "strike rate",
        "economy",
        "free hit",
        "super over",
        "odi",
        "test cricket",
        "t20i",
        "t20 cricket",
    ]
    return any(marker in lowered for marker in markers) and any(term in lowered for term in cricket_terms)


def _normalize_tokens(text: str) -> list[str]:
    raw = re.findall(r"[a-zA-Z0-9]+", text.lower())
    stopwords = {"what", "is", "the", "a", "an", "of", "in", "does", "how", "between", "explain"}
    return [token for token in raw if token not in stopwords]
