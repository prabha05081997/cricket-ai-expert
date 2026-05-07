"""Cricket knowledge base service.

Retrieves answers from the curated static knowledge base (rules, formats,
terminology).  Routing to this service is done by the LLM intent classifier
(intent == "cricket_knowledge") — this module only handles retrieval and
answer formatting.
"""

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
        """Score each knowledge item by token overlap with the question.

        This is retrieval over a small static dataset — not routing logic.
        The LLM has already decided this is a cricket_knowledge question;
        we just need to find the best matching entry.
        """
        query_tokens = _tokenize(question)
        if not query_tokens:
            return None

        best_item = None
        best_score = 0
        for item in KNOWLEDGE_ITEMS:
            score = sum(
                2
                for token in query_tokens
                for haystack in [
                    item["title"].lower(),
                    item["content"].lower(),
                    " ".join(item["topics"]).lower(),
                ]
                if token in haystack
            )
            # Boost the right powerplay variant when format is specified
            if item["id"].startswith("powerplay") and "powerplay" in query_tokens:
                if "odi" in query_tokens and item["id"] == "powerplay_odi":
                    score += 5
                if ("t20" in query_tokens or "t20i" in query_tokens) and item["id"] == "powerplay_t20":
                    score += 5
            if score > best_score:
                best_item = item
                best_score = score

        return best_item if best_score > 0 else None


def _tokenize(text: str) -> list[str]:
    """Tokenize text into lowercase words, removing common stopwords."""
    stopwords = {"what", "is", "the", "a", "an", "of", "in", "does", "how", "between", "explain"}
    return [
        token for token in re.findall(r"[a-zA-Z0-9]+", text.lower())
        if token not in stopwords
    ]
