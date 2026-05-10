from __future__ import annotations

from app.rag.llm import OllamaClient


class DummyResponse:
    def __init__(self, response: str) -> None:
        self._response = response

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, str]:
        return {"response": self._response}


def test_generate_answer_uses_match_narrative_prompt(monkeypatch):
    captured: dict[str, str] = {}

    def fake_post(url: str, json: dict, timeout: float):
        captured["prompt"] = json["prompt"]
        return DummyResponse("match narrative")

    monkeypatch.setattr("app.rag.llm.httpx.post", fake_post)
    client = OllamaClient("http://localhost", "test-model")
    result = client.generate_answer(
        "Who won the 2019 World Cup final?",
        [{"title": "Match summary", "text": "India beat England."}],
        route="match_narrative",
    )

    assert result == "match narrative"
    assert "cricket commentator" in captured["prompt"]
    assert "Do not invent match details" in captured["prompt"]


def test_generate_answer_uses_rag_fallback_prompt(monkeypatch):
    captured: dict[str, str] = {}

    def fake_post(url: str, json: dict, timeout: float):
        captured["prompt"] = json["prompt"]
        return DummyResponse("fallback answer")

    monkeypatch.setattr("app.rag.llm.httpx.post", fake_post)
    client = OllamaClient("http://localhost", "test-model")
    result = client.generate_answer(
        "Who scored the most runs in that match?",
        [{"title": "Match facts", "text": "Player X scored 120."}],
        route="rag_fallback",
    )

    assert result == "fallback answer"
    assert "I don't have data for that player or match in the current index." in captured["prompt"]


def test_generate_answer_uses_analytics_prompt(monkeypatch):
    captured: dict[str, str] = {}

    def fake_post(url: str, json: dict, timeout: float):
        captured["prompt"] = json["prompt"]
        return DummyResponse("analytics answer")

    monkeypatch.setattr("app.rag.llm.httpx.post", fake_post)
    client = OllamaClient("http://localhost", "test-model")
    result = client.generate_answer(
        "What is his career average?",
        [{"title": "Stats fact sheet", "text": "Average: 45.7."}],
        route="analytics",
    )

    assert result == "analytics answer"
    assert "cricket stats assistant" in captured["prompt"]
    assert "Answer the question using only the provided fact sheet." in captured["prompt"]
