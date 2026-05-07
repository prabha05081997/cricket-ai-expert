from app.knowledge.service import KnowledgeService


def test_knowledge_service_answers_dls_question() -> None:
    service = KnowledgeService()
    result = service.answer("What is the Duckworth-Lewis-Stern method?")

    assert result is not None
    assert "reset targets" in result["answer"]


def test_knowledge_service_picks_odi_powerplay() -> None:
    service = KnowledgeService()
    result = service.answer("What is a powerplay in ODI cricket?")

    assert result is not None
    assert "first 10 overs" in result["answer"]


def test_knowledge_service_returns_none_for_non_cricket_question() -> None:
    service = KnowledgeService()
    # A question with no tokens matching any knowledge item should return None
    result = service.retrieve("xyzzy frobble wibble")
    assert result is None
