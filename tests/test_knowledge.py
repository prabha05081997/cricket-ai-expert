from app.knowledge.service import KnowledgeService, looks_like_knowledge_question


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


def test_looks_like_knowledge_question() -> None:
    assert looks_like_knowledge_question("What is LBW in cricket?")
    assert looks_like_knowledge_question("Explain the difference between a no-ball and a wide")
    assert not looks_like_knowledge_question("Who won the 2011 World Cup final?")
