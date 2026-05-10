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


def test_knowledge_service_answers_lbw_question() -> None:
    service = KnowledgeService()
    result = service.answer("What is LBW in cricket?")

    assert result is not None
    assert "ball would have gone on to hit the stumps" in result["answer"].lower()


def test_knowledge_service_answers_ipl_question() -> None:
    service = KnowledgeService()
    result = service.answer("What is the IPL?")

    assert result is not None
    assert "player auction" in result["answer"].lower()


def test_knowledge_service_answers_first_test_question() -> None:
    service = KnowledgeService()
    result = service.answer("When was the first Test match played?")

    assert result is not None
    assert "1877" in result["answer"] or "melbourne" in result["answer"].lower()


def test_knowledge_service_answers_player_profile_question() -> None:
    service = KnowledgeService()
    result = service.answer("Who is Virat Kohli?")

    assert result is not None
    assert "prolific run scorer" in result["answer"].lower()
