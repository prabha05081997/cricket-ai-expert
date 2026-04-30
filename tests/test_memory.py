from app.chat.memory import resolve_follow_up_question, update_conversation_state


def test_resolve_follow_up_question_uses_match_and_player_context() -> None:
    state = {
        "last_match_label": "India vs Australia on 2024-01-01",
        "last_player_name": "Virat Kohli",
    }

    rewritten, explicit_player = resolve_follow_up_question(
        "What about in the chase?",
        state,
        player_resolver=lambda _: None,
    )

    assert explicit_player is None
    assert "India vs Australia on 2024-01-01" in rewritten
    assert "Virat Kohli" in rewritten


def test_update_conversation_state_picks_display_name_and_match_label() -> None:
    state = {}
    response = {
        "answer": "Sample",
        "sources": [
            {
                "display_name": "Rohit Sharma",
                "teams": "India, Sri Lanka",
                "date": "2014-11-13",
                "title": "Analytics record result",
            }
        ],
    }

    next_state = update_conversation_state(state, "question", response)

    assert next_state["last_player_name"] == "Rohit Sharma"
    assert next_state["last_match_label"] == "India, Sri Lanka on 2014-11-13"
