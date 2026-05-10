from app.chat.memory import update_conversation_state


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
                "match_id": "12345",
            }
        ],
    }

    next_state = update_conversation_state(state, "question", response)

    assert next_state["last_player_name"] == "Rohit Sharma"
    assert next_state["last_match_label"] == "India, Sri Lanka on 2014-11-13"
    assert next_state["last_match_id"] == "12345"


def test_update_conversation_state_preserves_existing_state() -> None:
    state = {"last_player_name": "Virat Kohli", "last_match_id": "old-match"}
    response = {"answer": "Sample", "sources": []}

    next_state = update_conversation_state(state, "new question", response)

    # No sources → existing state preserved
    assert next_state["last_player_name"] == "Virat Kohli"
    assert next_state["last_match_id"] == "old-match"
    assert next_state["last_question"] == "new question"
