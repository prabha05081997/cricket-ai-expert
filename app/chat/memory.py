from __future__ import annotations

import copy
from typing import Callable


ConversationState = dict[str, str]


def resolve_follow_up_question(
    question: str,
    state: ConversationState | None,
    player_resolver: Callable[[str], str | None] | None = None,
) -> tuple[str, str | None]:
    if not state:
        return question, None

    lowered = question.lower()
    explicit_player = player_resolver(question) if player_resolver is not None else None
    match_context_needed = any(
        marker in lowered
        for marker in [
            "that match",
            "that game",
            "that innings",
            "there",
            "in the chase",
            "during the chase",
            "in the final overs",
            "in that game",
            "in that match",
            "what happened then",
        ]
    )
    player_context_needed = any(
        marker in lowered
        for marker in [
            "what about",
            "how about",
            "his",
            "him",
            "he ",
            "that player",
            "that batter",
            "that bowler",
            "did he",
        ]
    )

    prefixes: list[str] = []
    if match_context_needed and state.get("last_match_label"):
        prefixes.append(f"In the match {state['last_match_label']}")
    if player_context_needed and not explicit_player and state.get("last_player_name"):
        prefixes.append(f"regarding {state['last_player_name']}")

    if not prefixes:
        return question, explicit_player

    rewritten = ", ".join(prefixes) + ", " + question
    return rewritten, explicit_player


def update_conversation_state(
    state: ConversationState | None,
    question: str,
    response: dict[str, object],
    explicit_player_name: str | None = None,
) -> ConversationState:
    next_state: ConversationState = copy.deepcopy(state or {})
    next_state["last_question"] = question

    if explicit_player_name:
        next_state["last_player_name"] = explicit_player_name

    sources = response.get("sources") or []
    if not isinstance(sources, list) or not sources:
        return next_state

    primary_source = sources[0]
    if not isinstance(primary_source, dict):
        return next_state

    player_name = primary_source.get("display_name") or primary_source.get("player_name")
    if isinstance(player_name, str) and player_name.strip():
        next_state["last_player_name"] = player_name.strip()

    teams = primary_source.get("teams")
    date = primary_source.get("date")
    if isinstance(teams, str) and teams.strip():
        if isinstance(date, str) and date.strip():
            next_state["last_match_label"] = f"{teams} on {date}"
        else:
            next_state["last_match_label"] = teams
    elif primary_source.get("title") and primary_source.get("match_id"):
        next_state["last_match_label"] = str(primary_source["title"])

    title = primary_source.get("title")
    if isinstance(title, str) and title.strip():
        next_state["last_topic"] = title.strip()

    return next_state

