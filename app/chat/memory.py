"""Conversation state management.

Stores and retrieves context from the previous turn so the LLM classifier
can resolve pronouns and follow-up references.

The LLM (classify_intent) handles all pronoun resolution and question
rewriting.  This module's only job is to persist the structured state
between turns and update it after each answer.
"""

from __future__ import annotations

import copy


ConversationState = dict[str, str]


def update_conversation_state(
    state: ConversationState | None,
    question: str,
    response: dict[str, object],
    explicit_player_name: str | None = None,
) -> ConversationState:
    """Build the next conversation state from the current response."""
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

    # Store match_id directly so follow-up queries can pin to the exact match
    match_id = primary_source.get("match_id")
    if isinstance(match_id, str) and match_id.strip():
        next_state["last_match_id"] = match_id.strip()

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
