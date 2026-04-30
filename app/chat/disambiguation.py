"""Player disambiguation logic.

When a question references a player name that matches multiple candidates with
similar confidence scores, the assistant should ask the user to clarify rather
than silently picking one.

Typical ambiguous cases:
- "How did Sharma do?" → could be Rohit Sharma, Ishant Sharma, Mohammed Shami, ...
- "What about Kohli?" → usually unambiguous, but could theoretically match others
- "How did Kumar perform?" → very common surname, many candidates

The module exposes:
- ``detect_player_ambiguity`` – given a question and a DB connection, returns a
  ``DisambiguationRequest`` if the question is ambiguous, or ``None`` if it is
  clear (single confident match or no player reference at all).
- ``resolve_pending_disambiguation`` – given a user reply and a pending
  disambiguation stored in conversation state, returns the resolved player name
  or ``None`` if the reply doesn't match any candidate.
- ``build_clarification_response`` – builds the response dict the ChatService
  returns when disambiguation is needed.
"""

from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass

from app.analytics.players import PlayerCandidate, resolve_player_name


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Minimum score for a candidate to be considered a real match at all.
_MIN_CANDIDATE_SCORE = 40.0

# If the top candidate's score exceeds this threshold AND the gap to the second
# candidate is at least _CLEAR_WIN_GAP, we consider it unambiguous.
_CLEAR_WIN_SCORE = 90.0
_CLEAR_WIN_GAP = 15.0

# Maximum number of candidates to present to the user.
_MAX_DISAMBIGUATION_OPTIONS = 4


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class DisambiguationRequest:
    """Returned when a question is ambiguous and needs user clarification."""

    original_question: str
    player_fragment: str  # the raw text extracted from the question
    candidates: list[PlayerCandidate]  # ordered best-first, already filtered


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def detect_player_ambiguity(
    question: str,
    connection: sqlite3.Connection,
) -> DisambiguationRequest | None:
    """Return a DisambiguationRequest if the question has an ambiguous player
    reference, or None if it is clear or contains no player reference.
    """
    fragment = _extract_player_fragment(question)
    if fragment is None:
        return None

    candidates = resolve_player_name(connection, fragment, limit=_MAX_DISAMBIGUATION_OPTIONS + 2)
    # Filter out very weak matches
    candidates = [c for c in candidates if c.score >= _MIN_CANDIDATE_SCORE]
    if not candidates:
        return None

    top = candidates[0]

    # Single strong match → no ambiguity
    if len(candidates) == 1 and top.score >= _CLEAR_WIN_SCORE:
        return None

    # Top candidate wins clearly over the rest → no ambiguity
    if len(candidates) >= 2:
        gap = top.score - candidates[1].score
        if top.score >= _CLEAR_WIN_SCORE and gap >= _CLEAR_WIN_GAP:
            return None

    # Multiple plausible candidates → ambiguous
    return DisambiguationRequest(
        original_question=question,
        player_fragment=fragment,
        candidates=candidates[:_MAX_DISAMBIGUATION_OPTIONS],
    )


def resolve_pending_disambiguation(
    user_reply: str,
    pending: dict[str, object],
) -> str | None:
    """Try to match the user's reply against the stored candidate list.

    Returns the canonical name of the chosen player, or None if the reply
    doesn't clearly match any candidate.

    ``pending`` is the dict stored under ``"pending_disambiguation"`` in
    conversation state.  It has the shape::

        {
            "original_question": str,
            "player_fragment": str,
            "candidates": [
                {"player_id": int, "canonical_name": str, "matched_alias": str},
                ...
            ],
        }
    """
    candidates: list[dict[str, object]] = pending.get("candidates", [])  # type: ignore[assignment]
    if not candidates:
        return None

    reply_lower = user_reply.lower().strip()

    # Check if the reply is a number (e.g. "1", "2", "3")
    if re.fullmatch(r"[1-9]", reply_lower):
        index = int(reply_lower) - 1
        if 0 <= index < len(candidates):
            return str(candidates[index]["canonical_name"])
        return None

    # Try to match by name substring
    for candidate in candidates:
        canonical = str(candidate["canonical_name"]).lower()
        matched_alias = str(candidate["matched_alias"]).lower()
        if reply_lower in canonical or canonical in reply_lower:
            return str(candidate["canonical_name"])
        if reply_lower in matched_alias or matched_alias in reply_lower:
            return str(candidate["canonical_name"])
        # Last-name match
        last_name = canonical.split()[-1] if canonical.split() else ""
        if last_name and (reply_lower == last_name or reply_lower.endswith(last_name)):
            return str(candidate["canonical_name"])

    return None


def build_clarification_response(
    request: DisambiguationRequest,
    conversation_state: dict[str, str] | None,
) -> dict[str, object]:
    """Build the response dict the ChatService returns when disambiguation is
    needed.  The response has ``answer_type = "disambiguation"`` so the UI can
    render it specially.
    """
    lines = [
        f'I found a few players matching **"{request.player_fragment}"**. '
        "Which one did you mean?",
        "",
    ]
    for i, candidate in enumerate(request.candidates, start=1):
        display = candidate.canonical_name
        lines.append(f"{i}. {display}")

    lines += [
        "",
        "You can reply with a number or type the name.",
    ]
    answer_text = "\n".join(lines)

    # Serialise candidates for storage in conversation state
    serialised_candidates = [
        {
            "player_id": candidate.player_id,
            "canonical_name": candidate.canonical_name,
            "matched_alias": candidate.matched_alias,
        }
        for candidate in request.candidates
    ]

    import copy

    next_state: dict[str, str] = copy.deepcopy(conversation_state or {})
    # Store the pending disambiguation so the next turn can resolve it
    import json

    next_state["pending_disambiguation"] = json.dumps(
        {
            "original_question": request.original_question,
            "player_fragment": request.player_fragment,
            "candidates": serialised_candidates,
        }
    )

    return {
        "answer": answer_text,
        "answer_type": "disambiguation",
        "sources": [],
        "conversation_state": next_state,
        "disambiguation_candidates": [c.canonical_name for c in request.candidates],
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Patterns that suggest the question is about a specific player.
_PLAYER_QUESTION_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"how did\s+(.+?)\s+(?:do|perform)\b", re.IGNORECASE),
    re.compile(r"how has\s+(.+?)\s+(?:done|performed)\b", re.IGNORECASE),
    re.compile(r"what did\s+(.+?)\s+(?:do|score|take)\b", re.IGNORECASE),
    re.compile(r"what was\s+(.+?)['']s\s+(?:performance|score|contribution)\b", re.IGNORECASE),
    re.compile(r"what about\s+(.+?)(?:\?|$)", re.IGNORECASE),
    re.compile(r"how about\s+(.+?)(?:\?|$)", re.IGNORECASE),
    re.compile(r"(?:show|tell)\s+me\s+(?:about\s+)?(.+?)['']s\s+(?:stats|performance|record)\b", re.IGNORECASE),
]

# Short tokens that are almost certainly not player names
_NON_PLAYER_TOKENS = {
    "him", "he", "she", "her", "they", "them", "it", "that", "this",
    "the", "a", "an", "player", "batter", "bowler", "fielder",
}


def _extract_player_fragment(question: str) -> str | None:
    """Extract the player-name fragment from a question, or return None."""
    for pattern in _PLAYER_QUESTION_PATTERNS:
        match = pattern.search(question)
        if match:
            fragment = match.group(1).strip(" ,?")
            # Skip pronouns and generic nouns
            if fragment.lower() in _NON_PLAYER_TOKENS:
                return None
            # Skip very short fragments that are likely not names
            if len(fragment) < 3:
                return None
            return fragment
    return None
