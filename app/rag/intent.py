"""LLM-based intent classification and entity extraction.

Replaces the brittle string-matching rule engine in service.py and stats.py.
The LLM classifies the user's question into one of a small set of intents and
extracts the structured parameters needed to answer it.

Intent taxonomy
---------------
player_performance   – how a specific player batted/bowled in a specific match
                       e.g. "How did Kohli do in the 2011 WC final?"
player_dismissal     – how/by whom a player was dismissed
                       e.g. "Who took his wicket?", "How was he out?"
aggregate_stats      – cross-match records / leaderboards
                       e.g. "Who has the most ODI wickets?", "Highest T20I score?"
match_narrative      – what happened in a match
                       e.g. "Who won the 2019 WC final?"
cricket_knowledge    – rules, formats, terminology
                       e.g. "What is DLS?", "Explain a powerplay"
general_chat         – anything else / out of scope

The LLM also resolves pronouns and follow-up references using the conversation
context we pass in, so memory.py no longer needs to do brittle rewriting.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema returned by the LLM
# ---------------------------------------------------------------------------

INTENT_SCHEMA = """
{
  "intent": "<one of: player_performance | player_dismissal | aggregate_stats | match_narrative | cricket_knowledge | mixed | general_chat>",
  "player": "<full player name if mentioned or resolved from context, else null>",
  "player2": "<second player name if the question compares two players, else null>",
  "match_type": "<cricket FORMAT code only: ODI | Test | T20I | T20 | null — NOT a tournament name>",
  "event": "<tournament or series name if mentioned (e.g. ICC Cricket World Cup, IPL, Asia Cup), else null>",
  "year": <integer year if mentioned, else null>,
  "team": "<team name if mentioned, else null>",
  "venue": "<venue name if mentioned, else null>",
  "metric": "<runs | wickets | economy | strike_rate | average | batting_average | highest_score | best_bowling_figures | best_economy | null>",
  "rewritten_question": "<the question rewritten in full explicit form with all pronouns and references resolved using the conversation context>"
}
"""

_SYSTEM_PROMPT = """\
You are a cricket assistant. Classify the question into exactly one intent and extract entities.

INTENT DEFINITIONS:
- player_performance: asking how a specific player performed in a specific match or series
- player_dismissal: asking how/by whom a player was dismissed (got out)
- aggregate_stats: asking for records, leaderboards, totals across multiple matches (most runs, most wickets, highest score ever)
- match_narrative: asking what happened in a match, who won, match result
- cricket_knowledge: asking about rules, formats, terminology, methods (DLS, powerplay, LBW)
- general_chat: anything else

EXAMPLES:
- "most ODI wickets ever?" → aggregate_stats
- "who has the most runs in T20Is?" → aggregate_stats
- "highest individual score in Tests?" → aggregate_stats
- "how did Kohli do in the final?" → player_performance
- "who dismissed him?" → player_dismissal
- "who won the 2019 World Cup?" → match_narrative
- "what is DLS?" → cricket_knowledge
- "How does Kohli's WC record compare to Sachin's?" → mixed
- "Is a strike rate of 140 good in T20Is?" → mixed

Given a user question and optional conversation context, output ONLY a valid JSON object matching this schema:
{schema}

Rules:
- Resolve pronouns (he, him, his, she, her) using last_player_name from context.
- Resolve match references (that match, that game, there) using last_match_label from context.
- For player_dismissal intent: player = the batter who was dismissed.
- For aggregate_stats: metric = the stat being asked about.
- match_type must be a FORMAT code only: ODI, Test, T20I, or T20. Never a tournament name.
- rewritten_question must be a complete, self-contained question with all references resolved.
- Output ONLY the JSON object. No explanation, no markdown, no extra text.
""".format(schema=INTENT_SCHEMA)


_MATCH_SELECTION_PROMPT = """\
You are a cricket assistant. Given a user question and a list of candidate matches, \
return the match_id of the single best matching match.

User question: {question}

Candidate matches (sorted oldest to newest — match_id | date | teams | event):
{candidates}

Rules:
- Matches are listed oldest first. The LAST entry is the most recent match.
- The FINAL of a tournament is always the LAST match (most recent date). Pick the last entry if the question asks about a final.
- A semi-final is the second-to-last match.
- If the question mentions specific teams, prefer matches involving those teams.
- Return ONLY a JSON object: {{"match_id": "<id>"}}
- Output ONLY the JSON object. No explanation.
"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


# Valid CricSheet match type codes. Anything the LLM returns that isn't in
# this set is likely a tournament name placed in the wrong field.
_VALID_MATCH_TYPES = {"ODI", "ODM", "Test", "T20", "T20I", "IT20", "MDM"}


class IntentResult:
    """Parsed result from the LLM intent classifier."""

    __slots__ = (
        "intent",
        "player",
        "player2",
        "match_type",
        "event",
        "year",
        "team",
        "venue",
        "metric",
        "rewritten_question",
        "pinned_match_id",
        "raw",
    )

    def __init__(self, data: dict[str, Any], original_question: str) -> None:
        self.intent: str = str(data.get("intent") or "general_chat")
        self.player: str | None = _nonempty(data.get("player"))
        self.player2: str | None = _nonempty(data.get("player2"))
        self.match_type: str | None = _validated_match_type(data.get("match_type"))
        self.event: str | None = _nonempty(data.get("event"))
        self.year: int | None = int(data["year"]) if data.get("year") else None
        self.team: str | None = _nonempty(data.get("team"))
        self.venue: str | None = _nonempty(data.get("venue"))
        self.metric: str | None = _nonempty(data.get("metric"))
        self.rewritten_question: str = (
            _nonempty(data.get("rewritten_question")) or original_question
        )
        self.pinned_match_id: str | None = None  # set externally from conversation state
        self.raw = data

    def __repr__(self) -> str:
        return (
            f"IntentResult(intent={self.intent!r}, player={self.player!r}, "
            f"event={self.event!r}, year={self.year}, "
            f"rewritten={self.rewritten_question!r})"
        )


def classify_intent(
    question: str,
    ollama_base_url: str,
    ollama_model: str,
    conversation_state: dict[str, str] | None = None,
) -> IntentResult:
    """Call the LLM to classify intent and extract entities.

    Falls back to a safe default (general_chat) if the LLM call fails or
    returns unparseable output.
    """
    context_block = _format_context(conversation_state)
    user_message = f"Conversation context:\n{context_block}\n\nUser question: {question}"

    try:
        response = httpx.post(
            f"{ollama_base_url.rstrip('/')}/api/chat",
            json={
                "model": ollama_model,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                "stream": False,
                "options": {"temperature": 0},  # deterministic classification
            },
            timeout=30.0,
        )
        response.raise_for_status()
        payload = response.json()
        raw_text = str(payload.get("message", {}).get("content", "")).strip()
        data = _parse_json(raw_text)
        return IntentResult(data, question)

    except Exception as exc:
        logger.warning("Intent classification failed (%s), using fallback", exc)
        return _fallback_intent(question, conversation_state)


def select_match(
    question: str,
    candidates: list[dict[str, Any]],
    ollama_base_url: str,
    ollama_model: str,
) -> str | None:
    """Ask the LLM to pick the best matching match_id from a candidate list.

    ``candidates`` is a list of dicts with keys: match_id, date, teams, event.
    Returns the chosen match_id string, or None if the LLM can't decide.
    """
    if not candidates:
        return None
    if len(candidates) == 1:
        return str(candidates[0]["match_id"])

    candidate_lines = "\n".join(
        f"- match_id={c['match_id']} | {c['teams']} | {c['date']} | {c['event']}"
        for c in candidates
    )
    prompt = _MATCH_SELECTION_PROMPT.format(
        question=question,
        candidates=candidate_lines,
    )
    try:
        response = httpx.post(
            f"{ollama_base_url.rstrip('/')}/api/generate",
            json={
                "model": ollama_model,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0},
            },
            timeout=20.0,
        )
        response.raise_for_status()
        raw_text = str(response.json().get("response", "")).strip()
        data = _parse_json(raw_text)
        return _nonempty(data.get("match_id"))
    except Exception as exc:
        logger.warning("Match selection failed (%s), using first candidate", exc)
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _format_context(state: dict[str, str] | None) -> str:
    if not state:
        return "None"
    parts = []
    if state.get("last_player_name"):
        parts.append(f"last_player_name: {state['last_player_name']}")
    if state.get("last_match_label"):
        parts.append(f"last_match_label: {state['last_match_label']}")
    if state.get("last_match_id"):
        parts.append(f"last_match_id: {state['last_match_id']}")
    if state.get("last_question"):
        parts.append(f"last_question: {state['last_question']}")
    return "\n".join(parts) if parts else "None"


def _parse_json(text: str) -> dict[str, Any]:
    """Extract and parse the first JSON object from the LLM response."""
    # Strip markdown code fences if present
    text = re.sub(r"```(?:json)?", "", text).strip()
    # Find the first { ... } block
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return json.loads(match.group(0))
    return {}


def _nonempty(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s and s.lower() not in {"null", "none", ""} else None


def _validated_match_type(value: Any) -> str | None:
    """Return the match type only if it's a known CricSheet format code.

    The LLM sometimes puts tournament names (e.g. "ICC Cricket World Cup")
    into the match_type field.  We discard anything that isn't a real format.
    """
    s = _nonempty(value)
    if s is None:
        return None
    # Normalise common aliases the LLM might use
    normalised = s.upper().replace(" ", "").replace("-", "")
    alias_map = {
        "T20INTERNATIONAL": "T20I",
        "TWENTYTWENTYINTERNATIONAL": "T20I",
        "ONEDAYINTERNATIONAL": "ODI",
        "TESTMATCH": "Test",
        "TEST": "Test",
        "T20I": "T20I",
        "IT20": "IT20",
        "T20": "T20",
        "ODI": "ODI",
        "ODM": "ODM",
        "MDM": "MDM",
    }
    resolved = alias_map.get(normalised)
    if resolved:
        return resolved
    # If the raw value is directly in the valid set (case-insensitive), use it
    for valid in _VALID_MATCH_TYPES:
        if s.upper() == valid.upper():
            return valid
    # Not a valid format code — discard it
    return None


def _fallback_intent(
    question: str,
    state: dict[str, str] | None,
) -> IntentResult:
    """Minimal keyword fallback used only when the LLM call itself fails."""
    lowered = question.lower()

    # Resolve pronouns from state if available
    player: str | None = None
    if state:
        last_player = state.get("last_player_name")
        if last_player and any(p in lowered for p in ["his", "him", "he ", "her", "she "]):
            player = last_player

    # Rough intent detection — intentionally minimal, just enough to not crash
    if any(w in lowered for w in ["dismissed", "wicket", "bowled", "caught", "how was", "how did he", "how did she"]):
        intent = "player_dismissal"
    elif any(w in lowered for w in ["how did", "how has", "what did", "performance", "score"]):
        intent = "player_performance"
    elif any(w in lowered for w in ["most", "highest", "best", "record", "most runs", "most wickets"]):
        intent = "aggregate_stats"
    elif any(w in lowered for w in ["compare", "better than", "as good as", "versus", " vs ", "compare to"]):
        intent = "mixed"
    elif any(w in lowered for w in ["what is", "explain", "what does", "how does", "difference between"]):
        intent = "cricket_knowledge"
    elif any(w in lowered for w in ["who won", "what happened", "result", "match"]):
        intent = "match_narrative"
    else:
        intent = "general_chat"

    # Rewrite using context if we have it
    rewritten = question
    if state and state.get("last_player_name"):
        for pronoun in ["his", "him", "he ", "her", "she "]:
            if pronoun in lowered:
                rewritten = f"Regarding {state['last_player_name']}: {question}"
                break

    return IntentResult(
        {
            "intent": intent,
            "player": player,
            "player2": None,
            "match_type": None,
            "event": None,
            "year": None,
            "team": None,
            "venue": None,
            "metric": None,
            "rewritten_question": rewritten,
        },
        question,
    )
