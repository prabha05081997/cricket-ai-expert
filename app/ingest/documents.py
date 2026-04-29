from __future__ import annotations

from collections import defaultdict

from app.domain import Document, MatchRecord


def build_documents(match: MatchRecord) -> list[Document]:
    documents: list[Document] = []
    documents.append(_build_match_summary(match))

    for innings_index, innings in enumerate(match.innings, start=1):
        documents.append(_build_innings_summary(match, innings, innings_index))

    player_rollup: dict[str, dict[str, object]] = defaultdict(lambda: {"batting": [], "bowling": []})
    for innings in match.innings:
        for batting in innings.get("batting", []):
            player_rollup[batting["player"]]["batting"].append((innings["team"], batting))
        for bowling in innings.get("bowling", []):
            player_rollup[bowling["player"]]["bowling"].append((innings["team"], bowling))

    for player_name, stats in sorted(player_rollup.items()):
        documents.append(_build_player_summary(match, player_name, stats))

    return documents


def _base_metadata(match: MatchRecord) -> dict[str, object]:
    return {
        "match_id": match.match_id,
        "date": match.date,
        "teams": match.teams,
        "players": match.player_of_match,
        "venue": match.venue,
        "match_type": match.match_type,
        "event_name": match.event_name,
        "source_file": match.source_file,
    }


def _build_match_summary(match: MatchRecord) -> Document:
    innings_lines = []
    for innings in match.innings:
        innings_lines.append(
            f"{innings['team']}: {innings['runs']}/{innings['wickets']}"
        )

    text = "\n".join(
        line
        for line in [
            f"Match: {' vs '.join(match.teams)}",
            f"Date: {match.date or 'unknown'}",
            f"Format: {match.match_type or 'unknown'}",
            f"Event: {match.event_name or 'unknown'}",
            f"Venue: {match.venue or 'unknown'}",
            f"City: {match.city or 'unknown'}",
            (
                f"Toss: {match.toss_winner} chose to {match.toss_decision}"
                if match.toss_winner and match.toss_decision
                else "Toss: unknown"
            ),
            f"Outcome: {match.outcome or 'unknown'}",
            (
                "Player of the match: " + ", ".join(match.player_of_match)
                if match.player_of_match
                else "Player of the match: unavailable"
            ),
            "Scoreline: " + "; ".join(innings_lines) if innings_lines else "Scoreline: unavailable",
        ]
        if line
    )

    return Document(
        doc_id=f"{match.match_id}:match-summary",
        match_id=match.match_id,
        title=f"Match summary for {' vs '.join(match.teams)}",
        document_type="match_summary",
        content=text,
        metadata=_base_metadata(match),
    )


def _build_innings_summary(match: MatchRecord, innings: dict[str, object], innings_index: int) -> Document:
    top_batters = ", ".join(
        f"{entry['player']} {entry['runs']}({entry['balls']})"
        for entry in innings.get("batting", [])[:3]
    ) or "No batting summary available"
    top_bowlers = ", ".join(
        f"{entry['player']} {entry['wickets']}/{entry['runs_conceded']}"
        for entry in innings.get("bowling", [])[:3]
    ) or "No bowling summary available"
    wickets = "; ".join(innings.get("wicket_events", [])[:6]) or "No wicket timeline available"

    text = "\n".join(
        [
            f"Innings {innings_index}: {innings.get('team', 'unknown team')}",
            f"Total: {innings.get('runs', 0)}/{innings.get('wickets', 0)}",
            f"Top batters: {top_batters}",
            f"Top bowlers: {top_bowlers}",
            f"Wicket timeline: {wickets}",
        ]
    )

    metadata = _base_metadata(match)
    metadata.update({"document_type": "innings_summary", "innings_number": innings_index, "innings_team": innings.get("team")})
    return Document(
        doc_id=f"{match.match_id}:innings:{innings_index}",
        match_id=match.match_id,
        title=f"Innings {innings_index} summary for {innings.get('team', 'unknown team')}",
        document_type="innings_summary",
        content=text,
        metadata=metadata,
    )


def _build_player_summary(match: MatchRecord, player_name: str, stats: dict[str, object]) -> Document:
    batting_lines = [
        f"for {team}: {entry['runs']} runs off {entry['balls']} balls with {entry['fours']} fours and {entry['sixes']} sixes"
        for team, entry in stats["batting"]
    ]
    bowling_lines = [
        f"for {team}: {entry['wickets']} wickets for {entry['runs_conceded']} runs from {entry['balls']} balls"
        for team, entry in stats["bowling"]
    ]
    text = "\n".join(
        [
            f"Player: {player_name}",
            f"Match: {' vs '.join(match.teams)} on {match.date or 'unknown date'}",
            "Batting: " + ("; ".join(batting_lines) if batting_lines else "Did not bat or no batting record captured"),
            "Bowling: " + ("; ".join(bowling_lines) if bowling_lines else "Did not bowl or no bowling record captured"),
            f"Outcome context: {match.outcome or 'unknown'}",
        ]
    )
    metadata = _base_metadata(match)
    metadata.update({"document_type": "player_performance", "player_name": player_name})
    return Document(
        doc_id=f"{match.match_id}:player:{player_name}",
        match_id=match.match_id,
        title=f"Player performance for {player_name}",
        document_type="player_performance",
        content=text,
        metadata=metadata,
    )
