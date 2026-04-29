from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import orjson

from app.domain import MatchRecord


def compute_file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_match_file(path: Path) -> MatchRecord:
    payload = orjson.loads(path.read_bytes())
    info = payload.get("info", {})
    dates = info.get("dates") or []
    teams = [team for team in info.get("teams", []) if team]
    toss = info.get("toss") or {}
    outcome = _format_outcome(info.get("outcome") or {})
    player_of_match = info.get("player_of_match") or []

    match_id = (
        info.get("match_id")
        or info.get("registry", {}).get("people", {}).get("match_id")
        or path.stem
    )

    innings = []
    for innings_entry in payload.get("innings", []):
        innings.append(_normalize_innings(innings_entry))

    return MatchRecord(
        match_id=str(match_id),
        source_file=str(path),
        date=str(dates[0]) if dates else None,
        teams=teams,
        gender=info.get("gender"),
        match_type=info.get("match_type"),
        event_name=(info.get("event") or {}).get("name"),
        venue=info.get("venue"),
        city=info.get("city"),
        toss_winner=toss.get("winner"),
        toss_decision=toss.get("decision"),
        outcome=outcome,
        player_of_match=player_of_match,
        innings=innings,
    )


def _normalize_innings(innings_entry: dict[str, Any]) -> dict[str, Any]:
    if "team" in innings_entry and "overs" in innings_entry:
        raw_innings = innings_entry
    elif len(innings_entry) == 1:
        (_, raw_innings), = innings_entry.items()
    else:
        raise ValueError(f"Unsupported innings structure with keys: {list(innings_entry.keys())}")

    team = raw_innings.get("team")
    overs = raw_innings.get("overs") or []

    runs = 0
    wickets = 0
    batting: dict[str, dict[str, Any]] = {}
    bowling: dict[str, dict[str, Any]] = {}
    wicket_events: list[str] = []

    for over in overs:
        over_number = over.get("over")
        for delivery in over.get("deliveries", []):
            batter = delivery.get("batter")
            bowler = delivery.get("bowler")
            runs_block = delivery.get("runs") or {}
            batter_runs = int(runs_block.get("batter", 0))
            total_runs = int(runs_block.get("total", 0))
            runs += total_runs

            if batter:
                entry = batting.setdefault(
                    batter,
                    {"player": batter, "runs": 0, "balls": 0, "fours": 0, "sixes": 0},
                )
                entry["runs"] += batter_runs
                entry["balls"] += 1
                if batter_runs == 4:
                    entry["fours"] += 1
                elif batter_runs == 6:
                    entry["sixes"] += 1

            if bowler:
                entry = bowling.setdefault(
                    bowler,
                    {"player": bowler, "runs_conceded": 0, "balls": 0, "wickets": 0},
                )
                entry["runs_conceded"] += total_runs
                entry["balls"] += 1

            wickets_block = delivery.get("wickets") or []
            for wicket in wickets_block:
                wickets += 1
                player_out = wicket.get("player_out", "unknown batter")
                kind = wicket.get("kind", "dismissed")
                wicket_events.append(f"Over {over_number}: {player_out} {kind}")
                if bowler and kind not in {"run out", "retired hurt", "retired out", "obstructing the field"}:
                    bowling[bowler]["wickets"] += 1

    return {
        "team": team,
        "runs": runs,
        "wickets": wickets,
        "batting": sorted(batting.values(), key=lambda item: (-item["runs"], item["player"])),
        "bowling": sorted(bowling.values(), key=lambda item: (-item["wickets"], item["player"])),
        "wicket_events": wicket_events[:12],
    }


def _format_outcome(outcome: dict[str, Any]) -> str | None:
    if not outcome:
        return None
    winner = outcome.get("winner")
    by = outcome.get("by") or {}
    if winner and by.get("runs") is not None:
        return f"{winner} won by {by['runs']} runs"
    if winner and by.get("wickets") is not None:
        return f"{winner} won by {by['wickets']} wickets"
    if winner:
        return f"{winner} won"
    if outcome.get("result"):
        return str(outcome["result"])
    return None
