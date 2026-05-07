"""Analytics layer tests.

These tests exercise the analytics query functions directly, bypassing the
LLM intent classifier.  This keeps the tests fast and deterministic.
"""
import sqlite3
from pathlib import Path

from app.analytics.players import sync_match_players
from app.analytics.stats import (
    AggregateQuery,
    DismissalQuery,
    PlayerMatchQuery,
    _answer_dismissal_question,
    _answer_player_match_question,
    sync_match_analytics,
)
from app.domain import MatchRecord
from app.ingest.registry import Registry


def _make_connection(tmp_path: Path) -> sqlite3.Connection:
    db_path = tmp_path / "registry.sqlite3"
    Registry(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _world_cup_final() -> MatchRecord:
    return MatchRecord(
        match_id="final-2011",
        source_file="/tmp/final-2011.json",
        date="2011-04-02",
        teams=["India", "Sri Lanka"],
        gender="male",
        match_type="ODI",
        event_name="ICC Cricket World Cup",
        venue="Wankhede Stadium",
        city="Mumbai",
        toss_winner="Sri Lanka",
        toss_decision="bat",
        outcome="India won by 6 wickets",
        player_of_match=["MS Dhoni"],
        innings=[
            {
                "team": "Sri Lanka",
                "runs": 274,
                "wickets": 6,
                "batting": [{"player": "DPMD Jayawardene", "runs": 103, "balls": 90, "fours": 13, "sixes": 0}],
                "bowling": [{"player": "Yuvraj Singh", "runs_conceded": 49, "balls": 60, "wickets": 2}],
                "wicket_events": [],
                "dismissals": [
                    {"batter": "DPMD Jayawardene", "bowler": "Yuvraj Singh", "kind": "caught", "over": 47},
                ],
            },
            {
                "team": "India",
                "runs": 277,
                "wickets": 4,
                "batting": [
                    {"player": "MS Dhoni", "runs": 91, "balls": 80, "fours": 8, "sixes": 2},
                    {"player": "V Kohli", "runs": 35, "balls": 52, "fours": 4, "sixes": 0},
                ],
                "bowling": [{"player": "V Kohli", "runs_conceded": 6, "balls": 6, "wickets": 0}],
                "wicket_events": [],
                "dismissals": [
                    {"batter": "V Kohli", "bowler": "TM Dilshan", "kind": "caught and bowled", "over": 21},
                ],
            },
        ],
    )


def test_highest_individual_score(tmp_path: Path) -> None:
    conn = _make_connection(tmp_path)
    match = MatchRecord(
        match_id="match-1",
        source_file="/tmp/match-1.json",
        date="2024-01-01",
        teams=["India", "Australia"],
        gender="male",
        match_type="ODI",
        event_name="Sample ODI",
        venue="Wankhede Stadium",
        city="Mumbai",
        toss_winner="India",
        toss_decision="bat",
        outcome="India won by 20 runs",
        player_of_match=["Virat Kohli"],
        innings=[
            {
                "team": "India",
                "runs": 300,
                "wickets": 6,
                "batting": [{"player": "Virat Kohli", "runs": 150, "balls": 120, "fours": 12, "sixes": 2}],
                "bowling": [{"player": "Mitchell Starc", "runs_conceded": 62, "balls": 60, "wickets": 2}],
                "wicket_events": [],
                "dismissals": [],
            },
        ],
    )
    sync_match_players(conn, match)
    sync_match_analytics(conn, match)
    conn.commit()

    from app.analytics.stats import AnalyticsQueryService, _query_highest_individual_score
    parsed = AggregateQuery(metric="highest_individual_score", match_type="ODI")
    row = _query_highest_individual_score(conn, parsed)
    assert row is not None
    assert int(row["runs"]) == 150


def test_most_wickets_with_format_filter(tmp_path: Path) -> None:
    conn = _make_connection(tmp_path)
    for i, wickets in enumerate([3, 4], start=1):
        match = MatchRecord(
            match_id=f"match-{i}",
            source_file=f"/tmp/match-{i}.json",
            date=f"2024-0{i}-01",
            teams=["India", "Australia"],
            gender="male",
            match_type="T20I",
            event_name="Series",
            venue="SCG",
            city="Sydney",
            toss_winner="India",
            toss_decision="field",
            outcome="India won",
            player_of_match=["Jasprit Bumrah"],
            innings=[{
                "team": "Australia",
                "runs": 160,
                "wickets": wickets,
                "batting": [{"player": "David Warner", "runs": 60, "balls": 40, "fours": 6, "sixes": 1}],
                "bowling": [{"player": "Jasprit Bumrah", "runs_conceded": 24, "balls": 24, "wickets": wickets}],
                "wicket_events": [],
                "dismissals": [],
            }],
        )
        sync_match_players(conn, match)
        sync_match_analytics(conn, match)
    conn.commit()

    from app.analytics.stats import _query_most_wickets
    parsed = AggregateQuery(metric="most_wickets", match_type="T20I")
    row = _query_most_wickets(conn, parsed)
    assert row is not None
    assert "Bumrah" in str(row["player_name"])
    assert int(row["total_wickets"]) == 7


def test_player_match_query_by_match_id(tmp_path: Path) -> None:
    conn = _make_connection(tmp_path)
    match = _world_cup_final()
    sync_match_players(conn, match)
    sync_match_analytics(conn, match)
    conn.commit()

    # Query by exact match_id — should find Kohli's 35 runs
    pmq = PlayerMatchQuery(player_name="V Kohli", match_id="final-2011")
    result = _answer_player_match_question(conn, pmq)
    assert result is not None
    assert "35 runs" in result["answer"]
    assert "52 balls" in result["answer"]


def test_dismissal_query_by_match_id(tmp_path: Path) -> None:
    conn = _make_connection(tmp_path)
    match = _world_cup_final()
    sync_match_players(conn, match)
    sync_match_analytics(conn, match)
    conn.commit()

    dq = DismissalQuery(batter_name="V Kohli", match_id="final-2011")
    result = _answer_dismissal_question(conn, dq)
    assert result is not None
    assert "Dilshan" in result["answer"]
    assert "caught and bowled" in result["answer"]


def test_venue_scope_in_filter_suffix(tmp_path: Path) -> None:
    conn = _make_connection(tmp_path)
    match = MatchRecord(
        match_id="match-1",
        source_file="/tmp/match-1.json",
        date="2024-01-01",
        teams=["Middlesex", "Surrey"],
        gender="male",
        match_type="MDM",
        event_name="County Championship",
        venue="Lord's",
        city="London",
        toss_winner="Middlesex",
        toss_decision="field",
        outcome="Middlesex won",
        player_of_match=[],
        innings=[{
            "team": "Surrey",
            "runs": 220,
            "wickets": 10,
            "batting": [{"player": "Player A", "runs": 55, "balls": 88, "fours": 4, "sixes": 1}],
            "bowling": [{"player": "TS Roland-Jones", "runs_conceded": 38, "balls": 90, "wickets": 6}],
            "wicket_events": [],
            "dismissals": [],
        }],
    )
    sync_match_players(conn, match)
    sync_match_analytics(conn, match)
    conn.commit()

    from app.analytics.stats import AnalyticsQueryService
    svc = AnalyticsQueryService(conn.execute("PRAGMA database_list").fetchone()[2])
    parsed = AggregateQuery(metric="most_wickets", venue="Lord's")
    result = svc._answer_aggregate(conn, parsed)
    assert result is not None
    assert "across all recorded matches at Lord's" in result["answer"]
