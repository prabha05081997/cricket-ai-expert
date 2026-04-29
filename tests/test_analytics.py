import sqlite3
from pathlib import Path

from app.analytics.players import sync_match_players
from app.analytics.stats import AnalyticsQueryService, sync_match_analytics
from app.domain import MatchRecord
from app.ingest.registry import Registry


def test_analytics_service_answers_highest_individual_score(tmp_path: Path) -> None:
    db_path = tmp_path / "registry.sqlite3"
    Registry(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

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
            },
            {
                "team": "Australia",
                "runs": 280,
                "wickets": 10,
                "batting": [{"player": "Steve Smith", "runs": 99, "balls": 95, "fours": 8, "sixes": 1}],
                "bowling": [{"player": "Jasprit Bumrah", "runs_conceded": 48, "balls": 60, "wickets": 4}],
                "wicket_events": [],
            },
        ],
    )

    sync_match_players(connection, match)
    sync_match_analytics(connection, match)
    connection.commit()

    service = AnalyticsQueryService(db_path)
    result = service.answer("What is the highest individual score in ODI cricket?")

    assert result is not None
    assert "150 by Virat Kohli" in str(result["answer"])


def test_analytics_service_answers_most_wickets_with_format_filter(tmp_path: Path) -> None:
    db_path = tmp_path / "registry.sqlite3"
    Registry(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    match_one = MatchRecord(
        match_id="match-1",
        source_file="/tmp/match-1.json",
        date="2024-01-01",
        teams=["India", "Australia"],
        gender="male",
        match_type="T20I",
        event_name="Series A",
        venue="SCG",
        city="Sydney",
        toss_winner="India",
        toss_decision="field",
        outcome="India won by 3 wickets",
        player_of_match=["Jasprit Bumrah"],
        innings=[
            {
                "team": "Australia",
                "runs": 165,
                "wickets": 7,
                "batting": [{"player": "David Warner", "runs": 70, "balls": 45, "fours": 7, "sixes": 2}],
                "bowling": [{"player": "Jasprit Bumrah", "runs_conceded": 24, "balls": 24, "wickets": 3}],
                "wicket_events": [],
            }
        ],
    )
    match_two = MatchRecord(
        match_id="match-2",
        source_file="/tmp/match-2.json",
        date="2024-02-01",
        teams=["India", "England"],
        gender="male",
        match_type="T20I",
        event_name="Series B",
        venue="Lord's",
        city="London",
        toss_winner="England",
        toss_decision="bat",
        outcome="India won by 10 runs",
        player_of_match=["Jasprit Bumrah"],
        innings=[
            {
                "team": "England",
                "runs": 155,
                "wickets": 8,
                "batting": [{"player": "Jos Buttler", "runs": 48, "balls": 30, "fours": 4, "sixes": 2}],
                "bowling": [{"player": "Jasprit Bumrah", "runs_conceded": 20, "balls": 24, "wickets": 4}],
                "wicket_events": [],
            }
        ],
    )

    for match in [match_one, match_two]:
        sync_match_players(connection, match)
        sync_match_analytics(connection, match)
    connection.commit()

    service = AnalyticsQueryService(db_path)
    result = service.answer("Who has the most wickets in T20I?")

    assert result is not None
    assert "Jasprit Bumrah" in str(result["answer"])
    assert "7 wickets" in str(result["answer"])
