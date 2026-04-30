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


def test_analytics_service_signals_venue_scope_for_all_matches(tmp_path: Path) -> None:
    db_path = tmp_path / "registry.sqlite3"
    Registry(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

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
        innings=[
            {
                "team": "Surrey",
                "runs": 220,
                "wickets": 10,
                "batting": [{"player": "Player A", "runs": 55, "balls": 88, "fours": 4, "sixes": 1}],
                "bowling": [{"player": "TS Roland-Jones", "runs_conceded": 38, "balls": 90, "wickets": 6}],
                "wicket_events": [],
            }
        ],
    )

    sync_match_players(connection, match)
    sync_match_analytics(connection, match)
    connection.commit()

    service = AnalyticsQueryService(db_path)
    result = service.answer("Who has the most wickets at Lord's?")

    assert result is not None
    assert "across all recorded matches at Lord's" in str(result["answer"])


def test_analytics_service_answers_player_performance_in_specific_final(tmp_path: Path) -> None:
    db_path = tmp_path / "registry.sqlite3"
    Registry(db_path)
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row

    final_match = MatchRecord(
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
            },
        ],
    )
    league_match = MatchRecord(
        match_id="league-2011",
        source_file="/tmp/league-2011.json",
        date="2011-02-27",
        teams=["India", "England"],
        gender="male",
        match_type="ODI",
        event_name="ICC Cricket World Cup",
        venue="M Chinnaswamy Stadium",
        city="Bengaluru",
        toss_winner="India",
        toss_decision="bat",
        outcome="Match tied",
        player_of_match=[],
        innings=[
            {
                "team": "India",
                "runs": 338,
                "wickets": 10,
                "batting": [{"player": "MS Dhoni", "runs": 31, "balls": 25, "fours": 3, "sixes": 1}],
                "bowling": [],
                "wicket_events": [],
            }
        ],
    )

    for match in [final_match, league_match]:
        sync_match_players(connection, match)
        sync_match_analytics(connection, match)
    connection.commit()

    service = AnalyticsQueryService(db_path)
    kohli_result = service.answer("How did Virat Kohli do in the 2011 World Cup final?")
    dhoni_result = service.answer("How did MS Dhoni do in the 2011 World Cup final?")

    assert kohli_result is not None
    assert "Virat Kohli scored 35 runs off 52 balls" in str(kohli_result["answer"])
    assert dhoni_result is not None
    assert "91 runs off 80 balls" in str(dhoni_result["answer"])
