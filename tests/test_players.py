import sqlite3
from pathlib import Path

from app.analytics.players import (
    generate_player_aliases,
    get_preferred_player_display_name,
    resolve_player_name,
    sync_external_player_directory,
    sync_match_players,
)
from app.domain import MatchRecord


def test_generate_player_aliases_includes_common_forms() -> None:
    aliases = dict(generate_player_aliases("Virat Kohli"))

    assert aliases["canonical"] == "virat kohli"
    assert aliases["first_name"] == "virat"
    assert aliases["last_name"] == "kohli"
    assert aliases["initials_last_name"] == "v kohli"


def test_resolve_player_name_matches_last_name_and_initials() -> None:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE player_aliases (
            player_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            normalized_alias TEXT NOT NULL,
            alias_type TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (player_id, normalized_alias)
        );
        CREATE TABLE match_players (
            match_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_name TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (match_id, player_id)
        );
        """
    )

    match = MatchRecord(
        match_id="match-1",
        source_file="/tmp/match-1.json",
        date="2024-01-01",
        teams=["India", "Australia"],
        gender="male",
        match_type="ODI",
        event_name="Sample",
        venue="Wankhede",
        city="Mumbai",
        toss_winner="India",
        toss_decision="bat",
        outcome="India won by 10 runs",
        player_of_match=["Virat Kohli"],
        innings=[
            {
                "team": "India",
                "runs": 250,
                "wickets": 8,
                "batting": [{"player": "Virat Kohli", "runs": 95, "balls": 88, "fours": 8, "sixes": 1}],
                "bowling": [{"player": "Mitchell Starc", "runs_conceded": 55, "balls": 60, "wickets": 2}],
                "wicket_events": [],
            }
        ],
    )

    sync_match_players(connection, match)

    last_name_results = resolve_player_name(connection, "Kohli")
    initials_results = resolve_player_name(connection, "V Kohli")

    assert last_name_results[0].canonical_name == "Virat Kohli"
    assert initials_results[0].canonical_name == "Virat Kohli"


def test_sync_external_player_directory_imports_csv_aliases(tmp_path: Path) -> None:
    players_dir = tmp_path / "players-data"
    players_dir.mkdir()
    (players_dir / "people.csv").write_text(
        "identifier,name,unique_name,key_cricinfo\n"
        "pid-1,V Kohli,V Kohli,253802\n",
        encoding="utf-8",
    )
    (players_dir / "names.csv").write_text(
        "identifier,name\n"
        "pid-1,Virat Kohli\n"
        "pid-1,Kohli\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.executescript(
        """
        CREATE TABLE players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE player_aliases (
            player_id INTEGER NOT NULL,
            alias TEXT NOT NULL,
            normalized_alias TEXT NOT NULL,
            alias_type TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (player_id, normalized_alias)
        );
        CREATE TABLE match_players (
            match_id TEXT NOT NULL,
            player_id INTEGER NOT NULL,
            team_name TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (match_id, player_id)
        );
        """
    )

    sync_external_player_directory(connection, players_dir)

    results = resolve_player_name(connection, "Kohli")
    assert results[0].canonical_name == "V Kohli"
    assert results[0].matched_alias.lower() == "kohli"

    display_name = get_preferred_player_display_name(connection, results[0].player_id, "V Kohli")
    assert display_name == "Virat Kohli"
