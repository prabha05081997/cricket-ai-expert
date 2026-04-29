from pathlib import Path

from app.ingest.parser import parse_match_file


def test_parse_match_file(tmp_path: Path) -> None:
    sample = {
        "info": {
            "dates": ["2024-01-01"],
            "teams": ["India", "Australia"],
            "gender": "male",
            "match_type": "ODI",
            "event": {"name": "Sample Series"},
            "venue": "Wankhede Stadium",
            "city": "Mumbai",
            "toss": {"winner": "India", "decision": "bat"},
            "outcome": {"winner": "India", "by": {"runs": 25}},
            "player_of_match": ["Virat Kohli"],
        },
        "innings": [
            {
                "1st innings": {
                    "team": "India",
                    "overs": [
                        {
                            "over": 0,
                            "deliveries": [
                                {
                                    "batter": "Virat Kohli",
                                    "bowler": "Mitchell Starc",
                                    "runs": {"batter": 4, "total": 4},
                                },
                                {
                                    "batter": "Virat Kohli",
                                    "bowler": "Mitchell Starc",
                                    "runs": {"batter": 1, "total": 1},
                                    "wickets": [{"player_out": "Rohit Sharma", "kind": "caught"}],
                                },
                            ],
                        }
                    ],
                }
            }
        ],
    }
    file_path = tmp_path / "match.json"
    file_path.write_text(__import__("json").dumps(sample), encoding="utf-8")

    match = parse_match_file(file_path)

    assert match.match_id == "match"
    assert match.teams == ["India", "Australia"]
    assert match.outcome == "India won by 25 runs"
    assert match.innings[0]["runs"] == 5
    assert match.innings[0]["wickets"] == 1
