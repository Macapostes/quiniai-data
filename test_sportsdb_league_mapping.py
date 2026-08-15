from snapshot_worker import _infer_league_key_from_sportsdb


def test_sportsdb_4400_is_spanish_segunda():
    assert (
        _infer_league_key_from_sportsdb(
            {"idLeague": "4400", "strLeague": "Spanish La Liga 2"}
        )
        == "soccer_spain_segunda_division"
    )


def test_spanish_la_liga_2_name_is_spanish_segunda():
    assert (
        _infer_league_key_from_sportsdb({"strLeague": "Spanish La Liga 2"})
        == "soccer_spain_segunda_division"
    )


def test_unknown_sportsdb_league_stays_unknown():
    assert _infer_league_key_from_sportsdb({"idLeague": "999999"}) == ""
