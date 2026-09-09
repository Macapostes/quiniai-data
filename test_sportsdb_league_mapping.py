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


def test_ligue_1_and_champions_are_mapped():
    assert (
        _infer_league_key_from_sportsdb({"idLeague": "4334", "strLeague": "French Ligue 1"})
        == "soccer_france_ligue_one"
    )
    assert (
        _infer_league_key_from_sportsdb({"idLeague": "4480", "strLeague": "UEFA Champions League"})
        == "soccer_uefa_champs_league"
    )
    assert (
        _infer_league_key_from_sportsdb(
            {"idLeague": "4354", "strLeague": "Ukrainian Premier League"}
        )
        == "soccer_ukraine_premier_league"
    )
