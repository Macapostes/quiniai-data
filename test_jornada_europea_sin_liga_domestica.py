"""J13: INTER-BRUJAS no puede quedarse con un partido de Segunda RFEF."""
import snapshot_worker as w


def _m(local, visit, league, pos):
    return {"local": local, "visitante": visit, "league": league, "kickoff": "2026-10-13T19:00:00Z",
            "odds": {"1": 1.5}, "quiniela_slots": [{"jornada": 13, "position": pos, "local": local, "visitante": visit}]}


def test_alias_del_boleto():
    assert w._team_similarity_score("BRUJAS", "Club Brugge") >= 0.9
    assert w._team_similarity_score("INTER", "Inter Milan") >= 0.9
    assert w._team_similarity_score("R. UNION SG", "Union Saint-Gilloise") >= 0.9
    assert w._team_similarity_score("U. CRAIOVA", "Universitatea Craiova") >= 0.9


def test_descarta_liga_domestica_en_jornada_europea():
    ms = [_m(f"A{i}", f"B{i}", "soccer_uefa_champs_league", i) for i in range(1, 5)]
    ms.append(_m("INTER", "BRUJAS", "sportsdb_5090", 5))
    out = w._descartar_emparejamientos_incoherentes(ms, 13)
    assert len(out) == 1
    assert ms[4]["league"] == "" and ms[4]["odds"] == {}
    assert ms[4]["local"] == "INTER" and ms[4]["kickoff"]
    assert all(m["league"] == "soccer_uefa_champs_league" for m in ms[:4])


def test_jornada_domestica_no_se_toca():
    ms = [_m(f"A{i}", f"B{i}", "soccer_spain_la_liga", i) for i in range(1, 5)]
    ms.append(_m("X", "Y", "soccer_uefa_champs_league", 5))
    assert w._descartar_emparejamientos_incoherentes(ms, 13) == []
