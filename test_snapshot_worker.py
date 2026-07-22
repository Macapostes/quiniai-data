import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import snapshot_worker as worker


def _complete_quantitative_match() -> dict:
    return {
        "league": "soccer_norway_eliteserien",
        "local": "Kristiansund BK",
        "visitante": "IK Start",
        "kickoff": (datetime.now(timezone.utc) + timedelta(days=2)).isoformat(),
        "odds": {"1": 2.1, "X": 3.4, "2": 3.0},
        "market_context": {
            "source": "odds",
            "normalized_percent": {"1": 44, "X": 27, "2": 29},
        },
        "official_quiniela_percentages": {"1": 50, "X": 25, "2": 25},
        "weather_context": {"temperature_c": 14},
        "travel_context": {"distance_km": 550},
        "competition_context": {
            "home_upcoming": [{"opponent": "KFUM"}],
            "away_upcoming": [{"opponent": "Viking"}],
        },
        "history_context": {
            "table_quality": {"valid": True},
            "home": {"recent_all": {"form": "WLLLD"}},
            "away": {"recent_all": {"form": "LWLLL"}},
            "head_to_head": {"meetings": 4},
        },
        "home_team_context": {},
        "away_team_context": {},
        "structured_context": {"injury_context": {}, "referee_context": {}},
        "match_news_context": {"items": []},
    }


class SnapshotWorkerQualityTests(unittest.TestCase):
    def test_conflicting_sportsdb_metadata_cannot_replace_known_league(self):
        match = {"league": "soccer_norway_eliteserien"}
        worker._apply_dynamic_league_metadata(
            match,
            {"idLeague": "4834", "strLeague": "_No League Soccer"},
        )
        self.assertEqual(match["league"], "soccer_norway_eliteserien")
        self.assertEqual(match["league_id"], "4358")
        self.assertEqual(match["league_name"], "Norwegian Eliteserien")

    def test_known_league_gets_its_canonical_id_without_sportsdb_metadata(self):
        match = {"league": "soccer_finland_veikkausliiga"}
        worker._apply_dynamic_league_metadata(match, {})
        self.assertEqual(match["league_id"], "4636")
        self.assertEqual(match["league_name"], "Finnish Veikkausliiga")

    def test_domestic_league_is_inferred_from_both_team_histories(self):
        histories = {
            "sportsdb_4636": [
                {"HomeTeam": "VPS", "AwayTeam": "Inter Turku"},
                {"HomeTeam": "HJK", "AwayTeam": "KuPS"},
            ],
            "soccer_sweden_allsvenskan": [
                {"HomeTeam": "AIK", "AwayTeam": "Malmo"},
            ],
        }
        self.assertEqual(
            worker._infer_league_from_histories("VPS", "FC INTER TURKU", histories),
            "soccer_finland_veikkausliiga",
        )

    def test_league_country_replaces_a_conflicting_cached_location(self):
        wrong_profile = {
            "team": "AIK",
            "country": "United States",
            "country_code": "US",
            "latitude": 40.0,
            "longitude": -75.0,
        }
        sweden = {
            "country": "Sweden",
            "country_code": "SE",
            "city": "Solna",
            "timezone": "Europe/Stockholm",
            "latitude": 59.36,
            "longitude": 18.0,
        }
        with patch.object(worker, "_geocode_team_profile_candidates", return_value=(sweden, "AIK")):
            repaired = worker._repair_profile_location("AIK", wrong_profile, "SE")
        self.assertEqual(repaired["country_code"], "SE")
        self.assertEqual(repaired["country"], "Sweden")

    def test_location_hint_removes_natural_language_prefix(self):
        self.assertEqual(worker._clean_location_hint("the city of Kristiansand"), "Kristiansand")
        self.assertEqual(worker._clean_location_hint("the town of Fredrikstad"), "Fredrikstad")

    def test_nordic_abbreviation_has_stable_location_override(self):
        profile = worker._repair_profile_location("IK Start", {}, "NO")
        self.assertEqual(profile["city"], "Kristiansand")
        self.assertEqual(profile["country_code"], "NO")
        mjallby = worker._repair_profile_location("Mj\u00e4llby AIF", {}, "SE")
        self.assertEqual(mjallby["city"], "Hallevik")
        self.assertEqual(mjallby["country_code"], "SE")

    def test_sportsdb_rejects_wrong_country_and_womens_team(self):
        wrong_country = {
            "strSport": "Soccer",
            "strTeam": "KFUM Odense",
            "strTeamAlternate": "KFUM",
            "strCountry": "Denmark",
        }
        womens_team = {
            "strSport": "Soccer",
            "strTeam": "Aalesunds Women",
            "strTeamAlternate": "Aalesunds FK",
            "strCountry": "Norway",
        }
        valid_team = {
            "strSport": "Soccer",
            "strTeam": "Aalesund",
            "strTeamAlternate": "Aalesunds FK",
            "strCountry": "Norway",
        }
        worker.THESPORTSDB_CACHE.pop("team:NO:KFUM", None)
        worker.THESPORTSDB_CACHE.pop("team:NO:AALESUNDS", None)
        with patch.object(
            worker,
            "_request_json",
            return_value={"teams": [wrong_country]},
        ):
            self.assertEqual(worker.fetch_the_sportsdb_team("KFUM", "NO"), {})
        with patch.object(
            worker,
            "_request_json",
            return_value={"teams": [womens_team, valid_team]},
        ):
            self.assertEqual(
                worker.fetch_the_sportsdb_team("AALESUNDS", "NO").get("strTeam"),
                "Aalesund",
            )

    def test_unverified_absences_prevent_perfect_confidence(self):
        confidence = worker._match_data_confidence(_complete_quantitative_match())
        self.assertLess(confidence["score"], 100)
        self.assertIn("noticias y bajas verificadas", confidence["faltan"])
        self.assertIn("arbitro confirmado", confidence["faltan"])

    def test_one_checked_roster_is_reported_as_partial_coverage(self):
        match = _complete_quantitative_match()
        match["structured_context"]["injury_context"] = {
            "home_team": {
                "verification_status": "sources_checked_no_confirmed_absence",
                "items": [],
            },
            "away_team": {"verification_status": "not_verified", "items": []},
        }
        confidence = worker._match_data_confidence(match)
        self.assertEqual(confidence["cobertura_cualitativa"]["roster_status"], "partial_verification")
        self.assertIn("bajas/convocatorias del otro equipo", confidence["faltan"])

    def test_incomplete_table_sample_is_rejected(self):
        table = {f"Team {idx}": {"played": 13} for idx in range(8)}
        table["Home"] = {"played": 3}
        table["Away"] = {"played": 2}
        quality = worker._table_quality_snapshot(table, "Home", "Away")
        self.assertFalse(quality["valid"])
        self.assertEqual(quality["minimum_expected"], 11)

    def test_active_context_refreshes_when_stale(self):
        match = _complete_quantitative_match()
        match["structured_context"]["updated_at"] = (
            datetime.now(timezone.utc) - timedelta(seconds=worker.ACTIVE_CONTEXT_REFRESH_SECONDS + 60)
        ).isoformat()
        self.assertTrue(worker._active_context_refresh_due(match))
        match["structured_context"]["updated_at"] = datetime.now(timezone.utc).isoformat()
        self.assertFalse(worker._active_context_refresh_due(match))

    def test_nordic_injury_terms_are_understood(self):
        self.assertEqual(worker._infer_injury_status("Spilleren er skadet og ute"), "out")
        self.assertEqual(worker._infer_injury_status("Avstängd efter senaste matchen"), "suspended")
        self.assertEqual(worker._infer_injury_status("Status rundt skadesituasjonen"), "watch")

    def test_injury_terms_do_not_match_inside_unrelated_words(self):
        self.assertEqual(worker._infer_injury_status("Billetter & partoutkort"), "watch")
        self.assertEqual(worker._infer_injury_status("Ackreditering/scouting"), "watch")
        self.assertEqual(
            worker._build_injury_entities(
                "SK Brann",
                [{"title": "Billetter & partoutkort", "source": "Web oficial", "link": ""}],
            ),
            [],
        )

    def test_womens_team_injury_is_not_assigned_to_mens_match(self):
        self.assertEqual(
            worker._build_injury_entities(
                "IF Brommapojkarna",
                [{
                    "title": "DAM: Patricia Fischerova drabbad av korsbandsskada",
                    "source": "Web oficial",
                    "link": "https://example.com/dam-patricia",
                }],
            ),
            [],
        )

    def test_calendar_year_leagues_keep_one_season_across_july(self):
        june = datetime(2026, 6, 15, tzinfo=timezone.utc)
        august = datetime(2026, 8, 15, tzinfo=timezone.utc)
        self.assertEqual(
            worker._league_season_code_for("soccer_norway_eliteserien", june),
            "2627",
        )
        self.assertEqual(
            worker._league_season_code_for("soccer_norway_eliteserien", august),
            "2627",
        )
        self.assertEqual(worker._league_season_code_for("soccer_spain_la_liga", june), "2526")

    def test_survival_pressure_rises_when_team_is_below_the_safe_line(self):
        rows = {
            f"Team {position}": {
                "team": f"Team {position}",
                "position": position,
                "played": 13,
                "points": 35 - position,
            }
            for position in range(1, 17)
        }
        del rows["Team 15"]
        del rows["Team 16"]
        rows["Kristiansund"] = {
            "team": "Kristiansund", "position": 15, "played": 13, "points": 12
        }
        rows["Start"] = {"team": "Start", "position": 16, "played": 14, "points": 7}
        rows["Team 14"] = {"team": "Team 14", "position": 14, "played": 13, "points": 12}
        kickoff = datetime(2026, 7, 25, tzinfo=timezone.utc)
        kristiansund = worker._team_objective_context(
            "soccer_norway_eliteserien", rows, "Kristiansund", kickoff
        )
        start = worker._team_objective_context(
            "soccer_norway_eliteserien", rows, "Start", kickoff
        )
        self.assertGreater(start["must_win_index"], kristiansund["must_win_index"])
        self.assertGreaterEqual(start["must_win_index"], 90)

    def test_monitor_api_404_switches_to_git_fallback_for_the_process(self):
        response = Mock(status_code=404)
        original_disabled = worker.MONITOR_GITHUB_API_DISABLED
        worker.MONITOR_GITHUB_API_DISABLED = False
        try:
            with patch.object(worker, "_monitor_github_headers", return_value={"Authorization": "test"}), patch.object(
                worker.requests, "get", return_value=response
            ) as get_mock:
                published = worker._github_monitor_upsert_many(
                    [("docs/monitor/audit-test.json", "{}")]
                )
                self.assertFalse(published)
                self.assertTrue(worker.MONITOR_GITHUB_API_DISABLED)
                worker._github_monitor_upsert_many(
                    [("docs/monitor/audit-test-2.json", "{}")]
                )
                self.assertEqual(get_mock.call_count, 1)
        finally:
            worker.MONITOR_GITHUB_API_DISABLED = original_disabled

    def test_monitor_api_write_404_also_switches_to_git_fallback(self):
        ref_response = Mock(status_code=200)
        ref_response.json.return_value = {"object": {"sha": "base"}}
        commit_response = Mock(status_code=200)
        commit_response.json.return_value = {"tree": {"sha": "tree-base"}}
        tree_response = Mock(status_code=201)
        tree_response.json.return_value = {"sha": "tree-new", "tree": []}
        new_commit_response = Mock(status_code=201)
        new_commit_response.json.return_value = {"sha": "commit-new"}
        update_response = Mock(status_code=404)
        original_disabled = worker.MONITOR_GITHUB_API_DISABLED
        worker.MONITOR_GITHUB_API_DISABLED = False
        try:
            with patch.object(worker, "_monitor_github_headers", return_value={"Authorization": "test"}), patch.object(
                worker.requests, "get", side_effect=[ref_response, commit_response]
            ), patch.object(
                worker.requests, "post", side_effect=[tree_response, new_commit_response]
            ), patch.object(worker.requests, "patch", return_value=update_response):
                published = worker._github_monitor_upsert_many(
                    [("docs/monitor/audit-write-test.json", "{}")]
                )
                self.assertFalse(published)
                self.assertTrue(worker.MONITOR_GITHUB_API_DISABLED)
        finally:
            worker.MONITOR_GITHUB_API_DISABLED = original_disabled


if __name__ == "__main__":
    unittest.main()
