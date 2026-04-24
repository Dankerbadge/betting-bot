from __future__ import annotations

import json
from pathlib import Path

from betbot import kalshi_temperature_recovery_advisor as advisor


def test_recovery_advisor_emits_risk_off_active_with_prioritized_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    call_order: list[str] = []

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        assert Path(output_dir) == tmp_path
        assert window_hours == 720.0
        assert min_bucket_samples == 10
        assert max_profile_age_hours == 336.0
        call_order.append("weather")
        return {
            "status": "ready",
            "overall": {"attempts_total": 280},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.72,
                    "stale_metar_attempt_share": 0.78,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "stale_metar_concentration",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        assert Path(output_dir) == tmp_path
        call_order.append("decision")
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [
                {
                    "key": "weather_global_risk_off_recommended",
                    "severity": "critical",
                    "summary": "Weather risk-off is still active.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        assert input_paths == [str(tmp_path)]
        assert top_n == 5
        call_order.append("growth")
        return {
            "status": "no_viable_config",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": True,
                        "risk_off_recommended": True,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert call_order == ["weather", "decision", "growth"]
    assert payload["remediation_plan"]["status"] == "risk_off_active"
    actions = payload["remediation_plan"]["prioritized_actions"]
    assert actions
    action_keys = [row["key"] for row in actions]
    assert "clear_weather_risk_off_state" in action_keys
    assert "reduce_negative_expectancy_regimes" in action_keys
    assert "plateau_break_negative_expectancy_share" in action_keys
    assert "reduce_stale_metar_pressure" in action_keys
    assert action_keys.index("reduce_negative_expectancy_regimes") < action_keys.index(
        "plateau_break_negative_expectancy_share"
    )
    assert action_keys.index("plateau_break_negative_expectancy_share") < action_keys.index(
        "reduce_stale_metar_pressure"
    )


def test_recovery_advisor_emits_bootstrap_action_for_risk_off_active_when_weather_attempt_gap_remains(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 120},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.20,
                    "stale_metar_negative_attempt_share": 0.18,
                    "stale_metar_attempt_share": 0.21,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_soft",
                    "reason": "manual_guardrail",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "bootstrap_shadow_trade_intents" in action_keys
    assert "increase_weather_sample_coverage" in action_keys
    assert action_keys.index("bootstrap_shadow_trade_intents") < action_keys.index("increase_weather_sample_coverage")


def test_recovery_advisor_omits_bootstrap_action_for_risk_off_active_when_weather_attempt_gap_cleared(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 260},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.22,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.24,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_soft",
                    "reason": "manual_guardrail",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "bootstrap_shadow_trade_intents" not in action_keys


def test_recovery_advisor_clears_when_metrics_under_targets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_cleared"
    assert payload["remediation_plan"]["prioritized_actions"] == []
    gaps = payload["remediation_plan"]["gap_to_clear"]
    assert float(gaps["weather_negative_expectancy_attempt_share"]) <= 0.0
    assert float(gaps["weather_stale_metar_negative_attempt_share"]) <= 0.0
    assert float(gaps["weather_stale_metar_attempt_share"]) <= 0.0
    assert int(gaps["weather_min_attempts"]) == 0


def test_recovery_advisor_uses_confidence_adjusted_weather_shares_for_gap_scoring(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.50,
                    "negative_expectancy_attempt_share_confidence_adjusted": 0.30,
                    "stale_metar_negative_attempt_share": 0.42,
                    "stale_metar_negative_attempt_share_confidence_adjusted": 0.20,
                    "stale_metar_attempt_share": 0.52,
                    "stale_metar_attempt_share_confidence_adjusted": 0.28,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "normal",
                    "reason": "confidence_adjusted_concentration_within_limits",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_cleared"
    weather_metrics = payload["metrics"]["weather"]
    assert float(weather_metrics["negative_expectancy_attempt_share_observed"]) == 0.50
    assert float(weather_metrics["negative_expectancy_attempt_share"]) == 0.30
    assert float(weather_metrics["stale_metar_negative_attempt_share_observed"]) == 0.42
    assert float(weather_metrics["stale_metar_negative_attempt_share"]) == 0.20
    assert float(weather_metrics["stale_metar_attempt_share_observed"]) == 0.52
    assert float(weather_metrics["stale_metar_attempt_share"]) == 0.28

    gaps = payload["remediation_plan"]["gap_to_clear"]
    assert float(gaps["weather_negative_expectancy_attempt_share"]) <= 0.0
    assert float(gaps["weather_stale_metar_negative_attempt_share"]) <= 0.0
    assert float(gaps["weather_stale_metar_attempt_share"]) <= 0.0


def test_recovery_advisor_prioritizes_explicit_risk_off_signal_when_weather_metrics_are_partial(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "degraded",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_negative_attempt_share": 0.18,
                    # `stale_metar_attempt_share` intentionally omitted.
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "partial_weather_metrics",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "no_viable_config",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": True,
                        "risk_off_recommended": True,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "clear_weather_risk_off_state" in action_keys
    assert "clear_optimizer_weather_hard_block" in action_keys
    assert payload["remediation_plan"]["targets"]["weather_stale_metar_attempt_share"]["current"] is None


def test_recovery_advisor_passes_through_decision_matrix_throughput_metrics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "settled_outcomes_delta_24h": 3.5,
                "settled_outcomes_delta_7d": 18.0,
                "combined_bucket_count_delta_24h": 2,
                "combined_bucket_count_delta_7d": 9,
                "targeted_constraint_rows": 14,
                "top_bottlenecks_count": 4,
                "bottleneck_source": "constraint_station_bootstrap",
            },
            "blocking_factors": [
                {
                    "key": "settled_outcome_growth_stalled",
                    "severity": "high",
                    "summary": "Settled coverage is still stalled.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert float(decision_metrics["settled_outcomes_delta_24h"]) == 3.5
    assert float(decision_metrics["settled_outcomes_delta_7d"]) == 18.0
    assert int(decision_metrics["combined_bucket_count_delta_24h"]) == 2
    assert int(decision_metrics["combined_bucket_count_delta_7d"]) == 9
    assert int(decision_metrics["targeted_constraint_rows"]) == 14
    assert int(decision_metrics["top_bottlenecks_count"]) == 4
    assert decision_metrics["bottleneck_source"] == "constraint_station_bootstrap"
    assert decision_metrics["settled_outcome_growth_stalled"] is True


def test_recovery_advisor_surfaces_suppression_metrics_from_latest_intents_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 12,
                "weather_pattern_negative_regime_suppression_blocked_count": 3,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    suppression = payload["metrics"]["suppression"]
    assert suppression["summary_available"] is True
    assert suppression["summary_source"] == "exact"
    assert suppression["enabled"] is True
    assert suppression["active"] is True
    assert suppression["status"] == "ready"
    assert int(suppression["candidate_count"]) == 12
    assert int(suppression["blocked_count"]) == 3
    assert float(suppression["blocked_share"]) == 0.25


def test_recovery_advisor_caps_suppression_blocked_share_when_blocked_exceeds_candidates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 4,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    suppression = payload["metrics"]["suppression"]
    assert int(suppression["candidate_count"]) == 4
    assert int(suppression["blocked_count"]) == 5
    assert float(suppression["blocked_share"]) == 1.0


def test_recovery_advisor_sets_suppression_blocked_share_when_candidates_are_zero(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 0,
                "weather_pattern_negative_regime_suppression_blocked_count": 2,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    suppression = payload["metrics"]["suppression"]
    assert int(suppression["candidate_count"]) == 0
    assert int(suppression["blocked_count"]) == 2
    assert float(suppression["blocked_share"]) == 1.0


def test_recovery_advisor_surfaces_trade_plan_blocker_metrics(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_plan_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 27,
                "intents_approved": 4,
                "policy_reason_counts": {
                    "approved": 4,
                    "inside_cutoff_window": 9,
                    "settlement_finalization_blocked": 11,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.21,
                    "stale_metar_negative_attempt_share": 0.19,
                    "stale_metar_attempt_share": 0.22,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    blockers = payload["metrics"]["trade_plan_blockers"]
    assert blockers["summary_available"] is True
    assert blockers["summary_source"] == "exact"
    assert int(blockers["intents_total"]) == 27
    assert int(blockers["intents_approved"]) == 4
    assert blockers["policy_reason_counts"] == {
        "approved": 4,
        "inside_cutoff_window": 9,
        "settlement_finalization_blocked": 11,
    }


def test_recovery_advisor_emits_refresh_market_horizon_inputs_for_cutoff_or_settlement_blockers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 44,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 7,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 300},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.63,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "refresh_market_horizon_inputs" in action_rows
    command_hint = str(action_rows["refresh_market_horizon_inputs"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-contract-specs" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-constraint-scan" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-settlement-state" in command_hint
    assert "--output-dir" in command_hint


def test_recovery_advisor_prioritizes_stale_station_concentration_action_after_timeout_protection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recovery_latest = tmp_path / "health" / "recovery" / "recovery_latest.json"
    recovery_latest.parent.mkdir(parents=True, exist_ok=True)
    recovery_latest.write_text(
        json.dumps(
            {
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:missing_script",
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 44,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 7,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 360},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.63,
                    "stale_negative_station_max_share": 0.57,
                    "stale_negative_station_hhi": 0.36,
                    "stale_negative_station_top": [
                        {"station": "KJFK", "share": 0.57},
                    ],
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    weather_metrics = payload["metrics"]["weather"]
    assert float(weather_metrics["stale_negative_station_max_share"]) == 0.57
    assert float(weather_metrics["stale_negative_station_hhi"]) == 0.36
    assert weather_metrics["stale_negative_station_top"] == [{"station": "KJFK", "share": 0.57}]

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "restore_stage_timeout_guardrail_script" in action_rows
    assert "reduce_stale_station_concentration" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("restore_stage_timeout_guardrail_script") < action_keys.index(
        "reduce_stale_station_concentration"
    )
    assert action_keys.index("reduce_stale_station_concentration") < action_keys.index(
        "refresh_market_horizon_inputs"
    )
    command_hint = str(action_rows["reduce_stale_station_concentration"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-metar-ingest" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-weather-pattern" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in command_hint
    assert "--intents-only" in command_hint
    assert "--weather-pattern-hardening-enabled" in command_hint
    assert "--output-dir" in command_hint


def test_recovery_advisor_does_not_emit_stale_station_concentration_action_without_breach(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 44,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 7,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 120},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.63,
                    "stale_negative_station_max_share": 0.38,
                    "stale_negative_station_hhi": 0.33,
                    "stale_negative_station_top": {"station": "KJFK", "share": 0.38},
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    weather_metrics = payload["metrics"]["weather"]
    assert float(weather_metrics["stale_negative_station_max_share"]) == 0.38
    assert float(weather_metrics["stale_negative_station_hhi"]) == 0.33
    assert weather_metrics["stale_negative_station_top"] == {"station": "KJFK", "share": 0.38}

    action_rows = {
        str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]
    }
    assert "refresh_market_horizon_inputs" in action_rows
    assert "reduce_stale_station_concentration" not in action_rows


def test_recovery_advisor_prioritizes_metar_ingest_quality_pipeline_repair_when_blockers_dominate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 40,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "metar_ingest_quality_insufficient": 32,
                    "inside_cutoff_window": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "repair_metar_ingest_quality_pipeline" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("repair_metar_ingest_quality_pipeline") < action_keys.index(
        "refresh_market_horizon_inputs"
    )
    command_hint = str(action_rows["repair_metar_ingest_quality_pipeline"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-metar-ingest" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-weather-pattern" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in command_hint
    assert "--intents-only" in command_hint
    assert "--weather-pattern-hardening-enabled" in command_hint


def test_recovery_advisor_insufficient_data_still_emits_metar_ingest_quality_pipeline_repair(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 18,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "metar_ingest_quality_insufficient": 14,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 36},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.82,
                    "stale_metar_negative_attempt_share": 0.57,
                    "stale_metar_attempt_share": 0.59,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "normal",
                    "reason": "concentration_within_limits",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "insufficient_data"
    action_rows = {
        str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]
    }
    assert "bootstrap_shadow_trade_intents" in action_rows
    assert "repair_metar_ingest_quality_pipeline" in action_rows
    command_hint = str(action_rows["repair_metar_ingest_quality_pipeline"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-metar-ingest" in command_hint


def test_recovery_advisor_prioritizes_taf_mapping_and_execution_friction_actions_before_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recovery_latest = tmp_path / "health" / "recovery" / "recovery_latest.json"
    recovery_latest.parent.mkdir(parents=True, exist_ok=True)
    recovery_latest.write_text(
        json.dumps(
            {
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:missing_script",
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 40,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "taf_station_missing": 18,
                    "inside_cutoff_window": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "yellow",
                    "source": "health/execution_cost_tape_latest.json",
                    "execution_cost_observations": {
                        "top_missing_coverage_buckets": {
                            "by_market": [
                                {"bucket": "kxhightphx-26apr23-b91.5"},
                                {"ticker": "KXHIGHTPHX-26APR23-B92.5"},
                                {"market_ticker": "KXHIGHTPHX-26APR23-B91.5"},
                                " KXHIGHTPHX-26APR23-B93.5 ",
                            ]
                        }
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    execution_friction = payload["metrics"]["growth_optimizer"]["execution_friction"]
    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_cost_top_missing_market_tickers"] == [
        "KXHIGHTPHX-26APR23-B91.5",
        "KXHIGHTPHX-26APR23-B92.5",
        "KXHIGHTPHX-26APR23-B93.5",
    ]
    assert execution_friction == {
        "available": True,
        "severe": True,
        "penalty": 0.2,
        "weighted_penalty": 0.11,
        "evidence_coverage": 0.74,
        "quote_two_sided_ratio": 0.39,
        "spread_median_dollars": 0.07,
        "spread_p90_dollars": 0.19,
    }

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "restore_stage_timeout_guardrail_script" in action_rows
    assert "repair_taf_station_mapping_pipeline" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("restore_stage_timeout_guardrail_script") < action_keys.index(
        "repair_taf_station_mapping_pipeline"
    )
    assert action_keys.index("restore_stage_timeout_guardrail_script") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_keys.index("repair_taf_station_mapping_pipeline") < action_keys.index(
        "refresh_market_horizon_inputs"
    )
    assert action_keys.index("reduce_execution_friction_pressure") < action_keys.index(
        "refresh_market_horizon_inputs"
    )

    taf_hint = str(action_rows["repair_taf_station_mapping_pipeline"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-contract-specs" in taf_hint
    assert "python3 -m betbot.cli kalshi-temperature-constraint-scan" in taf_hint
    assert "python3 -m betbot.cli kalshi-temperature-settlement-state" in taf_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in taf_hint
    assert "--intents-only" in taf_hint
    assert "--weather-pattern-hardening-enabled" in taf_hint

    friction_hint = str(action_rows["reduce_execution_friction_pressure"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-execution-cost-tape" in friction_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in friction_hint
    assert "--intents-only" in friction_hint
    assert "--weather-pattern-hardening-enabled" in friction_hint
    assert action_rows["reduce_execution_friction_pressure"].get("details") == {
        "top_missing_market_tickers": [
            "KXHIGHTPHX-26APR23-B91.5",
            "KXHIGHTPHX-26APR23-B92.5",
            "KXHIGHTPHX-26APR23-B93.5",
        ]
    }


def test_recovery_advisor_omits_taf_mapping_and_execution_friction_actions_without_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 50,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "taf_station_missing": 6,
                    "inside_cutoff_window": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": False,
                        "penalty": 0.06,
                        "weighted_penalty": 0.04,
                        "evidence_coverage": 0.90,
                        "quote_two_sided_ratio": 0.75,
                        "spread_median_dollars": 0.01,
                        "spread_p90_dollars": 0.03,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    action_rows = {
        str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]
    }
    assert "refresh_market_horizon_inputs" in action_rows
    assert "repair_taf_station_mapping_pipeline" not in action_rows
    assert "reduce_execution_friction_pressure" not in action_rows


def test_recovery_advisor_emits_execution_telemetry_pipeline_repair_when_telemetry_starvation_detected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 42,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 3,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "missing",
                "execution_cost_meets_candidate_samples": False,
                "execution_cost_meets_quote_coverage": False,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_calibration_not_ready",
                    "severity": "critical",
                    "summary": "Execution-cost tape is not calibrated.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_blocker_audit_artifact",
                "missing_execution_cost_tape_artifact",
                "missing_ws_state_artifact",
                "missing_execution_journal_data",
            ],
            "data_sources": {
                "blocker_audit": {
                    "status": "missing",
                    "source": "",
                },
                "execution_cost_tape": {
                    "status": "missing",
                    "source": "",
                    "ws_state_status": "missing",
                    "execution_journal_status": "missing",
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["telemetry_starvation_detected"] is True
    assert decision_metrics["telemetry_missing_contexts"] == [
        "blocker_audit",
        "execution_cost_tape",
        "execution_journal",
        "ws_state",
    ]

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index(
        "improve_execution_quote_coverage_shadow"
    )
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "refresh_market_horizon_inputs"
    )
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index("refresh_market_horizon_inputs")
    command_hint = str(action_rows["repair_execution_telemetry_pipeline"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-ws-state-collect" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in command_hint
    assert "--disable-weather-pattern-hardening" in command_hint
    assert "--disable-historical-selection-quality" in command_hint
    assert "--disable-enforce-probability-edge-thresholds" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-execution-cost-tape" in command_hint
    assert "python3 -m betbot.cli decision-matrix-hardening" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-recovery-advisor" in command_hint
    assert command_hint.index("kalshi-ws-state-collect") < command_hint.index("kalshi-temperature-trader")
    assert command_hint.index("kalshi-temperature-trader") < command_hint.index("kalshi-temperature-execution-cost-tape")
    assert command_hint.index("kalshi-temperature-execution-cost-tape") < command_hint.index(
        "decision-matrix-hardening"
    )
    assert command_hint.index("decision-matrix-hardening") < command_hint.index(
        "kalshi-temperature-recovery-advisor"
    )

    quote_command_hint = str(action_rows["improve_execution_quote_coverage_shadow"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-ws-state-collect" in quote_command_hint
    assert "python3 -m betbot.cli kalshi-temperature-trader" in quote_command_hint
    assert "--disable-weather-pattern-hardening" in quote_command_hint
    assert "--disable-historical-selection-quality" in quote_command_hint
    assert "--disable-enforce-probability-edge-thresholds" in quote_command_hint
    assert "python3 -m betbot.cli kalshi-temperature-execution-cost-tape" in quote_command_hint


def test_recovery_advisor_emits_execution_telemetry_pipeline_repair_when_execution_cost_calibration_is_insufficient(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 36,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 5,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 360},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.82,
                    "stale_metar_negative_attempt_share": 0.58,
                    "stale_metar_attempt_share": 0.61,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "red",
                "execution_cost_meets_candidate_samples": False,
                "execution_cost_meets_quote_coverage": True,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_calibration_not_ready",
                    "severity": "critical",
                    "summary": "Execution-cost tape candidate sample coverage is below threshold.",
                }
            ],
            "data_pipeline_gaps": [],
            "data_sources": {
                "blocker_audit": {
                    "status": "ready",
                    "source": "checkpoints/blocker_audit_168h_latest.json",
                },
                "execution_cost_tape": {
                    "status": "red",
                    "source": "health/execution_cost_tape_latest.json",
                    "ws_state_status": "ready",
                    "execution_journal_status": "ready",
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["telemetry_starvation_detected"] is False
    assert decision_metrics["telemetry_missing_contexts"] == []
    assert decision_metrics["execution_cost_calibration_starvation_detected"] is True

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index("refresh_market_horizon_inputs")


def test_recovery_advisor_emits_quote_coverage_shadow_remediation_when_quote_coverage_is_below_min(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 40,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 340},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.84,
                    "stale_metar_negative_attempt_share": 0.57,
                    "stale_metar_attempt_share": 0.60,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.22,
                "execution_cost_min_quote_coverage_ratio": 0.60,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [],
            "data_sources": {
                "blocker_audit": {
                    "status": "ready",
                    "source": "checkpoints/blocker_audit_168h_latest.json",
                },
                "execution_cost_tape": {
                    "status": "yellow",
                    "source": "health/execution_cost_tape_latest.json",
                    "ws_state_status": "ready",
                    "execution_journal_status": "ready",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.22,
                        "min_quote_coverage_ratio": 0.60,
                        "meets_quote_coverage": False,
                    },
                    "execution_cost_observations": {
                        "quote_coverage_decomposition": {
                            "rows_total": 320,
                            "rows_with_any_two_sided_quote": 70,
                            "rows_without_two_sided_quote": 250,
                        },
                        "quote_coverage_by_event_type": [
                            {
                                "event_type": "candidate_seen",
                                "rows": 200,
                                "rows_with_any_two_sided_quote": 40,
                                "rows_without_two_sided_quote": 160,
                                "quote_coverage_ratio": 0.2,
                            }
                        ],
                        "top_missing_coverage_buckets": {
                            "by_market_side": [
                                {
                                    "bucket": "KXHIGHTPHX-26APR23-B91.5|yes",
                                    "ticker": "KXHIGHTPHX-26APR23-B91.5",
                                    "side": "yes",
                                    "rows_without_two_sided_quote": 90,
                                    "share_of_uncovered_rows": 0.36,
                                },
                                {
                                    "ticker": "KXHIGHTPHX-26APR23-B92.5",
                                    "side": "NO",
                                    "rows_without_two_sided_quote": 44,
                                    "share_of_uncovered_rows": 0.18,
                                },
                                {
                                    "bucket": "raw-unparseable-target",
                                    "rows_without_two_sided_quote": 20,
                                    "share_of_uncovered_rows": 0.08,
                                },
                                {
                                    "bucket": "KXHIGHTPHX-26APR23-B91.5|yes",
                                    "ticker": "KXHIGHTPHX-26APR23-B91.5",
                                    "side": "yes",
                                    "rows_without_two_sided_quote": 10,
                                    "share_of_uncovered_rows": 0.04,
                                }
                            ]
                        },
                    },
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_cost_quote_coverage_starvation_detected"] is True
    assert decision_metrics["execution_cost_meets_quote_coverage"] is False
    assert round(float(decision_metrics["execution_cost_quote_coverage_ratio"]), 6) == 0.22
    assert round(float(decision_metrics["execution_cost_min_quote_coverage_ratio"]), 6) == 0.60
    assert round(float(decision_metrics["execution_cost_quote_coverage_shortfall"]), 6) == 0.38
    assert decision_metrics["execution_cost_top_missing_market_side"] == "KXHIGHTPHX-26APR23-B91.5|yes"
    assert round(float(decision_metrics["execution_cost_top_missing_market_side_share"]), 6) == 0.36
    decomposition = decision_metrics["execution_cost_quote_coverage_decomposition"]
    assert int(decomposition["rows_total"]) == 320
    assert int(decomposition["rows_without_two_sided_quote"]) == 250

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert action_rows["improve_execution_quote_coverage_shadow"]["details"] == {
        "top_missing_market_side_targets": [
            "KXHIGHTPHX-26APR23-B91.5|yes",
            "KXHIGHTPHX-26APR23-B92.5|no",
            "raw-unparseable-target",
        ]
    }
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index(
        "improve_execution_quote_coverage_shadow"
    )
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "refresh_market_horizon_inputs"
    )


def test_recovery_advisor_uses_fallback_top_missing_market_side_targets_when_bucket_rows_are_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 28,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 260},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.72,
                    "stale_metar_negative_attempt_share": 0.50,
                    "stale_metar_attempt_share": 0.55,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.19,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_cost_top_missing_market_side": "KXHIGHTPHX-26APR23-B93.5|no",
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "yellow",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.19,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert action_rows["improve_execution_quote_coverage_shadow"]["details"] == {
        "top_missing_market_side_targets": ["KXHIGHTPHX-26APR23-B93.5|no"]
    }


def test_recovery_advisor_promotes_execution_friction_pressure_when_siphon_pressure_is_high(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 40,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )

    wide_spread_tickers = [f"WIDE-{index:02d}" for index in range(1, 22)]

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 340},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.50,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.65,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "siphon_pressure_hot",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.22,
                "execution_cost_min_quote_coverage_ratio": 0.60,
                "execution_siphon_pressure": 0.72,
                "low_coverage_wide_spread_ticker_count": 6,
                "low_coverage_wide_spread_tickers": wide_spread_tickers,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_sources": {
                "blocker_audit": {
                    "status": "ready",
                    "source": "checkpoints/blocker_audit_168h_latest.json",
                },
                "execution_cost_tape": {
                    "status": "yellow",
                    "source": "health/execution_cost_tape_latest.json",
                    "ws_state_status": "ready",
                    "execution_journal_status": "ready",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.22,
                        "min_quote_coverage_ratio": 0.60,
                        "meets_quote_coverage": False,
                    },
                    "execution_cost_observations": {
                        "top_missing_coverage_buckets": {
                            "by_market": [
                                {
                                    "bucket": "KXHIGHTPHX-26APR23-B91.5",
                                    "ticker": "KXHIGHTPHX-26APR23-B91.5",
                                },
                                {
                                    "bucket": "KXHIGHTPHX-26APR23-B92.5",
                                    "ticker": "KXHIGHTPHX-26APR23-B92.5",
                                },
                            ]
                        }
                    },
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": False,
                        "severe": False,
                        "penalty": 0.02,
                        "weighted_penalty": 0.01,
                        "evidence_coverage": 0.55,
                        "quote_two_sided_ratio": 0.80,
                        "spread_median_dollars": 0.02,
                        "spread_p90_dollars": 0.05,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert round(float(decision_metrics["execution_siphon_pressure"]), 6) == 0.72
    assert int(decision_metrics["low_coverage_wide_spread_ticker_count"]) == 6
    assert decision_metrics["low_coverage_wide_spread_tickers"] == wide_spread_tickers[:20]

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert action_keys.index("improve_execution_quote_coverage_shadow") + 1 == action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_rows["reduce_execution_friction_pressure"]["details"] == {
        "top_missing_market_tickers": [
            "KXHIGHTPHX-26APR23-B91.5",
            "KXHIGHTPHX-26APR23-B92.5",
        ],
        "low_coverage_wide_spread_tickers": wide_spread_tickers[:20],
        "siphon_pressure_score": 0.72,
    }


def test_recovery_advisor_promotes_reduce_execution_friction_pressure_when_siphon_trend_worsens(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_exclusions_state_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "active_count": 12,
                "candidate_count": 20,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "ready",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": True,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "execution_siphon_trend": {
                        "status": "ready",
                        "worsening": True,
                        "quote_coverage_ratio_delta": -0.08,
                        "siphon_pressure_score_delta": 0.12,
                        "candidate_rows_delta": -22,
                    },
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.62,
                        "min_quote_coverage_ratio": 0.60,
                        "meets_quote_coverage": True,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert int(decision_metrics["execution_cost_exclusion_state_active_count"]) == 12
    assert int(decision_metrics["execution_cost_exclusion_state_candidate_count"]) == 20
    assert decision_metrics["execution_siphon_trend_status"] == "ready"
    assert decision_metrics["execution_siphon_trend_worsening"] is True
    assert decision_metrics["execution_siphon_trend_material_worsening"] is True
    assert round(float(decision_metrics["execution_siphon_trend_quote_coverage_ratio_delta"]), 6) == -0.08
    assert round(float(decision_metrics["execution_siphon_trend_pressure_delta"]), 6) == 0.12
    assert round(float(decision_metrics["execution_siphon_trend_siphon_pressure_score_delta"]), 6) == 0.12
    assert int(decision_metrics["execution_siphon_trend_candidate_rows_delta"]) == -22

    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert action_keys.index("reduce_execution_friction_pressure") < action_keys.index(
        "repair_execution_telemetry_pipeline"
    )
    details = dict(action_rows["reduce_execution_friction_pressure"]["details"])
    assert details["exclusion_state_active_count"] == 12
    assert details["siphon_trend_worsening"] is True
    assert details["siphon_trend_quote_coverage_ratio_delta"] == -0.08
    assert details["siphon_trend_pressure_delta"] == 0.12
    assert details["siphon_pressure_score"] == 0.31
    trend_context = dict(details.get("siphon_trend_context") or {})
    assert trend_context["worsening"] is True
    assert trend_context["material_worsening"] is True
    assert trend_context["quote_coverage_ratio_delta"] == -0.08
    assert trend_context["siphon_pressure_score_delta"] == 0.12


def test_recovery_advisor_preserves_execution_friction_order_when_siphon_trend_is_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "ready",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": True,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.62,
                        "min_quote_coverage_ratio": 0.60,
                        "meets_quote_coverage": True,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_siphon_trend_status"] is None
    assert decision_metrics["execution_siphon_trend_worsening"] is None
    assert decision_metrics["execution_siphon_trend_material_worsening"] is False
    assert decision_metrics["execution_siphon_trend_quote_coverage_ratio_delta"] is None
    assert decision_metrics["execution_siphon_trend_pressure_delta"] is None
    assert decision_metrics["execution_siphon_trend_siphon_pressure_score_delta"] is None
    assert decision_metrics["execution_siphon_trend_candidate_rows_delta"] is None
    assert int(decision_metrics["execution_cost_exclusion_state_active_count"]) == 0

    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_rows["reduce_execution_friction_pressure"]["details"] == {
        "siphon_pressure_score": 0.31,
    }


def test_recovery_advisor_promotes_execution_actions_from_material_trend_in_fallback_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_trend": {
                    "status": "ready",
                    "worsening": True,
                    "quote_coverage_ratio_delta": -0.06,
                    "siphon_pressure_score_delta": 0.09,
                    "candidate_rows_delta": -28,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.48,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.35,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "yellow",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.48,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": False,
                        "penalty": 0.03,
                        "weighted_penalty": 0.02,
                        "evidence_coverage": 0.72,
                        "quote_two_sided_ratio": 0.71,
                        "spread_median_dollars": 0.02,
                        "spread_p90_dollars": 0.05,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_siphon_trend_status"] == "ready"
    assert decision_metrics["execution_siphon_trend_worsening"] is True
    assert decision_metrics["execution_siphon_trend_material_worsening"] is True
    assert round(float(decision_metrics["execution_siphon_trend_quote_coverage_ratio_delta"]), 6) == -0.06
    assert round(float(decision_metrics["execution_siphon_trend_siphon_pressure_score_delta"]), 6) == 0.09
    assert int(decision_metrics["execution_siphon_trend_candidate_rows_delta"]) == -28

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_keys.index("reduce_execution_friction_pressure") < action_keys.index(
        "repair_execution_telemetry_pipeline"
    )
    assert action_rows["improve_execution_quote_coverage_shadow"]["details"] == {
        "siphon_trend_context": {
            "status": "ready",
            "worsening": True,
            "quote_coverage_ratio_delta": -0.06,
            "siphon_pressure_score_delta": 0.09,
            "candidate_rows_delta": -28,
            "material_worsening": True,
        }
    }


def test_recovery_advisor_promotes_execution_actions_for_material_side_pressure_from_fallback_payload(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "side_pressure": {
                        "dominant_side": "no",
                        "dominant_side_share": 0.79,
                        "side_imbalance": 0.58,
                        "pressure_score": 0.84,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.52,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_siphon_side_pressure_dominant_side"] == "no"
    assert round(float(decision_metrics["execution_siphon_side_pressure_dominant_side_share"]), 6) == 0.79
    assert round(float(decision_metrics["execution_siphon_side_pressure_imbalance"]), 6) == 0.58
    assert round(float(decision_metrics["execution_siphon_side_pressure_score"]), 6) == 0.84
    assert decision_metrics["execution_siphon_side_pressure_materially_high"] is True

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_keys.index("reduce_execution_friction_pressure") < action_keys.index(
        "repair_execution_telemetry_pipeline"
    )
    expected_side_context = {
        "dominant_side": "no",
        "dominant_side_share": 0.79,
        "side_imbalance": 0.58,
        "side_pressure_score": 0.84,
    }
    assert action_rows["improve_execution_quote_coverage_shadow"]["details"] == {
        "siphon_side_pressure_context": expected_side_context
    }
    assert action_rows["reduce_execution_friction_pressure"]["details"] == {
        "siphon_pressure_score": 0.31,
        "siphon_side_pressure_context": expected_side_context,
    }


def test_recovery_advisor_parses_execution_side_target_state_counts_from_exclusions_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_exclusions_state_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "active_market_side_target_count": 2,
                "candidate_market_side_target_count": 7,
                "adaptive_downshift": {
                    "last_drop_market_side_count": 4,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "ready",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": True,
            },
            "blocking_factors": [],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.62,
                        "min_quote_coverage_ratio": 0.60,
                        "meets_quote_coverage": True,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": False,
                        "severe": False,
                        "penalty": 0.01,
                        "weighted_penalty": 0.01,
                        "evidence_coverage": 0.75,
                        "quote_two_sided_ratio": 0.81,
                        "spread_median_dollars": 0.01,
                        "spread_p90_dollars": 0.03,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert int(decision_metrics["execution_cost_exclusion_state_side_active_count"]) == 2
    assert int(decision_metrics["execution_cost_exclusion_state_side_candidate_count"]) == 7
    assert int(decision_metrics["execution_cost_exclusion_state_side_recent_downshift_count"]) == 4
    assert decision_metrics["execution_cost_exclusion_state_side_state_present"] is True


def test_recovery_advisor_includes_side_target_state_context_in_execution_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "side_pressure": {
                        "dominant_side": "no",
                        "dominant_side_share": 0.79,
                        "side_imbalance": 0.58,
                        "pressure_score": 0.84,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health" / "execution_cost_exclusions_state_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "active_market_side_target_count": 1,
                "candidate_market_side_target_count": 6,
                "adaptive_downshift": {
                    "last_drop_market_side_count": 2,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.52,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert int(decision_metrics["execution_cost_exclusion_state_side_active_count"]) == 1
    assert int(decision_metrics["execution_cost_exclusion_state_side_candidate_count"]) == 6
    assert int(decision_metrics["execution_cost_exclusion_state_side_recent_downshift_count"]) == 2
    expected_side_state_context = {
        "active_side_target_count": 1,
        "candidate_side_target_count": 6,
        "recently_downshifted_side_target_count": 2,
        "weak_side_target_state": True,
    }

    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    improve_details = dict(action_rows["improve_execution_quote_coverage_shadow"].get("details") or {})
    reduce_details = dict(action_rows["reduce_execution_friction_pressure"].get("details") or {})
    assert improve_details.get("siphon_side_exclusion_state_context") == expected_side_state_context
    assert reduce_details.get("siphon_side_exclusion_state_context") == expected_side_state_context


def test_recovery_advisor_promotes_execution_order_when_side_pressure_state_is_weak_under_quote_coverage_stress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "side_pressure": {
                        "dominant_side": "no",
                        "dominant_side_share": 0.79,
                        "side_imbalance": 0.58,
                        "pressure_score": 0.84,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.52,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    state_path = tmp_path / "health" / "execution_cost_exclusions_state_latest.json"
    state_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "active_count": 0,
                "candidate_count": 0,
                "active_market_side_target_count": 1,
                "candidate_market_side_target_count": 8,
                "adaptive_downshift": {
                    "last_drop_market_side_count": 1,
                },
            }
        ),
        encoding="utf-8",
    )
    weak_payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    weak_order = [str(row.get("key")) for row in weak_payload["remediation_plan"]["prioritized_actions"]]
    assert weak_order.index("reduce_execution_friction_pressure") < weak_order.index(
        "improve_execution_quote_coverage_shadow"
    )
    assert weak_order.index("improve_execution_quote_coverage_shadow") < weak_order.index(
        "repair_execution_telemetry_pipeline"
    )
    weak_action_rows = {
        str(row.get("key")): row for row in weak_payload["remediation_plan"]["prioritized_actions"]
    }
    expected_ordering_context = {
        "active": True,
        "reason": "weak_side_target_state_with_material_side_pressure_under_quote_coverage_stress",
        "quote_coverage_starvation_detected": True,
        "severe_quote_coverage_shortfall": False,
        "quote_coverage_shortfall": 0.03,
    }
    assert dict(
        weak_action_rows["reduce_execution_friction_pressure"].get("details") or {}
    ).get("execution_friction_ordering_context") == expected_ordering_context
    assert dict(
        weak_action_rows["improve_execution_quote_coverage_shadow"].get("details") or {}
    ).get("execution_friction_ordering_context") == expected_ordering_context

    state_path.write_text(
        json.dumps(
            {
                "status": "ready",
                "active_count": 0,
                "candidate_count": 0,
                "active_market_side_target_count": 6,
                "candidate_market_side_target_count": 8,
                "adaptive_downshift": {
                    "last_drop_market_side_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )
    strong_payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    strong_order = [str(row.get("key")) for row in strong_payload["remediation_plan"]["prioritized_actions"]]
    assert strong_order.index("repair_execution_telemetry_pipeline") < strong_order.index(
        "improve_execution_quote_coverage_shadow"
    )
    assert strong_order.index("improve_execution_quote_coverage_shadow") < strong_order.index(
        "reduce_execution_friction_pressure"
    )


def test_recovery_advisor_preserves_execution_friction_order_without_quote_coverage_stress_gate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "side_pressure": {
                        "dominant_side": "no",
                        "dominant_side_share": 0.79,
                        "side_imbalance": 0.58,
                        "pressure_score": 0.84,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health" / "execution_cost_exclusions_state_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "active_count": 0,
                "candidate_count": 0,
                "active_market_side_target_count": 1,
                "candidate_market_side_target_count": 8,
                "adaptive_downshift": {
                    "last_drop_market_side_count": 1,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert "execution_friction_ordering_context" not in dict(
        action_rows["reduce_execution_friction_pressure"].get("details") or {}
    )
    assert "execution_friction_ordering_context" not in dict(
        action_rows["improve_execution_quote_coverage_shadow"].get("details") or {}
    )


def test_recovery_advisor_parses_side_pressure_from_execution_siphon_payload_flat_fields(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "dominant_uncovered_side": "no",
                    "dominant_uncovered_side_share": 0.639069,
                    "side_imbalance_magnitude": 0.639069,
                    "side_pressure_score_contribution": 0.408409,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.52,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_siphon_side_pressure_dominant_side"] == "no"
    assert round(float(decision_metrics["execution_siphon_side_pressure_dominant_side_share"]), 6) == 0.639069
    assert round(float(decision_metrics["execution_siphon_side_pressure_imbalance"]), 6) == 0.639069
    assert round(float(decision_metrics["execution_siphon_side_pressure_score"]), 6) == 0.408409
    assert decision_metrics["execution_siphon_side_pressure_materially_high"] is True

    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    assert "improve_execution_quote_coverage_shadow" in action_rows
    details = dict(action_rows["improve_execution_quote_coverage_shadow"].get("details") or {})
    side_context = dict(details.get("siphon_side_pressure_context") or {})
    assert side_context == {
        "dominant_side": "no",
        "dominant_side_share": 0.639069,
        "side_imbalance": 0.639069,
        "side_pressure_score": 0.408409,
    }


def test_recovery_advisor_preserves_execution_action_order_when_side_pressure_metrics_are_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {},
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "health").mkdir(parents=True, exist_ok=True)
    (tmp_path / "health" / "execution_cost_tape_latest.json").write_text(
        json.dumps(
            {
                "status": "ready",
                "execution_siphon_pressure": {
                    "status": "ready",
                    "pressure_score": 0.31,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.52,
                "execution_cost_min_quote_coverage_ratio": 0.55,
                "execution_siphon_pressure": 0.31,
            },
            "blocking_factors": [
                {
                    "key": "execution_cost_quote_coverage_below_min",
                    "severity": "high",
                    "summary": "Execution-cost quote coverage is below minimum.",
                }
            ],
            "data_pipeline_gaps": [
                "missing_execution_cost_tape_artifact",
            ],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "calibration_readiness": {
                        "quote_coverage_ratio": 0.52,
                        "min_quote_coverage_ratio": 0.55,
                        "meets_quote_coverage": False,
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    },
                    "execution_friction": {
                        "available": True,
                        "severe": True,
                        "penalty": 0.20,
                        "weighted_penalty": 0.11,
                        "evidence_coverage": 0.74,
                        "quote_two_sided_ratio": 0.39,
                        "spread_median_dollars": 0.07,
                        "spread_p90_dollars": 0.19,
                    },
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["execution_siphon_side_pressure_dominant_side"] is None
    assert decision_metrics["execution_siphon_side_pressure_dominant_side_share"] is None
    assert decision_metrics["execution_siphon_side_pressure_imbalance"] is None
    assert decision_metrics["execution_siphon_side_pressure_score"] is None
    assert decision_metrics["execution_siphon_side_pressure_materially_high"] is False

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "improve_execution_quote_coverage_shadow" in action_rows
    assert "reduce_execution_friction_pressure" in action_rows
    assert "repair_execution_telemetry_pipeline" in action_rows
    assert action_keys.index("repair_execution_telemetry_pipeline") < action_keys.index(
        "reduce_execution_friction_pressure"
    )
    assert action_keys.index("reduce_execution_friction_pressure") < action_keys.index(
        "improve_execution_quote_coverage_shadow"
    )
    assert "details" not in action_rows["improve_execution_quote_coverage_shadow"]
    assert action_rows["reduce_execution_friction_pressure"]["details"] == {
        "siphon_pressure_score": 0.31,
    }


def test_recovery_advisor_omits_execution_telemetry_pipeline_repair_without_telemetry_starvation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 42,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "inside_cutoff_window": 3,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "ready",
                "execution_cost_meets_candidate_samples": True,
                "execution_cost_meets_quote_coverage": True,
            },
            "blocking_factors": [],
            "data_pipeline_gaps": [],
            "data_sources": {
                "blocker_audit": {
                    "status": "ready",
                    "source": "checkpoints/blocker_audit_168h_latest.json",
                },
                "execution_cost_tape": {
                    "status": "ready",
                    "source": "health/execution_cost_tape_latest.json",
                    "ws_state_status": "ready",
                    "execution_journal_status": "ready",
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["telemetry_starvation_detected"] is False
    assert decision_metrics["telemetry_missing_contexts"] == []
    assert decision_metrics["execution_cost_calibration_starvation_detected"] is False

    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "repair_execution_telemetry_pipeline" not in action_keys


def test_recovery_advisor_emits_execution_telemetry_pipeline_repair_during_insufficient_data(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "degraded",
            "overall": {"attempts_total": 140},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_negative_attempt_share": 0.21,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "partial_weather_metrics",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "missing",
                "execution_cost_meets_candidate_samples": False,
                "execution_cost_meets_quote_coverage": False,
            },
            "blocking_factors": [],
            "data_pipeline_gaps": [
                "missing_blocker_audit_artifact",
                "missing_execution_cost_tape_artifact",
                "missing_ws_state_artifact",
                "missing_execution_journal_data",
            ],
            "data_sources": {
                "blocker_audit": {
                    "status": "missing",
                    "source": "",
                },
                "execution_cost_tape": {
                    "status": "missing",
                    "source": "",
                },
            },
            "pipeline_backlog": [
                {
                    "id": "execution_cost_tape",
                    "data_sources": [
                        "kalshi_ws_state_latest.json",
                        "outputs/kalshi_execution_journal.sqlite3",
                        "health/execution_cost_tape_latest.json",
                    ],
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "insufficient_data"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "bootstrap_shadow_trade_intents" in action_keys
    assert "repair_execution_telemetry_pipeline" in action_keys
    assert action_keys.index("bootstrap_shadow_trade_intents") < action_keys.index(
        "repair_execution_telemetry_pipeline"
    )


def test_recovery_advisor_prioritizes_hard_block_overconcentration_rebalance_before_refresh(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recovery_latest = tmp_path / "health" / "recovery" / "recovery_latest.json"
    recovery_latest.parent.mkdir(parents=True, exist_ok=True)
    recovery_latest.write_text(
        json.dumps(
            {
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:missing_script",
                ]
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 30,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "weather_pattern_multi_bucket_hard_block": 8,
                    "weather_pattern_negative_regime_bucket_suppressed": 7,
                    "inside_cutoff_window": 3,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "restore_stage_timeout_guardrail_script" in action_rows
    assert "rebalance_weather_pattern_hard_block_pressure" in action_rows
    assert "refresh_market_horizon_inputs" in action_rows
    assert action_keys.index("restore_stage_timeout_guardrail_script") < action_keys.index(
        "rebalance_weather_pattern_hard_block_pressure"
    )
    assert action_keys.index("rebalance_weather_pattern_hard_block_pressure") < action_keys.index(
        "refresh_market_horizon_inputs"
    )
    command_hint = str(action_rows["rebalance_weather_pattern_hard_block_pressure"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-trader" in command_hint
    assert "--intents-only" in command_hint
    assert "--weather-pattern-hardening-enabled" in command_hint
    assert "--weather-pattern-negative-bucket-suppression-enabled" in command_hint
    assert "--weather-pattern-negative-bucket-suppression-min-samples 20" in command_hint
    assert "--weather-pattern-negative-bucket-suppression-negative-expectancy-threshold -0.08" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-weather-pattern" in command_hint
    assert "python3 -m betbot.cli decision-matrix-hardening" in command_hint
    assert "--output-dir" in command_hint


def test_recovery_advisor_omits_hard_block_overconcentration_rebalance_when_pressure_is_low(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 80,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "weather_pattern_multi_bucket_hard_block": 4,
                    "weather_pattern_negative_regime_bucket_suppressed": 3,
                    "inside_cutoff_window": 3,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.88,
                    "stale_metar_negative_attempt_share": 0.60,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    action_rows = {
        str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]
    }
    assert "refresh_market_horizon_inputs" in action_rows
    assert "rebalance_weather_pattern_hard_block_pressure" not in action_rows


def test_recovery_advisor_emits_reduce_stale_metar_action_for_trade_plan_stale_blockers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 31,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "metar_observation_stale": 12,
                    "expected_edge_below_min": 19,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 350},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.22,
                    "stale_metar_attempt_share": 0.23,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "reduce_stale_metar_pressure" in action_rows
    command_hint = str(action_rows["reduce_stale_metar_pressure"].get("command_hint"))
    assert command_hint.startswith("python3 -m betbot.cli kalshi-temperature-metar-ingest ")
    assert "--output-dir" in command_hint


def test_recovery_advisor_emits_weather_confidence_adjusted_signal_repair_action_when_fallback_is_persistent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.22,
                    "stale_metar_negative_attempt_share": 0.20,
                    "stale_metar_attempt_share": 0.24,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "weather_confidence_adjusted_raw_fallback_active": True,
                "weather_confidence_adjusted_raw_fallback_consecutive_count": 4,
                "weather_confidence_adjusted_raw_fallback_persistent": True,
            },
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "repair_weather_confidence_adjusted_signal_pipeline" in action_rows
    command_hint = str(action_rows["repair_weather_confidence_adjusted_signal_pipeline"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-weather-pattern" in command_hint
    assert "python3 -m betbot.cli decision-matrix-hardening" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-recovery-advisor" in command_hint
    assert "--output-dir" in command_hint


def test_recovery_advisor_does_not_emit_weather_confidence_adjusted_signal_repair_action_without_persistence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 420},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.22,
                    "stale_metar_negative_attempt_share": 0.20,
                    "stale_metar_attempt_share": 0.24,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "weather_confidence_adjusted_raw_fallback_active": True,
                "weather_confidence_adjusted_raw_fallback_consecutive_count": 1,
                "weather_confidence_adjusted_raw_fallback_persistent": False,
            },
            "thresholds": {
                "weather_confidence_adjusted_fallback_consecutive_threshold": 3,
            },
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_cleared"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "repair_weather_confidence_adjusted_signal_pipeline" not in action_keys


def test_recovery_advisor_emits_expected_edge_probe_action_with_diagnostic_disables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "expected_edge_below_min": 14,
                    "historical_profitability_expected_edge_below_min": 9,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 350},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.62,
                    "stale_metar_negative_attempt_share": 0.22,
                    "stale_metar_attempt_share": 0.23,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_soft",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "probe_expected_edge_floor_with_hardening_disabled" in action_rows
    command_hint = str(action_rows["probe_expected_edge_floor_with_hardening_disabled"].get("command_hint"))
    assert command_hint.startswith("python3 -m betbot.cli kalshi-temperature-trader ")
    assert "--output-dir" in command_hint
    assert "--intents-only" in command_hint
    assert "--no-weather-pattern-risk-off-enabled" in command_hint
    assert "--no-weather-pattern-negative-bucket-suppression-enabled" in command_hint
    assert "--disable-weather-pattern-hardening" in command_hint
    assert "--disable-historical-selection-quality" in command_hint
    assert "--disable-enforce-probability-edge-thresholds" in command_hint

    assert "apply_expected_edge_relief_shadow_profile" in action_rows
    relief_command = str(action_rows["apply_expected_edge_relief_shadow_profile"].get("command_hint"))
    assert relief_command.startswith("python3 -m betbot.cli kalshi-temperature-trader ")
    assert "--output-dir" in relief_command
    assert "--intents-only" in relief_command
    assert "--disable-weather-pattern-hardening" in relief_command
    assert "--no-weather-pattern-risk-off-enabled" in relief_command
    assert "--no-weather-pattern-negative-bucket-suppression-enabled" in relief_command
    assert "--disable-historical-selection-quality" in relief_command
    assert "--disable-enforce-probability-edge-thresholds" in relief_command

    action_keys = [str(row.get("key")) for row in actions]
    assert action_keys.index("reduce_negative_expectancy_regimes") < action_keys.index(
        "probe_expected_edge_floor_with_hardening_disabled"
    )
    assert action_keys.index("probe_expected_edge_floor_with_hardening_disabled") < action_keys.index(
        "apply_expected_edge_relief_shadow_profile"
    )
    assert "plateau_break_negative_expectancy_share" not in action_rows


def test_recovery_advisor_suppresses_expected_edge_probe_and_relief_when_negative_pressure_is_high(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "intents_total": 38,
                "intents_approved": 0,
                "policy_reason_counts": {
                    "expected_edge_below_min": 14,
                    "historical_profitability_expected_edge_below_min": 9,
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 350},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.22,
                    "stale_metar_attempt_share": 0.23,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "reduce_negative_expectancy_regimes" in action_rows
    assert "plateau_break_negative_expectancy_share" in action_rows
    assert "probe_expected_edge_floor_with_hardening_disabled" not in action_rows
    assert "apply_expected_edge_relief_shadow_profile" not in action_rows


def test_recovery_advisor_emits_retune_action_when_suppression_ineffective(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 8,
                "weather_pattern_negative_regime_suppression_blocked_count": 0,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 300},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.63,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "retune_negative_regime_suppression" in action_rows
    command_hint = str(action_rows["retune_negative_regime_suppression"].get("command_hint"))
    assert command_hint.startswith("python3 -m betbot.cli kalshi-temperature-recovery-loop ")
    assert "--output-dir" in command_hint
    assert "--max-iterations 6" in command_hint
    assert "--stall-iterations 2" in command_hint
    assert "--min-gap-improvement 0.0025" in command_hint
    assert "--execute-actions" in command_hint
    assert "--weather-window-hours 336" in command_hint
    assert "--plateau-negative-regime-suppression-enabled" in command_hint
    assert "--plateau-negative-regime-suppression-min-bucket-samples 14" in command_hint
    assert "--plateau-negative-regime-suppression-expectancy-threshold -0.045" in command_hint
    assert "--plateau-negative-regime-suppression-top-n 16" in command_hint
    assert "python3 -c" not in command_hint


def test_recovery_advisor_emits_retune_action_for_suppression_overblocking(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 350},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 1.0,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.63,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    assert "retune_negative_regime_suppression" in action_rows
    command_hint = str(action_rows["retune_negative_regime_suppression"].get("command_hint"))
    assert "--plateau-negative-regime-suppression-top-n 4" in command_hint


def test_recovery_advisor_prioritizes_retune_earlier_under_severe_negative_expectancy_pressure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    (tmp_path / "kalshi_temperature_trade_intents_summary_latest.json").write_text(
        json.dumps(
            {
                "weather_pattern_negative_regime_suppression_enabled": True,
                "weather_pattern_negative_regime_suppression_active": True,
                "weather_pattern_negative_regime_suppression_status": "ready",
                "weather_pattern_negative_regime_suppression_candidate_count": 10,
                "weather_pattern_negative_regime_suppression_blocked_count": 5,
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 400},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 1.0,
                    "stale_metar_negative_attempt_share": 0.61,
                    "stale_metar_attempt_share": 0.62,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "execution_cost_tape_status": "yellow",
                "execution_cost_meets_quote_coverage": False,
                "execution_cost_quote_coverage_ratio": 0.12,
                "execution_cost_min_quote_coverage_ratio": 0.60,
            },
            "blocking_factors": [],
            "data_sources": {
                "execution_cost_tape": {
                    "status": "yellow",
                    "execution_cost_observations": {
                        "top_missing_coverage_buckets": {
                            "by_market_side": [
                                {"bucket": "KXHIGHBOS-26APR23-B75|no", "rows_without_two_sided_quote": 10}
                            ]
                        }
                    },
                }
            },
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "improve_execution_quote_coverage_shadow" in action_keys
    assert "retune_negative_regime_suppression" in action_keys
    assert "plateau_break_negative_expectancy_share" in action_keys
    assert "reduce_negative_expectancy_regimes" in action_keys
    assert action_keys.index("improve_execution_quote_coverage_shadow") < action_keys.index(
        "retune_negative_regime_suppression"
    )
    assert action_keys.index("retune_negative_regime_suppression") < action_keys.index(
        "plateau_break_negative_expectancy_share"
    )
    assert action_keys.index("plateau_break_negative_expectancy_share") < action_keys.index(
        "reduce_negative_expectancy_regimes"
    )


def test_recovery_advisor_emits_settled_outcome_coverage_action_for_explicit_blocker_in_insufficient_data(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "degraded",
            "overall": {"attempts_total": 160},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.24,
                    "stale_metar_negative_attempt_share": 0.22,
                    # `stale_metar_attempt_share` intentionally omitted to force insufficient_data.
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "partial_weather_metrics",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [
                {
                    "key": "insufficient_settled_outcomes",
                    "severity": "critical",
                    "summary": "Settled independent outcomes are below threshold.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "insufficient_data"
    actions = payload["remediation_plan"]["prioritized_actions"]
    action_keys = [str(row.get("key")) for row in actions]
    assert action_keys
    assert action_keys[0] == "bootstrap_shadow_trade_intents"
    action_rows = {str(row.get("key")): row for row in actions}
    assert "bootstrap_shadow_trade_intents" in action_rows
    bootstrap_hint = str(action_rows["bootstrap_shadow_trade_intents"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-trader" in bootstrap_hint
    assert "--intents-only" in bootstrap_hint
    assert "--disable-weather-pattern-hardening" in bootstrap_hint
    assert "--no-weather-pattern-risk-off-enabled" in bootstrap_hint
    assert "--no-weather-pattern-negative-bucket-suppression-enabled" in bootstrap_hint
    assert "--disable-historical-selection-quality" in bootstrap_hint
    assert "--disable-enforce-probability-edge-thresholds" in bootstrap_hint
    assert "increase_settled_outcome_coverage" in action_rows
    command_hint = str(action_rows["increase_settled_outcome_coverage"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-settlement-state" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-profitability" in command_hint
    assert "python3 -m betbot.cli decision-matrix-hardening" in command_hint
    assert command_hint.index("kalshi-temperature-settlement-state") < command_hint.index(
        "kalshi-temperature-settled-outcome-throughput"
    )
    assert command_hint.index("kalshi-temperature-settled-outcome-throughput") < command_hint.index(
        "kalshi-temperature-profitability"
    )
    assert command_hint.index("kalshi-temperature-profitability") < command_hint.index("decision-matrix-hardening")
    assert "--output-dir" in command_hint
    assert "increase_weather_sample_coverage" in action_rows
    assert "refresh_decision_matrix_weather_signals" in action_rows


def test_recovery_advisor_prioritizes_settled_outcome_coverage_for_stalled_growth_blocker(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 280},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.72,
                    "stale_metar_attempt_share": 0.78,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "stale_metar_concentration",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [
                {
                    "key": "settled_outcome_growth_stalled",
                    "severity": "high",
                    "summary": "Settled outcome growth has stalled.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "no_viable_config",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": True,
                        "risk_off_recommended": True,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    actions = payload["remediation_plan"]["prioritized_actions"]
    action_keys = [str(row.get("key")) for row in actions]
    assert action_keys
    assert action_keys[0] == "increase_settled_outcome_coverage"
    assert "clear_weather_risk_off_state" in action_keys
    action_rows = {str(row.get("key")): row for row in actions}
    command_hint = str(action_rows["increase_settled_outcome_coverage"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput" in command_hint


def test_recovery_advisor_emits_settled_outcome_velocity_recovery_action_when_guardrail_is_active(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 360},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.62,
                    "stale_metar_negative_attempt_share": 0.34,
                    "stale_metar_attempt_share": 0.38,
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "stable_regime",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "coverage_velocity_guardrail_active": True,
                "coverage_velocity_guardrail_cleared": False,
                "coverage_velocity_positive_streak": 1,
                "coverage_velocity_non_positive_streak": 0,
                "coverage_velocity_required_positive_streak": 2,
                "coverage_velocity_selected_growth_delta_24h": 4,
                "coverage_velocity_selected_growth_delta_7d": 6,
            },
            "blocking_factors": [
                {
                    "key": "coverage_velocity_guardrail_not_cleared",
                    "severity": "critical",
                    "summary": "Coverage velocity remains below the positive streak threshold.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "risk_off_active"
    decision_metrics = payload["metrics"]["decision_matrix"]
    assert decision_metrics["coverage_velocity_guardrail_active"] is True
    assert decision_metrics["coverage_velocity_guardrail_cleared"] is False
    assert decision_metrics["coverage_velocity_positive_streak"] == 1
    assert decision_metrics["coverage_velocity_required_positive_streak"] == 2
    action_rows = {str(row.get("key")): row for row in payload["remediation_plan"]["prioritized_actions"]}
    assert "recover_settled_outcome_velocity" in action_rows
    command_hint = str(action_rows["recover_settled_outcome_velocity"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-settled-outcome-throughput" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-profitability" in command_hint
    assert "python3 -m betbot.cli decision-matrix-hardening" in command_hint


def test_recovery_advisor_emits_settled_outcome_coverage_action_from_threshold_shortfall_fallback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "degraded",
            "overall": {"attempts_total": 150},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.23,
                    "stale_metar_negative_attempt_share": 0.21,
                    # `stale_metar_attempt_share` intentionally omitted to force insufficient_data.
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "partial_weather_metrics",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "settled_outcomes": 4,
            },
            "thresholds": {
                "min_settled_outcomes": 25,
            },
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "insufficient_data"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "bootstrap_shadow_trade_intents" in action_keys
    assert "increase_settled_outcome_coverage" in action_keys


def test_recovery_advisor_does_not_emit_settled_outcome_coverage_action_when_settled_outcomes_are_sufficient(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "degraded",
            "overall": {"attempts_total": 150},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.23,
                    "stale_metar_negative_attempt_share": 0.21,
                    # `stale_metar_attempt_share` intentionally omitted to force insufficient_data.
                },
                "risk_off_recommendation": {
                    "active": False,
                    "status": "monitor_only",
                    "reason": "partial_weather_metrics",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {
                "weather_risk_off_recommended": False,
                "settled_outcomes": 30,
            },
            "thresholds": {
                "min_settled_outcomes": 25,
            },
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    assert payload["remediation_plan"]["status"] == "insufficient_data"
    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "bootstrap_shadow_trade_intents" in action_keys
    assert "increase_settled_outcome_coverage" not in action_keys


def test_recovery_advisor_writes_output_and_latest_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 250},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.55,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_soft",
                    "reason": "regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [
                {
                    "key": "weather_global_risk_off_recommended",
                    "severity": "critical",
                    "summary": "Weather risk-off recommendation is active.",
                }
            ],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "no_viable_config",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": True,
                        "risk_off_recommended": True,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    output_file = Path(payload["output_file"])
    latest_file = Path(payload["latest_file"])
    assert output_file.exists()
    assert latest_file.exists()
    assert output_file.name.startswith("kalshi_temperature_recovery_advisor_")
    assert latest_file.name == "kalshi_temperature_recovery_advisor_latest.json"

    latest_payload = json.loads(latest_file.read_text(encoding="utf-8"))
    assert latest_payload["remediation_plan"]["status"] == payload["remediation_plan"]["status"]
    summary = advisor.summarize_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))
    assert json.loads(summary)["status"] == "ready"


def test_recovery_advisor_surfaces_recovery_watchdog_missing_script_and_prioritizes_restore_action(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recovery_latest = tmp_path / "health" / "recovery" / "recovery_latest.json"
    recovery_latest.parent.mkdir(parents=True, exist_ok=True)
    recovery_latest.write_text(
        json.dumps(
            {
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:missing_script",
                ]
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.92,
                    "stale_metar_negative_attempt_share": 0.63,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    watchdog = payload["metrics"]["recovery_watchdog"]
    assert watchdog["summary_available"] is True
    assert watchdog["summary_file_used"] == str(recovery_latest)
    assert watchdog["latest_stage_timeout_repair_action"] == "repair_coldmath_stage_timeout_guardrails:missing_script"
    assert watchdog["latest_stage_timeout_repair_status"] == "missing_script"
    assert watchdog["severe_issue"] is True

    actions = payload["remediation_plan"]["prioritized_actions"]
    assert actions
    assert actions[0]["key"] == "restore_stage_timeout_guardrail_script"
    command_hint = str(actions[0].get("command_hint"))
    assert "set_coldmath_stage_timeout_guardrails.sh" in command_hint
    assert "coldmath-hardening" in command_hint


def test_recovery_advisor_prioritizes_failed_stage_timeout_repair_before_existing_actions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    recovery_latest = tmp_path / "health" / "recovery" / "recovery_latest.json"
    recovery_latest.parent.mkdir(parents=True, exist_ok=True)
    recovery_latest.write_text(
        json.dumps(
            {
                "actions_attempted": [
                    "repair_coldmath_stage_timeout_guardrails:ok",
                    "repair_coldmath_stage_timeout_guardrails:failed",
                ]
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.62,
                    "stale_metar_attempt_share": 0.64,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    watchdog = payload["metrics"]["recovery_watchdog"]
    assert watchdog["summary_available"] is True
    assert watchdog["summary_file_used"] == str(recovery_latest)
    assert watchdog["latest_stage_timeout_repair_action"] == "repair_coldmath_stage_timeout_guardrails:failed"
    assert watchdog["latest_stage_timeout_repair_status"] == "failed"
    assert watchdog["severe_issue"] is True

    actions = payload["remediation_plan"]["prioritized_actions"]
    assert actions
    assert actions[0]["key"] == "rerun_stage_timeout_guardrail_hardening"
    command_hint = str(actions[0].get("command_hint"))
    assert "set_coldmath_stage_timeout_guardrails.sh" in command_hint
    assert "coldmath-hardening" in command_hint


def test_recovery_advisor_surfaces_recovery_effectiveness_scoreboard_from_loop_latest(
    tmp_path: Path,
    monkeypatch,
) -> None:
    loop_latest = tmp_path / "health" / "kalshi_temperature_recovery_loop_latest.json"
    loop_latest.parent.mkdir(parents=True, exist_ok=True)
    loop_latest.write_text(
        json.dumps(
            {
                "adaptive_effectiveness_thresholds": {
                    "min_executions": 3,
                    "min_worsening_ratio": 0.8,
                    "min_average_negative_share_delta": 0.0,
                },
                "action_effectiveness": {
                    "reduce_negative_expectancy_regimes": {
                        "executed_count": 5,
                        "worsening_count": 5,
                        "average_negative_share_delta": 0.04,
                    },
                    "reduce_stale_metar_pressure": {
                        "executed_count": 5,
                        "worsening_count": 1,
                        "average_negative_share_delta": -0.01,
                    },
                },
                "iteration_logs": [
                    {
                        "iteration": 1,
                        "executed_actions": [
                            {
                                "key": "reduce_negative_expectancy_regimes",
                                "status": "executed",
                                "effect_status": "no_effect",
                                "effect_reason": "trade_summary_not_updated",
                            }
                        ],
                    },
                    {
                        "iteration": 2,
                        "executed_actions": [
                            {
                                "key": "reduce_negative_expectancy_regimes",
                                "status": "executed",
                                "effect_status": "no_effect",
                                "effect_reason": "trade_summary_zero_activity",
                            },
                            {
                                "key": "reduce_stale_metar_pressure",
                                "status": "executed",
                                "effect_status": "verified",
                                "effect_reason": "",
                            },
                        ],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.63,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    effectiveness = payload["metrics"]["recovery_effectiveness"]
    assert effectiveness["summary_available"] is True
    assert effectiveness["summary_file_used"] == str(loop_latest)
    assert effectiveness["thresholds_used"] == {
        "min_executions": 3,
        "min_worsening_ratio": 0.8,
        "min_average_negative_share_delta": 0.0,
    }
    assert effectiveness["scoreboard"]["reduce_negative_expectancy_regimes"] == {
        "executed_count": 5,
        "worsening_count": 5,
        "worsening_ratio": 1.0,
        "average_negative_share_delta": 0.04,
        "persistently_harmful": True,
    }
    assert effectiveness["scoreboard"]["reduce_stale_metar_pressure"] == {
        "executed_count": 5,
        "worsening_count": 1,
        "worsening_ratio": 0.2,
        "average_negative_share_delta": -0.01,
        "persistently_harmful": False,
    }
    assert effectiveness["persistently_harmful_actions"] == ["reduce_negative_expectancy_regimes"]
    assert effectiveness["no_effect_thresholds"] == {"min_repeated_no_effect_count": 2}
    assert effectiveness["no_effect_actions"]["reduce_negative_expectancy_regimes"] == {
        "no_effect_count": 2,
        "latest_effect_reason": "trade_summary_zero_activity",
        "latest_iteration": 2,
        "reason_counts": {
            "trade_summary_not_updated": 1,
            "trade_summary_zero_activity": 1,
        },
        "repeated_no_effect": True,
    }
    assert effectiveness["repeated_no_effect_actions"] == ["reduce_negative_expectancy_regimes"]
    assert effectiveness["repeated_no_effect_blockers"] == [
        {
            "action_key": "reduce_negative_expectancy_regimes",
            "no_effect_count": 2,
            "latest_effect_reason": "trade_summary_zero_activity",
            "latest_iteration": 2,
            "reason_counts": {
                "trade_summary_not_updated": 1,
                "trade_summary_zero_activity": 1,
            },
            "summary": (
                "reduce_negative_expectancy_regimes returned no_effect 2 times "
                "(latest reason: trade_summary_zero_activity)."
            ),
        }
    ]


def test_recovery_advisor_demotes_persistently_harmful_actions_behind_non_harmful_routes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    loop_latest = tmp_path / "health" / "kalshi_temperature_recovery_loop_latest.json"
    loop_latest.parent.mkdir(parents=True, exist_ok=True)
    loop_latest.write_text(
        json.dumps(
            {
                "adaptive_effectiveness_thresholds": {
                    "min_executions": 3,
                    "min_worsening_ratio": 0.8,
                    "min_average_negative_share_delta": 0.0,
                },
                "action_effectiveness": {
                    "reduce_negative_expectancy_regimes": {
                        "executed_count": 4,
                        "worsening_count": 4,
                        "average_negative_share_delta": 0.03,
                    },
                    "reduce_stale_metar_pressure": {
                        "executed_count": 4,
                        "worsening_count": 0,
                        "average_negative_share_delta": -0.02,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.63,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert "reduce_negative_expectancy_regimes" in action_keys
    assert "reduce_stale_metar_pressure" in action_keys
    assert action_keys.index("reduce_stale_metar_pressure") < action_keys.index("reduce_negative_expectancy_regimes")
    assert payload["remediation_plan"]["demoted_actions_for_effectiveness"] == [
        "reduce_negative_expectancy_regimes"
    ]


def test_recovery_advisor_promotes_market_horizon_refresh_when_repeated_trader_no_effect_detected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    loop_latest = tmp_path / "health" / "kalshi_temperature_recovery_loop_latest.json"
    loop_latest.parent.mkdir(parents=True, exist_ok=True)
    loop_latest.write_text(
        json.dumps(
            {
                "iteration_logs": [
                    {
                        "iteration": 1,
                        "executed_actions": [
                            {
                                "key": "reduce_negative_expectancy_regimes",
                                "status": "executed",
                                "effect_status": "no_effect",
                                "effect_reason": "trade_summary_not_updated",
                            }
                        ],
                    },
                    {
                        "iteration": 2,
                        "executed_actions": [
                            {
                                "key": "reduce_negative_expectancy_regimes",
                                "status": "executed",
                                "effect_status": "no_effect",
                                "effect_reason": "trade_summary_zero_activity",
                            }
                        ],
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.63,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": False},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    effectiveness = payload["metrics"]["recovery_effectiveness"]
    assert effectiveness["repeated_no_effect_actions"] == ["reduce_negative_expectancy_regimes"]

    actions = payload["remediation_plan"]["prioritized_actions"]
    action_rows = {str(row.get("key")): row for row in actions}
    action_keys = [str(row.get("key")) for row in actions]
    assert "refresh_market_horizon_inputs" in action_rows
    assert "reduce_negative_expectancy_regimes" in action_rows
    assert action_keys.index("refresh_market_horizon_inputs") < action_keys.index("reduce_negative_expectancy_regimes")
    command_hint = str(action_rows["refresh_market_horizon_inputs"].get("command_hint"))
    assert "python3 -m betbot.cli kalshi-temperature-contract-specs" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-constraint-scan" in command_hint
    assert "python3 -m betbot.cli kalshi-temperature-settlement-state" in command_hint
    assert "--output-dir" in command_hint


def test_recovery_advisor_preserves_action_order_when_recovery_effectiveness_artifact_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_weather(*, output_dir: str, window_hours: float, min_bucket_samples: int, max_profile_age_hours: float):
        return {
            "status": "ready",
            "overall": {"attempts_total": 320},
            "profile": {
                "regime_risk": {
                    "negative_expectancy_attempt_share": 0.90,
                    "stale_metar_negative_attempt_share": 0.63,
                    "stale_metar_attempt_share": 0.66,
                },
                "risk_off_recommendation": {
                    "active": True,
                    "status": "risk_off_hard",
                    "reason": "negative_regime_pressure",
                },
            },
        }

    def fake_decision(*, output_dir: str, **_: object):
        return {
            "status": "ready",
            "observed_metrics": {"weather_risk_off_recommended": True},
            "blocking_factors": [],
        }

    def fake_growth(*, input_paths: list[str], top_n: int):
        return {
            "status": "ready",
            "search": {
                "robustness": {
                    "weather_risk": {
                        "hard_block_active": False,
                        "risk_off_recommended": False,
                    }
                }
            },
        }

    monkeypatch.setattr(advisor, "run_kalshi_temperature_weather_pattern", fake_weather)
    monkeypatch.setattr(advisor, "run_decision_matrix_hardening", fake_decision)
    monkeypatch.setattr(advisor, "run_kalshi_temperature_growth_optimizer", fake_growth)

    payload = advisor.run_kalshi_temperature_recovery_advisor(output_dir=str(tmp_path))

    effectiveness = payload["metrics"]["recovery_effectiveness"]
    assert effectiveness["summary_available"] is False
    assert effectiveness["summary_file_used"] == ""
    assert effectiveness["persistently_harmful_actions"] == []
    assert effectiveness["no_effect_thresholds"] == {"min_repeated_no_effect_count": 2}
    assert effectiveness["no_effect_actions"] == {}
    assert effectiveness["repeated_no_effect_actions"] == []
    assert effectiveness["repeated_no_effect_blockers"] == []

    action_keys = [str(row.get("key")) for row in payload["remediation_plan"]["prioritized_actions"]]
    assert action_keys.index("reduce_negative_expectancy_regimes") < action_keys.index("reduce_stale_metar_pressure")
    assert payload["remediation_plan"]["demoted_actions_for_effectiveness"] == []
