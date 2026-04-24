from __future__ import annotations

from betbot.kalshi_temperature_trader import (
    TemperaturePolicyGate,
    TemperatureTradeIntent,
    _entry_price_min_probability_floor,
)


def _build_intent(
    *,
    max_entry_price_dollars: float,
    settlement_confidence_score: float = 0.82,
    **overrides: object,
) -> TemperatureTradeIntent:
    payload: dict[str, object] = {
        "intent_id": "intent-hardening",
        "captured_at": "2026-04-08T12:00:00+00:00",
        "policy_version": "temperature_policy_v1",
        "underlying_key": "KXHIGHNY|KNYC|2026-04-08",
        "series_ticker": "KXHIGHNY",
        "event_ticker": "KXHIGHNY-26APR08",
        "market_ticker": "KXHIGHNY-26APR08-B72",
        "market_title": "72F or above",
        "settlement_station": "KNYC",
        "settlement_timezone": "America/New_York",
        "target_date_local": "2026-04-08",
        "constraint_status": "yes_impossible",
        "constraint_reason": "Observed max already above threshold",
        "side": "no",
        "max_entry_price_dollars": max_entry_price_dollars,
        "intended_contracts": 1,
        "settlement_confidence_score": settlement_confidence_score,
        "observed_max_settlement_quantized": 74.0,
        "close_time": "2026-04-09T00:00:00Z",
        "hours_to_close": 12.0,
        "spec_hash": "abc123",
        "metar_snapshot_sha": "sha123",
        "metar_observation_time_utc": "2026-04-08T11:55:00Z",
        "metar_observation_age_minutes": 5.0,
        "market_snapshot_seq": 10,
    }
    payload.update(overrides)
    return TemperatureTradeIntent(**payload)


def test_entry_price_probability_floor_increases_with_price() -> None:
    low = _entry_price_min_probability_floor(0.55)
    high = _entry_price_min_probability_floor(0.99)
    assert low is None
    assert isinstance(high, float)
    assert high > 0.95
    assert 0.0 < high < 1.0


def test_gate_applies_entry_price_probability_floor_only_when_enabled() -> None:
    intent = _build_intent(max_entry_price_dollars=0.99)

    disabled = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=None,
        enforce_probability_edge_thresholds=True,
        enforce_entry_price_probability_floor=False,
        enforce_sparse_evidence_hardening=False,
        min_expected_edge_net=None,
        fallback_min_expected_edge_net=-1.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_interval_consistency=False,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert disabled.min_probability_confidence_required == 0.6

    enabled = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=None,
        enforce_probability_edge_thresholds=True,
        enforce_entry_price_probability_floor=True,
        enforce_sparse_evidence_hardening=False,
        min_expected_edge_net=None,
        fallback_min_expected_edge_net=-1.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_interval_consistency=False,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert isinstance(enabled.min_probability_confidence_required, float)
    assert enabled.min_probability_confidence_required > 0.6
    assert "min_probability_confidence_raised_for_entry_price" in enabled.decision_notes


def test_sparse_evidence_hardening_raises_probability_and_edge_thresholds() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.88,
        forecast_model_status="degraded",
        taf_status="",
        taf_volatility_score=2.0,
        yes_possible_gap=2.5,
        consensus_weighted_support_ratio=0.0,
        consensus_profile_support_count=0,
        speci_shock_active=False,
        speci_shock_confidence=None,
        speci_shock_weight=None,
    )

    base = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.78,
        min_expected_edge_net=0.01,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_interval_consistency=False,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]

    hardened = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.78,
        min_expected_edge_net=0.01,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=True,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_interval_consistency=False,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]

    assert isinstance(base.min_probability_confidence_required, float)
    assert isinstance(hardened.min_probability_confidence_required, float)
    assert hardened.min_probability_confidence_required > base.min_probability_confidence_required

    assert isinstance(base.min_expected_edge_net_required, float)
    assert isinstance(hardened.min_expected_edge_net_required, float)
    assert hardened.min_expected_edge_net_required >= base.min_expected_edge_net_required
    assert "sparse_evidence_probability_raise=" in hardened.decision_notes


def test_policy_gate_hard_blocks_signal_bucket_with_poor_historical_quality() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.985,
        settlement_confidence_score=0.7,
        side="yes",
        constraint_status="yes_interval_certain",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "signal_type": {
                "yes_interval_certain": {
                    "penalty_ratio": 0.95,
                    "boost_ratio": 0.0,
                    "samples": 32,
                }
            }
        },
    }
    decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert decision.decision_reason == "historical_quality_signal_type_hard_block"
    assert bool(decision.historical_quality_signal_hard_block_active)
    assert "historical_quality_signal_hard_block=yes_interval_certain" in decision.decision_notes


def test_policy_gate_hard_blocks_station_hour_bucket_with_poor_historical_quality() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.985,
        settlement_confidence_score=0.69,
        side="yes",
        constraint_status="yes_likely_locked",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "station": {
                "KNYC": {
                    "penalty_ratio": 0.88,
                    "boost_ratio": 0.0,
                    "samples": 28,
                }
            },
            "local_hour": {
                "7": {
                    "penalty_ratio": 0.84,
                    "boost_ratio": 0.0,
                    "samples": 24,
                }
            },
        },
    }
    decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert decision.decision_reason == "historical_quality_station_hour_hard_block"
    assert bool(decision.historical_quality_station_hour_hard_block_active)
    assert "historical_quality_station_hour_hard_block=station:KNYC:local_hour:7" in decision.decision_notes


def test_policy_gate_expectancy_hardening_raises_edge_and_probability_thresholds() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.97,
        settlement_confidence_score=0.72,
        side="no",
        constraint_status="yes_impossible",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "signal_type": {
                "yes_impossible": {
                    "penalty_ratio": 0.32,
                    "boost_ratio": 0.0,
                    "samples": 44,
                    "expectancy_per_trade": -0.24,
                    "win_rate": 0.36,
                }
            }
        },
    }
    base_decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        enforce_historical_expectancy_edge_hardening=False,
        enforce_historical_bucket_hard_blocks=False,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    hardened_decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        enforce_historical_expectancy_edge_hardening=True,
        enforce_historical_bucket_hard_blocks=False,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert isinstance(base_decision.min_probability_confidence_required, float)
    assert isinstance(hardened_decision.min_probability_confidence_required, float)
    assert (
        hardened_decision.min_probability_confidence_required
        > base_decision.min_probability_confidence_required
    )
    assert isinstance(base_decision.min_expected_edge_net_required, float)
    assert isinstance(hardened_decision.min_expected_edge_net_required, float)
    assert hardened_decision.min_expected_edge_net_required > base_decision.min_expected_edge_net_required
    assert (hardened_decision.historical_expectancy_pressure_score or 0.0) > 0.0
    assert "historical_expectancy_edge_raise=" in hardened_decision.decision_notes


def test_policy_gate_hard_blocks_multi_bucket_negative_expectancy_profile() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.985,
        settlement_confidence_score=0.72,
        side="no",
        constraint_status="yes_impossible",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "signal_type": {
                "yes_impossible": {
                    "penalty_ratio": 0.40,
                    "boost_ratio": 0.0,
                    "samples": 64,
                    "expectancy_per_trade": -0.23,
                    "win_rate": 0.41,
                }
            },
            "station": {
                "KNYC": {
                    "penalty_ratio": 0.34,
                    "boost_ratio": 0.0,
                    "samples": 58,
                    "expectancy_per_trade": -0.19,
                    "win_rate": 0.44,
                }
            },
        },
    }
    decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        enforce_historical_bucket_hard_blocks=False,
        enforce_historical_expectancy_edge_hardening=False,
        enforce_historical_expectancy_hard_blocks=True,
        historical_expectancy_hard_block_negative_threshold=-0.10,
        historical_expectancy_hard_block_min_samples=24,
        historical_expectancy_hard_block_min_bucket_matches=2,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert decision.decision_reason == "historical_expectancy_hard_block"
    assert bool(decision.historical_expectancy_hard_block_active)
    assert "historical_expectancy_hard_block_triggered=true" in decision.decision_notes
    assert "historical_expectancy_hard_block_hits=signal_type:yes_impossible" in decision.decision_notes


def test_policy_gate_hard_blocks_single_bucket_on_negative_expectancy_even_with_high_win_rate() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.985,
        settlement_confidence_score=0.75,
        side="no",
        constraint_status="yes_impossible",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "signal_type": {
                "yes_impossible": {
                    "penalty_ratio": 0.36,
                    "boost_ratio": 0.0,
                    "samples": 72,
                    "expectancy_per_trade": -0.31,
                    "win_rate": 0.83,
                }
            }
        },
    }
    decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        enforce_historical_bucket_hard_blocks=False,
        enforce_historical_expectancy_edge_hardening=False,
        enforce_historical_expectancy_hard_blocks=True,
        historical_expectancy_hard_block_negative_threshold=-0.10,
        historical_expectancy_hard_block_min_samples=24,
        historical_expectancy_hard_block_min_bucket_matches=1,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert decision.decision_reason == "historical_expectancy_hard_block"
    assert bool(decision.historical_expectancy_hard_block_active)
    assert "historical_expectancy_hard_block_triggered=true" in decision.decision_notes


def test_policy_gate_does_not_hard_block_when_bucket_missing_expectancy_and_win_rate() -> None:
    intent = _build_intent(
        max_entry_price_dollars=0.985,
        settlement_confidence_score=0.75,
        side="no",
        constraint_status="yes_impossible",
    )
    profile = {
        "enabled": True,
        "status": "ready",
        "bucket_profiles": {
            "station": {
                "KNYC": {
                    "penalty_ratio": 0.45,
                    "boost_ratio": 0.0,
                    "samples": 250,
                    # No expectancy_per_trade / win_rate fields: these buckets
                    # should not trigger expectancy hard blocks.
                }
            }
        },
    }
    decision = TemperaturePolicyGate(
        min_settlement_confidence=0.6,
        min_probability_confidence=0.6,
        min_expected_edge_net=0.0,
        min_edge_to_risk_ratio=None,
        fallback_min_edge_to_risk_ratio=-1.0,
        enforce_probability_edge_thresholds=True,
        enforce_sparse_evidence_hardening=False,
        enforce_interval_consistency=False,
        enforce_historical_bucket_hard_blocks=False,
        enforce_historical_expectancy_edge_hardening=False,
        enforce_historical_expectancy_hard_blocks=True,
        historical_expectancy_hard_block_negative_threshold=-0.10,
        historical_expectancy_hard_block_min_samples=24,
        historical_expectancy_hard_block_min_bucket_matches=1,
        historical_selection_quality_profile=profile,
        require_market_snapshot_seq=True,
        require_metar_snapshot_sha=True,
    ).evaluate(intents=[intent])[0]
    assert not bool(decision.historical_expectancy_hard_block_active)
    assert decision.decision_reason != "historical_expectancy_hard_block"
