from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from betbot.ladder import LadderEvent
from betbot.types import BetCandidate, Decision


def _parse_optional_float(value: str) -> float | None:
    value = value.strip()
    if value == "":
        return None
    return float(value)


def _parse_optional_int(value: str) -> int | None:
    value = value.strip()
    if value == "":
        return None
    parsed = int(value)
    if parsed not in (0, 1):
        raise ValueError("outcome must be 0 or 1 when provided")
    return parsed


def load_candidates(path: str) -> list[BetCandidate]:
    rows: list[BetCandidate] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"timestamp", "event_id", "selection", "odds", "model_prob"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        for row in reader:
            ts = datetime.fromisoformat(row["timestamp"])
            odds = float(row["odds"])
            model_prob = float(row["model_prob"])
            decision_prob = _parse_optional_float(row.get("decision_prob", ""))
            if odds <= 1.0:
                raise ValueError(f"Odds must be > 1.0 for event_id={row['event_id']}")
            if not (0.0 <= model_prob <= 1.0):
                raise ValueError(f"model_prob must be in [0,1] for event_id={row['event_id']}")
            if decision_prob is not None and not (0.0 <= decision_prob <= 1.0):
                raise ValueError(f"decision_prob must be in [0,1] for event_id={row['event_id']}")

            rows.append(
                BetCandidate(
                    timestamp=ts,
                    event_id=row["event_id"],
                    selection=row["selection"],
                    odds=odds,
                    model_prob=model_prob,
                    decision_prob=decision_prob,
                    edge_rank_score=_parse_optional_float(row.get("edge_rank_score", "")),
                    closing_odds=_parse_optional_float(row.get("closing_odds", "")),
                    outcome=_parse_optional_int(row.get("outcome", "")),
                )
            )
    rows.sort(
        key=lambda r: (
            r.timestamp,
            -(r.edge_rank_score if r.edge_rank_score is not None else float("-inf")),
            r.event_id,
            r.selection,
        )
    )
    return rows


def write_decisions(path: Path, decisions: list[Decision]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "timestamp",
                "event_id",
                "selection",
                "odds",
                "model_prob",
                "decision_prob",
                "ev",
                "kelly_full",
                "kelly_used",
                "stake",
                "status",
                "reason",
                "bankroll_before",
                "bankroll_after",
                "pnl",
                "closing_odds",
                "outcome",
            ]
        )
        for d in decisions:
            writer.writerow(
                [
                    d.timestamp.isoformat(),
                    d.event_id,
                    d.selection,
                    f"{d.odds:.6f}",
                    f"{d.model_prob:.6f}",
                    f"{d.decision_prob:.6f}",
                    f"{d.ev:.6f}",
                    f"{d.kelly_full:.6f}",
                    f"{d.kelly_used:.6f}",
                    f"{d.stake:.2f}",
                    d.status,
                    d.reason,
                    f"{d.bankroll_before:.2f}",
                    f"{d.bankroll_after:.2f}",
                    f"{d.pnl:.2f}",
                    "" if d.closing_odds is None else f"{d.closing_odds:.6f}",
                    "" if d.outcome is None else d.outcome,
                ]
            )


def write_ladder_events(path: Path, events: list[LadderEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "timestamp",
                "rung_reached",
                "next_target",
                "total_wealth",
                "risk_wallet_before",
                "risk_wallet_after",
                "locked_vault_before",
                "locked_vault_after",
                "withdrawn",
                "planning_p",
                "success_probability_after",
                "min_success_required",
                "reason",
            ]
        )
        for event in events:
            writer.writerow(
                [
                    event.timestamp.isoformat(),
                    f"{event.rung_reached:.2f}",
                    "" if event.next_target is None else f"{event.next_target:.2f}",
                    f"{event.total_wealth:.2f}",
                    f"{event.risk_wallet_before:.2f}",
                    f"{event.risk_wallet_after:.2f}",
                    f"{event.locked_vault_before:.2f}",
                    f"{event.locked_vault_after:.2f}",
                    f"{event.withdrawn:.2f}",
                    f"{event.planning_p:.6f}",
                    "" if event.success_probability_after is None else f"{event.success_probability_after:.6f}",
                    f"{event.min_success_required:.6f}",
                    event.reason,
                ]
            )
