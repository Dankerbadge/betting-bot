from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReconcileResult:
    status: str
    open_orders: int
    mismatches: int


def reconcile_order_lifecycle(*, open_orders: int, mismatches: int) -> ReconcileResult:
    if mismatches > 0:
        return ReconcileResult(status="failed", open_orders=int(open_orders), mismatches=int(mismatches))
    return ReconcileResult(status="ok", open_orders=int(open_orders), mismatches=0)
