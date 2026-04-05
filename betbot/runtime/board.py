from __future__ import annotations


def render_board_text(board_projection: dict[str, object]) -> str:
    degraded_sources = list(board_projection.get("degraded_sources") or [])
    lines = [
        f"run_id: {board_projection.get('run_id')}",
        f"cycle_id: {board_projection.get('cycle_id')}",
        f"overall_status: {board_projection.get('overall_status')}",
        f"phase: {board_projection.get('phase')}",
        f"permission_lane: {board_projection.get('lane')}",
        f"degraded_sources: {', '.join(degraded_sources) if degraded_sources else 'none'}",
    ]
    return "\n".join(lines)
