# BETBOT Runtime Doctrine

## Operating Rules
- Live execution uses `live_execute` lane only.
- Approval artifacts are required for all live submissions.
- Non-allowlisted news can never directly gate ticket eligibility.
- Degraded cycles must still emit structured reports and board projections.

## Risk and Exposure
- Enforce bankroll and open-risk caps from policy config.
- Blocked/failed required sources must prevent live submission.
- Penalty-optional sources can degrade score but cannot silently disappear.

## Lane and Source Policy
- Lane permissions are defined in `data/policy/lanes.yaml`.
- News domain allowlist is defined in `data/policy/news_sources.yaml`.
- Repo-local config layers must be fingerprinted per run.

## Recovery
- Every blocked/degraded cycle must include `recovery_recommendation`.
- Mapping failures block affected markets first, not global cycle by default.

## Reporting Contract
- Emit `cycle_latest.json` and `board_latest.json` each cycle.
- Event JSONL is append-only source of truth.
