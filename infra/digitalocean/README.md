# DigitalOcean Setup (Temperature Shadow Stack)

This runbook is for a fast, repeatable launch on a new Ubuntu 24.04 droplet.

## Recommended Droplet Choice

- Region: `NYC` (best latency from Parsippany/NJ area)
- Type: `Dedicated CPU -> CPU-Optimized (Regular CPU)`
- Preferred size target: `8 vCPU / 16 GB / 100 GB SSD / 6 TB transfer`
- Fallback if capacity constrained: `4 vCPU / 8 GB` dedicated until your requested tier is granted.

## 1) Create Droplet

- Image: `Ubuntu 24.04 (LTS) x64`
- Auth: SSH key only (disable password login)
- Add monitoring and backups

After droplet is created, SSH in as your deploy user.

## 2) Clone Repo On Droplet

```bash
git clone <your_repo_url> "$HOME/betting-bot"
cd "$HOME/betting-bot"
```

## 3) Bootstrap Runtime

```bash
bash infra/digitalocean/bootstrap_ubuntu_2404.sh "$HOME/betting-bot"
```

This installs OS packages, creates `.venv`, installs dependencies, and seeds `/etc/betbot/temperature-shadow.env`.

## 4) Add Secrets

Copy your credential file and lock permissions:

```bash
sudo cp data/research/account_onboarding.local.env /etc/betbot/account_onboarding.local.env
sudo chmod 600 /etc/betbot/account_onboarding.local.env
```

If your env references private key/token files, copy those paths/files to the droplet too.

## 5) Configure Runtime Env

Edit `/etc/betbot/temperature-shadow.env`:

- `BETBOT_ROOT`
- `OUTPUT_DIR`
- `BETBOT_ENV_FILE`
- optional `METAR_AGE_POLICY_JSON`
- optional `SPECI_CALIBRATION_JSON`
- optional alpha-worker knobs (`ALPHA_*`)
- optional ColdMath hardening knobs (`COLDMATH_*`) for passive snapshot + ingest + replication-plan cycles
- `MAX_MARKETS` (breadth/per-cycle scan budget)
- optional adaptive scan budget knobs (`ADAPTIVE_MAX_MARKETS_*`)
- optional overlap-pressure scan knobs (`ADAPTIVE_RANGE_POSSIBLE_*`)
- optional adaptive interval-gap knobs (`ADAPTIVE_INTERVAL_GAP_*`)
- optional adaptive replan-cooldown knobs (`REPLAN_COOLDOWN_ADAPTIVE_*`)
- optional adaptive replan-throughput backstop knobs (`REPLAN_BACKSTOP_ADAPTIVE_*`)
- `CONTRACT_SPECS_REFRESH_SECONDS` (recommend 900)
- `SETTLEMENT_REFRESH_SECONDS` (recommend 180)
- optional adaptive settlement refresh knobs (`SETTLEMENT_REFRESH_ADAPTIVE_*`, `SETTLEMENT_TOP_N_PRESSURE_*`, `FINAL_REPORT_CACHE_TTL_MINUTES_PRESSURE`)
- optional persistent settlement-backlog escalation knobs (`SETTLEMENT_PRESSURE_ESCALATION_*`)
- quality-gate enforcement knobs (`ENFORCE_PROBABILITY_EDGE_THRESHOLDS`, `ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR`, `FALLBACK_MIN_PROBABILITY_CONFIDENCE`, `FALLBACK_MIN_EXPECTED_EDGE_NET`)
- optional approval-guardrail controller knobs (`APPROVAL_GUARDRAIL_ESCALATION_*`, `APPROVAL_GUARDRAIL_RELAXATION_*`) for bounded above-band tightening + below-band starvation relief (including latest-sample override when rolling fallback masks short starvation bursts)
- optional edge-starvation entry-cap relief knobs (`ADAPTIVE_ENTRY_CAP_RELIEF_*`) including persistent streak escalation and settlement-backlog dampening controls
- near-stale quality guard knobs (`METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO`, `METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN`, `METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN`)
- risk/cadence values (defaults are conservative shadow settings)

Recommended high-breadth shadow-sim baseline on 4 vCPU:

- `MAX_ORDERS=80`
- `MAX_INTENTS_PER_UNDERLYING=16`
- `MAX_MARKETS=900` (base), with adaptive breadth enabled
- `ADAPTIVE_MAX_MARKETS_ENABLED=1`
- `ADAPTIVE_MAX_MARKETS_MAX=9000`
- `REPLAN_MARKET_SIDE_COOLDOWN_MINUTES=1`
- `REPLAN_MARKET_SIDE_PRICE_CHANGE_OVERRIDE_DOLLARS=0.02`
- `REPLAN_MARKET_SIDE_ALPHA_CHANGE_OVERRIDE=0.2`
- `REPLAN_MARKET_SIDE_CONFIDENCE_CHANGE_OVERRIDE=0.03`
- `REPLAN_MARKET_SIDE_MIN_OBSERVATION_ADVANCE_MINUTES=2`
- `REPLAN_MARKET_SIDE_MIN_ORDERS_BACKSTOP=1`
- `REPLAN_COOLDOWN_ADAPTIVE_ENABLED=1`
- `REPLAN_COOLDOWN_ADAPTIVE_MINUTES_MIN=1`
- `REPLAN_COOLDOWN_ADAPTIVE_MINUTES_MAX=12`
- `REPLAN_COOLDOWN_ADAPTIVE_TARGET_BLOCKED_RATIO=0.55`
- `REPLAN_COOLDOWN_ADAPTIVE_BAND_RATIO=0.15`
- `REPLAN_BACKSTOP_ADAPTIVE_ENABLED=1`
- `REPLAN_BACKSTOP_ADAPTIVE_MIN=10`
- `REPLAN_BACKSTOP_ADAPTIVE_MAX=16`
- `REPLAN_BACKSTOP_ADAPTIVE_TARGET_BLOCKED_RATIO=0.70`
- `REPLAN_BACKSTOP_ADAPTIVE_TARGET_PLANNED_RATIO=0.30`
- `REPLAN_BACKSTOP_ADAPTIVE_REQUIRE_BREADTH_RISE=1`
- `REPLAN_BACKSTOP_ADAPTIVE_MIN_UNIQUE_MARKET_SIDES_FOR_UP=4`
- `REPLAN_BACKSTOP_ADAPTIVE_MIN_UNIQUE_UNDERLYINGS_FOR_UP=2`
- `REPLAN_BACKSTOP_ADAPTIVE_STAGNATION_RELIEF_ENABLED=1`
- `REPLAN_BACKSTOP_ADAPTIVE_STAGNATION_RELIEF_MIN_CYCLES=4`
- `ADAPTIVE_BLOCKER_METRICS_MAX_AGE_SECONDS=300`
- `ADAPTIVE_SETTLEMENT_METRICS_MAX_AGE_SECONDS=900`
- `ADAPTIVE_REPLAN_METRICS_MAX_AGE_SECONDS=300`
- `ADAPTIVE_RANGE_POSSIBLE_RATE_HIGH_THRESHOLD=0.08`
- `ADAPTIVE_RANGE_POSSIBLE_RATE_EXTREME_THRESHOLD=0.15`
- `ADAPTIVE_RANGE_POSSIBLE_ROLLING_WINDOW_TAG=4h`
- `ADAPTIVE_RANGE_POSSIBLE_ROLLING_METRICS_MAX_AGE_SECONDS=21600`
- `ADAPTIVE_INTERVAL_GAP_ENABLED=0` (enable only after review; this relaxes yes-side gap bounds)
- `ADAPTIVE_INTERVAL_GAP_MIN=0.0`
- `ADAPTIVE_INTERVAL_GAP_MAX=1.0`
- `ADAPTIVE_INTERVAL_GAP_STEP_UP=0.05`
- `ADAPTIVE_INTERVAL_GAP_STEP_DOWN=0.03`
- `ADAPTIVE_INTERVAL_GAP_MIN_INTENTS=120`
- `ADAPTIVE_INTERVAL_GAP_TARGET_RANGE_RATE=0.10`
- `ADAPTIVE_INTERVAL_GAP_EXTREME_RANGE_RATE=0.18`
- `ADAPTIVE_INTERVAL_GAP_MAX_STALE_RATE=0.18`
- `ADAPTIVE_INTERVAL_GAP_MIN_APPROVAL_RATE=0.08`
- `PLANNING_BANKROLL_DOLLARS=1000`
- `DAILY_RISK_CAP_PCT=10.0`
- `MAX_TOTAL_DEPLOYED_PCT=0.65`
- `MAX_SAME_STATION_EXPOSURE_PCT=0.35`
- `MAX_SAME_HOUR_CLUSTER_EXPOSURE_PCT=0.45`
- `MAX_SAME_UNDERLYING_EXPOSURE_PCT=0.30`
- `MAX_LIVE_SUBMISSIONS_PER_DAY=100` (only relevant if live mode is enabled)

If cooldown throughput becomes the dominant blocker (for example, approved intents stay high but
`intents_selected_for_plan` stays pinned near the same low value), apply this bounded release preset:

- `REPLAN_BACKSTOP_ADAPTIVE_MAX=24`
- `REPLAN_BACKSTOP_ADAPTIVE_STEP_UP=2`
- `REPLAN_BACKSTOP_ADAPTIVE_TARGET_BLOCKED_RATIO=0.65`
- `REPLAN_BACKSTOP_ADAPTIVE_REQUIRE_BREADTH_RISE=0`
- `REPLAN_BACKSTOP_ADAPTIVE_STAGNATION_RELIEF_MIN_CYCLES=2`

This keeps the market-side cooldown in place while allowing the adaptive backstop to release extra
throughput when quality is clean (low stale pressure, healthy approval rate).

On the current shared-4 droplet profile, a cooldown probe showed:

- `8m/4m/2m` produced the same throughput (`planned=10` on the frozen snapshot),
- `1m` increased throughput to `planned=15` with `15` unique market-sides,
- `0m` removed the guardrail entirely (`planned=26`), which is not recommended as a default.

Recommended settlement-pressure baseline on 4 vCPU:

- `SETTLEMENT_REFRESH_SECONDS=180`
- `SETTLEMENT_REFRESH_ADAPTIVE_ENABLED=1`
- `SETTLEMENT_REFRESH_MIN_SECONDS=60`
- `SETTLEMENT_REFRESH_PRESSURE_MIN_COUNT=8`
- `SETTLEMENT_REFRESH_PRESSURE_MIN_RATE=0.03`
- `SETTLEMENT_REFRESH_PRESSURE_MIN_PENDING=1`
- `SETTLEMENT_TOP_N=80`
- `SETTLEMENT_TOP_N_PRESSURE_MAX=260`
- `SETTLEMENT_TOP_N_PRESSURE_BLOCKED_DIVISOR=6`
- `SETTLEMENT_TOP_N_PRESSURE_BLOCKED_UNDERLYING_MULTIPLIER=3`
- `SETTLEMENT_TOP_N_PRESSURE_HOLD_FLOOR=60`
- `FINAL_REPORT_CACHE_TTL_MINUTES=15`
- `FINAL_REPORT_CACHE_TTL_MINUTES_PRESSURE=2`
- `SETTLEMENT_PRESSURE_HOLD_CYCLES=3`
- `SETTLEMENT_PRESSURE_FORCE_REFRESH_SECONDS=15`
- `SETTLEMENT_PRESSURE_RETRY_ATTEMPTS=4`

## 6) Preflight Before Service Install

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/preflight_temperature_shadow.sh
```

Only continue if preflight has zero failures.

## 7) Install And Start systemd Service

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_shadow.sh
```

## 8) Optional: Install Periodic Readiness Reporting Timer

This emits `live-readiness`, `bankroll-validation`, and `alpha-gap-report` artifacts
on a fixed schedule (default every 4 hours). It also refreshes the rolling
weekly blocker audit when `BLOCKER_AUDIT_ENABLED=1`:

- The reporting runner is lock-protected (`$OUTPUT_DIR/.readiness_reports.lock`)
  so overlapping timer/manual invocations skip safely instead of colliding.
- Progress is written to `$OUTPUT_DIR/health/readiness_runner_latest.json` with
  stage + heartbeat updates (useful when runs are long but healthy).

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_reporting.sh
```

Use a custom cadence (example: every 2 hours):

```bash
bash infra/digitalocean/install_systemd_temperature_reporting.sh 2h
```

## 8a) Optional: Install Dedicated 12h Alpha Summary Timer

This emits a concise alpha summary artifact and sends one Discord message every 12h:

- rolling 12h alpha flow (intents, approvals, planned)
- independent breadth and concentration
- live-readiness horizon scorecard (`1d/7d/14d/21d/28d/3mo/6mo/1yr`) plus overall recommendation
- capital deployment confidence score (`0-100`) with banded guidance (`SHADOW_ONLY/SHADOW_PLUS/PILOT_CANDIDATE/SCALE_CANDIDATE`)
- conservative score cap when settled independent outcomes are still zero (prevents inflated confidence before settlement-aged evidence)
- pilot-threshold delta block (`65.0` target): total points needed plus top horizon drivers
- pilot gate checklist (`14d+21d`): checks passed/open, minimum flips needed, and top open gate reasons
- projected bankroll PnL/ROI for the window
- persistent trial balance check-in (`1d/7d/14d/21d/28d/3mo/6mo/1yr`) using existing reset/refill state
- last settled selection visibility (unique market-side + unique order-instance) with if-live counterfactual PnL
- duplicate shadow-order reuse pressure since reset (planned rows vs unique order instances vs duplicate-row ratio)
- ops webhook heartbeat includes decision-matrix lane status (`strict`, `bootstrap`, or `bootstrap blocked`) with bootstrap expiry timing when available
- 3–5 optimization suggestions based on blockers + alpha-gap context
- guardrail recommendation sampling now prefers all in-window intents summaries/CSVs and falls back to latest-only only when window sampling is unavailable

Trial-balance artifact hardening:

- `duplicate_shadow_order_ids` is capped to top offenders (top 250) to avoid oversized JSON payloads
- full duplicate scope is still reported via:
  - `duplicate_shadow_order_ids_total_unique`
  - `duplicate_shadow_order_ids_returned`
  - `duplicate_shadow_order_ids_truncated`
  - `duplicate_shadow_order_ids_truncated_count`

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_alpha_summary.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_alpha_summary.sh 12h
```

Main env knobs in `/etc/betbot/temperature-shadow.env`:

- `ALPHA_SUMMARY_HOURS=12`
- `ALPHA_SUMMARY_TOP_N=10`
- `ALPHA_SUMMARY_REFERENCE_BANKROLL_DOLLARS=1000`
- `ALPHA_SUMMARY_SLIPPAGE_BPS_LIST=0,5,10`
- `ALPHA_SUMMARY_SUGGESTION_COUNT=5`
- `ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_ENABLED=0`
- `ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_STREAK_REQUIRED=1`
- `ALPHA_SUMMARY_APPROVAL_AUTO_APPLY_MIN_ROWS=1000`
- `ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ENABLED=1`
- `ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_STREAK_REQUIRED=3`
- `ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_STREAK_REQUIRED=2`
- `ALPHA_SUMMARY_APPROVAL_AUTO_RELEASE_ZERO_APPROVED_MIN_INTENTS=100`
- `ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_RATIO_TO_WINDOW=0.25`
- `ALPHA_SUMMARY_APPROVAL_GUARDRAIL_BASIS_MIN_ABS_INTENTS=1000`
- `ALPHA_SUMMARY_DEPLOY_CONFIDENCE_NEGATIVE_PNL_CAP=49`
- `ALPHA_SUMMARY_DEPLOY_CONFIDENCE_HYSA_FAIL_CAP=54`
- `ALPHA_SUMMARY_DEPLOY_CONFIDENCE_HYSA_FAIL_REQUIRES_SETTLED=1`
- `ALPHA_SUMMARY_QUALITY_DRIFT_APPROVAL_DELTA_PP_MIN=3.0`
- `ALPHA_SUMMARY_QUALITY_DRIFT_MIN_INTENTS_PER_WINDOW=1000`
- `ALPHA_SUMMARY_QUALITY_DRIFT_MAX_RESOLVED_SIDES_DELTA=0`
- `ALPHA_SUMMARY_GATE_COVERAGE_ALERT_MIN_APPROVED_ROWS=1000`
- `ALPHA_SUMMARY_GATE_COVERAGE_EXPECTED_EDGE_MIN=0.60`
- `ALPHA_SUMMARY_GATE_COVERAGE_PROBABILITY_MIN=0.60`
- `ALPHA_SUMMARY_GATE_COVERAGE_ALPHA_MIN=0.30`
- `ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_ENABLED=0`
- `ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_STREAK_REQUIRED=2`
- `ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_ROWS=1000`
- `ALPHA_SUMMARY_GATE_COVERAGE_AUTO_APPLY_MIN_LEVEL=red`
- `ALPHA_SUMMARY_SEND_WEBHOOK=1`
- `ALPHA_SUMMARY_DISCORD_MODE=concise` (`concise` or `detailed`)
- `ALPHA_SUMMARY_WEBHOOK_URL` (falls back to `ALERT_WEBHOOK_URL`)

Recommended default for 12h summary cadence: keep both auto-apply toggles disabled and use alert-only mode; enable auto-apply only if you run summary cadence at much shorter intervals and explicitly want automatic threshold mutation.

## 8aa) Optional: Install Weekly Top-20 Blocker Audit Timer

This emits a rolling weekly blocker audit artifact and optional Discord message:

- top non-approved reasons (count + share of blocked + share of intents)
- largest blocker and recommended close action
- top-5 action list so threshold changes stay tied to blocker closure
- decision-matrix lane row (`strict`, `bootstrap`, or `bootstrap blocked`) with bootstrap expiry timing when available

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_blocker_audit.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_blocker_audit.sh 3d
```

Main env knobs in `/etc/betbot/temperature-shadow.env`:

- `BLOCKER_AUDIT_HOURS=168`
- `BLOCKER_AUDIT_TOP_N=20`
- `BLOCKER_AUDIT_SEND_WEBHOOK=1`
- `BLOCKER_AUDIT_DISCORD_MODE=concise` (`concise` or `detailed`)
- `BLOCKER_AUDIT_WEBHOOK_URL` (falls back to `ALPHA_SUMMARY_WEBHOOK_URL`, then `ALERT_WEBHOOK_URL`)
- `BLOCKER_AUDIT_WEBHOOK_TIMEOUT_SECONDS=5`
- `BLOCKER_AUDIT_ENABLED=1`
- `BLOCKER_AUDIT_RUN_SCRIPT` (optional override path)

## 8ab) Optional: Install Log Maintenance Timer (Recommended)

This adds a low-priority maintenance timer that keeps log volume predictable by:

- running `logrotate` on cadence (without forcing high-cost full rotations)
- compressing a bounded number of rollover files each run
- pruning old compressed rollovers
- writing machine-readable health artifacts to:
  - `$OUTPUT_DIR/health/log_maintenance/log_maintenance_latest.json`
  - `$OUTPUT_DIR/health/log_maintenance/log_maintenance_*.json`

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/install_systemd_temperature_log_maintenance.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_log_maintenance.sh 30m
```

Main env knobs in `/etc/betbot/temperature-shadow.env`:

- `LOG_MAINTENANCE_ENABLED=1`
- `LOG_MAINTENANCE_RUN_LOGROTATE=1`
- `LOG_MAINTENANCE_AUTO_INSTALL_LOGROTATE=1`
- `LOG_MAINTENANCE_MAX_COMPRESS_PER_RUN=2`
- `LOG_MAINTENANCE_MIN_ROLLOVER_AGE_MINUTES=10`
- `LOG_MAINTENANCE_PRUNE_DAYS=21`
- `LOG_MAINTENANCE_GZIP_LEVEL=6`
- `LOG_MAINTENANCE_MAX_BYTES_WARN` / `LOG_MAINTENANCE_MAX_BYTES_CRIT`
- `LOG_MAINTENANCE_INSTALLER_SCRIPT` (optional override path)
- `LOG_MAINT_ALERT_ENABLED=1`
- `LOG_MAINT_ALERT_WEBHOOK_URL` (falls back to `ALERT_WEBHOOK_URL`)
- `LOG_MAINT_ALERT_WEBHOOK_TIMEOUT_SECONDS=5`
- `LOG_MAINT_ALERT_NOTIFY_STATUS_CHANGE_ONLY=1`
- `LOG_MAINT_ALERT_MIN_INTERVAL_SECONDS=10800`
- `LOG_MAINT_ALERT_GROWTH_BYTES_THRESHOLD=1073741824` (~1 GiB)
- `LOG_MAINT_ALERT_MESSAGE_MODE=concise`
- `LOG_MAINT_ALERT_STATE_FILE` (optional persisted dedupe state)

## 8ac) Optional: Install Discord Route Guard Timer (Recommended)

This adds a periodic route-separation monitor that:

- runs webhook routing audit automatically
- checks collisions by **effective route** (`webhook + thread_id`)
- writes machine-readable route-health artifacts to:
  - `$OUTPUT_DIR/health/discord_route_guard/discord_route_guard_latest.json`
  - `$OUTPUT_DIR/health/discord_route_guard/discord_route_guard_*.json`
- sends concise alerts when route-separation regresses or recovers

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/install_systemd_temperature_discord_route_guard.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_discord_route_guard.sh 20m
```

Main env knobs in `/etc/betbot/temperature-shadow.env`:

- `DISCORD_ROUTE_GUARD_ENABLED=1`
- `DISCORD_ROUTE_GUARD_STRICT=1`
- `DISCORD_ROUTE_GUARD_TIMER_EXPECTED=1` (optional explicit strict-check expectation)
- `DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=1` (recommended strict-check behavior)
- `DISCORD_ROUTE_GUARD_SERVICE_FAIL_ON_COLLISION=0` (keep route-guard service non-failing by default)
- `DISCORD_ROUTE_GUARD_WEBHOOK_URL`
- `DISCORD_ROUTE_GUARD_WEBHOOK_THREAD_ID`
- `DISCORD_ROUTE_GUARD_WEBHOOK_TIMEOUT_SECONDS=5`
- `DISCORD_ROUTE_GUARD_WEBHOOK_USERNAME="BetBot Route Guard"`
- `DISCORD_ROUTE_GUARD_NOTIFY_STATUS_CHANGE_ONLY=1`
- `DISCORD_ROUTE_GUARD_MESSAGE_MODE=concise`
- `DISCORD_ROUTE_GUARD_APPLY_CMD` (optional remediation command text shown in alerts)
- `DISCORD_ROUTE_GUARD_DIR` / `DISCORD_ROUTE_GUARD_STATE_FILE`

`check_temperature_shadow.sh --strict` auto-enforces route guard when the
route-guard timer is installed, even if `DISCORD_ROUTE_GUARD_TIMER_EXPECTED`
is omitted. It also auto-fails on non-green route separation when expected
unless `DISCORD_ROUTE_GUARD_STRICT_FAIL_ON_COLLISION=0` (or legacy
`DISCORD_ROUTE_GUARD_FAIL_ON_COLLISION=0`) is explicitly set.
When strict fails, remediation output includes missing `*_WEBHOOK_THREAD_ID`
keys and a direct apply command.

## 8ad) Recommended For ColdMath Pivot: Install ColdMath Hardening Timer

This installs a dedicated passive hardening cycle focused on the ColdMath lane:

- refreshes ColdMath wallet snapshots from Polymarket public APIs
- rebuilds Polymarket weather-market ingest alignment against those snapshots
- regenerates ColdMath replication plan artifacts using latest available state
- writes machine-readable health artifacts to:
  - `$COLDMATH_OUTPUT_DIR/health/coldmath_hardening_latest.json`
  - `$COLDMATH_OUTPUT_DIR/health/coldmath_hardening_*.json`

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_coldmath_hardening.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_coldmath_hardening.sh 1h
```

Main env knobs in `/etc/betbot/temperature-shadow.env`:

- `COLDMATH_HARDENING_ENABLED=1`
- `COLDMATH_WALLET_ADDRESS` (required when enabled)
- `COLDMATH_OUTPUT_DIR` / `COLDMATH_SNAPSHOT_DIR`
- `COLDMATH_STALE_HOURS`
- `COLDMATH_DATA_API_BASE_URL` / `COLDMATH_GAMMA_BASE_URL`
- `COLDMATH_API_TIMEOUT_SECONDS`
- `COLDMATH_POSITIONS_PAGE_SIZE` / `COLDMATH_POSITIONS_MAX_PAGES`
- `COLDMATH_REFRESH_CLOSED_POSITIONS` + `COLDMATH_CLOSED_POSITIONS_*`
- `COLDMATH_REFRESH_TRADES` / `COLDMATH_REFRESH_ACTIVITY`
- `COLDMATH_INCLUDE_TAKER_ONLY_TRADES` / `COLDMATH_INCLUDE_ALL_TRADE_ROLES`
- `COLDMATH_TRADES_*` / `COLDMATH_ACTIVITY_*`
- `COLDMATH_MARKET_INGEST_ENABLED`
- `COLDMATH_MARKET_INGEST_REFRESH_SNAPSHOT`
- `COLDMATH_MARKET_MAX_MARKETS` / `COLDMATH_MARKET_PAGE_SIZE` / `COLDMATH_MARKET_MAX_PAGES`
- `COLDMATH_MARKET_TIMEOUT_SECONDS` / `COLDMATH_MARKET_INCLUDE_INACTIVE`
- `COLDMATH_REPLICATION_ENABLED`
- `COLDMATH_REPLICATION_TOP_N`
- `COLDMATH_REPLICATION_MARKET_TICKERS` (optional explicit ticker set)
- `COLDMATH_REPLICATION_REQUIRE_LIQUIDITY_FILTER`
- `COLDMATH_REPLICATION_REQUIRE_TWO_SIDED_QUOTES`
- `COLDMATH_REPLICATION_MAX_SPREAD_DOLLARS`
- `COLDMATH_REPLICATION_MIN_LIQUIDITY_SCORE`
- `COLDMATH_REPLICATION_MAX_FAMILY_CANDIDATES`
- `COLDMATH_REPLICATION_MAX_FAMILY_SHARE`
- `COLDMATH_HARDENING_FAIL_ON_NOISE` (`1` = non-zero service exit when support signal is weak)
- `COLDMATH_ACTIONABLE_MIN_POSITIONS_ROWS`
- `COLDMATH_ACTIONABLE_MIN_CANDIDATES`
- `COLDMATH_ACTIONABLE_MIN_MATCHED_RATIO`
- `COLDMATH_ACTIONABLE_REQUIRE_INGEST`
- `COLDMATH_ACTIONABLE_REQUIRE_REPLICATION`
- `COLDMATH_ACTIONABLE_ALLOW_MATRIX_BOOTSTRAP`
- `COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_MAX_HOURS`
- `COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_DISABLE_AT_SETTLED_OUTCOMES`
- `COLDMATH_ACTIONABLE_MATRIX_BOOTSTRAP_STATE_FILE`
- `COLDMATH_LANE_ALERT_ENABLED`
- `COLDMATH_LANE_ALERT_NOTIFY_STATUS_CHANGE_ONLY`
- `COLDMATH_LANE_ALERT_WEBHOOK_URL`
- `COLDMATH_LANE_ALERT_WEBHOOK_THREAD_ID`
- `COLDMATH_LANE_ALERT_WEBHOOK_TIMEOUT_SECONDS`
- `COLDMATH_LANE_ALERT_WEBHOOK_USERNAME`
- `COLDMATH_LANE_ALERT_MESSAGE_MODE`
- `COLDMATH_LANE_ALERT_DEGRADED_STATUSES`
- `COLDMATH_LANE_ALERT_DEGRADED_STREAK_THRESHOLD`
- `COLDMATH_LANE_ALERT_DEGRADED_STREAK_NOTIFY_EVERY`
- `COLDMATH_LANE_ALERT_STATE_FILE`

Noise-control note:

- the hardening cycle now emits `targeted_trading_support` in the health artifact
- this section tracks pass/fail checks for snapshot strength, ingest alignment, replication candidate depth, and decision-matrix strict/bootstrap signals
- bootstrap expiry guard disables bootstrap pass when elapsed window or settled-outcome threshold is reached
- optional lane-transition alerting notifies on decision-matrix lane flips (`strict`, `bootstrap`, `bootstrap blocked`) with stateful dedupe
- optional degraded-streak escalation notifies when `matrix_failed`/`bootstrap_blocked` persist for consecutive runs
- tune `COLDMATH_ACTIONABLE_*` thresholds upward when you want stricter confidence/win-rate bias

## 8b) Recommended: Install Pipeline Recovery Watchdog

This installs a root-owned watchdog timer that:

- auto-detects stale/missing pipeline health artifacts
- auto-detects reporting service stuck in `activating` too long
- auto-detects reporting service in `failed` state and self-heals
- auto-detects stale/missing 12h alpha summary artifact and triggers refresh
- restarts failed/stuck services (`shadow`, `reporting.timer`)
  - `reporting.service` failed-state recovery bypasses restart cooldown
  - `reporting.service` long-activating restarts can be deferred unless
    readiness is stale or pipeline is red (`RECOVERY_REPORTING_ACTIVATING_RESTART_REQUIRE_STALE=1`)
- triggers one-shot METAR and settlement refresh when critical staleness is detected
- forces readiness-report refresh when pipeline health is red
- detects stale/red log-maintenance health and triggers
  `betbot-temperature-log-maintenance.service` (cooldowned)
- writes machine-readable recovery artifacts to:
  - `$OUTPUT_DIR/health/recovery/recovery_latest.json`
  - `$OUTPUT_DIR/health/recovery/recovery_event_*.json`

Useful recovery knobs in `/etc/betbot/temperature-shadow.env`:

- `RECOVERY_ENABLE_ALPHA_SUMMARY_TRIGGER=1`
- `RECOVERY_REQUIRE_ALPHA_SUMMARY_TIMER=1`
- `RECOVERY_ALPHA_SUMMARY_STALE_CRIT_SECONDS=46800`
- `RECOVERY_ALPHA_SUMMARY_TRIGGER_COOLDOWN_SECONDS=900`
- `RECOVERY_ENABLE_LOG_MAINTENANCE_TRIGGER=1`
- `RECOVERY_REQUIRE_LOG_MAINTENANCE_TIMER=1`
- `RECOVERY_ENABLE_LOG_MAINTENANCE_TIMER_ENABLE=1`
- `RECOVERY_LOG_MAINTENANCE_STALE_CRIT_SECONDS=7200`
- `RECOVERY_LOG_MAINTENANCE_TRIGGER_ON_YELLOW=0`
- `RECOVERY_LOG_MAINTENANCE_TIMER_ENABLE_COOLDOWN_SECONDS=900`
- `RECOVERY_LOG_MAINTENANCE_TRIGGER_COOLDOWN_SECONDS=900`
- `RECOVERY_LOG_MAINTENANCE_SERVICE_NAME=betbot-temperature-log-maintenance.service`
- `RECOVERY_LOG_MAINTENANCE_TIMER_NAME=betbot-temperature-log-maintenance.timer`

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/install_systemd_temperature_recovery.sh
```

Use a faster/slower watchdog cadence (example: every 3 minutes):

```bash
bash infra/digitalocean/install_systemd_temperature_recovery.sh 3m
```

## 8c) Recommended: Install Nightly Recovery Chaos Check

This installs a nightly controlled recovery drill that:

- briefly stops shadow service
- runs pipeline recovery immediately
- verifies shadow recovers within timeout
- writes machine-readable chaos artifacts to:
  - `$OUTPUT_DIR/health/recovery/chaos_check_latest.json`
  - `$OUTPUT_DIR/health/recovery/chaos_check_*.json`

Safety guard:

- auto-skips when `ALLOW_LIVE_ORDERS=1` to avoid live disruption.

Install with default nightly schedule (`06:40 UTC` + randomized delay):

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/install_systemd_temperature_recovery_chaos.sh
```

Custom schedule example:

```bash
bash infra/digitalocean/install_systemd_temperature_recovery_chaos.sh "*-*-* 07:15:00 UTC" 10m
```

Useful chaos-drill knobs in `/etc/betbot/temperature-shadow.env`:

- `RECOVERY_CHAOS_ENABLE_WORKER_DRILLS=1`
- `RECOVERY_CHAOS_NOTIFY_ON_PASS=0`
- `RECOVERY_CHAOS_WEBHOOK_URL` (falls back to recovery/global alert webhook)
- `RECOVERY_CHAOS_WEBHOOK_TIMEOUT_SECONDS=5`
- `RECOVERY_CHAOS_WEBHOOK_MESSAGE_MODE=concise`
- `RECOVERY_CHAOS_WAIT_RECOVER_SECONDS=120`
- `RECOVERY_CHAOS_WORKER_WAIT_RECOVER_SECONDS=90`

Ad-hoc stale-metrics adaptive drill (validates that stale artifacts do not
drive data-dependent adaptive decisions):

```bash
bash infra/digitalocean/run_temperature_stale_metrics_drill.sh /etc/betbot/temperature-shadow.env
```

This writes:

- `$OUTPUT_DIR/recovery_chaos/stale_metrics_drill/stale_metrics_drill_*.json`
- `$OUTPUT_DIR/recovery_chaos/stale_metrics_drill/stale_metrics_drill_latest.json`

Install periodic stale-metrics drill timer (recommended for continuous
adaptive hardening):

```bash
cd "$HOME/betting-bot"
bash infra/digitalocean/install_systemd_temperature_stale_metrics_drill.sh
```

Custom interval example:

```bash
bash infra/digitalocean/install_systemd_temperature_stale_metrics_drill.sh 4h 20m
```

Timer/service controls:

```bash
sudo systemctl status betbot-temperature-stale-metrics-drill.timer
sudo systemctl list-timers 'betbot-temperature-stale-metrics-drill*'
sudo journalctl -u betbot-temperature-stale-metrics-drill.service -n 80 --no-pager
```

Useful stale-drill env knobs in `/etc/betbot/temperature-shadow.env`:

- `STALE_METRICS_DRILL_TIMER_EXPECTED`
- `STALE_METRICS_DRILL_FAIL_ON_DRILL_FAILURE`
- `STALE_METRICS_DRILL_ALERT_ENABLED`
- `STALE_METRICS_DRILL_ALERT_WEBHOOK_URL`
- `STALE_METRICS_DRILL_ALERT_WEBHOOK_TIMEOUT_SECONDS`
- `STALE_METRICS_DRILL_ALERT_NOTIFY_ON_PASS`
- `STALE_METRICS_DRILL_ALERT_NOTIFY_STATUS_CHANGE_ONLY`
- `STALE_METRICS_DRILL_ALERT_MESSAGE_MODE`
- `STALE_METRICS_DRILL_ALERT_STATE_FILE`

## 9) Optional: Install Parallel Alpha Workers

This starts background workers that:

- auto-build station/hour METAR-age policy overrides from recent intents
- collect websocket-state snapshots to feed sequence overlap / live-gate authority
- run exploratory `kalshi-temperature-trader --intents-only` profiles
- refresh fast `live-readiness` / `go-live-gate` / `bankroll-validation` / `alpha-gap-report` diagnostics
- publish compact alpha dashboard JSON (`alpha_worker_dashboard_latest.json`)
  including:
  - `explorer_profiles.all_profiles` (all active explore_* profiles)
  - `explorer_profiles.rankings` (top profiles by approvals / unique tickers)
  - `conservative_headline` (unique-market-side + underlying-family deployment basis)
- optionally run CDO prewarm if enabled

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_alpha_workers.sh
```

If you want shadow loop to consume auto policy output, set:

```bash
METAR_AGE_POLICY_JSON=$AUTO_METAR_POLICY_PATH
```

in `/etc/betbot/temperature-shadow.env`, then restart shadow service.

Useful alpha-worker websocket knobs in `/etc/betbot/temperature-shadow.env`:

- `ALPHA_WS_COLLECT_ENABLED=1`
- `ALPHA_WS_COLLECT_REFRESH_SECONDS`
- `ALPHA_WS_COLLECT_RUN_SECONDS`
- `ALPHA_WS_COLLECT_CHANNELS`
- `ALPHA_WS_COLLECT_MARKET_TICKERS` (optional explicit ticker list)
- `ALPHA_WS_STATE_MAX_AGE_SECONDS`

Useful alpha-worker explorer/breadth knobs in `/etc/betbot/temperature-shadow.env`:

- `ALPHA_EXPLORER_PROFILE_PARALLELISM`
- `ALPHA_EXPLORER_ENABLE_WIDE_PROFILE` / `ALPHA_EXPLORER_ENABLE_ULTRA_PROFILE`
- `ALPHA_EXPLORER_ENABLE_ULTRA_AGE_ONLY_PROFILE`
- `ALPHA_EXPLORER_TAF_STALE_GRACE_MINUTES`
- `ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE`
- `ALPHA_EXPLORER_TAF_STALE_GRACE_MAX_RANGE_WIDTH`
- `ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_WIDE` / `ALPHA_EXPLORER_RELAXED_MAX_YES_GAP_ULTRA`
- `ALPHA_EXPLORER_RELAXED_AGE_WIDE_MINUTES` / `ALPHA_EXPLORER_RELAXED_AGE_ULTRA_MINUTES`
- `ALPHA_EXPLORER_MAX_MARKETS_ADAPTIVE_ENABLED`
- `ALPHA_EXPLORER_MAX_MARKETS_MIN` / `ALPHA_EXPLORER_MAX_MARKETS_MAX`
- `ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SECONDS`
- `ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS`
- `ALPHA_EXPLORER_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS`
- `ALPHA_EXPLORER_MAX_MARKETS_STEP_UP` / `ALPHA_EXPLORER_MAX_MARKETS_STEP_UP_FAST`
- `ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN` / `ALPHA_EXPLORER_MAX_MARKETS_STEP_DOWN_FAST`
- `ALPHA_EXPLORER_ADAPTIVE_LOAD_LOW_MILLI` / `ALPHA_EXPLORER_ADAPTIVE_LOAD_HIGH_MILLI`
- `ALPHA_EXPLORER_ADAPTIVE_LOAD_VERY_HIGH_MILLI`
- `ALPHA_EXPLORER_ENFORCE_PROBABILITY_EDGE_THRESHOLDS`
- `ALPHA_EXPLORER_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR`
- `ALPHA_EXPLORER_FALLBACK_MIN_PROBABILITY_CONFIDENCE`
- `ALPHA_EXPLORER_FALLBACK_MIN_EXPECTED_EDGE_NET`
- `ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO`
- `ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN`
- `ALPHA_EXPLORER_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN`

Recommended high-breadth alpha-worker profile on 4 vCPU:

- `ALPHA_WORKER_LOOP_SECONDS=3`
- `ALPHA_POLICY_TOP_N=80`
- `ALPHA_EXPLORER_REFRESH_SECONDS=10`
- `ALPHA_EXPLORER_TOP_N=120`
- `ALPHA_EXPLORER_PROFILE_PARALLELISM=6`
- `ALPHA_EXPLORER_MAX_MARKETS=4000`
- `ALPHA_EXPLORER_MAX_MARKETS_MIN=3000`
- `ALPHA_EXPLORER_MAX_MARKETS_MAX=7000`
- `ALPHA_WS_COLLECT_REFRESH_SECONDS=15`
- `ALPHA_VALIDATION_TOP_N=30`

## 9b) Optional: Install Dedicated Breadth Worker

This starts a separate continuous worker focused on maximizing independent
market-side discovery using spare CPU. It:

- refreshes temperature inputs on a fast cadence
- runs multiple `kalshi-temperature-trader --intents-only` profile variants in parallel
- writes `breadth_worker_dashboard_latest.json` with union breadth metrics
  and `breadth_headline` / `adaptive_guidance` sections for tuning direction
- writes `breadth_worker_consensus_latest.json` with cross-profile fused
  market-side consensus scores (auto-consumed by `kalshi-temperature-trader`
  when present)

```bash
cd "$HOME/betting-bot"
export BETBOT_DEPLOY_USER=betbot
bash infra/digitalocean/install_systemd_temperature_breadth_worker.sh
```

Set in `/etc/betbot/temperature-shadow.env` before enabling:

- `BREADTH_WORKER_ENABLED=1`
- `BREADTH_PROFILE_PARALLELISM`
- `BREADTH_PROFILE_PARALLELISM_ADAPTIVE_ENABLED`
- `BREADTH_PROFILE_PARALLELISM_MIN` / `BREADTH_PROFILE_PARALLELISM_MAX`
- `BREADTH_ADAPTIVE_LOAD_VERY_LOW_MILLI` / `BREADTH_ADAPTIVE_LOAD_LOW_MILLI`
- `BREADTH_ADAPTIVE_LOAD_HIGH_MILLI` / `BREADTH_ADAPTIVE_LOAD_VERY_HIGH_MILLI`
- `BREADTH_ADAPTIVE_PARALLELISM_STEP_UP` / `BREADTH_ADAPTIVE_PARALLELISM_STEP_UP_FAST`
- `BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN` / `BREADTH_ADAPTIVE_PARALLELISM_STEP_DOWN_FAST`
- `BREADTH_MAX_MARKETS_ADAPTIVE_ENABLED`
- `BREADTH_MAX_MARKETS_MIN` / `BREADTH_MAX_MARKETS_MAX`
- `BREADTH_MAX_MARKETS_TARGET_SCAN_SECONDS`
- `BREADTH_MAX_MARKETS_TARGET_SCAN_SLACK_SECONDS`
- `BREADTH_MAX_MARKETS_TARGET_SCAN_HARD_SLACK_SECONDS`
- `BREADTH_MAX_MARKETS_STEP_UP` / `BREADTH_MAX_MARKETS_STEP_UP_FAST`
- `BREADTH_MAX_MARKETS_STEP_DOWN` / `BREADTH_MAX_MARKETS_STEP_DOWN_FAST`
- `BREADTH_MAX_MARKETS`
- `BREADTH_TARGET_PRESSURE_CAP_MULTIPLIER`
- `BREADTH_RELAXED_GAP_PRIMARY` / `BREADTH_RELAXED_GAP_WIDE` / `BREADTH_RELAXED_GAP_ULTRA`
- `BREADTH_RELAXED_AGE_PRIMARY` / `BREADTH_RELAXED_AGE_WIDE` / `BREADTH_RELAXED_AGE_ULTRA`
- `BREADTH_ENABLE_WIDE_GAP_PROFILE` / `BREADTH_ENABLE_ULTRA_GAP_PROFILE`
- `BREADTH_ENABLE_ULTRA_AGE_ONLY_PROFILE`
- `BREADTH_WORKER_LOOP_SECONDS`
- `BREADTH_WORKER_LOOP_SECONDS_MIN` / `BREADTH_WORKER_LOOP_SECONDS_MAX`
- `BREADTH_LOOP_SLEEP_ADAPTIVE_ENABLED`
- `BREADTH_INPUT_REFRESH_SECONDS`
- `BREADTH_CONSENSUS_TOP_N`
- `BREADTH_CONSENSUS_MIN_PROFILE_SUPPORT`
- `BREADTH_CONSENSUS_MIN_SUPPORT_RATIO`
- `BREADTH_TARGET_UNIQUE_MARKET_SIDES_APPROVED`
- `BREADTH_TARGET_UNIQUE_UNDERLYINGS`
- `BREADTH_TARGET_CONSENSUS_CANDIDATES`
- `BREADTH_TARGET_STEP_UP` / `BREADTH_TARGET_STEP_UP_FAST`
- `BREADTH_REPLAN_PRESSURE_ENABLED`
- `BREADTH_REPLAN_PRESSURE_MIN_INPUT_COUNT`
- `BREADTH_REPLAN_PRESSURE_BLOCKED_RATIO_MIN`
- `BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_MARKET_SIDES`
- `BREADTH_REPLAN_PRESSURE_MAX_UNIQUE_UNDERLYINGS`
- `BREADTH_REPLAN_PRESSURE_LEVEL`
- `BREADTH_REPLAN_PRESSURE_REQUIRE_TARGET_DEFICIT`
- `BREADTH_OVERLAP_PRESSURE_ENABLED`
- `BREADTH_OVERLAP_PRESSURE_MIN_INTENTS`
- `BREADTH_OVERLAP_PRESSURE_RATIO_MIN` / `BREADTH_OVERLAP_PRESSURE_RATIO_HIGH`
- `BREADTH_OVERLAP_PRESSURE_REQUIRE_LOW_STALE`
- `BREADTH_OVERLAP_PRESSURE_MAX_STALE_RATE`
- `BREADTH_HEADROOM_EXPLORATION_ENABLED`
- `BREADTH_HEADROOM_EXPLORATION_MAX_LOAD_PER_VCPU`
- `BREADTH_HEADROOM_EXPLORATION_MIN_INTENTS`
- `BREADTH_HEADROOM_EXPLORATION_MIN_APPROVAL_RATE`
- `BREADTH_HEADROOM_EXPLORATION_MAX_STALE_RATE`
- `BREADTH_HEADROOM_EXPLORATION_LEVEL`
- `BREADTH_HEADROOM_EXPLORATION_REQUIRE_TARGET_DEFICIT`
- `BREADTH_TAF_STALE_GRACE_MINUTES`
- `BREADTH_TAF_STALE_GRACE_MAX_VOLATILITY_SCORE`
- `BREADTH_TAF_STALE_GRACE_MAX_RANGE_WIDTH`
- `BREADTH_ENFORCE_PROBABILITY_EDGE_THRESHOLDS`
- `BREADTH_ENFORCE_ENTRY_PRICE_PROBABILITY_FLOOR`
- `BREADTH_FALLBACK_MIN_PROBABILITY_CONFIDENCE`
- `BREADTH_FALLBACK_MIN_EXPECTED_EDGE_NET`
- `BREADTH_METAR_FRESHNESS_QUALITY_BOUNDARY_RATIO`
- `BREADTH_METAR_FRESHNESS_QUALITY_PROBABILITY_MARGIN`
- `BREADTH_METAR_FRESHNESS_QUALITY_EXPECTED_EDGE_MARGIN`

## 10) Verify Health

```bash
bash infra/digitalocean/check_temperature_shadow.sh
bash infra/digitalocean/check_temperature_shadow_quick.sh
# or explicit env path:
bash infra/digitalocean/check_temperature_shadow.sh --env /etc/betbot/temperature-shadow.env
sudo journalctl -u betbot-temperature-shadow -f
sudo journalctl -u betbot-temperature-recovery -f
sudo journalctl -u betbot-temperature-recovery-chaos -f
sudo journalctl -u betbot-temperature-alpha-workers -f
```

Strict health gate mode (non-zero exit if `health/live_status_latest.json` is yellow/red):

```bash
bash infra/digitalocean/check_temperature_shadow.sh --strict
bash infra/digitalocean/check_temperature_shadow_quick.sh --strict
# strict with explicit env path:
bash infra/digitalocean/check_temperature_shadow_quick.sh --strict --env /etc/betbot/temperature-shadow.env
```

`check_temperature_shadow.sh` now also prints:

- `alpha_focus_14h`: top blocker reasons + weakest stations/hours by approval rate
- `bankroll_breadth`: resolved/deployed breadth and repeated-entry multiplier
- `bankroll_hysa`: excess return vs HYSA assumption for current window
- `bankroll_concentration`: duplicate-count and concentration-warning visibility
- `log_maintenance_latest`: latest log maintenance health + usage/compression stats
- `live_status settlement_refresh_plan`: active settlement-pressure plan values (`pressure_active`, effective refresh seconds/top_n)
- `alpha_focus_14h settled_quality`: default prediction headline on unique market-side outcomes (`rows_audit_only` printed separately)
- `blocker_audit_latest`: weekly largest blocker + close action

`check_temperature_shadow_quick.sh` is the short-form companion for operators:

- concise service status (`shadow`, `alpha_summary_timer`, `discord_route_guard_timer`)
- artifact freshness ages (`live_status`, `alpha_summary`, `discord_route_guard`)
- one-line live + alpha context with clear timescales:
  - `cycle_approval_rate` / `cycle_stale_rate` (latest-cycle only)
  - `flow12h` / `planned12h` (rolling 12h context)
  - settled basis + projected bankroll pnl
- discord route/thread-map readiness (`guard status`, `missing required thread keys`)
- explicit `confidence_pnl_divergence` warning when deploy confidence is high but projected bankroll PnL is negative

## 11) Operational Notes

- Default mode is shadow (`ALLOW_LIVE_ORDERS=0`).
- Keep live disabled until shadow validation is green.
- Keep services running as `betbot` (not `root`).
- Restart service after env changes:

```bash
sudo systemctl restart betbot-temperature-shadow
sudo systemctl restart betbot-temperature-reporting.timer
sudo systemctl restart betbot-temperature-recovery.timer
sudo systemctl restart betbot-temperature-alpha-workers
sudo systemctl restart betbot-temperature-breadth-worker
sudo systemctl restart betbot-temperature-blocker-audit.timer
sudo systemctl restart betbot-temperature-log-maintenance.timer
sudo systemctl restart betbot-temperature-discord-route-guard.timer
```

- The loop writes machine-readable per-cycle health to:
  - `$OUTPUT_DIR/health/live_status_latest.json`
- Fast SPECI-driven rescans can be enabled with:
  - `FAST_RESCAN_ENABLED=1`
  - `FAST_RESCAN_MIN_DELTA_C`
  - `FAST_RESCAN_MAX_OBS_AGE_MINUTES`
- Optional webhook alerts (Slack-compatible payload):
  - `ALERT_WEBHOOK_URL`
  - `ALERT_WEBHOOK_THREAD_ID` (optional default Discord thread target)
  - `SHADOW_ALERT_WEBHOOK_URL` (shadow-loop alerts; falls back to `ALERT_WEBHOOK_URL`)
  - `SHADOW_ALERT_WEBHOOK_THREAD_ID` (falls back to `ALERT_WEBHOOK_THREAD_ID`)
  - `SHADOW_ALERT_WEBHOOK_TIMEOUT_SECONDS`
  - `ALERT_NOTIFY_YELLOW` / `ALERT_NOTIFY_RED` / `ALERT_NOTIFY_MILESTONE`
  - `ALERT_NOTIFY_STATUS_CHANGE_ONLY`
- Optional dedicated per-bot webhook routing:
  - `ALPHA_SUMMARY_WEBHOOK_URL`
  - `ALPHA_SUMMARY_WEBHOOK_THREAD_ID`
  - `ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID`
  - `ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID`
  - `BLOCKER_AUDIT_WEBHOOK_URL`
  - `BLOCKER_AUDIT_WEBHOOK_THREAD_ID`
  - `PIPELINE_ALERT_WEBHOOK_URL`
  - `PIPELINE_ALERT_WEBHOOK_THREAD_ID`
  - `RECOVERY_WEBHOOK_URL`
  - `RECOVERY_WEBHOOK_THREAD_ID`
  - `RECOVERY_CHAOS_WEBHOOK_URL`
  - `RECOVERY_CHAOS_WEBHOOK_THREAD_ID`
  - `STALE_METRICS_DRILL_ALERT_WEBHOOK_URL`
  - `STALE_METRICS_DRILL_ALERT_WEBHOOK_THREAD_ID`
  - `LOG_MAINT_ALERT_WEBHOOK_URL`
  - `LOG_MAINT_ALERT_WEBHOOK_THREAD_ID`
  - `DISCORD_ROUTE_GUARD_WEBHOOK_URL`
  - `DISCORD_ROUTE_GUARD_WEBHOOK_THREAD_ID`
  - `DISCORD_WEBHOOK_SEPARATION_STRICT` (`1` exits audit non-zero when bots share the same effective route: webhook + thread)
- Audit webhook routing + collision groups (route-aware):

```bash
bash infra/digitalocean/audit_discord_webhook_routing.sh /etc/betbot/temperature-shadow.env
```

Security note: audit route hints intentionally show Discord webhook `id` only (no token fragments).

Strict mode:

```bash
bash infra/digitalocean/audit_discord_webhook_routing.sh /etc/betbot/temperature-shadow.env strict
```

Apply thread IDs safely (creates timestamped backup of env file; accepts raw IDs, `<#...>` mentions, or full Discord thread URLs):

```bash
bash infra/digitalocean/set_discord_thread_ids.sh --restart --audit /etc/betbot/temperature-shadow.env \
  SHADOW_ALERT_WEBHOOK_THREAD_ID=https://discord.com/channels/<guild>/<channel>/<thread> \
  ALPHA_SUMMARY_WEBHOOK_ALPHA_THREAD_ID=<#thread_id> \
  ALPHA_SUMMARY_WEBHOOK_OPS_THREAD_ID=<thread_id>
```

Route smoke test (dry-run by default):

```bash
bash infra/digitalocean/test_discord_routes.sh --env /etc/betbot/temperature-shadow.env
```

Send one probe per unique route:

```bash
bash infra/digitalocean/test_discord_routes.sh --env /etc/betbot/temperature-shadow.env --send
```

Thread-map helper template (paste all thread URLs once):

```bash
sudo cp infra/digitalocean/discord_thread_map.template.env /etc/betbot/discord-thread-map.env
# edit /etc/betbot/discord-thread-map.env
sudo bash infra/digitalocean/check_discord_thread_map.sh --env /etc/betbot/temperature-shadow.env --strict
sudo bash infra/digitalocean/apply_discord_thread_map.sh --env /etc/betbot/temperature-shadow.env
```

Single-command preflight + apply (only applies when complete):

```bash
sudo bash infra/digitalocean/check_discord_thread_map.sh --env /etc/betbot/temperature-shadow.env --strict --apply
```

`apply_discord_thread_map.sh` auto-detects required thread keys from the latest
route-guard artifact when available, so future stream-key additions are applied
without editing the helper script.

Notes:
- `apply_discord_thread_map.sh` defaults to `/etc/betbot/discord-thread-map.env` when present.
- It applies all non-empty `*_WEBHOOK_THREAD_ID` values, restarts stack services, and reruns strict route audit.
- `check_discord_thread_map.sh` shows required thread keys, map/env sync state, and exits non-zero in `--strict` mode when not ready.
- When required keys are missing, `check_discord_thread_map.sh` now prints a paste-ready `set_discord_thread_ids.sh` command template for the exact missing keys.

- The reporting timer emits first-settlement milestone bundles to:
  - `$OUTPUT_DIR/checkpoints/milestones/`
- Recovery watchdog machine-readable artifacts:
  - `$OUTPUT_DIR/health/recovery/recovery_latest.json`
  - `$OUTPUT_DIR/health/recovery/recovery_event_*.json`
- Recovery chaos artifacts:
  - `$OUTPUT_DIR/health/recovery/chaos_check_latest.json`
  - `$OUTPUT_DIR/health/recovery/chaos_check_*.json`
- Install log rotation for loop logs (recommended on always-on workers):
- Install baseline logrotate rule for loop logs:

```bash
bash infra/digitalocean/install_logrotate_temperature_logs.sh
```

  - Keeps `logs/*.log` bounded by size + rotation count.
  - Prevents very large logs from slowing diagnostics and consuming disk.
  - Use with the timer above for recurring low-impact maintenance.
  - Tunable via env:
    - `BETBOT_LOG_DIR`
    - `BETBOT_LOG_ROTATE_SIZE`
    - `BETBOT_LOG_ROTATE_COUNT`
    - `BETBOT_LOG_ROTATE_DAILY`
    - `BETBOT_LOGROTATE_FORCE_NOW` (default `0`; set `1` for one-time immediate rotation)
    - `BETBOT_LOGROTATE_SU_USER` / `BETBOT_LOGROTATE_SU_GROUP` (default `root/root`)

- Enable basic host firewall policy (allow SSH, deny all other inbound):

```bash
sudo ufw allow OpenSSH
sudo ufw --force enable
sudo ufw status verbose
```

## Optional: Stop / Disable

```bash
sudo systemctl stop betbot-temperature-shadow
sudo systemctl disable betbot-temperature-shadow
sudo systemctl stop betbot-temperature-reporting.timer
sudo systemctl disable betbot-temperature-reporting.timer
sudo systemctl stop betbot-temperature-recovery.timer
sudo systemctl disable betbot-temperature-recovery.timer
sudo systemctl stop betbot-temperature-recovery-chaos.timer
sudo systemctl disable betbot-temperature-recovery-chaos.timer
sudo systemctl stop betbot-temperature-alpha-workers
sudo systemctl disable betbot-temperature-alpha-workers
sudo systemctl stop betbot-temperature-breadth-worker
sudo systemctl disable betbot-temperature-breadth-worker
sudo systemctl stop betbot-temperature-blocker-audit.timer
sudo systemctl disable betbot-temperature-blocker-audit.timer
sudo systemctl stop betbot-temperature-log-maintenance.timer
sudo systemctl disable betbot-temperature-log-maintenance.timer
sudo systemctl stop betbot-temperature-discord-route-guard.timer
sudo systemctl disable betbot-temperature-discord-route-guard.timer
```
`check_temperature_shadow.sh` now surfaces `scan_cap_bound_with_headroom=true` when adaptive scan breadth is pinned at max while host load remains low; treat that as an action signal to raise `ADAPTIVE_MAX_MARKETS_MAX` (or lower scan cost) instead of leaving CPU headroom unused.
