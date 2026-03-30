# Betting Bot (Strict Statistical v1)

This project is a deterministic, testable baseline for a probability-driven betting/trading bot.

It is designed around:

- Positive edge only (`EV > threshold`)
- Explicit risk controls (fractional Kelly + hard stake caps)
- Ruin-aware guardrails (daily loss and drawdown stops)
- Reproducible backtests and paper decisions with logs

## Quick Start

```bash
cd "/Users/dankerbadge/Documents/Betting Bot"
git init -b main
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install .
# Optional (locked dev toolchain: pytest + ruff + mypy + pre-commit):
pip install -r requirements-dev.lock.txt
betbot --help
python -m betbot --help
python -m betbot.cli backtest --config data/sample_config.json --input data/sample_backtest.csv --starting-bankroll 100
python -m betbot.cli paper --input data/sample_candidates.csv --starting-bankroll 100
python -m betbot.cli backtest --config data/sample_ladder_config.json --input data/sample_backtest.csv --starting-bankroll 100
python -m betbot.cli analyze --starting-bankroll 10 --risk-per-effort 10 --history-input data/sample_backtest.csv
python -m betbot.cli alpha-scoreboard --planning-bankroll 40 --benchmark-annual-return 0.10
python -m betbot.cli ladder-grid --config data/sample_config.json --input data/sample_backtest.csv --starting-bankroll 100
python -m betbot.cli research-audit --research-dir data/research --venues kalshi,therundown --jurisdictions new_york
python -m betbot.cli canonical-universe --output-dir data/research
python -m betbot.cli odds-audit --input data/sample_opticodds_history.csv --max-gap-minutes 60
python -m betbot.cli onboarding-check --env-file data/research/account_onboarding.env.template
python -m betbot.cli live-smoke --env-file data/research/account_onboarding.local.env
python -m betbot.cli live-snapshot --env-file data/research/account_onboarding.local.env
python -m betbot.cli live-candidates --env-file data/research/account_onboarding.local.env --sport-id 4 --event-date 2026-03-27
python -m betbot.cli live-paper --env-file data/research/account_onboarding.local.env --sport-id 4 --event-date 2026-03-27 --starting-bankroll 1000
python -m betbot.cli kalshi-mlb-map --env-file data/research/account_onboarding.local.env --event-date 2026-03-27
python -m betbot.cli kalshi-nonsports-scan --env-file data/research/account_onboarding.local.env
python -m betbot.cli kalshi-nonsports-capture --env-file data/research/account_onboarding.local.env
python -m betbot.cli kalshi-nonsports-quality --history-csv outputs/kalshi_nonsports_history.csv
python -m betbot.cli kalshi-nonsports-signals --history-csv outputs/kalshi_nonsports_history.csv
python -m betbot.cli kalshi-micro-plan --env-file data/research/account_onboarding.local.env --planning-bankroll 40 --daily-risk-cap 3
python -m betbot.cli kalshi-autopilot --env-file data/research/account_onboarding.local.env
python -m betbot.cli kalshi-watchdog --env-file data/research/account_onboarding.local.env --allow-live-orders --loops 1
python -m betbot.cli kalshi-micro-execute --env-file data/research/account_onboarding.local.env
python -m betbot.cli kalshi-micro-reconcile --env-file data/research/account_onboarding.local.env
python -m betbot.cli kalshi-micro-status --env-file data/research/account_onboarding.local.env
python -m unittest discover -s tests -p "test_*.py"
# Optional (if pytest is installed):
pytest -q
# Optional shortcuts:
make install-dev
make lock-dev
make secrets-check
make test
make test-pytest
make lint
make typecheck
make check
make clean
make precommit-install
```

Outputs are written to `outputs/`.

## Security Notes

- Keep credentials only in `data/research/account_onboarding.local.env` and `.secrets/`.
- Keep private key files under `.secrets/` with `chmod 600`.
- Never commit local env files or key material (`.gitignore` blocks these paths).
- Use `data/research/account_onboarding.env.template` as the shareable scaffold.

## Quality Gates

- `make secrets-check` fails if sensitive local secret paths are tracked by git.
- `make lint` runs Ruff static analysis.
- `make typecheck` runs Mypy over selected core modules.
- `make check` runs CLI sanity checks, lint, typecheck, unittest, and pytest.
- `make precommit-install` installs the pre-commit hook set from `.pre-commit-config.yaml`.
- `make precommit-run` executes the full pre-commit policy locally.
- `make lock-dev` refreshes `requirements-dev.lock.txt` from pinned direct dev dependencies.

## Maintenance

- `make clean` removes build/test/type-check caches and local packaging artifacts.
- `make precommit-run` runs all configured hooks across the repository.
- Dependabot config at `.github/dependabot.yml` keeps pip and GitHub Actions dependencies fresh weekly.

## Probability Path Analysis

`analyze` computes:

- Hitting probability to each target bankroll for a list of `p` values.
- Conditional rung-to-rung transition probabilities.
- Eventual-success sensitivity and required starting units for 90%/95% survivability.
- Optional Bayesian conservative planning `p` from historical settled outcomes (`outcome` column), using a Beta posterior credible interval.

## Alpha Scoreboard

Use `alpha-scoreboard` to answer one operational question each cycle: are current projected returns likely to beat your benchmark after costs, and what research should be prioritized next.

It reads the newest artifacts by default:

- `outputs/kalshi_micro_prior_plan_summary_*.json`
- `outputs/daily_ops_report_*.json` (if available)
- `outputs/kalshi_nonsports_research_queue_*.csv` (if available)

Example:

```bash
python -m betbot.cli alpha-scoreboard \
  --planning-bankroll 40 \
  --benchmark-annual-return 0.10
```

Output:

- `outputs/alpha_scoreboard_*.json`

## Canonical Universe

Use `canonical-universe` to keep a stable internal 40-ticker research library for macro-release and EIA-settled weather-to-energy markets.

This command writes:

- [canonical_contract_mapping.csv](/Users/dankerbadge/Documents/Betting%20Bot/data/research/canonical_contract_mapping.csv)
- [canonical_threshold_library.csv](/Users/dankerbadge/Documents/Betting%20Bot/data/research/canonical_threshold_library.csv)

Example:

```bash
python -m betbot.cli canonical-universe --output-dir data/research
```

## Data Schema

Required CSV columns:

- `timestamp` (ISO 8601, e.g. `2026-03-27T12:00:00`)
- `event_id` (string)
- `selection` (string)
- `odds` (decimal odds, > 1.0)
- `model_prob` (0 to 1)

Optional columns:

- `closing_odds` (decimal odds, > 1.0)
- `outcome` (`1` win, `0` loss)

`backtest` requires `outcome` for settled bets.

## Conservative Probability Floor

Optional config field:

- `planning_prob_floor`: if set, decision EV and stake sizing use `min(model_prob, planning_prob_floor)`.

This is useful when you want all decisions constrained by a conservative Bayesian lower bound from the `analyze` command.

## Withdrawal Ladder Policy

Optional ladder config fields:

- `ladder_enabled`
- `ladder_rungs` (ascending total-wealth milestones)
- `ladder_min_success_prob`
- `ladder_withdraw_step`
- `ladder_min_risk_wallet`
- `ladder_risk_per_effort`
- `ladder_planning_p`

When enabled, each rung uses a probability-gated rule:

- choose the largest withdrawal that still keeps probability of reaching next rung above `ladder_min_success_prob`.
- transferred funds move to a locked vault and are excluded from trading risk.

Ladder events are written to `outputs/*_ladder_events_*.csv`.

## Ladder Policy Grid Optimizer

Use `ladder-grid` to sweep ladder policy choices and rank by score:

- score formula: `net_profit_total_wealth - drawdown_penalty * max_drawdown_total_wealth * starting_bankroll`
- result artifacts:
- `outputs/ladder_grid_results_*.csv` (all scenarios)
- `outputs/ladder_grid_pareto_*.csv` (Pareto front: maximize profit, minimize drawdown)
- `outputs/best_ladder_config_*.json` (best scenario exported as a runnable config)
- `outputs/ladder_grid_summary_*.json` (best and top-k scenarios)

Example:

```bash
python -m betbot.cli ladder-grid \
  --config data/sample_config.json \
  --input data/sample_backtest.csv \
  --starting-bankroll 100 \
  --first-rung-offsets 5,10,20 \
  --rung-step-offsets 20,30 \
  --rung-count-values 3,4 \
  --min-success-probs 0.6,0.7,0.8 \
  --planning-ps 0.52,0.55,0.58 \
  --withdraw-steps 10 \
  --min-risk-wallet-values 10 \
  --drawdown-penalty 0.25 \
  --top-k 10 \
  --pareto-k 20
```

## Research Readiness Audit

Use `research-audit` to score how complete your Kalshi + OpticOdds operational research is before live automation.

Expected files in `data/research/`:

- `settlement_matrix.csv`
- `execution_envelope.csv`
- `compliance_matrix.csv`

Template files are already scaffolded for you, along with:

- `data/research/account_onboarding.env.template`

Example:

```bash
python -m betbot.cli research-audit \
  --research-dir data/research \
  --venues kalshi,therundown \
  --jurisdictions new_york
```

Output:

- `outputs/research_audit_*.json` (scores, blockers, warnings)

## Odds History Quality Audit

Use `odds-audit` to validate data integrity before backtesting or CLV analysis.

Required input columns:

- `timestamp`
- `event_id`
- `market`
- `book`
- `odds`

Optional but recommended:

- `commence_time` (enables pre-start closing integrity checks)

Example:

```bash
python -m betbot.cli odds-audit \
  --input data/sample_opticodds_history.csv \
  --max-gap-minutes 60
```

Outputs:

- `outputs/odds_audit_*.json`
- `outputs/odds_audit_issues_*.csv`

## Account Onboarding Check

Use `onboarding-check` after you populate your env file to verify key prerequisites before live API work.

Recommended setup:

```bash
cp data/research/account_onboarding.env.template data/research/account_onboarding.local.env
# Put your key file in .secrets/ and update KALSHI_PRIVATE_KEY_PATH accordingly.
chmod 600 data/research/account_onboarding.local.env
```

Example:

```bash
python -m betbot.cli onboarding-check \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/onboarding_check_*.json`

## Live API Smoke Test

Use `live-smoke` to verify your stored Kalshi and odds-provider credentials with a single authenticated request to each service.

Example:

```bash
python -m betbot.cli live-smoke \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/live_smoke_*.json`

## DNS Doctor And Recovery

DNS outages are now handled as a recoverable network class on both HTTP and websocket paths. When system DNS resolution fails, the bot can resolve supported hosts (Kalshi/TheRundown/weather) against public resolvers and retry automatically.

Use `dns-doctor` to verify host health before live cycles:

```bash
python -m betbot.cli dns-doctor \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/dns_doctor_*.json`

Optional controls:

- `BETBOT_DISABLE_DNS_RECOVERY=1` disables recovery fallback.
- `BETBOT_DNS_RECOVERY_ALL_HOSTS=1` enables recovery fallback for every hostname (not just supported trading/data hosts).

## Kalshi Autopilot And Watchdog

Use `kalshi-autopilot` for a single guarded execution pass: DNS/smoke/websocket preflight gates run first, automatic self-heal retries remediate and retry in-loop, and only persistent failures force dry-run.
Preflight retries are adaptive: timeout and websocket-collect windows can expand per retry (`--preflight-retry-timeout-multiplier`, `--preflight-retry-ws-collect-increment-seconds`).
When a preflight retry succeeds with a higher timeout, autopilot carries that effective timeout into the supervisor pass instead of dropping back to the lower base timeout.
Autopilot preflight smoke is Kalshi-focused by default; include odds-provider smoke only when needed via `--preflight-live-smoke-include-odds-provider`.
Kalshi live-smoke network failures now count as upstream incidents for preflight self-heal retries (instead of being treated as generic non-retryable smoke failures).
In upstream-only mode, autopilot can still retry websocket-state gate failures (`stale`/`empty`/`desynced`) because those are often transient collection-window issues; disable this behavior with `--disable-preflight-self-heal-retry-ws-state-gate-failures`.

Example:

```bash
python -m betbot.cli kalshi-autopilot \
  --env-file data/research/account_onboarding.local.env \
  --preflight-self-heal-attempts 2 \
  --preflight-self-heal-pause-seconds 10 \
  --allow-live-orders
```

Use `kalshi-watchdog` for continuous autonomous operation with persistent live kill-switch memory. It keeps running autopilot loops, performs in-loop self-heal retries (`--self-heal-attempts-per-loop`, `--self-heal-pause-seconds`) when upstream DNS/network failures appear, applies upstream backoff, runs remediation DNS checks, and only then escalates to kill-switch mode if failures persist.
The watchdog deduplicates remediation work: it skips outer DNS-doctor runs when the autopilot attempt already performed DNS checks/remediation.
Watchdog in-loop retries are adaptive as well: timeout and websocket-collect windows can increase per re-attempt (`--self-heal-retry-timeout-multiplier`, `--self-heal-retry-ws-collect-increment-seconds`) so retries do not repeat identical conditions.

Example:

```bash
python -m betbot.cli kalshi-watchdog \
  --env-file data/research/account_onboarding.local.env \
  --allow-live-orders \
  --preflight-self-heal-attempts 2 \
  --preflight-self-heal-pause-seconds 10 \
  --self-heal-attempts-per-loop 2 \
  --self-heal-pause-seconds 10 \
  --loops 0
```

`kalshi-supervisor` also self-heals exchange-status DNS/network failures before disabling live mode (`--exchange-status-self-heal-attempts`, `--exchange-status-self-heal-pause-seconds`) with adaptive timeout growth (`--exchange-status-self-heal-timeout-multiplier`).
Supervisor remediation retries for prior-trader and arb loops now also use adaptive timeout growth (`--failure-remediation-timeout-multiplier`, `--failure-remediation-timeout-cap-seconds`) so retry attempts do not reuse identical timeout budgets.
To avoid redundant work, supervisor skips remediation exchange-status refresh calls when live orders are not requested (or when the retry has already been forced into dry-run mode).
`kalshi-ws-state-collect` now preserves a previously ready websocket-state snapshot when a recollection run fails upstream, so transient DNS/WebSocket outages do not overwrite a fresh good state file.

Outputs:

- `outputs/kalshi_autopilot_summary_*.json`
- `outputs/kalshi_watchdog_summary_*.json`
- `outputs/kalshi_live_kill_switch_state.json`

## Live Snapshot

Use `live-snapshot` when you want a read-only JSON artifact with your current Kalshi balance plus a preview of the sports catalog from TheRundown.

Example:

```bash
python -m betbot.cli live-snapshot \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/live_snapshot_*.json`

## Live Candidates

Use `live-candidates` to pull live TheRundown odds, build consensus no-vig probabilities across multiple books, and write a CSV that can be fed straight into `paper`.

Example:

```bash
python -m betbot.cli live-candidates \
  --env-file data/research/account_onboarding.local.env \
  --sport-id 4 \
  --event-date 2026-03-27
```

Outputs:

- `outputs/live_candidates_<sport>_<date>_*.csv`
- `outputs/live_candidates_summary_<sport>_<date>_*.json`

## Live Paper

Use `live-paper` for the full read-only workflow: fetch live candidates from TheRundown, then run the bot's paper engine on that candidate set.

Example:

```bash
python -m betbot.cli live-paper \
  --env-file data/research/account_onboarding.local.env \
  --sport-id 4 \
  --event-date 2026-03-27 \
  --starting-bankroll 1000
```

## Sports Archive

Use `sports-archive` to run the live sports paper workflow across one or more dates and append a stable rolling CSV of summary metrics. This is the memory layer for sports automations: it records candidate counts, positive-EV counts, paper accepts, and the top edge seen for each requested date.

Example:

```bash
python -m betbot.cli sports-archive \
  --env-file data/research/account_onboarding.local.env \
  --sport-id 4 \
  --event-dates 2026-03-27,2026-03-28 \
  --starting-bankroll 1000
```

Outputs:

- `outputs/live_paper_archive.csv`
- `outputs/sports_archive_summary_<sport>_*.json`

## Kalshi MLB Map

Use `kalshi-mlb-map` to join TheRundown MLB moneyline consensus with Kalshi MLB winner markets and rank gross edge at the Kalshi Yes ask.

Example:

```bash
python -m betbot.cli kalshi-mlb-map \
  --env-file data/research/account_onboarding.local.env \
  --event-date 2026-03-27
```

Outputs:

- `outputs/kalshi_mlb_map_<date>_*.csv`
- `outputs/kalshi_mlb_map_summary_<date>_*.json`

## Kalshi Non-Sports Scan

Use `kalshi-nonsports-scan` to rank near-term open Kalshi markets outside Sports based on a small-risk execution heuristic. The scan favors tighter spreads, stronger displayed liquidity, closer resolution, and enough displayed ask size to support a `$10` buy at the current best Yes ask.

Example:

```bash
python -m betbot.cli kalshi-nonsports-scan \
  --env-file data/research/account_onboarding.local.env \
  --max-hours-to-close 336 \
  --excluded-categories Sports
```

Outputs:

- `outputs/kalshi_nonsports_scan_*.csv`
- `outputs/kalshi_nonsports_scan_summary_*.json`

## Kalshi Non-Sports Capture

Use `kalshi-nonsports-capture` to append the latest non-sports board scan to a stable history CSV. This is the accumulation layer we can later use for board-quality analysis and model research.

Example:

```bash
python -m betbot.cli kalshi-nonsports-capture \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/kalshi_nonsports_history.csv`

## Kalshi Non-Sports Quality

Use `kalshi-nonsports-quality` to aggregate the captured history into persistent board-quality scores. This helps separate one-off flashes from markets that keep showing up with usable two-sided books.

Example:

```bash
python -m betbot.cli kalshi-nonsports-quality \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_quality_*.csv`
- `outputs/kalshi_nonsports_quality_summary_*.json`

## Kalshi Non-Sports Signals

Use `kalshi-nonsports-signals` to turn the captured history into stability-backed signal labels. This is stricter than the quality pass: it looks for repeated two-sided observations, acceptable spread behavior, and bid stability before calling a market signal-eligible.

Example:

```bash
python -m betbot.cli kalshi-nonsports-signals \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_signals_*.csv`
- `outputs/kalshi_nonsports_signals_summary_*.json`

## Kalshi Non-Sports Persistence

Use `kalshi-nonsports-persistence` to measure whether the same non-sports markets stay tradeable across repeated snapshots. This is the board-memory layer for automation: it highlights markets that keep showing usable bids and tight spreads instead of flashing once and disappearing.

Example:

```bash
python -m betbot.cli kalshi-nonsports-persistence \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_persistence_*.csv`
- `outputs/kalshi_nonsports_persistence_summary_*.json`

## Kalshi Non-Sports Deltas

Use `kalshi-nonsports-deltas` to compare the latest two non-sports captures. This is the change-detection layer for automation: it tells us whether the board is improving, stale, or deteriorating instead of treating every hourly snapshot as equally important.

Example:

```bash
python -m betbot.cli kalshi-nonsports-deltas \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_deltas_*.csv`
- `outputs/kalshi_nonsports_deltas_summary_*.json`

## Kalshi Non-Sports Categories

Use `kalshi-nonsports-categories` to aggregate non-sports board quality by category across the captured history. This is the board-shape layer for automation: it shows whether repeated two-sided liquidity is broadening across categories or staying trapped in one thin vertical.

Example:

```bash
python -m betbot.cli kalshi-nonsports-categories \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_categories_*.csv`
- `outputs/kalshi_nonsports_categories_summary_*.json`

## Kalshi Non-Sports Pressure

Use `kalshi-nonsports-pressure` to spot markets that are building toward a more tradeable state even if they still fail the hard live-trading gate. This is the early-warning layer for automation: it highlights two-sided markets whose bid, spread, and recent history are strengthening, but that may still sit below the `$0.05` Yes-bid floor.

Example:

```bash
python -m betbot.cli kalshi-nonsports-pressure \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_pressure_*.csv`
- `outputs/kalshi_nonsports_pressure_summary_*.json`

## Kalshi Non-Sports Thresholds

Use `kalshi-nonsports-thresholds` to forecast which sub-threshold markets are approaching the live review thresholds. This is the “how close is it really?” layer for automation: it estimates whether a two-sided market is plausibly moving toward the `$0.05` Yes-bid and `$0.02` spread targets soon, rather than just showing generic pressure.

Example:

```bash
python -m betbot.cli kalshi-nonsports-thresholds \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_thresholds_*.csv`
- `outputs/kalshi_nonsports_thresholds_summary_*.json`

## Kalshi Non-Sports Priors

Use `kalshi-nonsports-priors` to compare your own fair-value priors against the latest captured non-sports board. This is the thesis-input layer for automation: once you add probabilities to [kalshi_nonsports_priors.csv](/Users/dankerbadge/Documents/Betting%20Bot/data/research/kalshi_nonsports_priors.csv), the bot can surface which markets show positive edge on the current `Yes` side or the implied `No` side without changing the live trade gate automatically.

Example:

```bash
python -m betbot.cli kalshi-nonsports-priors \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Outputs:

- `outputs/kalshi_nonsports_priors_*.csv`
- `outputs/kalshi_nonsports_priors_summary_*.json`

## Kalshi Focus Dossier

Use `kalshi-focus-dossier` to build one compact report for the current top-focus market. It merges the latest quote, recent quote changes, board-regime context, pressure state, threshold forecast, and any manual prior so the bot can distinguish "same market again" from "same market again, but now with a thesis."

Example:

```bash
python -m betbot.cli kalshi-focus-dossier \
  --history-csv outputs/kalshi_nonsports_history.csv \
  --watch-history-csv outputs/kalshi_micro_watch_history.csv
```

Output:

- `outputs/kalshi_focus_dossier_*.json`

## Kalshi Micro Prior Plan

Use `kalshi-micro-prior-plan` to turn thesis-backed priors into read-only maker-order previews. Unlike the generic micro plan, this command can choose the implied `No` side when your fair value says the current market is overpriced on `Yes`.

Canonical overlays are supported through [canonical_contract_mapping.csv](/Users/dankerbadge/Documents/Betting%20Bot/data/research/canonical_contract_mapping.csv) and [canonical_threshold_library.csv](/Users/dankerbadge/Documents/Betting%20Bot/data/research/canonical_threshold_library.csv). Add `--require-canonical-mapping` to limit plans to mapped canonical tickers only.

Example:

```bash
python -m betbot.cli kalshi-micro-prior-plan \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_prior_plan_*.csv`
- `outputs/kalshi_micro_prior_plan_summary_*.json`

## Kalshi Micro Prior Execute

Use `kalshi-micro-prior-execute` to run the normal micro execution loop from the side-aware prior plan. In dry-run mode it fetches the live orderbook and shows exactly how a `Yes` or `No` maker order would be staged without touching the exchange.

By default, live-order mode requires canonical mapping (`--disable-require-canonical-for-live` can override this).

Example:

```bash
python -m betbot.cli kalshi-micro-prior-execute \
  --env-file data/research/account_onboarding.local.env \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_prior_execute_summary_*.json`
- `outputs/kalshi_micro_execute_*.csv`
- `outputs/kalshi_micro_execute_summary_*.json`

## Kalshi Micro Prior Trader

Use `kalshi-micro-prior-trader` as the unattended-safe wrapper around the prior-backed path. It can capture a fresh non-sports board, evaluate the prior trade gate, and then either hold or run the side-aware micro execute loop.

By default, live-order mode requires canonical mapping (`--disable-require-canonical-for-live` can override this).

Example:

```bash
python -m betbot.cli kalshi-micro-prior-trader \
  --env-file data/research/account_onboarding.local.env \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

For explicit live runs without permanently changing your saved env file, add both `--allow-live-orders` and `--use-temp-live-env`. The trader will create a temporary env copy with `BETBOT_ENABLE_LIVE_ORDERS=1`, use it for execute plus reconcile, and delete it afterward.

Output:

- `outputs/kalshi_micro_prior_trader_summary_*.json`

## Kalshi Micro Prior Watch

Use `kalshi-micro-prior-watch` for one prior-aware monitoring cycle. It captures the board once, refreshes the generic status snapshot from that same scan, then refreshes the prior-backed trader in dry-run mode so the output contains regime, gate, side, edge, and top-order cost details together.

Example:

```bash
python -m betbot.cli kalshi-micro-prior-watch \
  --env-file data/research/account_onboarding.local.env \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_prior_watch_summary_*.json`

## Kalshi Non-Sports Research Queue

Use `kalshi-nonsports-research-queue` to rank uncovered non-sports markets that look worth researching next. It excludes markets already covered by priors, then scores the remaining live two-sided markets using persistence, pressure, execution fit, timing, category novelty, and cheap-side capital efficiency.

Example:

```bash
python -m betbot.cli kalshi-nonsports-research-queue \
  --priors-csv data/research/kalshi_nonsports_priors.csv \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_nonsports_research_queue_*.csv`
- `outputs/kalshi_nonsports_research_queue_summary_*.json`

## Kalshi Micro Gate

Use `kalshi-micro-gate` to decide whether the current non-sports board is strong enough for tiny live automation. It combines plan readiness, daily live-cap headroom, persistent market behavior, signal strength, market pressure, category concentration, and the latest board-change read before returning a pass or hold.

Example:

```bash
python -m betbot.cli kalshi-micro-gate \
  --env-file data/research/account_onboarding.local.env \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_gate_*.json`

## Kalshi Micro Trader

Use `kalshi-micro-trader` as the single unattended-safe entrypoint. It runs the gate first, then executes and reconciles only if the gate passes. If the gate fails, it writes a hold summary instead of touching the exchange.

Example:

```bash
python -m betbot.cli kalshi-micro-trader \
  --env-file data/research/account_onboarding.local.env \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_trader_summary_*.json`

## Kalshi Micro Watch

Use `kalshi-micro-watch` as the sequential read-only monitor. It captures the board first, then runs status from that exact scan so the monitoring loop does not race itself or immediately re-fetch the same board. Status now maintains a stable watch-history CSV and derives a board-regime label from repeated runs, so the bot can tell the difference between concentrated penny noise, pressure-building boards, threshold-approaching boards, improving-but-thin boards, and genuinely trade-ready states.

Example:

```bash
python -m betbot.cli kalshi-micro-watch \
  --env-file data/research/account_onboarding.local.env \
  --history-csv outputs/kalshi_nonsports_history.csv
```

Output:

- `outputs/kalshi_micro_watch_summary_*.json`
- `outputs/kalshi_micro_watch_history.csv`

## Kalshi Micro Plan

Use `kalshi-micro-plan` to prepare a read-only, tiny-bankroll Kalshi workflow before going live. It builds `1`-contract, `post_only`, `good_till_canceled` order previews from the current non-sports board and enforces a small daily cash-risk cap.

Example:

```bash
python -m betbot.cli kalshi-micro-plan \
  --env-file data/research/account_onboarding.local.env \
  --planning-bankroll 40 \
  --daily-risk-cap 3 \
  --contracts-per-order 1 \
  --max-orders 3
```

Outputs:

- `outputs/kalshi_micro_plan_*.csv`
- `outputs/kalshi_micro_plan_summary_*.json`

## Kalshi Micro Execute

Use `kalshi-micro-execute` to turn the micro plan into either:

- a dry-run execution journal, or
- a real maker-order smoke test after you explicitly enable live writes.

Safety rules:

- default mode is read-only dry-run
- live writes require `--allow-live-orders`
- live writes also require `BETBOT_ENABLE_LIVE_ORDERS=1` in the env file
- Sports is excluded by default and the command refuses live writes if Sports is not excluded
- live submissions are also capped by a persistent ledger in `outputs/kalshi_micro_trade_ledger.csv` unless you override the path

Example dry-run:

```bash
python -m betbot.cli kalshi-micro-execute \
  --env-file data/research/account_onboarding.local.env \
  --planning-bankroll 40 \
  --daily-risk-cap 3
```

Example live smoke test after funding:

```bash
python -m betbot.cli kalshi-micro-execute \
  --env-file data/research/account_onboarding.local.env \
  --planning-bankroll 40 \
  --daily-risk-cap 3 \
  --allow-live-orders \
  --cancel-resting-immediately
```

Example maker hold-window test:

```bash
python -m betbot.cli kalshi-micro-execute \
  --env-file data/research/account_onboarding.local.env \
  --planning-bankroll 40 \
  --daily-risk-cap 3 \
  --allow-live-orders \
  --resting-hold-seconds 120
```

Outputs:

- `outputs/kalshi_micro_execute_*.csv`
- `outputs/kalshi_micro_execute_summary_*.json`
- `outputs/kalshi_micro_trade_ledger.csv`

## Kalshi Micro Reconcile

Use `kalshi-micro-reconcile` after a micro execution run to collect the resulting order states, queue positions, position exposure, realized PnL, and fees.

If you do not pass `--execute-summary-file`, the command uses the newest `kalshi_micro_execute_summary_*.json` file in `outputs/`.

Example:

```bash
python -m betbot.cli kalshi-micro-reconcile \
  --env-file data/research/account_onboarding.local.env
```

Outputs:

- `outputs/kalshi_micro_reconcile_*.csv`
- `outputs/kalshi_micro_reconcile_summary_*.json`

## Kalshi Micro Status

Use `kalshi-micro-status` for a single read-only ops snapshot. It runs a fresh dry-run execution cycle, attempts reconciliation against that cycle, and returns a compact status summary with balance, board quality, market pressure, threshold approach, category concentration, exposure, fees, and a simple recommendation.

Example:

```bash
python -m betbot.cli kalshi-micro-status \
  --env-file data/research/account_onboarding.local.env
```

Output:

- `outputs/kalshi_micro_status_*.json`
- includes same-day ledger totals so automations can see how many live submissions and how much live cost budget remain
- includes quality-summary fields from the captured board history
- includes signal-summary fields from the captured board history
- includes pressure-summary fields so the bot can flag markets that are strengthening before they become truly tradeable
- includes threshold-summary fields so the bot can flag markets that appear to be approaching the real live-review thresholds
- includes prior-summary fields so user-supplied fair values can be compared to the live board
- includes category-summary fields so automations can see whether the board is broadening or stuck in one thin vertical
- appends a stable status/watch history CSV at `outputs/kalshi_micro_watch_history.csv`
- includes a `board_regime` label derived from repeated status runs

Outputs:

- `outputs/kalshi_mlb_map_<date>_*.csv`
- `outputs/kalshi_mlb_map_summary_<date>_*.json`

## Monitoring Dashboard

Use `dashboard.py` (basic) for a separate read-only Streamlit operator console that watches `outputs/` and surfaces:

- latest status / execute / reconcile snapshots
- trade-gate and balance-health warnings
- latest execute attempts and reconcile rows
- plain-English "What this means right now" and "What needs attention"
- simple glossary for non-technical operators

Use `dashboard_advanced.py` for the richer view with execution-history charts and deeper telemetry tables.

Files:

- `dashboard.py` (default basic view)
- `dashboard_basic.py` (same basic view, explicit filename)
- `dashboard_advanced.py` (advanced view)
- `monitoring_requirements.txt`

Basic dashboard:

```bash
cd "/Users/dankerbadge/Documents/Betting Bot"
python3 -m venv .monitoring-venv
source .monitoring-venv/bin/activate
pip install -r monitoring_requirements.txt
streamlit run dashboard.py --server.address 127.0.0.1 --server.port 8501
```

Advanced dashboard:

```bash
streamlit run dashboard_advanced.py --server.address 127.0.0.1 --server.port 8502
```

Both dashboards are intentionally read-only and do not expose order actions.

## Risk Notes

This code is educational infrastructure, not a promise of profitability.

- If your true net edge is not positive, staking cannot rescue the strategy.
- Inputs are assumed honest and timestamp-consistent.
- Real deployment still requires exchange/sportsbook execution constraints, compliance checks, and monitoring.
