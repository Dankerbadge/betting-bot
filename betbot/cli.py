from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys

from betbot.alpha_scoreboard import run_alpha_scoreboard
from betbot.backtest import run_backtest
from betbot.bayes import conservative_planning_p
from betbot.canonical_universe import run_canonical_universe
from betbot.commands.runtime_ops import run_effective_config, run_policy_check, run_render_board
from betbot.config import load_config
from betbot.dns_guard import run_dns_doctor
from betbot.io import load_candidates
from betbot.kalshi_focus_dossier import run_kalshi_focus_dossier
from betbot.kalshi_execution_frontier import run_kalshi_execution_frontier
from betbot.kalshi_autopilot import run_kalshi_autopilot
from betbot.kalshi_watchdog import run_kalshi_watchdog
from betbot.kalshi_micro_execute import run_kalshi_micro_execute
from betbot.kalshi_micro_gate import run_kalshi_micro_gate
from betbot.kalshi_micro_prior_execute import run_kalshi_micro_prior_execute
from betbot.kalshi_micro_prior_plan import run_kalshi_micro_prior_plan
from betbot.kalshi_micro_prior_trader import run_kalshi_micro_prior_trader
from betbot.kalshi_micro_prior_watch import run_kalshi_micro_prior_watch
from betbot.kalshi_micro_reconcile import run_kalshi_micro_reconcile
from betbot.kalshi_micro_status import run_kalshi_micro_status
from betbot.kalshi_micro_trader import run_kalshi_micro_trader
from betbot.kalshi_micro_watch import run_kalshi_micro_watch
from betbot.kalshi_mlb_map import run_kalshi_mlb_map
from betbot.kalshi_micro_plan import run_kalshi_micro_plan
from betbot.kalshi_arb_scan import run_kalshi_arb_scan
from betbot.kalshi_supervisor import run_kalshi_supervisor
from betbot.kalshi_ws_state import run_kalshi_ws_state_collect, run_kalshi_ws_state_replay
from betbot.kalshi_nonsports_categories import run_kalshi_nonsports_categories
from betbot.kalshi_nonsports_auto_priors import run_kalshi_nonsports_auto_priors
from betbot.kalshi_nonsports_capture import run_kalshi_nonsports_capture
from betbot.kalshi_nonsports_deltas import run_kalshi_nonsports_deltas
from betbot.kalshi_nonsports_persistence import run_kalshi_nonsports_persistence
from betbot.kalshi_nonsports_pressure import run_kalshi_nonsports_pressure
from betbot.kalshi_nonsports_priors import run_kalshi_nonsports_priors
from betbot.kalshi_nonsports_quality import run_kalshi_nonsports_quality
from betbot.kalshi_nonsports_research_queue import run_kalshi_nonsports_research_queue
from betbot.kalshi_nonsports_signals import run_kalshi_nonsports_signals
from betbot.kalshi_nonsports_scan import run_kalshi_nonsports_scan
from betbot.kalshi_nonsports_thresholds import run_kalshi_nonsports_thresholds
from betbot.kalshi_temperature_constraints import run_kalshi_temperature_constraint_scan
from betbot.kalshi_temperature_contract_specs import run_kalshi_temperature_contract_specs
from betbot.kalshi_temperature_metar_ingest import run_kalshi_temperature_metar_ingest
from betbot.kalshi_temperature_trader import run_kalshi_temperature_trader
from betbot.kalshi_weather_catalog import run_kalshi_weather_catalog
from betbot.kalshi_weather_priors import run_kalshi_weather_priors, run_kalshi_weather_station_history_prewarm
from betbot.kalshi_climate_availability import run_kalshi_climate_realtime_router
from betbot.live_candidates import run_live_candidates
from betbot.live_paper import run_live_paper
from betbot.sports_archive import run_sports_archive
from betbot.ladder_grid import parse_float_list, parse_int_list, run_ladder_grid
from betbot.live_snapshot import run_live_snapshot
from betbot.live_smoke import run_live_smoke
from betbot.odds_audit import run_odds_audit
from betbot.onboarding import run_onboarding_check
from betbot.paper import run_paper
from betbot.polymarket_market_ingest import run_polymarket_market_data_ingest
from betbot.probability_path import (
    eventual_success_probability,
    hitting_probability,
    required_starting_units,
    units_from_dollars,
)
from betbot.research_audit import run_research_audit


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Strict-statistics betting bot CLI")

    subparsers = parser.add_subparsers(dest="command", required=True)

    backtest = subparsers.add_parser("backtest", help="Run historical backtest")
    backtest.add_argument("--config", help="Optional JSON config path", default=None)
    backtest.add_argument("--input", required=True, help="Input CSV path")
    backtest.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    backtest.add_argument("--output-dir", default="outputs", help="Output directory")

    paper = subparsers.add_parser("paper", help="Run paper decision engine")
    paper.add_argument("--config", help="Optional JSON config path", default=None)
    paper.add_argument("--input", required=True, help="Input CSV path")
    paper.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    paper.add_argument("--output-dir", default="outputs", help="Output directory")
    paper.add_argument(
        "--simulate-with-outcomes",
        action="store_true",
        help="If outcome exists, settle paper bets for simulation",
    )

    analyze = subparsers.add_parser(
        "analyze",
        help="Compute probability-path and conservative planning stats",
    )
    analyze.add_argument(
        "--starting-bankroll",
        type=float,
        default=10.0,
        help="Starting bankroll in dollars",
    )
    analyze.add_argument(
        "--risk-per-effort",
        type=float,
        default=10.0,
        help="Fixed risk per attempt in dollars",
    )
    analyze.add_argument(
        "--targets",
        default="20,50,100,250,1000,10000",
        help="Comma-separated target bankrolls in dollars",
    )
    analyze.add_argument(
        "--p-values",
        default="0.50,0.51,0.52,0.55,0.60",
        help="Comma-separated p values to evaluate",
    )
    analyze.add_argument(
        "--history-input",
        default=None,
        help="Optional CSV with outcome column to estimate Bayesian planning p",
    )
    analyze.add_argument(
        "--confidence",
        type=float,
        default=0.95,
        help="Credible interval confidence for Bayesian estimate",
    )
    analyze.add_argument("--output-dir", default="outputs", help="Output directory")

    effective_config = subparsers.add_parser(
        "effective-config",
        help="Render merged runtime config with deterministic fingerprints",
    )
    effective_config.add_argument(
        "--repo-root",
        default=None,
        help="Optional repository root override for config resolution",
    )

    policy_check = subparsers.add_parser(
        "policy-check",
        help="Validate lane permissions and emit policy snapshot",
    )
    policy_check.add_argument(
        "--lane",
        default="research",
        help="Permission lane to evaluate",
    )
    policy_check.add_argument(
        "--lane-policy-path",
        default=None,
        help="Optional path to lane policy YAML",
    )

    render_board = subparsers.add_parser(
        "render-board",
        help="Render board projection from runtime cycle/board JSON",
    )
    render_board.add_argument(
        "--board-json",
        default=None,
        help="Optional explicit board JSON path",
    )
    render_board.add_argument(
        "--cycle-json",
        default=None,
        help="Optional explicit cycle JSON path",
    )
    render_board.add_argument("--output-dir", default="outputs", help="Output directory")

    alpha_scoreboard = subparsers.add_parser(
        "alpha-scoreboard",
        help="Score current projected edge versus a benchmark and output prioritized research targets",
    )
    alpha_scoreboard.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll used to compute deployed fraction and bankroll-level compounding",
    )
    alpha_scoreboard.add_argument(
        "--benchmark-annual-return",
        type=float,
        default=0.10,
        help="Annual benchmark return target as a decimal (0.10 = 10 percent)",
    )
    alpha_scoreboard.add_argument(
        "--plan-summary-file",
        default=None,
        help="Optional explicit kalshi_micro_prior_plan_summary JSON file path",
    )
    alpha_scoreboard.add_argument(
        "--daily-ops-file",
        default=None,
        help="Optional explicit daily_ops_report JSON file path",
    )
    alpha_scoreboard.add_argument(
        "--research-queue-csv",
        default=None,
        help="Optional explicit kalshi_nonsports_research_queue CSV path",
    )
    alpha_scoreboard.add_argument(
        "--top-research-targets",
        type=int,
        default=5,
        help="Maximum research targets embedded in the output summary",
    )
    alpha_scoreboard.add_argument("--output-dir", default="outputs", help="Output directory")

    ladder_grid = subparsers.add_parser(
        "ladder-grid",
        help="Sweep ladder-policy parameters and rank results",
    )
    ladder_grid.add_argument("--config", help="Optional JSON config path", default=None)
    ladder_grid.add_argument("--input", required=True, help="Input CSV path")
    ladder_grid.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    ladder_grid.add_argument(
        "--first-rung-offsets",
        default="5,10,20",
        help="Comma-separated first rung offsets in dollars",
    )
    ladder_grid.add_argument(
        "--rung-step-offsets",
        default="20,30",
        help="Comma-separated rung step offsets in dollars",
    )
    ladder_grid.add_argument(
        "--rung-count-values",
        default="3,4",
        help="Comma-separated rung counts",
    )
    ladder_grid.add_argument(
        "--min-success-probs",
        default="0.60,0.70,0.80",
        help="Comma-separated ladder minimum success probabilities",
    )
    ladder_grid.add_argument(
        "--planning-ps",
        default="0.52,0.55,0.58",
        help="Comma-separated ladder planning p values",
    )
    ladder_grid.add_argument(
        "--withdraw-steps",
        default="10",
        help="Comma-separated withdrawal step sizes",
    )
    ladder_grid.add_argument(
        "--min-risk-wallet-values",
        default="10",
        help="Comma-separated minimum risk wallet values",
    )
    ladder_grid.add_argument(
        "--drawdown-penalty",
        type=float,
        default=0.0,
        help="Score penalty multiplier for drawdown (higher penalizes volatility)",
    )
    ladder_grid.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="Number of top scenarios returned in summary",
    )
    ladder_grid.add_argument(
        "--pareto-k",
        type=int,
        default=20,
        help="Maximum number of Pareto-front scenarios returned in summary",
    )
    ladder_grid.add_argument("--output-dir", default="outputs", help="Output directory")

    research_audit = subparsers.add_parser(
        "research-audit",
        help="Audit settlement/execution/compliance research completeness",
    )
    research_audit.add_argument(
        "--research-dir",
        default="data/research",
        help="Directory containing research matrix CSVs",
    )
    research_audit.add_argument(
        "--venues",
        default="kalshi,therundown",
        help="Comma-separated venues to audit",
    )
    research_audit.add_argument(
        "--jurisdictions",
        default="new_york",
        help="Comma-separated jurisdictions to audit",
    )
    research_audit.add_argument("--output-dir", default="outputs", help="Output directory")

    canonical_universe = subparsers.add_parser(
        "canonical-universe",
        help="Build canonical ticker contract-mapping and threshold libraries for macro + energy release research",
    )
    canonical_universe.add_argument(
        "--output-dir",
        default="data/research",
        help="Directory where canonical_contract_mapping.csv and canonical_threshold_library.csv are written",
    )

    odds_audit = subparsers.add_parser(
        "odds-audit",
        help="Audit historical odds data quality for backtest safety",
    )
    odds_audit.add_argument("--input", required=True, help="Input CSV path")
    odds_audit.add_argument(
        "--max-gap-minutes",
        type=float,
        default=60.0,
        help="Maximum acceptable quote gap per event/market/book group",
    )
    odds_audit.add_argument("--output-dir", default="outputs", help="Output directory")

    onboarding = subparsers.add_parser(
        "onboarding-check",
        help="Validate Kalshi + OpticOdds onboarding env prerequisites",
    )
    onboarding.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    onboarding.add_argument("--output-dir", default="outputs", help="Output directory")

    live_smoke = subparsers.add_parser(
        "live-smoke",
        help="Run authenticated Kalshi and odds-provider smoke tests",
    )
    live_smoke.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_smoke.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP timeout per request",
    )
    live_smoke.add_argument(
        "--skip-odds-provider-check",
        action="store_true",
        help="Skip TheRundown/odds-provider smoke and verify Kalshi only",
    )
    live_smoke.add_argument("--output-dir", default="outputs", help="Output directory")

    dns_doctor = subparsers.add_parser(
        "dns-doctor",
        help="Diagnose DNS readiness using system and public resolvers",
    )
    dns_doctor.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    dns_doctor.add_argument(
        "--hosts",
        default="",
        help="Optional comma-separated hostnames to test (defaults derive from env)",
    )
    dns_doctor.add_argument(
        "--timeout-seconds",
        type=float,
        default=1.5,
        help="DNS query timeout budget per host",
    )
    dns_doctor.add_argument("--output-dir", default="outputs", help="Output directory")

    live_snapshot = subparsers.add_parser(
        "live-snapshot",
        help="Capture a read-only snapshot from Kalshi and the odds provider",
    )
    live_snapshot.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_snapshot.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP timeout per request",
    )
    live_snapshot.add_argument(
        "--sports-preview-limit",
        type=int,
        default=5,
        help="Number of TheRundown sports records to include in the snapshot",
    )
    live_snapshot.add_argument("--output-dir", default="outputs", help="Output directory")

    live_candidates = subparsers.add_parser(
        "live-candidates",
        help="Fetch TheRundown odds and write a candidate CSV for the paper engine",
    )
    live_candidates.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_candidates.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    live_candidates.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format as interpreted by TheRundown offset rule",
    )
    live_candidates.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    live_candidates.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    live_candidates.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    live_candidates.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    live_candidates.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    live_candidates.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    live_candidates.add_argument("--output-dir", default="outputs", help="Output directory")

    live_paper = subparsers.add_parser(
        "live-paper",
        help="Fetch live candidates from TheRundown and run the paper engine in one step",
    )
    live_paper.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    live_paper.add_argument(
        "--config",
        default=None,
        help="Optional JSON config path for paper decisions",
    )
    live_paper.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    live_paper.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format as interpreted by TheRundown offset rule",
    )
    live_paper.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    live_paper.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    live_paper.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    live_paper.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    live_paper.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    live_paper.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    live_paper.add_argument(
        "--enrich-candidates",
        action="store_true",
        help="Apply optional sports evidence enrichment before paper decisions",
    )
    live_paper.add_argument(
        "--enrichment-csv",
        default=None,
        help="Optional CSV of external evidence used to adjust model_prob",
    )
    live_paper.add_argument(
        "--enrichment-freshness-hours",
        type=float,
        default=12.0,
        help="Maximum age for evidence rows before enrichment is skipped as stale",
    )
    live_paper.add_argument(
        "--enrichment-max-logit-shift",
        type=float,
        default=0.35,
        help="Maximum absolute logit shift applied to model_prob per candidate",
    )
    live_paper.add_argument(
        "--enrichment-include-non-moneyline",
        action="store_true",
        help="Allow enrichment on non-moneyline markets (default enriches moneyline only)",
    )
    live_paper.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    live_paper.add_argument("--output-dir", default="outputs", help="Output directory")

    sports_archive = subparsers.add_parser(
        "sports-archive",
        help="Run live sports paper flows across one or more dates and append a rolling archive",
    )
    sports_archive.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    sports_archive.add_argument(
        "--config",
        default=None,
        help="Optional JSON config path for paper decisions",
    )
    sports_archive.add_argument(
        "--sport-id",
        required=True,
        type=int,
        help="TheRundown sport ID, for example 4 for NBA",
    )
    sports_archive.add_argument(
        "--event-dates",
        required=True,
        help="Comma-separated event dates in YYYY-MM-DD format",
    )
    sports_archive.add_argument(
        "--starting-bankroll",
        required=True,
        type=float,
        help="Starting bankroll amount",
    )
    sports_archive.add_argument(
        "--archive-csv",
        default=None,
        help="Optional rolling archive CSV path, default outputs/live_paper_archive.csv",
    )
    sports_archive.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    sports_archive.add_argument(
        "--market-ids",
        default="1,2,3",
        help="Comma-separated market IDs, default moneyline/spread/total",
    )
    sports_archive.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    sports_archive.add_argument(
        "--offset-minutes",
        type=int,
        default=300,
        help="TheRundown date offset in minutes, default 300 per docs",
    )
    sports_archive.add_argument(
        "--include-in-play",
        action="store_true",
        help="Keep in-play events instead of only pregame events",
    )
    sports_archive.add_argument(
        "--enrich-candidates",
        action="store_true",
        help="Apply optional sports evidence enrichment before paper decisions",
    )
    sports_archive.add_argument(
        "--enrichment-csv",
        default=None,
        help="Optional CSV of external evidence used to adjust model_prob",
    )
    sports_archive.add_argument(
        "--enrichment-freshness-hours",
        type=float,
        default=12.0,
        help="Maximum age for evidence rows before enrichment is skipped as stale",
    )
    sports_archive.add_argument(
        "--enrichment-max-logit-shift",
        type=float,
        default=0.35,
        help="Maximum absolute logit shift applied to model_prob per candidate",
    )
    sports_archive.add_argument(
        "--enrichment-include-non-moneyline",
        action="store_true",
        help="Allow enrichment on non-moneyline markets (default enriches moneyline only)",
    )
    sports_archive.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    sports_archive.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_mlb_map = subparsers.add_parser(
        "kalshi-mlb-map",
        help="Map TheRundown MLB moneylines to Kalshi winner markets and score gross edge",
    )
    kalshi_mlb_map.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with account keys and runtime settings",
    )
    kalshi_mlb_map.add_argument(
        "--event-date",
        required=True,
        help="Event date in YYYY-MM-DD format",
    )
    kalshi_mlb_map.add_argument(
        "--affiliate-ids",
        default="19,22,23",
        help="Comma-separated sportsbook affiliate IDs, default DraftKings/BetMGM/FanDuel",
    )
    kalshi_mlb_map.add_argument(
        "--min-books",
        type=int,
        default=2,
        help="Minimum number of books required to form consensus fair probabilities",
    )
    kalshi_mlb_map.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_mlb_map.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_scan = subparsers.add_parser(
        "kalshi-nonsports-scan",
        help="Rank near-term non-sports Kalshi markets for a small-risk execution workflow",
    )
    kalshi_nonsports_scan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_nonsports_scan.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_nonsports_scan.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_nonsports_scan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_nonsports_scan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_nonsports_scan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of ranked markets embedded in the JSON summary",
    )
    kalshi_nonsports_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_nonsports_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_capture = subparsers.add_parser(
        "kalshi-nonsports-capture",
        help="Append the latest non-sports Kalshi board scan to a stable history CSV",
    )
    kalshi_nonsports_capture.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_nonsports_capture.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_nonsports_capture.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_nonsports_capture.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_nonsports_capture.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_nonsports_capture.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of ranked markets embedded in the scan summary",
    )
    kalshi_nonsports_capture.add_argument(
        "--history-csv",
        default=None,
        help="Optional stable history CSV path; defaults to outputs/kalshi_nonsports_history.csv",
    )
    kalshi_nonsports_capture.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_nonsports_capture.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_quality = subparsers.add_parser(
        "kalshi-nonsports-quality",
        help="Aggregate captured non-sports history into persistent board-quality scores",
    )
    kalshi_nonsports_quality.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-mean-yes-bid",
        type=float,
        default=0.05,
        help="Minimum average Yes bid for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--min-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum two-sided observation ratio for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--max-mean-spread",
        type=float,
        default=0.03,
        help="Maximum average spread for a market to qualify as meaningful",
    )
    kalshi_nonsports_quality.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-quality markets embedded in the summary",
    )
    kalshi_nonsports_quality.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_signals = subparsers.add_parser(
        "kalshi-nonsports-signals",
        help="Convert captured non-sports history into stability-backed trade signals",
    )
    kalshi_nonsports_signals.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-stable-ratio",
        type=float,
        default=0.5,
        help="Minimum stable-observation ratio for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-latest-yes-bid",
        type=float,
        default=0.05,
        help="Minimum latest Yes bid for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--min-mean-yes-bid",
        type=float,
        default=0.05,
        help="Minimum average Yes bid for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--max-mean-spread",
        type=float,
        default=0.03,
        help="Maximum average spread for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--max-yes-bid-stddev",
        type=float,
        default=0.03,
        help="Maximum Yes-bid standard deviation for a market to become signal-eligible",
    )
    kalshi_nonsports_signals.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-signal markets embedded in the summary",
    )
    kalshi_nonsports_signals.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_persistence = subparsers.add_parser(
        "kalshi-nonsports-persistence",
        help="Measure whether non-sports markets stay tradeable across repeated snapshots",
    )
    kalshi_nonsports_persistence.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum latest Yes bid for a snapshot to count as tradeable",
    )
    kalshi_nonsports_persistence.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a snapshot to count as tradeable",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-tradeable-snapshot-count",
        type=int,
        default=2,
        help="Minimum tradeable snapshots required for persistent-tradeable status",
    )
    kalshi_nonsports_persistence.add_argument(
        "--min-consecutive-tradeable-snapshots",
        type=int,
        default=2,
        help="Minimum consecutive tradeable snapshots required for persistent-tradeable status",
    )
    kalshi_nonsports_persistence.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-persistence markets embedded in the summary",
    )
    kalshi_nonsports_persistence.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_deltas = subparsers.add_parser(
        "kalshi-nonsports-deltas",
        help="Compare the latest two non-sports captures to detect board improvement or decay",
    )
    kalshi_nonsports_deltas.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum Yes bid for a snapshot to count as tradeable",
    )
    kalshi_nonsports_deltas.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a snapshot to count as tradeable",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-bid-improvement",
        type=float,
        default=0.01,
        help="Minimum Yes-bid increase to mark a two-sided market as improved",
    )
    kalshi_nonsports_deltas.add_argument(
        "--min-spread-improvement",
        type=float,
        default=0.01,
        help="Minimum spread tightening to mark a two-sided market as improved",
    )
    kalshi_nonsports_deltas.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top delta markets embedded in the summary",
    )
    kalshi_nonsports_deltas.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_categories = subparsers.add_parser(
        "kalshi-nonsports-categories",
        help="Aggregate non-sports board health by category across captured history",
    )
    kalshi_nonsports_categories.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_categories.add_argument(
        "--min-tradeable-yes-bid",
        type=float,
        default=0.05,
        help="Minimum Yes bid for a category observation to count as tradeable",
    )
    kalshi_nonsports_categories.add_argument(
        "--max-tradeable-spread",
        type=float,
        default=0.03,
        help="Maximum spread for a category observation to count as tradeable",
    )
    kalshi_nonsports_categories.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top categories embedded in the summary",
    )
    kalshi_nonsports_categories.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_pressure = subparsers.add_parser(
        "kalshi-nonsports-pressure",
        help="Spot non-sports markets that are building pressure toward a more tradeable state",
    )
    kalshi_nonsports_pressure.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations required before a market can be labeled as build",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-latest-yes-bid",
        type=float,
        default=0.02,
        help="Minimum latest Yes bid for a market to be considered pressure-building",
    )
    kalshi_nonsports_pressure.add_argument(
        "--max-latest-spread",
        type=float,
        default=0.02,
        help="Maximum latest spread for a market to be considered pressure-building",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum two-sided observation ratio required for pressure-build status",
    )
    kalshi_nonsports_pressure.add_argument(
        "--min-recent-bid-change",
        type=float,
        default=0.01,
        help="Minimum latest Yes-bid increase to count as recent pressure",
    )
    kalshi_nonsports_pressure.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-pressure markets embedded in the summary",
    )
    kalshi_nonsports_pressure.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_thresholds = subparsers.add_parser(
        "kalshi-nonsports-thresholds",
        help="Forecast which non-sports markets are approaching the live review thresholds",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--target-yes-bid",
        type=float,
        default=0.05,
        help="Target Yes bid used for tradeability forecasting",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--target-spread",
        type=float,
        default=0.02,
        help="Target spread used for tradeability forecasting",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--recent-window",
        type=int,
        default=5,
        help="Number of most recent observations to use for the trend forecast",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--max-hours-to-target",
        type=float,
        default=6.0,
        help="Maximum forecast hours to count as approaching the threshold",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--min-recent-two-sided-ratio",
        type=float,
        default=0.5,
        help="Minimum recent two-sided ratio required for threshold approach status",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--min-observations",
        type=int,
        default=3,
        help="Minimum observations required before threshold forecasting applies",
    )
    kalshi_nonsports_thresholds.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top-threshold markets embedded in the summary",
    )
    kalshi_nonsports_thresholds.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_priors = subparsers.add_parser(
        "kalshi-nonsports-priors",
        help="Compare user-supplied non-sports fair-value priors against the latest captured board",
    )
    kalshi_nonsports_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_nonsports_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top prior-backed markets embedded in the summary",
    )
    kalshi_nonsports_priors.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts used for fee-aware per-contract edge estimates",
    )
    kalshi_nonsports_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_research_queue = subparsers.add_parser(
        "kalshi-nonsports-research-queue",
        help="Rank uncovered non-sports markets that look worth researching next",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_research_queue.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top research candidates embedded in the summary",
    )
    kalshi_nonsports_research_queue.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_nonsports_auto_priors = subparsers.add_parser(
        "kalshi-nonsports-auto-priors",
        help="Generate thesis-backed auto priors for uncovered non-sports markets using external news evidence",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of fair probabilities that will be upserted with auto-generated rows",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to evaluate per run",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--max-headlines-per-market",
        type=int,
        default=8,
        help="Maximum evidence headlines to score per market",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum average source-quality score required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum number of high-trust evidence sources required before writing an auto prior",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--disable-protect-manual",
        action="store_true",
        help="Allow auto rows to overwrite manual rows (disabled by default for safety)",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate auto priors without writing back into the priors CSV",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical mapping CSV used for optional mapped-ticker scoping",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--restrict-to-mapped-live-tickers",
        action="store_true",
        help="Only generate auto priors for live market tickers currently mapped in canonical_contract_mapping.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--allowed-canonical-niches",
        default="",
        help="Optional comma-separated canonical niches (for example: macro_release,weather_energy_transmission,weather_climate)",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--allowed-categories",
        default="",
        help="Optional comma-separated category allow-list from history.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--disallowed-categories",
        default="Sports",
        help="Optional comma-separated category block-list from history.csv",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per evidence request",
    )
    kalshi_nonsports_auto_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top auto priors embedded in the summary",
    )
    kalshi_nonsports_auto_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_catalog = subparsers.add_parser(
        "kalshi-weather-catalog",
        help="Build a weather-market catalog with settlement-spec metadata from captured non-sports history",
    )
    kalshi_weather_catalog.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_catalog.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top weather markets embedded in the summary",
    )
    kalshi_weather_catalog.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_priors = subparsers.add_parser(
        "kalshi-weather-priors",
        help="Generate weather-contract priors (daily rain, daily temperature, monthly anomaly) from weather-specific data sources",
    )
    kalshi_weather_priors.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV where generated weather priors are upserted",
    )
    kalshi_weather_priors.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_priors.add_argument(
        "--allowed-contract-families",
        default="daily_rain,daily_temperature",
        help="Comma-separated weather contract families to process",
    )
    kalshi_weather_priors.add_argument(
        "--max-markets",
        type=int,
        default=30,
        help="Maximum weather markets to process per run",
    )
    kalshi_weather_priors.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per weather data request",
    )
    kalshi_weather_priors.add_argument(
        "--historical-lookback-years",
        type=int,
        default=15,
        help="Years of station-level historical day-of-year samples to use when NOAA CDO token is available",
    )
    kalshi_weather_priors.add_argument(
        "--station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Max age for cached station-history snapshots before forcing a fresh CDO pull",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-gridpoint-data",
        action="store_true",
        help="Disable NWS forecastGridData enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-observations",
        action="store_true",
        help="Disable NWS station observations enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nws-alerts",
        action="store_true",
        help="Disable NWS active alerts enrichment for rain/temperature priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-ncei-normals",
        action="store_true",
        help="Disable NCEI daily normals enrichment for station/day priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-mrms-qpe",
        action="store_true",
        help="Disable NOAA MRMS QPE metadata enrichment for rain priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-nbm-snapshot",
        action="store_true",
        help="Disable NOAA NBM snapshot metadata enrichment for weather priors",
    )
    kalshi_weather_priors.add_argument(
        "--disable-protect-manual",
        action="store_true",
        help="Allow generated weather priors to overwrite manual rows (disabled by default for safety)",
    )
    kalshi_weather_priors.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate weather priors without writing back to the priors CSV",
    )
    kalshi_weather_priors.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top weather priors embedded in the summary",
    )
    kalshi_weather_priors.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_weather_prewarm = subparsers.add_parser(
        "kalshi-weather-prewarm",
        help="Prewarm NOAA CDO station/day climatology cache for daily weather markets",
    )
    kalshi_weather_prewarm.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_weather_prewarm.add_argument(
        "--historical-lookback-years",
        type=int,
        default=15,
        help="Years of station-level historical day-of-year samples to cache",
    )
    kalshi_weather_prewarm.add_argument(
        "--station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Max age for cached station-history snapshots before forcing refresh",
    )
    kalshi_weather_prewarm.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per station/day prewarm fetch",
    )
    kalshi_weather_prewarm.add_argument(
        "--max-station-day-keys",
        type=int,
        default=500,
        help="Maximum unique station/day keys to prewarm per run",
    )
    kalshi_weather_prewarm.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_contract_specs = subparsers.add_parser(
        "kalshi-temperature-contract-specs",
        help="Build a canonical contract-spec snapshot for Kalshi temperature markets from live event/market metadata",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--env-file",
        default=".env",
        help="Env-style file used to resolve Kalshi environment and credentials",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per Kalshi events request",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Kalshi events page size",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum events pages to scan per run",
    )
    kalshi_temperature_contract_specs.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Number of top contract specs embedded in the summary",
    )
    kalshi_temperature_contract_specs.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_constraint_scan = subparsers.add_parser(
        "kalshi-temperature-constraint-scan",
        help="Scan Kalshi temperature contract specs against intraday station observations for hard constraint opportunities",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit kalshi_temperature_contract_specs CSV path (latest in output-dir used when omitted)",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout per station observations request",
    )
    kalshi_temperature_constraint_scan.add_argument(
        "--max-markets",
        type=int,
        default=100,
        help="Maximum markets to evaluate per run",
    )
    kalshi_temperature_constraint_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_metar_ingest = subparsers.add_parser(
        "kalshi-temperature-metar-ingest",
        help="Ingest AviationWeather METAR cache files and update per-station local-day maxima for Kalshi temperature workflows",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit kalshi_temperature_contract_specs CSV path used for station-timezone mapping",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--cache-url",
        default="https://aviationweather.gov/data/cache/metars.cache.csv.gz",
        help="METAR cache CSV GZ URL",
    )
    kalshi_temperature_metar_ingest.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout per METAR cache request",
    )
    kalshi_temperature_metar_ingest.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_temperature_trader = subparsers.add_parser(
        "kalshi-temperature-trader",
        help="Deterministic temperature intent -> policy gate -> execution bridge on top of kalshi-micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--env-file",
        default=".env",
        help="Env-style file used to resolve Kalshi environment and credentials",
    )
    kalshi_temperature_trader.add_argument(
        "--specs-csv",
        default=None,
        help="Optional explicit contract-spec CSV; uses constraint source or latest snapshot when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--constraint-csv",
        default=None,
        help="Optional explicit constraint-scan CSV; runs fresh scan when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-summary-json",
        default=None,
        help="Optional explicit METAR summary JSON; latest summary in output-dir used when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--metar-state-json",
        default=None,
        help="Optional explicit METAR state JSON; inferred from summary or output-dir when omitted",
    )
    kalshi_temperature_trader.add_argument(
        "--ws-state-json",
        default=None,
        help="Optional explicit websocket state JSON; defaults to kalshi_ws_state_latest.json in output-dir",
    )
    kalshi_temperature_trader.add_argument(
        "--policy-version",
        default="temperature_policy_v1",
        help="Policy version tag attached to intents",
    )
    kalshi_temperature_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per order for approved intents",
    )
    kalshi_temperature_trader.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum approved intents to convert into executable plans",
    )
    kalshi_temperature_trader.add_argument(
        "--max-markets",
        type=int,
        default=100,
        help="Maximum markets evaluated when running an implicit constraint scan",
    )
    kalshi_temperature_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=12.0,
        help="HTTP timeout used by scan and execution calls",
    )
    kalshi_temperature_trader.add_argument(
        "--yes-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for YES-side intents",
    )
    kalshi_temperature_trader.add_argument(
        "--no-max-entry-price",
        type=float,
        default=0.95,
        help="Maximum entry price for NO-side intents",
    )
    kalshi_temperature_trader.add_argument(
        "--min-settlement-confidence",
        type=float,
        default=0.6,
        help="Minimum settlement confidence score required for intent approval",
    )
    kalshi_temperature_trader.add_argument(
        "--max-metar-age-minutes",
        type=float,
        default=20.0,
        help="Maximum allowed METAR observation age in minutes",
    )
    kalshi_temperature_trader.add_argument(
        "--min-hours-to-close",
        type=float,
        default=0.0,
        help="Minimum hours-to-close required for approval (cutoff window)",
    )
    kalshi_temperature_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=48.0,
        help="Maximum hours-to-close for active horizon filtering",
    )
    kalshi_temperature_trader.add_argument(
        "--max-intents-per-underlying",
        type=int,
        default=1,
        help="Maximum approved intents per underlying key (series|station|date)",
    )
    kalshi_temperature_trader.add_argument(
        "--disable-require-market-snapshot-seq",
        action="store_true",
        help="Allow intents when market snapshot sequence is missing from ws-state",
    )
    kalshi_temperature_trader.add_argument(
        "--require-metar-snapshot-sha",
        action="store_true",
        help="Require METAR raw snapshot SHA in intent evidence",
    )
    kalshi_temperature_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Allow live order writes (still subject to existing micro-execute safety gates)",
    )
    kalshi_temperature_trader.add_argument(
        "--intents-only",
        action="store_true",
        help="Build intents and bridge plans only; skip micro-execution",
    )
    kalshi_temperature_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Planning bankroll forwarded to micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Daily risk cap forwarded to micro-execute",
    )
    kalshi_temperature_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Forwarded to micro-execute cancel behavior",
    )
    kalshi_temperature_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Forwarded to micro-execute resting hold duration",
    )
    kalshi_temperature_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="Forwarded live submission cap",
    )
    kalshi_temperature_trader.add_argument(
        "--max-live-cost-per-day-dollars",
        type=float,
        default=3.0,
        help="Forwarded live cost cap",
    )
    kalshi_temperature_trader.add_argument(
        "--enforce-trade-gate",
        action="store_true",
        help="Enable micro-execute trade gate checks",
    )
    kalshi_temperature_trader.add_argument(
        "--enforce-ws-state-authority",
        action="store_true",
        help="Enable micro-execute websocket state authority gate",
    )
    kalshi_temperature_trader.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum websocket state staleness before gate blocks",
    )
    kalshi_temperature_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    polymarket_market_ingest = subparsers.add_parser(
        "polymarket-market-ingest",
        help="Optional market-data ingest adapter for Polymarket temperature markets (no execution)",
    )
    polymarket_market_ingest.add_argument(
        "--max-markets",
        type=int,
        default=500,
        help="Maximum normalized markets to keep per run",
    )
    polymarket_market_ingest.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Gamma API page size per request",
    )
    polymarket_market_ingest.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum pages to scan per run",
    )
    polymarket_market_ingest.add_argument(
        "--gamma-base-url",
        default="https://gamma-api.polymarket.com",
        help="Polymarket Gamma API base URL",
    )
    polymarket_market_ingest.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per Gamma request",
    )
    polymarket_market_ingest.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive/closed markets when fetching pages",
    )
    polymarket_market_ingest.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_focus_dossier = subparsers.add_parser(
        "kalshi-focus-dossier",
        help="Build a compact dossier for the current top-focus non-sports market",
    )
    kalshi_focus_dossier.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_focus_dossier.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used to choose the current focus market",
    )
    kalshi_focus_dossier.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_focus_dossier.add_argument(
        "--recent-observation-limit",
        type=int,
        default=5,
        help="Number of recent observations embedded in the dossier",
    )
    kalshi_focus_dossier.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_plan = subparsers.add_parser(
        "kalshi-micro-prior-plan",
        help="Build a read-only side-aware maker plan from non-sports priors",
    )
    kalshi_micro_prior_plan.add_argument(
        "--env-file",
        default=None,
        help="Optional env-style file used for live balance and incentive lookups",
    )
    kalshi_micro_prior_plan.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_plan.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_plan.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_plan.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_plan.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_plan.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_plan.add_argument(
        "--selection-lane",
        choices=["maker_edge", "probability_first", "kelly_unified"],
        default="maker_edge",
        help="Plan ranking lane: maker-edge, probability-first compounding, or Kelly-unified",
    )
    kalshi_micro_prior_plan.add_argument(
        "--min-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate (0-1)",
    )
    kalshi_micro_prior_plan.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_plan.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_plan.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_plan.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays and use global maker filters only",
    )
    kalshi_micro_prior_plan.add_argument(
        "--require-canonical-mapping",
        action="store_true",
        help="Only allow plans for live markets that are mapped to a canonical ticker",
    )
    kalshi_micro_prior_plan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top plans embedded in the summary",
    )
    kalshi_micro_prior_plan.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_plan.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_plan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_execute = subparsers.add_parser(
        "kalshi-micro-prior-execute",
        help="Run a read-only or explicit live micro execution cycle from prior-backed side-aware plans",
    )
    kalshi_micro_prior_execute.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_execute.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_execute.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_execute.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_execute.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_execute.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_execute.add_argument(
        "--selection-lane",
        choices=["maker_edge", "probability_first", "kelly_unified"],
        default="maker_edge",
        help="Plan ranking lane: maker-edge, probability-first compounding, or Kelly-unified",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate for planning (0-1)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-live-selected-fair-probability",
        type=float,
        default=None,
        help="Optional minimum selected-side fair probability gate for live trade admission (0-1)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_execute.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-daily-weather-live-only",
        action="store_true",
        help="Allow non-daily-weather contracts to pass live gating (default enforces daily weather only)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-daily-weather-board-coverage",
        action="store_true",
        help="Allow live mode even when captured history is missing daily weather board coverage",
    )
    kalshi_micro_prior_execute.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit orders if all other live safety checks pass",
    )
    kalshi_micro_prior_execute.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_micro_prior_execute.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_execute.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_execute.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_execute.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_execute.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_execute.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-report-json",
        default=None,
        help="Optional explicit execution-frontier report JSON path; if omitted, latest report by mtime is used",
    )
    kalshi_micro_prior_execute.add_argument(
        "--execution-frontier-max-report-age-seconds",
        type=float,
        default=10800.0,
        help="Maximum accepted age for the selected execution-frontier report before gating treats it as stale",
    )
    kalshi_micro_prior_execute.add_argument(
        "--enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced). Default: enabled.",
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_false",
        help="Disable websocket-state authority gating (not recommended for unattended live mode)",
    )
    kalshi_micro_prior_execute.set_defaults(enforce_ws_state_authority=True)
    kalshi_micro_prior_execute.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_prior_execute.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_prior_execute.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_execute.add_argument(
        "--daily-weather-board-max-age-seconds",
        type=float,
        default=900.0,
        help="Maximum allowed age for daily-weather board snapshot before live daily-weather gating blocks",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-enabled",
        action="store_true",
        help="Promote climate-router tradable rows into a capped pilot lane before execution",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-summary-json",
        default=None,
        help="Optional explicit climate-router summary JSON path; defaults to latest router summary in output-dir",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-max-orders-per-run",
        type=int,
        default=1,
        help="Maximum promoted router pilot orders per run",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-contracts-cap",
        type=int,
        default=1,
        help="Maximum contracts per promoted router pilot order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-required-ev-dollars",
        type=float,
        default=0.01,
        help="Minimum expected value dollars per promoted router pilot order",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-allowed-classes",
        default="tradable",
        help=(
            "Comma-separated climate opportunity classes eligible for pilot promotion "
            "(e.g. tradable,hot_positive)"
        ),
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-allowed-families",
        default="",
        help="Optional comma-separated contract-family allowlist for pilot promotion",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-excluded-families",
        default="",
        help="Optional comma-separated contract-family denylist for pilot promotion",
    )
    kalshi_micro_prior_execute.add_argument(
        "--climate-router-pilot-policy-scope-override-enabled",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_true",
        help=(
            "Allow climate-router pilot rows to bypass daily-weather-only scope under pilot safety caps "
            "(max 1 order/run, contracts cap 1)"
        ),
    )
    kalshi_micro_prior_execute.add_argument(
        "--disable-climate-router-pilot-policy-scope-override",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_false",
        help="Disable pilot-only policy-scope override and keep strict daily-weather-only gating",
    )
    kalshi_micro_prior_execute.set_defaults(climate_router_pilot_policy_scope_override_enabled=False)
    kalshi_micro_prior_execute.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_trader = subparsers.add_parser(
        "kalshi-micro-prior-trader",
        help="Run the unattended-safe prior-backed trader loop with optional capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_trader.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_trader.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used for regime and focus-market context",
    )
    kalshi_micro_prior_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_trader.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-daily-weather-live-only",
        action="store_true",
        help="Allow non-daily-weather contracts to pass live gating (default enforces daily weather only)",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-daily-weather-board-coverage",
        action="store_true",
        help="Allow live mode even when captured history is missing daily weather board coverage",
    )
    kalshi_micro_prior_trader.add_argument(
        "--daily-weather-board-max-age-seconds",
        type=float,
        default=900.0,
        help="Maximum allowed age for daily-weather board snapshot before live daily-weather gating blocks",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-enabled",
        action="store_true",
        help="Promote climate-router tradable rows into a capped pilot lane before execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-summary-json",
        default=None,
        help="Optional explicit climate-router summary JSON path; defaults to latest router summary in output-dir",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-max-orders-per-run",
        type=int,
        default=1,
        help="Maximum promoted router pilot orders per run",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-contracts-cap",
        type=int,
        default=1,
        help="Maximum contracts per promoted router pilot order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-required-ev-dollars",
        type=float,
        default=0.01,
        help="Minimum expected value dollars per promoted router pilot order",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-allowed-classes",
        default="tradable",
        help=(
            "Comma-separated climate opportunity classes eligible for pilot promotion "
            "(e.g. tradable,hot_positive)"
        ),
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-allowed-families",
        default="",
        help="Optional comma-separated contract-family allowlist for pilot promotion",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-excluded-families",
        default="",
        help="Optional comma-separated contract-family denylist for pilot promotion",
    )
    kalshi_micro_prior_trader.add_argument(
        "--climate-router-pilot-policy-scope-override-enabled",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_true",
        help=(
            "Allow climate-router pilot rows to bypass daily-weather-only scope under pilot safety caps "
            "(max 1 order/run, contracts cap 1)"
        ),
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-climate-router-pilot-policy-scope-override",
        dest="climate_router_pilot_policy_scope_override_enabled",
        action="store_false",
        help="Disable pilot-only policy-scope override and keep strict daily-weather-only gating",
    )
    kalshi_micro_prior_trader.set_defaults(climate_router_pilot_policy_scope_override_enabled=False)
    kalshi_micro_prior_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit orders if all prior-trade safety checks pass",
    )
    kalshi_micro_prior_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_micro_prior_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_trader.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_trader.add_argument(
        "--skip-capture",
        action="store_true",
        help="Reuse existing history instead of capturing a fresh board snapshot first",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=4000.0,
        help="Capture markets closing within this many hours before prior execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page during fresh capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--max-pages",
        type=int,
        default=12,
        help="Maximum Kalshi event pages to scan during fresh capture",
    )
    kalshi_micro_prior_trader.add_argument(
        "--use-temp-live-env",
        action="store_true",
        help="When live orders are enabled, create a temporary env copy with BETBOT_ENABLE_LIVE_ORDERS=1 for execute and reconcile",
    )
    kalshi_micro_prior_trader.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_trader.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-report-json",
        default=None,
        help="Optional explicit execution-frontier report JSON path; if omitted, latest report by mtime is used",
    )
    kalshi_micro_prior_trader.add_argument(
        "--execution-frontier-max-report-age-seconds",
        type=float,
        default=10800.0,
        help="Maximum accepted age for the selected execution-frontier report before gating treats it as stale",
    )
    kalshi_micro_prior_trader.add_argument(
        "--enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced). Default: enabled.",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-enforce-ws-state-authority",
        dest="enforce_ws_state_authority",
        action="store_false",
        help="Disable websocket-state authority gating (not recommended for unattended live mode)",
    )
    kalshi_micro_prior_trader.set_defaults(enforce_ws_state_authority=True)
    kalshi_micro_prior_trader.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_prior_trader.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_prior_trader.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-refresh-weather-priors",
        action="store_true",
        help="Skip weather-specific prior refresh before news auto-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-prewarm-weather-station-history",
        action="store_true",
        help="Skip station-day climatology prewarm before weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-prior-max-markets",
        type=int,
        default=30,
        help="Maximum weather markets to process during weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-allowed-contract-families",
        default="daily_rain,daily_temperature",
        help="Comma-separated weather contract families for weather-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-prewarm-max-station-day-keys",
        type=int,
        default=500,
        help="Maximum unique station/day keys to prewarm each unattended trader cycle",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-historical-lookback-years",
        type=int,
        default=15,
        help="Station-history lookback years used by weather prewarm and weather priors",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-weather-station-history-cache-max-age-hours",
        type=float,
        default=24.0,
        help="Maximum cache age for station-history snapshots before refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before prior-trader execution",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_micro_prior_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_prior_watch = subparsers.add_parser(
        "kalshi-micro-prior-watch",
        help="Run one prior-aware watch cycle: capture, status, and prior-trader dry-run from the same loop",
    )
    kalshi_micro_prior_watch.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_prior_watch.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_micro_prior_watch.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_micro_prior_watch.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional watch-history CSV used for regime and focus-market context",
    )
    kalshi_micro_prior_watch.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_micro_prior_watch.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_micro_prior_watch.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for the generic status planning path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_micro_prior_watch.add_argument(
        "--canonical-mapping-csv",
        default="data/research/canonical_contract_mapping.csv",
        help="Canonical-to-live mapping CSV path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--canonical-threshold-csv",
        default="data/research/canonical_threshold_library.csv",
        help="Canonical threshold-library CSV path",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-canonical-thresholds",
        action="store_true",
        help="Disable canonical threshold overlays for dry-run analysis; live execution still enforces canonical niche policy",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-require-canonical-for-live",
        action="store_true",
        help="Disable canonical mapping requirement in dry-run reports only; live execution still requires canonical mapping",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_prior_watch.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_prior_watch.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_micro_prior_watch.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_watch.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_prior_watch.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for live reads or writes",
    )
    kalshi_micro_prior_watch.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_micro_prior_watch.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_prior_watch.add_argument(
        "--include-incentives",
        action="store_true",
        help="Include incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_micro_prior_watch.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before prior-trader execution",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_micro_prior_watch.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_arb_scan = subparsers.add_parser(
        "kalshi-arb-scan",
        help="Scan mutually-exclusive Kalshi events for fee-buffered partition arbitrage opportunities",
    )
    kalshi_arb_scan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_arb_scan.add_argument(
        "--fee-buffer-per-contract",
        type=float,
        default=0.01,
        help="Fee/slippage buffer per market leg in dollars",
    )
    kalshi_arb_scan.add_argument(
        "--min-margin-dollars",
        type=float,
        default=0.0,
        help="Only keep opportunities with at least this expected margin",
    )
    kalshi_arb_scan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_arb_scan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_arb_scan.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of opportunities embedded in the summary",
    )
    kalshi_arb_scan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_arb_scan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_supervisor = subparsers.add_parser(
        "kalshi-supervisor",
        help="Run an operational Kalshi loop with rate-limited cycles, status checks, prior-trader execution, and arb scanning",
    )
    kalshi_supervisor.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_supervisor.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_supervisor.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_supervisor.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional ledger CSV path override",
    )
    kalshi_supervisor.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_supervisor.add_argument(
        "--cycles",
        type=int,
        default=1,
        help="Number of supervisor cycles to run",
    )
    kalshi_supervisor.add_argument(
        "--sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between cycles",
    )
    kalshi_supervisor.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Allow live orders when exchange status indicates trading is active",
    )
    kalshi_supervisor.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="Cancel resting orders immediately after submission",
    )
    kalshi_supervisor.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="Seconds to leave a resting order on the book before canceling it",
    )
    kalshi_supervisor.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_supervisor.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_supervisor.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_supervisor.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum number of planned orders",
    )
    kalshi_supervisor.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_supervisor.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_supervisor.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_supervisor.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_supervisor.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_supervisor.add_argument(
        "--min-live-maker-edge",
        type=float,
        default=0.01,
        help="Minimum maker edge required before a prior-backed live order can pass the gate",
    )
    kalshi_supervisor.add_argument(
        "--min-live-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker edge net estimated fees required before a prior-backed live order can pass the gate",
    )
    kalshi_supervisor.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_supervisor.add_argument(
        "--read-requests-per-minute",
        type=float,
        default=120.0,
        help="Read request throttle for the supervisor",
    )
    kalshi_supervisor.add_argument(
        "--write-requests-per-minute",
        type=float,
        default=30.0,
        help="Write request throttle for the supervisor",
    )
    kalshi_supervisor.add_argument(
        "--disable-failure-remediation",
        action="store_true",
        help="Disable supervisor-level retries/remediation on transient failure states",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-max-retries",
        type=int,
        default=2,
        help="Maximum remediation retries per cycle when transient failures occur",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-backoff-seconds",
        type=float,
        default=5.0,
        help="Base exponential backoff for remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_supervisor.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--disable-arb-scan",
        action="store_true",
        help="Disable partition-arb scanning in each cycle",
    )
    kalshi_supervisor.add_argument(
        "--disable-incentives",
        action="store_true",
        help="Disable incentive-program bonus estimates in prior net-edge calculations",
    )
    kalshi_supervisor.add_argument(
        "--disable-auto-refresh-priors",
        action="store_true",
        help="Skip auto-prior refresh before each prior-trader cycle",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-max-markets",
        type=int,
        default=15,
        help="Maximum uncovered markets to process during auto-prior refresh",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-evidence-count",
        type=int,
        default=2,
        help="Minimum evidence items required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-evidence-quality",
        type=float,
        default=0.55,
        help="Minimum evidence-quality score required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--auto-prior-min-high-trust-sources",
        type=int,
        default=1,
        help="Minimum high-trust sources required to write an auto prior",
    )
    kalshi_supervisor.add_argument(
        "--disable-enforce-ws-state-authority",
        action="store_true",
        help="Allow live supervisor cycles to proceed without websocket-state authority gating",
    )
    kalshi_supervisor.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_supervisor.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_supervisor.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-attempts",
        type=int,
        default=2,
        help="Remediation retries when exchange status is unavailable due to upstream issues",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between exchange-status remediation retries",
    )
    kalshi_supervisor.add_argument(
        "--disable-exchange-status-dns-remediation",
        action="store_true",
        help="Disable DNS-doctor remediation before exchange-status retry attempts",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to exchange-status timeout on each remediation retry",
    )
    kalshi_supervisor.add_argument(
        "--exchange-status-self-heal-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used during exchange-status remediation retries",
    )
    kalshi_supervisor.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_autopilot = subparsers.add_parser(
        "kalshi-autopilot",
        help="Run fully-guarded autonomous live development loop with preflight gates and progressive scaling",
    )
    kalshi_autopilot.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_autopilot.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_autopilot.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_autopilot.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_autopilot.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_autopilot.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Request live mode (still subject to automated safety gates)",
    )
    kalshi_autopilot.add_argument("--cycles", type=int, default=1, help="Number of supervisor cycles to run")
    kalshi_autopilot.add_argument(
        "--sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between supervisor cycles",
    )
    kalshi_autopilot.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_autopilot.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_autopilot.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one run",
    )
    kalshi_autopilot.add_argument("--contracts-per-order", type=int, default=1, help="Contracts per planned order")
    kalshi_autopilot.add_argument("--max-orders", type=int, default=3, help="Maximum number of planned orders")
    kalshi_autopilot.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_autopilot.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_autopilot.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_autopilot.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_autopilot.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_autopilot.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_autopilot.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-dns-doctor",
        action="store_true",
        help="Skip DNS preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-live-smoke",
        action="store_true",
        help="Skip live-smoke preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--preflight-live-smoke-include-odds-provider",
        action="store_true",
        help="Include odds-provider smoke check in autopilot preflight (Kalshi-only by default)",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-ws-state-collect",
        action="store_true",
        help="Skip websocket-state preflight gate",
    )
    kalshi_autopilot.add_argument(
        "--ws-collect-run-seconds",
        type=float,
        default=45.0,
        help="Seconds to collect websocket state during preflight",
    )
    kalshi_autopilot.add_argument(
        "--ws-collect-max-events",
        type=int,
        default=250,
        help="Maximum websocket events to collect during preflight",
    )
    kalshi_autopilot.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_autopilot.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_autopilot.add_argument(
        "--preflight-self-heal-attempts",
        type=int,
        default=2,
        help="In-run preflight remediation retries before forcing dry-run",
    )
    kalshi_autopilot.add_argument(
        "--preflight-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between preflight remediation retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-upstream-only",
        action="store_true",
        help="Allow preflight remediation retries for non-upstream failures too",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-retry-ws-state-gate-failures",
        action="store_true",
        help="Disable retries for websocket-state stale/empty/desynced gate failures in upstream-only mode",
    )
    kalshi_autopilot.add_argument(
        "--disable-preflight-self-heal-dns-remediation",
        action="store_true",
        help="Disable remediation DNS-doctor runs between preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to preflight timeout on each retry attempt",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by adaptive preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collection window added per preflight retry",
    )
    kalshi_autopilot.add_argument(
        "--preflight-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collection window used by adaptive preflight retries",
    )
    kalshi_autopilot.add_argument(
        "--disable-progressive-scaling",
        action="store_true",
        help="Disable adaptive scale-up logic based on consecutive green autopilot runs",
    )
    kalshi_autopilot.add_argument(
        "--scaling-lookback-runs",
        type=int,
        default=20,
        help="How many recent autopilot summaries to inspect for scaling signal",
    )
    kalshi_autopilot.add_argument(
        "--scaling-green-runs-per-step",
        type=int,
        default=3,
        help="Consecutive green runs required before each scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-live-submissions",
        type=int,
        default=1,
        help="Additional live submissions per day per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-live-cost-dollars",
        type=float,
        default=1.0,
        help="Additional live cost cap per day per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-step-daily-risk-cap-dollars",
        type=float,
        default=1.0,
        help="Additional daily risk cap per scaling step",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-live-submissions-per-day",
        type=int,
        default=12,
        help="Absolute upper bound for live submissions per day after scaling",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-live-cost-per-day-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for live cost per day after scaling",
    )
    kalshi_autopilot.add_argument(
        "--scaling-hard-max-daily-risk-cap-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for daily risk cap after scaling",
    )
    kalshi_autopilot.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_watchdog = subparsers.add_parser(
        "kalshi-watchdog",
        help="Run continuous guarded-autopilot loop with upstream remediation and persistent live kill-switch",
    )
    kalshi_watchdog.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_watchdog.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="CSV of user-supplied fair probabilities for specific Kalshi non-sports markets",
    )
    kalshi_watchdog.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Stable history CSV path from kalshi-nonsports-capture",
    )
    kalshi_watchdog.add_argument("--ledger-csv", default=None, help="Optional ledger CSV path override")
    kalshi_watchdog.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_watchdog.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Request live mode (still subject to automated safety gates and kill-switch)",
    )
    kalshi_watchdog.add_argument(
        "--loops",
        type=int,
        default=0,
        help="Watchdog loop count; set 0 to run continuously",
    )
    kalshi_watchdog.add_argument(
        "--sleep-between-loops-seconds",
        type=float,
        default=60.0,
        help="Sleep between healthy watchdog loops",
    )
    kalshi_watchdog.add_argument(
        "--autopilot-cycles",
        type=int,
        default=1,
        help="Number of supervisor cycles to run per watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--autopilot-sleep-between-cycles-seconds",
        type=float,
        default=20.0,
        help="Delay between supervisor cycles inside each watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout for reads or writes",
    )
    kalshi_watchdog.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Reference bankroll for planning fractions",
    )
    kalshi_watchdog.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total notional cost planned for one loop",
    )
    kalshi_watchdog.add_argument("--contracts-per-order", type=int, default=1, help="Contracts per planned order")
    kalshi_watchdog.add_argument("--max-orders", type=int, default=3, help="Maximum number of planned orders")
    kalshi_watchdog.add_argument(
        "--min-maker-edge",
        type=float,
        default=0.005,
        help="Minimum maker-entry edge required to include a prior-backed plan",
    )
    kalshi_watchdog.add_argument(
        "--min-maker-edge-net-fees",
        type=float,
        default=0.0,
        help="Minimum maker-entry edge net estimated fees required to include a prior-backed plan",
    )
    kalshi_watchdog.add_argument(
        "--max-entry-price",
        type=float,
        default=0.99,
        help="Maximum maker-entry price allowed for a planned order",
    )
    kalshi_watchdog.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day (unused slots carry forward)",
    )
    kalshi_watchdog.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum estimated notional cost submitted live per trading day",
    )
    kalshi_watchdog.add_argument(
        "--failure-remediation-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to timeout on each supervisor remediation retry",
    )
    kalshi_watchdog.add_argument(
        "--failure-remediation-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by supervisor remediation retries",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-dns-doctor",
        action="store_true",
        help="Skip DNS preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-live-smoke",
        action="store_true",
        help="Skip live-smoke preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--preflight-live-smoke-include-odds-provider",
        action="store_true",
        help="Include odds-provider smoke check in watchdog/autopilot preflight (Kalshi-only by default)",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-ws-state-collect",
        action="store_true",
        help="Skip websocket-state preflight gate inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--ws-collect-run-seconds",
        type=float,
        default=45.0,
        help="Seconds to collect websocket state during preflight",
    )
    kalshi_watchdog.add_argument(
        "--ws-collect-max-events",
        type=int,
        default=250,
        help="Maximum websocket events to collect during preflight",
    )
    kalshi_watchdog.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_watchdog.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_watchdog.add_argument(
        "--preflight-self-heal-attempts",
        type=int,
        default=2,
        help="In-run preflight remediation retries inside autopilot before forcing dry-run",
    )
    kalshi_watchdog.add_argument(
        "--preflight-self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between preflight remediation retries inside autopilot",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-upstream-only",
        action="store_true",
        help="Allow preflight remediation retries for non-upstream failures too",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-retry-ws-state-gate-failures",
        action="store_true",
        help="Disable retries for websocket-state stale/empty/desynced gate failures in upstream-only mode",
    )
    kalshi_watchdog.add_argument(
        "--disable-preflight-self-heal-dns-remediation",
        action="store_true",
        help="Disable remediation DNS-doctor runs between preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to preflight timeout on each retry attempt",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by adaptive preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collection window added per preflight retry",
    )
    kalshi_watchdog.add_argument(
        "--preflight-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collection window used by adaptive preflight retries",
    )
    kalshi_watchdog.add_argument(
        "--disable-progressive-scaling",
        action="store_true",
        help="Disable adaptive scale-up logic based on consecutive green autopilot runs",
    )
    kalshi_watchdog.add_argument(
        "--scaling-lookback-runs",
        type=int,
        default=20,
        help="How many recent autopilot summaries to inspect for scaling signal",
    )
    kalshi_watchdog.add_argument(
        "--scaling-green-runs-per-step",
        type=int,
        default=3,
        help="Consecutive green runs required before each scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-live-submissions",
        type=int,
        default=1,
        help="Additional live submissions per day per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-live-cost-dollars",
        type=float,
        default=1.0,
        help="Additional live cost cap per day per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-step-daily-risk-cap-dollars",
        type=float,
        default=1.0,
        help="Additional daily risk cap per scaling step",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-live-submissions-per-day",
        type=int,
        default=12,
        help="Absolute upper bound for live submissions per day after scaling",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-live-cost-per-day-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for live cost per day after scaling",
    )
    kalshi_watchdog.add_argument(
        "--scaling-hard-max-daily-risk-cap-dollars",
        type=float,
        default=12.0,
        help="Absolute upper bound for daily risk cap after scaling",
    )
    kalshi_watchdog.add_argument(
        "--upstream-incident-threshold",
        type=int,
        default=3,
        help="Consecutive upstream incidents before kill-switch engages",
    )
    kalshi_watchdog.add_argument(
        "--kill-switch-cooldown-seconds",
        type=float,
        default=1800.0,
        help="Kill-switch hold period after escalation",
    )
    kalshi_watchdog.add_argument(
        "--healthy-runs-to-clear-kill-switch",
        type=int,
        default=1,
        help="Healthy autopilot runs required to clear an active kill-switch early",
    )
    kalshi_watchdog.add_argument(
        "--upstream-retry-backoff-base-seconds",
        type=float,
        default=15.0,
        help="Base backoff used after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--upstream-retry-backoff-max-seconds",
        type=float,
        default=300.0,
        help="Maximum backoff used after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-attempts-per-loop",
        type=int,
        default=2,
        help="In-loop remediation retries before deferring to next watchdog loop",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-pause-seconds",
        type=float,
        default=10.0,
        help="Pause between in-loop remediation retry attempts",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-timeout-multiplier",
        type=float,
        default=1.5,
        help="Multiplier applied to watchdog in-loop retry timeout on each autopilot re-attempt",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-timeout-cap-seconds",
        type=float,
        default=45.0,
        help="Maximum timeout used by watchdog in-loop autopilot re-attempts",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-ws-collect-increment-seconds",
        type=float,
        default=15.0,
        help="Additional websocket collect window added per watchdog in-loop autopilot re-attempt",
    )
    kalshi_watchdog.add_argument(
        "--self-heal-retry-ws-collect-max-seconds",
        type=float,
        default=180.0,
        help="Maximum websocket collect window used by watchdog in-loop autopilot re-attempts",
    )
    kalshi_watchdog.add_argument(
        "--disable-remediation-dns-doctor",
        action="store_true",
        help="Disable remediation DNS doctor runs after upstream incidents",
    )
    kalshi_watchdog.add_argument(
        "--kill-switch-state-json",
        default=None,
        help="Optional path for persistent kill-switch state JSON",
    )
    kalshi_watchdog.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_plan = subparsers.add_parser(
        "kalshi-micro-plan",
        help="Build a tiny read-only Kalshi order plan for a small bankroll",
    )
    kalshi_micro_plan.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_plan.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the order-planning budget",
    )
    kalshi_micro_plan.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_plan.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_plan.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_plan.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_plan.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_plan.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_plan.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_plan.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_plan.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_plan.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_plan.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_plan.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_execute = subparsers.add_parser(
        "kalshi-micro-execute",
        help="Dry-run or execute a tiny non-sports Kalshi maker-order workflow",
    )
    kalshi_micro_execute.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_execute.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the order-planning budget",
    )
    kalshi_micro_execute.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_execute.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_execute.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_execute.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_execute.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_execute.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_execute.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_execute.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_execute.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_execute.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_execute.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit live orders. Also requires BETBOT_ENABLE_LIVE_ORDERS in the env file.",
    )
    kalshi_micro_execute.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="If positive, let a resting maker order sit for this many seconds before canceling it.",
    )
    kalshi_micro_execute.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="After a successful submit, immediately cancel any resting order to smoke-test submit/cancel flow.",
    )
    kalshi_micro_execute.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_execute.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_execute.add_argument(
        "--disable-auto-duplicate-janitor",
        action="store_true",
        help="Disable the live duplicate-order janitor that cancels excess same-price open orders before submit",
    )
    kalshi_micro_execute.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_execute.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_execute.add_argument(
        "--execution-event-log-csv",
        default=None,
        help="Optional persistent execution-event log CSV path; defaults to outputs/kalshi_execution_event_log.csv",
    )
    kalshi_micro_execute.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_execute.add_argument(
        "--execution-frontier-recent-rows",
        type=int,
        default=5000,
        help="Recent execution-event rows to scan when building each frontier snapshot",
    )
    kalshi_micro_execute.add_argument(
        "--enforce-ws-state-authority",
        action="store_true",
        help="Fail closed for live orders unless websocket state is ready (not missing/stale/desynced)",
    )
    kalshi_micro_execute.add_argument(
        "--ws-state-json",
        default=None,
        help="Path to websocket-state JSON snapshot; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_micro_execute.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Maximum allowed websocket-state age before live orders are blocked",
    )
    kalshi_micro_execute.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Persistent non-sports history CSV path used for the trade gate",
    )
    kalshi_micro_execute.add_argument(
        "--enforce-trade-gate",
        action="store_true",
        help="Require the multi-snapshot trade gate to pass before any live write is allowed",
    )
    kalshi_micro_execute.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_execute.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_gate = subparsers.add_parser(
        "kalshi-micro-gate",
        help="Evaluate whether the current non-sports board is strong enough for tiny live automation",
    )
    kalshi_micro_gate.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_gate.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the gate planning budget",
    )
    kalshi_micro_gate.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_gate.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_gate.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_gate.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_gate.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_gate.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_gate.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Persistent non-sports history CSV path used for the gate",
    )
    kalshi_micro_gate.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_gate.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_trader = subparsers.add_parser(
        "kalshi-micro-trader",
        help="Run the gated micro trader: gate first, then execute and reconcile only if allowed",
    )
    kalshi_micro_trader.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_trader.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the trader planning budget",
    )
    kalshi_micro_trader.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_trader.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_trader.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_trader.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_trader.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_trader.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_trader.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_trader.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_trader.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_trader.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_trader.add_argument(
        "--allow-live-orders",
        action="store_true",
        help="Actually submit live orders after the gate passes. Also requires BETBOT_ENABLE_LIVE_ORDERS in the env file.",
    )
    kalshi_micro_trader.add_argument(
        "--resting-hold-seconds",
        type=float,
        default=0.0,
        help="If positive, let a resting maker order sit for this many seconds before canceling it.",
    )
    kalshi_micro_trader.add_argument(
        "--cancel-resting-immediately",
        action="store_true",
        help="After a successful submit, immediately cancel any resting order.",
    )
    kalshi_micro_trader.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_trader.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_trader.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_trader.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_trader.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for status/watch history; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_trader.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Persistent non-sports history CSV path used for the trade gate",
    )
    kalshi_micro_trader.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_trader.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_watch = subparsers.add_parser(
        "kalshi-micro-watch",
        help="Run a sequential read-only watch cycle: capture first, then status from that same snapshot",
    )
    kalshi_micro_watch.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_watch.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the watch planning budget",
    )
    kalshi_micro_watch.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_watch.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_watch.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_watch.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_watch.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_watch.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_watch.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_watch.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_watch.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_watch.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_watch.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_watch.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_watch.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_watch.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Persistent non-sports history CSV path used for the watch cycle",
    )
    kalshi_micro_watch.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_watch.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for watch-run summaries; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_watch.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_reconcile = subparsers.add_parser(
        "kalshi-micro-reconcile",
        help="Audit orders, queue positions, fees, and exposure after a micro execution run",
    )
    kalshi_micro_reconcile.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_reconcile.add_argument(
        "--execute-summary-file",
        default=None,
        help="Optional kalshi_micro_execute_summary JSON path; defaults to the newest one in outputs/",
    )
    kalshi_micro_reconcile.add_argument(
        "--book-db-path",
        default=None,
        help="Optional SQLite portfolio-book path; defaults to outputs/kalshi_portfolio_book.sqlite3",
    )
    kalshi_micro_reconcile.add_argument(
        "--execution-journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_micro_reconcile.add_argument(
        "--max-historical-pages",
        type=int,
        default=5,
        help="Maximum historical-order pages to scan when an order is no longer in current orders",
    )
    kalshi_micro_reconcile.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_reconcile.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_execution_frontier = subparsers.add_parser(
        "kalshi-execution-frontier",
        help="Summarize the execution frontier from the persistent execution journal",
    )
    kalshi_execution_frontier.add_argument(
        "--journal-db-path",
        default=None,
        help="Optional execution-journal SQLite path; defaults to outputs/kalshi_execution_journal.sqlite3",
    )
    kalshi_execution_frontier.add_argument(
        "--event-log-csv",
        default=None,
        help="Legacy alias for journal path. If this ends with .csv, .sqlite3 will be used beside it.",
    )
    kalshi_execution_frontier.add_argument(
        "--recent-rows",
        type=int,
        default=5000,
        help="Recent execution events to scan",
    )
    kalshi_execution_frontier.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_ws_state_replay = subparsers.add_parser(
        "kalshi-ws-state-replay",
        help="Replay websocket NDJSON events into authoritative ws-state JSON and health summary",
    )
    kalshi_ws_state_replay.add_argument(
        "--events-ndjson",
        required=True,
        help="Path to NDJSON websocket events (orderbook snapshot/delta, user orders/fills, positions)",
    )
    kalshi_ws_state_replay.add_argument(
        "--ws-state-json",
        default=None,
        help="Output websocket-state JSON path; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_ws_state_replay.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Max staleness threshold included in the generated authority summary",
    )
    kalshi_ws_state_replay.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_ws_state_collect = subparsers.add_parser(
        "kalshi-ws-state-collect",
        help="Connect to Kalshi websocket channels directly and maintain authoritative ws-state JSON",
    )
    kalshi_ws_state_collect.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file containing KALSHI credentials and environment",
    )
    kalshi_ws_state_collect.add_argument(
        "--channels",
        default="orderbook_snapshot,orderbook_delta,user_orders,user_fills,market_positions",
        help="Comma-separated websocket channels to subscribe",
    )
    kalshi_ws_state_collect.add_argument(
        "--market-tickers",
        default="",
        help="Optional comma-separated market tickers for market-scoped channels",
    )
    kalshi_ws_state_collect.add_argument(
        "--run-seconds",
        type=float,
        default=120.0,
        help="Wall-clock runtime budget for this collector pass",
    )
    kalshi_ws_state_collect.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after this many normalized events (0 means no explicit limit)",
    )
    kalshi_ws_state_collect.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=10.0,
        help="Socket connect timeout for each websocket handshake",
    )
    kalshi_ws_state_collect.add_argument(
        "--read-timeout-seconds",
        type=float,
        default=1.0,
        help="Read timeout for websocket receive loop",
    )
    kalshi_ws_state_collect.add_argument(
        "--ping-interval-seconds",
        type=float,
        default=15.0,
        help="Client ping cadence to keep the websocket session active",
    )
    kalshi_ws_state_collect.add_argument(
        "--flush-state-every-seconds",
        type=float,
        default=2.0,
        help="How often to persist current ws-state JSON during collection",
    )
    kalshi_ws_state_collect.add_argument(
        "--reconnect-max-attempts",
        type=int,
        default=8,
        help="Maximum reconnects after websocket disconnects within one run",
    )
    kalshi_ws_state_collect.add_argument(
        "--reconnect-backoff-seconds",
        type=float,
        default=1.0,
        help="Base reconnect backoff (exponential by reconnect count)",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-events-ndjson",
        default=None,
        help="Optional NDJSON event log path; defaults to outputs/kalshi_ws_events_<stamp>.ndjson",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-state-json",
        default=None,
        help="Output websocket-state JSON path; defaults to outputs/kalshi_ws_state_latest.json",
    )
    kalshi_ws_state_collect.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Max staleness threshold embedded in state health summaries",
    )
    kalshi_ws_state_collect.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_climate_realtime_router = subparsers.add_parser(
        "kalshi-climate-realtime-router",
        help="Ingest real-time climate availability and route climate opportunities by modeled edge + market availability",
    )
    kalshi_climate_realtime_router.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file containing KALSHI credentials and environment",
    )
    kalshi_climate_realtime_router.add_argument(
        "--priors-csv",
        default="data/research/kalshi_nonsports_priors.csv",
        help="Path to priors CSV (includes weather priors rows)",
    )
    kalshi_climate_realtime_router.add_argument(
        "--history-csv",
        default="outputs/kalshi_nonsports_history.csv",
        help="Path to persistent non-sports history CSV",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-db-path",
        default=None,
        help="Optional explicit SQLite path for climate availability store (default: outputs/kalshi_climate_availability.sqlite3)",
    )
    kalshi_climate_realtime_router.add_argument(
        "--market-tickers",
        default="",
        help="Optional comma-separated market tickers to monitor; defaults to top climate rows by edge",
    )
    kalshi_climate_realtime_router.add_argument(
        "--ws-channels",
        default="orderbook_snapshot,orderbook_delta,ticker,public_trades,user_fills,market_positions",
        help="Comma-separated websocket channels to subscribe during realtime ingest",
    )
    kalshi_climate_realtime_router.add_argument(
        "--run-seconds",
        type=float,
        default=45.0,
        help="Realtime websocket ingest duration in seconds",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-markets",
        type=int,
        default=40,
        help="Maximum climate market tickers to monitor when market-tickers is not provided",
    )
    kalshi_climate_realtime_router.add_argument(
        "--seed-recent-markets",
        dest="seed_recent_markets",
        action="store_true",
        default=True,
        help="Seed monitored tickers with recently updated open markets using GET /markets?min_updated_ts",
    )
    kalshi_climate_realtime_router.add_argument(
        "--no-seed-recent-markets",
        dest="seed_recent_markets",
        action="store_false",
        help="Disable recent-market discovery seeding and rely on priors-ranked climate rows only",
    )
    kalshi_climate_realtime_router.add_argument(
        "--recent-markets-min-updated-seconds",
        type=float,
        default=900.0,
        help="Recency window (seconds) forwarded to Kalshi recent-market discovery min_updated_ts",
    )
    kalshi_climate_realtime_router.add_argument(
        "--recent-markets-timeout-seconds",
        type=float,
        default=8.0,
        help="HTTP timeout for recent-market discovery requests",
    )
    kalshi_climate_realtime_router.add_argument(
        "--ws-state-max-age-seconds",
        type=float,
        default=30.0,
        help="Staleness threshold forwarded to websocket collector health checks",
    )
    kalshi_climate_realtime_router.add_argument(
        "--min-theoretical-edge-net-fees",
        type=float,
        default=0.005,
        help="Minimum net edge used to classify modeled-positive opportunities",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-quote-age-seconds",
        type=float,
        default=900.0,
        help="Reserved for downstream quote-freshness routing guardrail",
    )
    kalshi_climate_realtime_router.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Planning bankroll context for routing summaries",
    )
    kalshi_climate_realtime_router.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Total routed risk cap for tradable climate opportunities",
    )
    kalshi_climate_realtime_router.add_argument(
        "--max-risk-per-bet",
        type=float,
        default=1.0,
        help="Maximum routed risk dollars per climate opportunity",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-lookback-days",
        type=float,
        default=7.0,
        help="Lookback horizon for availability-rate metrics",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-recent-seconds",
        type=float,
        default=900.0,
        help="Recency window for tradable/priced classification",
    )
    kalshi_climate_realtime_router.add_argument(
        "--availability-hot-trade-window-seconds",
        type=float,
        default=300.0,
        help="Recency window for public-trade activity to classify hot strips",
    )
    kalshi_climate_realtime_router.add_argument(
        "--include-contract-families",
        default="daily_rain,daily_temperature,daily_snow,monthly_climate_anomaly",
        help="Comma-separated climate contract families to include in routing",
    )
    kalshi_climate_realtime_router.add_argument(
        "--skip-realtime-collect",
        action="store_true",
        help="Skip websocket ingest and run routing from persisted availability DB + priors only",
    )
    kalshi_climate_realtime_router.add_argument("--output-dir", default="outputs", help="Output directory")

    kalshi_micro_status = subparsers.add_parser(
        "kalshi-micro-status",
        help="Run a fresh read-only micro status cycle and summarize whether to hold, watch, or act",
    )
    kalshi_micro_status.add_argument(
        "--env-file",
        default="data/research/account_onboarding.env.template",
        help="Path to env-style file with runtime settings",
    )
    kalshi_micro_status.add_argument(
        "--planning-bankroll",
        type=float,
        default=40.0,
        help="Paper bankroll used for the status planning budget",
    )
    kalshi_micro_status.add_argument(
        "--daily-risk-cap",
        type=float,
        default=3.0,
        help="Maximum total dollars to expose across the planned orders",
    )
    kalshi_micro_status.add_argument(
        "--contracts-per-order",
        type=int,
        default=1,
        help="Contracts per planned order",
    )
    kalshi_micro_status.add_argument(
        "--max-orders",
        type=int,
        default=3,
        help="Maximum planned orders",
    )
    kalshi_micro_status.add_argument(
        "--min-yes-bid",
        type=float,
        default=0.01,
        help="Minimum best Yes bid required to join the bid as maker",
    )
    kalshi_micro_status.add_argument(
        "--max-yes-ask",
        type=float,
        default=0.10,
        help="Maximum best Yes ask allowed for planned markets",
    )
    kalshi_micro_status.add_argument(
        "--max-spread",
        type=float,
        default=0.02,
        help="Maximum bid/ask spread allowed for planned markets",
    )
    kalshi_micro_status.add_argument(
        "--max-hours-to-close",
        type=float,
        default=336.0,
        help="Only keep markets closing within this many hours",
    )
    kalshi_micro_status.add_argument(
        "--excluded-categories",
        default="Sports",
        help="Comma-separated Kalshi event categories to exclude",
    )
    kalshi_micro_status.add_argument(
        "--page-limit",
        type=int,
        default=200,
        help="Events requested per Kalshi API page",
    )
    kalshi_micro_status.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum Kalshi event pages to scan",
    )
    kalshi_micro_status.add_argument(
        "--max-live-submissions-per-day",
        type=int,
        default=3,
        help="New submission slots accrued per trading day from the persistent ledger (unused slots carry forward)",
    )
    kalshi_micro_status.add_argument(
        "--max-live-cost-per-day",
        type=float,
        default=3.0,
        help="Maximum total planned live entry cost allowed per trading day from the persistent ledger",
    )
    kalshi_micro_status.add_argument(
        "--ledger-csv",
        default=None,
        help="Optional persistent trade ledger CSV path; defaults to outputs/kalshi_micro_trade_ledger.csv",
    )
    kalshi_micro_status.add_argument(
        "--history-csv",
        default=None,
        help="Optional persistent non-sports history CSV path; defaults to outputs/kalshi_nonsports_history.csv",
    )
    kalshi_micro_status.add_argument(
        "--timeout-seconds",
        type=float,
        default=15.0,
        help="HTTP timeout per request",
    )
    kalshi_micro_status.add_argument(
        "--watch-history-csv",
        default=None,
        help="Optional stable CSV for status-run history; defaults to outputs/kalshi_micro_watch_history.csv",
    )
    kalshi_micro_status.add_argument("--output-dir", default="outputs", help="Output directory")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = 0

    if args.command == "backtest":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_backtest(
            candidates=candidates,
            cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
        )
    elif args.command == "paper":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_paper(
            candidates=candidates,
            cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
            simulate_with_outcomes=args.simulate_with_outcomes,
        )
    elif args.command == "analyze":
        start_units = units_from_dollars(args.starting_bankroll, args.risk_per_effort)
        targets = [float(x.strip()) for x in args.targets.split(",") if x.strip()]
        p_values = [float(x.strip()) for x in args.p_values.split(",") if x.strip()]

        target_rows = []
        for target_dollars in targets:
            target_units = units_from_dollars(target_dollars, args.risk_per_effort)
            row = {
                "target_bankroll": target_dollars,
                "target_units": target_units,
                "start_units": start_units,
            }
            for p in p_values:
                if target_units <= start_units:
                    row[f"p_{p:.2f}"] = 1.0
                else:
                    row[f"p_{p:.2f}"] = round(
                        hitting_probability(start_units, target_units, p), 6
                    )
            target_rows.append(row)

        rung_rows = []
        rung_levels = [args.starting_bankroll] + targets
        for idx in range(len(rung_levels) - 1):
            current = rung_levels[idx]
            nxt = rung_levels[idx + 1]
            current_units = units_from_dollars(current, args.risk_per_effort)
            next_units = units_from_dollars(nxt, args.risk_per_effort)
            row = {
                "from_bankroll": current,
                "to_bankroll": nxt,
                "from_units": current_units,
                "to_units": next_units,
            }
            for p in p_values:
                if next_units <= current_units:
                    row[f"p_{p:.2f}"] = 1.0
                else:
                    row[f"p_{p:.2f}"] = round(
                        hitting_probability(current_units, next_units, p), 6
                    )
            rung_rows.append(row)

        survivability = []
        for p in p_values:
            survivability.append(
                {
                    "p": p,
                    "eventual_success_prob": round(
                        eventual_success_probability(start_units, p), 6
                    ),
                    "units_for_90pct_success": required_starting_units(0.90, p),
                    "units_for_95pct_success": required_starting_units(0.95, p),
                }
            )

        summary = {
            "analysis_timestamp": datetime.now().isoformat(),
            "starting_bankroll": args.starting_bankroll,
            "risk_per_effort": args.risk_per_effort,
            "start_units": start_units,
            "targets": targets,
            "p_values": p_values,
            "hitting_probabilities": target_rows,
            "rung_transitions": rung_rows,
            "survivability": survivability,
        }

        if args.history_input:
            history_candidates = load_candidates(args.history_input)
            outcomes = [c.outcome for c in history_candidates if c.outcome in (0, 1)]
            wins = sum(outcomes)
            trials = len(outcomes)
            if trials > 0:
                summary["bayesian_planning"] = conservative_planning_p(
                    wins=wins,
                    trials=trials,
                    confidence=args.confidence,
                )
            else:
                summary["bayesian_planning"] = {
                    "warning": "No settled outcomes (0/1) found in history input"
                }

        out_dir = Path(args.output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = out_dir / f"probability_analysis_{stamp}.json"
        output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        summary["output_file"] = str(output_path)
    elif args.command == "effective-config":
        summary = run_effective_config(repo_root=args.repo_root)
    elif args.command == "policy-check":
        summary = run_policy_check(
            lane=args.lane,
            lane_policy_path=args.lane_policy_path,
        )
    elif args.command == "render-board":
        summary = run_render_board(
            board_json=args.board_json,
            cycle_json=args.cycle_json,
            output_dir=args.output_dir,
        )
    elif args.command == "alpha-scoreboard":
        summary = run_alpha_scoreboard(
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            benchmark_annual_return=args.benchmark_annual_return,
            plan_summary_file=args.plan_summary_file,
            daily_ops_report_file=args.daily_ops_file,
            research_queue_csv=args.research_queue_csv,
            top_research_targets=args.top_research_targets,
        )
    elif args.command == "ladder-grid":
        cfg = load_config(args.config)
        candidates = load_candidates(args.input)
        summary = run_ladder_grid(
            candidates=candidates,
            base_cfg=cfg,
            starting_bankroll=args.starting_bankroll,
            output_dir=args.output_dir,
            first_rung_offsets=parse_float_list(args.first_rung_offsets),
            rung_step_offsets=parse_float_list(args.rung_step_offsets),
            rung_counts=parse_int_list(args.rung_count_values),
            min_success_probs=parse_float_list(args.min_success_probs),
            planning_ps=parse_float_list(args.planning_ps),
            withdraw_steps=parse_float_list(args.withdraw_steps),
            min_risk_wallets=parse_float_list(args.min_risk_wallet_values),
            drawdown_penalty=args.drawdown_penalty,
            top_k=args.top_k,
            pareto_k=args.pareto_k,
        )
    elif args.command == "research-audit":
        venues = [x.strip() for x in args.venues.split(",") if x.strip()]
        jurisdictions = [x.strip() for x in args.jurisdictions.split(",") if x.strip()]
        summary = run_research_audit(
            research_dir=args.research_dir,
            venues=venues,
            jurisdictions=jurisdictions,
            output_dir=args.output_dir,
        )
    elif args.command == "canonical-universe":
        summary = run_canonical_universe(output_dir=args.output_dir)
    elif args.command == "odds-audit":
        summary = run_odds_audit(
            input_csv=args.input,
            output_dir=args.output_dir,
            max_gap_minutes=args.max_gap_minutes,
        )
    elif args.command == "onboarding-check":
        summary = run_onboarding_check(
            env_file=args.env_file,
            output_dir=args.output_dir,
        )
    elif args.command == "live-smoke":
        summary = run_live_smoke(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            include_odds_provider_check=not args.skip_odds_provider_check,
        )
        if summary.get("status") == "failed":
            exit_code = 1
    elif args.command == "dns-doctor":
        summary = run_dns_doctor(
            env_file=args.env_file,
            hosts=tuple(item.strip() for item in args.hosts.split(",") if item.strip()),
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "live-snapshot":
        summary = run_live_snapshot(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            sports_preview_limit=args.sports_preview_limit,
        )
    elif args.command == "live-candidates":
        summary = run_live_candidates(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_date=args.event_date,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "live-paper":
        summary = run_live_paper(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_date=args.event_date,
            starting_bankroll=args.starting_bankroll,
            config_path=args.config,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            enrich_candidates=args.enrich_candidates,
            enrichment_csv=args.enrichment_csv,
            enrichment_freshness_hours=args.enrichment_freshness_hours,
            enrichment_max_logit_shift=args.enrichment_max_logit_shift,
            enrichment_moneyline_only=not args.enrichment_include_non_moneyline,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "sports-archive":
        summary = run_sports_archive(
            env_file=args.env_file,
            sport_id=args.sport_id,
            event_dates=tuple(item.strip() for item in args.event_dates.split(",") if item.strip()),
            starting_bankroll=args.starting_bankroll,
            config_path=args.config,
            output_dir=args.output_dir,
            archive_csv=args.archive_csv,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            market_ids=tuple(parse_int_list(args.market_ids)),
            min_books=args.min_books,
            offset_minutes=args.offset_minutes,
            include_in_play=args.include_in_play,
            enrich_candidates=args.enrich_candidates,
            enrichment_csv=args.enrichment_csv,
            enrichment_freshness_hours=args.enrichment_freshness_hours,
            enrichment_max_logit_shift=args.enrichment_max_logit_shift,
            enrichment_moneyline_only=not args.enrichment_include_non_moneyline,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-mlb-map":
        summary = run_kalshi_mlb_map(
            env_file=args.env_file,
            event_date=args.event_date,
            output_dir=args.output_dir,
            affiliate_ids=tuple(item.strip() for item in args.affiliate_ids.split(",") if item.strip()),
            min_books=args.min_books,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-nonsports-scan":
        summary = run_kalshi_nonsports_scan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            max_hours_to_close=args.max_hours_to_close,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-capture":
        summary = run_kalshi_nonsports_capture(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            timeout_seconds=args.timeout_seconds,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            max_hours_to_close=args.max_hours_to_close,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-quality":
        summary = run_kalshi_nonsports_quality(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_mean_yes_bid=args.min_mean_yes_bid,
            min_two_sided_ratio=args.min_two_sided_ratio,
            max_mean_spread=args.max_mean_spread,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-signals":
        summary = run_kalshi_nonsports_signals(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_stable_ratio=args.min_stable_ratio,
            min_latest_yes_bid=args.min_latest_yes_bid,
            min_mean_yes_bid=args.min_mean_yes_bid,
            max_mean_spread=args.max_mean_spread,
            max_yes_bid_stddev=args.max_yes_bid_stddev,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-persistence":
        summary = run_kalshi_nonsports_persistence(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            min_tradeable_snapshot_count=args.min_tradeable_snapshot_count,
            min_consecutive_tradeable_snapshots=args.min_consecutive_tradeable_snapshots,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-deltas":
        summary = run_kalshi_nonsports_deltas(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            min_bid_improvement=args.min_bid_improvement,
            min_spread_improvement=args.min_spread_improvement,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-categories":
        summary = run_kalshi_nonsports_categories(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_tradeable_yes_bid=args.min_tradeable_yes_bid,
            max_tradeable_spread=args.max_tradeable_spread,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-pressure":
        summary = run_kalshi_nonsports_pressure(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            min_observations=args.min_observations,
            min_latest_yes_bid=args.min_latest_yes_bid,
            max_latest_spread=args.max_latest_spread,
            min_two_sided_ratio=args.min_two_sided_ratio,
            min_recent_bid_change=args.min_recent_bid_change,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-thresholds":
        summary = run_kalshi_nonsports_thresholds(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            target_yes_bid=args.target_yes_bid,
            target_spread=args.target_spread,
            recent_window=args.recent_window,
            max_hours_to_target=args.max_hours_to_target,
            min_recent_two_sided_ratio=args.min_recent_two_sided_ratio,
            min_observations=args.min_observations,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-priors":
        summary = run_kalshi_nonsports_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
            contracts_per_order=args.contracts_per_order,
        )
    elif args.command == "kalshi-nonsports-research-queue":
        summary = run_kalshi_nonsports_research_queue(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-nonsports-auto-priors":
        allowed_canonical_niches = tuple(
            value.strip()
            for value in str(args.allowed_canonical_niches or "").split(",")
            if value.strip()
        )
        allowed_categories = tuple(
            value.strip()
            for value in str(args.allowed_categories or "").split(",")
            if value.strip()
        )
        disallowed_categories = tuple(
            value.strip()
            for value in str(args.disallowed_categories or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_nonsports_auto_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            canonical_mapping_csv=args.canonical_mapping_csv,
            allowed_canonical_niches=allowed_canonical_niches,
            restrict_to_mapped_live_tickers=args.restrict_to_mapped_live_tickers,
            allowed_categories=(allowed_categories or None),
            disallowed_categories=(disallowed_categories or None),
            top_n=args.top_n,
            max_markets=args.max_markets,
            timeout_seconds=args.timeout_seconds,
            max_headlines_per_market=args.max_headlines_per_market,
            min_evidence_count=args.min_evidence_count,
            min_evidence_quality=args.min_evidence_quality,
            min_high_trust_sources=args.min_high_trust_sources,
            protect_manual=not args.disable_protect_manual,
            write_back_to_priors=not args.dry_run,
        )
    elif args.command == "kalshi-weather-catalog":
        summary = run_kalshi_weather_catalog(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-weather-priors":
        allowed_contract_families = tuple(
            value.strip()
            for value in str(args.allowed_contract_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_weather_priors(
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            allowed_contract_families=allowed_contract_families,
            max_markets=args.max_markets,
            timeout_seconds=args.timeout_seconds,
            historical_lookback_years=args.historical_lookback_years,
            station_history_cache_max_age_hours=args.station_history_cache_max_age_hours,
            include_nws_gridpoint_data=not args.disable_nws_gridpoint_data,
            include_nws_observations=not args.disable_nws_observations,
            include_nws_alerts=not args.disable_nws_alerts,
            include_ncei_normals=not args.disable_ncei_normals,
            include_mrms_qpe=not args.disable_mrms_qpe,
            include_nbm_snapshot=not args.disable_nbm_snapshot,
            protect_manual=not args.disable_protect_manual,
            write_back_to_priors=not args.dry_run,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-weather-prewarm":
        summary = run_kalshi_weather_station_history_prewarm(
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            historical_lookback_years=args.historical_lookback_years,
            station_history_cache_max_age_hours=args.station_history_cache_max_age_hours,
            timeout_seconds=args.timeout_seconds,
            max_station_day_keys=args.max_station_day_keys,
        )
    elif args.command == "kalshi-temperature-contract-specs":
        summary = run_kalshi_temperature_contract_specs(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-temperature-constraint-scan":
        summary = run_kalshi_temperature_constraint_scan(
            specs_csv=args.specs_csv,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            max_markets=args.max_markets,
        )
    elif args.command == "kalshi-temperature-metar-ingest":
        summary = run_kalshi_temperature_metar_ingest(
            output_dir=args.output_dir,
            specs_csv=args.specs_csv,
            cache_url=args.cache_url,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-temperature-trader":
        summary = run_kalshi_temperature_trader(
            env_file=args.env_file,
            output_dir=args.output_dir,
            specs_csv=args.specs_csv,
            constraint_csv=args.constraint_csv,
            metar_summary_json=args.metar_summary_json,
            metar_state_json=args.metar_state_json,
            ws_state_json=args.ws_state_json,
            policy_version=args.policy_version,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            max_markets=args.max_markets,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            intents_only=args.intents_only,
            min_settlement_confidence=args.min_settlement_confidence,
            max_metar_age_minutes=args.max_metar_age_minutes,
            min_hours_to_close=args.min_hours_to_close,
            max_hours_to_close=args.max_hours_to_close,
            max_intents_per_underlying=args.max_intents_per_underlying,
            yes_max_entry_price_dollars=args.yes_max_entry_price,
            no_max_entry_price_dollars=args.no_max_entry_price,
            require_market_snapshot_seq=not args.disable_require_market_snapshot_seq,
            require_metar_snapshot_sha=args.require_metar_snapshot_sha,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day_dollars,
            enforce_trade_gate=args.enforce_trade_gate,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "polymarket-market-ingest":
        summary = run_polymarket_market_data_ingest(
            output_dir=args.output_dir,
            max_markets=args.max_markets,
            page_size=args.page_size,
            max_pages=args.max_pages,
            only_active=not args.include_inactive,
            gamma_base_url=args.gamma_base_url,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-focus-dossier":
        summary = run_kalshi_focus_dossier(
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            recent_observation_limit=args.recent_observation_limit,
        )
    elif args.command == "kalshi-micro-prior-plan":
        summary = run_kalshi_micro_prior_plan(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            selection_lane=args.selection_lane,
            min_selected_fair_probability=args.min_selected_fair_probability,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping=args.require_canonical_mapping,
            top_n=args.top_n,
            book_db_path=args.book_db_path,
            include_incentives=args.include_incentives,
        )
    elif args.command == "kalshi-micro-prior-execute":
        climate_router_pilot_allowed_classes = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_classes or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_excluded_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_excluded_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_micro_prior_execute(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            selection_lane=args.selection_lane,
            min_selected_fair_probability=args.min_selected_fair_probability,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            min_live_selected_fair_probability=args.min_live_selected_fair_probability,
            timeout_seconds=args.timeout_seconds,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            execution_frontier_report_json=args.execution_frontier_report_json,
            execution_frontier_max_report_age_seconds=args.execution_frontier_max_report_age_seconds,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            enforce_daily_weather_live_only=not args.disable_daily_weather_live_only,
            require_daily_weather_board_coverage_for_live=not args.disable_daily_weather_board_coverage,
            daily_weather_board_max_age_seconds=args.daily_weather_board_max_age_seconds,
            climate_router_pilot_enabled=args.climate_router_pilot_enabled,
            climate_router_summary_json=args.climate_router_summary_json,
            climate_router_pilot_max_orders_per_run=args.climate_router_pilot_max_orders_per_run,
            climate_router_pilot_contracts_cap=args.climate_router_pilot_contracts_cap,
            climate_router_pilot_required_ev_dollars=args.climate_router_pilot_required_ev_dollars,
            climate_router_pilot_allowed_classes=climate_router_pilot_allowed_classes,
            climate_router_pilot_allowed_families=climate_router_pilot_allowed_families,
            climate_router_pilot_excluded_families=climate_router_pilot_excluded_families,
            climate_router_pilot_policy_scope_override_enabled=(
                args.climate_router_pilot_policy_scope_override_enabled
            ),
            include_incentives=args.include_incentives,
        )
    elif args.command == "kalshi-micro-prior-trader":
        auto_weather_allowed_contract_families = tuple(
            value.strip()
            for value in str(args.auto_weather_allowed_contract_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_classes = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_classes or "").split(",")
            if value.strip()
        )
        climate_router_pilot_allowed_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_allowed_families or "").split(",")
            if value.strip()
        )
        climate_router_pilot_excluded_families = tuple(
            value.strip()
            for value in str(args.climate_router_pilot_excluded_families or "").split(",")
            if value.strip()
        )
        summary = run_kalshi_micro_prior_trader(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            include_incentives=args.include_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            execution_frontier_report_json=args.execution_frontier_report_json,
            execution_frontier_max_report_age_seconds=args.execution_frontier_max_report_age_seconds,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            enforce_daily_weather_live_only=not args.disable_daily_weather_live_only,
            require_daily_weather_board_coverage_for_live=not args.disable_daily_weather_board_coverage,
            daily_weather_board_max_age_seconds=args.daily_weather_board_max_age_seconds,
            climate_router_pilot_enabled=args.climate_router_pilot_enabled,
            climate_router_summary_json=args.climate_router_summary_json,
            climate_router_pilot_max_orders_per_run=args.climate_router_pilot_max_orders_per_run,
            climate_router_pilot_contracts_cap=args.climate_router_pilot_contracts_cap,
            climate_router_pilot_required_ev_dollars=args.climate_router_pilot_required_ev_dollars,
            climate_router_pilot_allowed_classes=climate_router_pilot_allowed_classes,
            climate_router_pilot_allowed_families=climate_router_pilot_allowed_families,
            climate_router_pilot_excluded_families=climate_router_pilot_excluded_families,
            climate_router_pilot_policy_scope_override_enabled=(
                args.climate_router_pilot_policy_scope_override_enabled
            ),
            capture_before_execute=not args.skip_capture,
            capture_max_hours_to_close=args.max_hours_to_close,
            capture_page_limit=args.page_limit,
            capture_max_pages=args.max_pages,
            use_temporary_live_env=args.use_temp_live_env,
            auto_refresh_weather_priors=not args.disable_auto_refresh_weather_priors,
            auto_prewarm_weather_station_history=not args.disable_auto_prewarm_weather_station_history,
            auto_weather_prior_max_markets=args.auto_weather_prior_max_markets,
            auto_weather_allowed_contract_families=auto_weather_allowed_contract_families,
            auto_weather_prewarm_max_station_day_keys=args.auto_weather_prewarm_max_station_day_keys,
            auto_weather_historical_lookback_years=args.auto_weather_historical_lookback_years,
            auto_weather_station_history_cache_max_age_hours=args.auto_weather_station_history_cache_max_age_hours,
        )
    elif args.command == "kalshi-micro-prior-watch":
        summary = run_kalshi_micro_prior_watch(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            watch_history_csv=args.watch_history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            canonical_mapping_csv=args.canonical_mapping_csv,
            canonical_threshold_csv=args.canonical_threshold_csv,
            prefer_canonical_thresholds=not args.disable_canonical_thresholds,
            require_canonical_mapping_for_live=not args.disable_require_canonical_for_live,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            include_incentives=args.include_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
        )
    elif args.command == "kalshi-arb-scan":
        summary = run_kalshi_arb_scan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            timeout_seconds=args.timeout_seconds,
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            fee_buffer_per_contract_dollars=args.fee_buffer_per_contract,
            min_margin_dollars=args.min_margin_dollars,
            top_n=args.top_n,
        )
    elif args.command == "kalshi-supervisor":
        summary = run_kalshi_supervisor(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            cycles=args.cycles,
            sleep_between_cycles_seconds=args.sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            min_live_maker_edge=args.min_live_maker_edge,
            min_live_maker_edge_net_fees=args.min_live_maker_edge_net_fees,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            read_requests_per_minute=args.read_requests_per_minute,
            write_requests_per_minute=args.write_requests_per_minute,
            failure_remediation_enabled=not args.disable_failure_remediation,
            failure_remediation_max_retries=args.failure_remediation_max_retries,
            failure_remediation_backoff_seconds=args.failure_remediation_backoff_seconds,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            exchange_status_self_heal_attempts=args.exchange_status_self_heal_attempts,
            exchange_status_self_heal_pause_seconds=args.exchange_status_self_heal_pause_seconds,
            exchange_status_run_dns_doctor=not args.disable_exchange_status_dns_remediation,
            exchange_status_self_heal_timeout_multiplier=args.exchange_status_self_heal_timeout_multiplier,
            exchange_status_self_heal_timeout_cap_seconds=args.exchange_status_self_heal_timeout_cap_seconds,
            run_arb_scan_each_cycle=not args.disable_arb_scan,
            include_incentives=not args.disable_incentives,
            auto_refresh_priors=not args.disable_auto_refresh_priors,
            auto_prior_max_markets=args.auto_prior_max_markets,
            auto_prior_min_evidence_count=args.auto_prior_min_evidence_count,
            auto_prior_min_evidence_quality=args.auto_prior_min_evidence_quality,
            auto_prior_min_high_trust_sources=args.auto_prior_min_high_trust_sources,
            enforce_ws_state_authority=not args.disable_enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "kalshi-autopilot":
        summary = run_kalshi_autopilot(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            allow_live_orders=args.allow_live_orders,
            cycles=args.cycles,
            sleep_between_cycles_seconds=args.sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            preflight_run_dns_doctor=not args.disable_preflight_dns_doctor,
            preflight_run_live_smoke=not args.disable_preflight_live_smoke,
            preflight_live_smoke_include_odds_provider_check=args.preflight_live_smoke_include_odds_provider,
            preflight_run_ws_state_collect=not args.disable_preflight_ws_state_collect,
            ws_collect_run_seconds=args.ws_collect_run_seconds,
            ws_collect_max_events=args.ws_collect_max_events,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            preflight_self_heal_attempts=args.preflight_self_heal_attempts,
            preflight_self_heal_pause_seconds=args.preflight_self_heal_pause_seconds,
            preflight_self_heal_upstream_only=not args.disable_preflight_self_heal_upstream_only,
            preflight_self_heal_retry_ws_state_gate_failures=not args.disable_preflight_self_heal_retry_ws_state_gate_failures,
            preflight_self_heal_run_dns_doctor=not args.disable_preflight_self_heal_dns_remediation,
            preflight_retry_timeout_multiplier=args.preflight_retry_timeout_multiplier,
            preflight_retry_timeout_cap_seconds=args.preflight_retry_timeout_cap_seconds,
            preflight_retry_ws_collect_increment_seconds=args.preflight_retry_ws_collect_increment_seconds,
            preflight_retry_ws_collect_max_seconds=args.preflight_retry_ws_collect_max_seconds,
            enable_progressive_scaling=not args.disable_progressive_scaling,
            scaling_lookback_runs=args.scaling_lookback_runs,
            scaling_green_runs_per_step=args.scaling_green_runs_per_step,
            scaling_step_live_submissions=args.scaling_step_live_submissions,
            scaling_step_live_cost_dollars=args.scaling_step_live_cost_dollars,
            scaling_step_daily_risk_cap_dollars=args.scaling_step_daily_risk_cap_dollars,
            scaling_hard_max_live_submissions_per_day=args.scaling_hard_max_live_submissions_per_day,
            scaling_hard_max_live_cost_per_day_dollars=args.scaling_hard_max_live_cost_per_day_dollars,
            scaling_hard_max_daily_risk_cap_dollars=args.scaling_hard_max_daily_risk_cap_dollars,
        )
    elif args.command == "kalshi-watchdog":
        summary = run_kalshi_watchdog(
            env_file=args.env_file,
            output_dir=args.output_dir,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            allow_live_orders=args.allow_live_orders,
            loops=args.loops,
            sleep_between_loops_seconds=args.sleep_between_loops_seconds,
            autopilot_cycles=args.autopilot_cycles,
            autopilot_sleep_between_cycles_seconds=args.autopilot_sleep_between_cycles_seconds,
            timeout_seconds=args.timeout_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_maker_edge=args.min_maker_edge,
            min_maker_edge_net_fees=args.min_maker_edge_net_fees,
            max_entry_price_dollars=args.max_entry_price,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            failure_remediation_timeout_multiplier=args.failure_remediation_timeout_multiplier,
            failure_remediation_timeout_cap_seconds=args.failure_remediation_timeout_cap_seconds,
            preflight_run_dns_doctor=not args.disable_preflight_dns_doctor,
            preflight_run_live_smoke=not args.disable_preflight_live_smoke,
            preflight_live_smoke_include_odds_provider_check=args.preflight_live_smoke_include_odds_provider,
            preflight_run_ws_state_collect=not args.disable_preflight_ws_state_collect,
            ws_collect_run_seconds=args.ws_collect_run_seconds,
            ws_collect_max_events=args.ws_collect_max_events,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            preflight_self_heal_attempts=args.preflight_self_heal_attempts,
            preflight_self_heal_pause_seconds=args.preflight_self_heal_pause_seconds,
            preflight_self_heal_upstream_only=not args.disable_preflight_self_heal_upstream_only,
            preflight_self_heal_retry_ws_state_gate_failures=not args.disable_preflight_self_heal_retry_ws_state_gate_failures,
            preflight_self_heal_run_dns_doctor=not args.disable_preflight_self_heal_dns_remediation,
            preflight_retry_timeout_multiplier=args.preflight_retry_timeout_multiplier,
            preflight_retry_timeout_cap_seconds=args.preflight_retry_timeout_cap_seconds,
            preflight_retry_ws_collect_increment_seconds=args.preflight_retry_ws_collect_increment_seconds,
            preflight_retry_ws_collect_max_seconds=args.preflight_retry_ws_collect_max_seconds,
            enable_progressive_scaling=not args.disable_progressive_scaling,
            scaling_lookback_runs=args.scaling_lookback_runs,
            scaling_green_runs_per_step=args.scaling_green_runs_per_step,
            scaling_step_live_submissions=args.scaling_step_live_submissions,
            scaling_step_live_cost_dollars=args.scaling_step_live_cost_dollars,
            scaling_step_daily_risk_cap_dollars=args.scaling_step_daily_risk_cap_dollars,
            scaling_hard_max_live_submissions_per_day=args.scaling_hard_max_live_submissions_per_day,
            scaling_hard_max_live_cost_per_day_dollars=args.scaling_hard_max_live_cost_per_day_dollars,
            scaling_hard_max_daily_risk_cap_dollars=args.scaling_hard_max_daily_risk_cap_dollars,
            upstream_incident_threshold=args.upstream_incident_threshold,
            kill_switch_cooldown_seconds=args.kill_switch_cooldown_seconds,
            healthy_runs_to_clear_kill_switch=args.healthy_runs_to_clear_kill_switch,
            upstream_retry_backoff_base_seconds=args.upstream_retry_backoff_base_seconds,
            upstream_retry_backoff_max_seconds=args.upstream_retry_backoff_max_seconds,
            self_heal_attempts_per_run=args.self_heal_attempts_per_loop,
            self_heal_pause_seconds=args.self_heal_pause_seconds,
            self_heal_retry_timeout_multiplier=args.self_heal_retry_timeout_multiplier,
            self_heal_retry_timeout_cap_seconds=args.self_heal_retry_timeout_cap_seconds,
            self_heal_retry_ws_collect_increment_seconds=args.self_heal_retry_ws_collect_increment_seconds,
            self_heal_retry_ws_collect_max_seconds=args.self_heal_retry_ws_collect_max_seconds,
            run_dns_doctor_on_upstream=not args.disable_remediation_dns_doctor,
            kill_switch_state_json=args.kill_switch_state_json,
        )
    elif args.command == "kalshi-micro-plan":
        summary = run_kalshi_micro_plan(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-gate":
        summary = run_kalshi_micro_gate(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            auto_cancel_duplicate_open_orders=not args.disable_auto_duplicate_janitor,
            ledger_csv=args.ledger_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-trader":
        summary = run_kalshi_micro_trader(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            allow_live_orders=args.allow_live_orders,
            cancel_resting_immediately=args.cancel_resting_immediately,
            resting_hold_seconds=args.resting_hold_seconds,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            watch_history_csv=args.watch_history_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-watch":
        summary = run_kalshi_micro_watch(
            env_file=args.env_file,
            output_dir=args.output_dir,
            history_csv=args.history_csv,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            timeout_seconds=args.timeout_seconds,
        )
    elif args.command == "kalshi-micro-execute":
        summary = run_kalshi_micro_execute(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout_seconds,
            allow_live_orders=args.allow_live_orders,
            resting_hold_seconds=args.resting_hold_seconds,
            cancel_resting_immediately=args.cancel_resting_immediately,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            book_db_path=args.book_db_path,
            execution_event_log_csv=args.execution_event_log_csv,
            execution_journal_db_path=args.execution_journal_db_path,
            execution_frontier_recent_rows=args.execution_frontier_recent_rows,
            history_csv=args.history_csv,
            enforce_trade_gate=args.enforce_trade_gate,
            enforce_ws_state_authority=args.enforce_ws_state_authority,
            ws_state_json=args.ws_state_json,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "kalshi-micro-reconcile":
        summary = run_kalshi_micro_reconcile(
            env_file=args.env_file,
            execute_summary_file=args.execute_summary_file,
            output_dir=args.output_dir,
            book_db_path=args.book_db_path,
            execution_journal_db_path=args.execution_journal_db_path,
            timeout_seconds=args.timeout_seconds,
            max_historical_pages=args.max_historical_pages,
        )
    elif args.command == "kalshi-execution-frontier":
        frontier_journal_db_path = args.journal_db_path
        if frontier_journal_db_path is None and args.event_log_csv:
            event_log_path = Path(args.event_log_csv)
            frontier_journal_db_path = (
                str(event_log_path.with_suffix(".sqlite3"))
                if event_log_path.suffix.lower() == ".csv"
                else str(event_log_path)
            )
        summary = run_kalshi_execution_frontier(
            output_dir=args.output_dir,
            journal_db_path=frontier_journal_db_path,
            recent_events=args.recent_rows,
        )
    elif args.command == "kalshi-ws-state-replay":
        summary = run_kalshi_ws_state_replay(
            events_ndjson=args.events_ndjson,
            output_dir=args.output_dir,
            ws_state_json=args.ws_state_json,
            max_staleness_seconds=args.ws_state_max_age_seconds,
        )
    elif args.command == "kalshi-ws-state-collect":
        summary = run_kalshi_ws_state_collect(
            env_file=args.env_file,
            channels=tuple(item.strip() for item in args.channels.split(",") if item.strip()),
            market_tickers=tuple(item.strip() for item in args.market_tickers.split(",") if item.strip()),
            output_dir=args.output_dir,
            ws_events_ndjson=args.ws_events_ndjson,
            ws_state_json=args.ws_state_json,
            max_staleness_seconds=args.ws_state_max_age_seconds,
            run_seconds=args.run_seconds,
            max_events=args.max_events,
            connect_timeout_seconds=args.connect_timeout_seconds,
            read_timeout_seconds=args.read_timeout_seconds,
            ping_interval_seconds=args.ping_interval_seconds,
            flush_state_every_seconds=args.flush_state_every_seconds,
            reconnect_max_attempts=args.reconnect_max_attempts,
            reconnect_backoff_seconds=args.reconnect_backoff_seconds,
        )
    elif args.command == "kalshi-climate-realtime-router":
        summary = run_kalshi_climate_realtime_router(
            env_file=args.env_file,
            priors_csv=args.priors_csv,
            history_csv=args.history_csv,
            output_dir=args.output_dir,
            availability_db_path=args.availability_db_path,
            market_tickers=tuple(item.strip() for item in args.market_tickers.split(",") if item.strip()),
            ws_channels=tuple(item.strip() for item in args.ws_channels.split(",") if item.strip()),
            run_seconds=args.run_seconds,
            max_markets=args.max_markets,
            seed_recent_markets=args.seed_recent_markets,
            recent_markets_min_updated_seconds=args.recent_markets_min_updated_seconds,
            recent_markets_timeout_seconds=args.recent_markets_timeout_seconds,
            ws_state_max_age_seconds=args.ws_state_max_age_seconds,
            min_theoretical_edge_net_fees=args.min_theoretical_edge_net_fees,
            max_quote_age_seconds=args.max_quote_age_seconds,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            max_risk_per_bet_dollars=args.max_risk_per_bet,
            availability_lookback_days=args.availability_lookback_days,
            availability_recent_seconds=args.availability_recent_seconds,
            availability_hot_trade_window_seconds=args.availability_hot_trade_window_seconds,
            include_contract_families=tuple(
                item.strip() for item in args.include_contract_families.split(",") if item.strip()
            ),
            skip_realtime_collect=args.skip_realtime_collect,
        )
    elif args.command == "kalshi-micro-status":
        summary = run_kalshi_micro_status(
            env_file=args.env_file,
            output_dir=args.output_dir,
            planning_bankroll_dollars=args.planning_bankroll,
            daily_risk_cap_dollars=args.daily_risk_cap,
            contracts_per_order=args.contracts_per_order,
            max_orders=args.max_orders,
            min_yes_bid_dollars=args.min_yes_bid,
            max_yes_ask_dollars=args.max_yes_ask,
            max_spread_dollars=args.max_spread,
            max_hours_to_close=args.max_hours_to_close,
            excluded_categories=tuple(
                item.strip() for item in args.excluded_categories.split(",") if item.strip()
            ),
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_live_submissions_per_day=args.max_live_submissions_per_day,
            max_live_cost_per_day_dollars=args.max_live_cost_per_day,
            ledger_csv=args.ledger_csv,
            watch_history_csv=args.watch_history_csv,
            history_csv=args.history_csv,
            timeout_seconds=args.timeout_seconds,
        )
    else:
        raise ValueError(f"Unsupported command: {args.command}")

    print(json.dumps(summary, indent=2))
    if exit_code:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    try:
        main()
    except ValueError as exc:
        raise SystemExit(str(exc))
