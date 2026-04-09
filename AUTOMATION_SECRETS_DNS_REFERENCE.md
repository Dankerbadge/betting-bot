# Betting Bot Automation Secret + DNS Reference

This file is the source-of-truth key map for every production automation process in this repo.

## 1) Hourly Overnight Automation
Process:
- `scripts/launchd_com.openai.codex.betbot.hourly.plist`
- `scripts/hourly_alpha_overnight.sh`
- `scripts/automation_preflight.py --profile hourly`

Secret keys required:
- `KALSHI_ACCESS_KEY_ID`
- `KALSHI_PRIVATE_KEY_PATH`
- `KALSHI_ENV`
- `ODDS_PROVIDER`
- `THERUNDOWN_API_KEY` + `THERUNDOWN_BASE_URL` when `ODDS_PROVIDER=therundown`
- `OPTICODDS_API_KEY` + `OPTICODDS_BASE_URL` when `ODDS_PROVIDER=opticodds`

Weather token keys used by weather-history refresh path:
- `BETBOT_NOAA_CDO_TOKEN` (preferred direct env token)
- `NOAA_CDO_TOKEN` (alias)
- `NCEI_CDO_TOKEN` (alias)
- `BETBOT_WEATHER_CDO_TOKEN_FILE` (file fallback path)

Template-only credentials (tracked for completeness, not runtime required today):
- `THERUNDOWN_LOGIN_EMAIL`
- `THERUNDOWN_LOGIN_PASSWORD`

DNS hosts verified by preflight:
- Kalshi: `api.elections.kalshi.com`, `trading-api.kalshi.com`, `demo-api.kalshi.co`
- Odds provider host from the configured base URL
- Weather sources: `api.weather.gov`, `www.ncei.noaa.gov`, `storage.googleapis.com`, `noaa-mrms-pds.s3.amazonaws.com`, `noaa-nbm-grib2-pds.s3.amazonaws.com`

## 2) Monthly Climate Live Attempt Harness
Process:
- `scripts/monthly_climate_live_attempt_harness.sh`
- `scripts/automation_preflight.py --profile monthly`

Secret keys required:
- Same trading env key set as the hourly automation.

DNS hosts verified by preflight:
- Same host set as the hourly automation.

## 3) Paper-Live DB Sync Automation
Process:
- `scripts/paper_live_db_sync.sh`
- `scripts/automation_preflight.py --profile supabase_sync`

Secret keys required:
- `OPSBOT_SUPABASE_URL`
- `OPSBOT_SUPABASE_SERVICE_ROLE_KEY`
- `OPSBOT_SUPABASE_PROJECT_REF`

Additional tracked keys:
- `OPSBOT_SUPABASE_ANON_KEY` (dashboard-only; not required by ingest path)
- `OPSBOT_FORBIDDEN_PROJECT_HINT` (defaults to `legacy_external_project`)

Hard validation enforced:
- `OPSBOT_SUPABASE_URL` host must match `OPSBOT_SUPABASE_PROJECT_REF`.
- Forbidden-hint guardrail blocks old legacy external project project references.

DNS hosts verified by preflight:
- Supabase project host parsed from `OPSBOT_SUPABASE_URL`

## 4) Supabase Ingestion + Acceptance Processes
Process:
- `separate_projects/supabase-bot-state/scripts/ingest_outputs_to_supabase.py`
- `separate_projects/supabase-bot-state/scripts/acceptance_gate.py`

Secret keys required:
- Ingestion: `OPSBOT_SUPABASE_URL`, `OPSBOT_SUPABASE_SERVICE_ROLE_KEY`, `OPSBOT_SUPABASE_PROJECT_REF`
- Acceptance gate additionally requires dashboard read credential:
  - `OPSBOT_SUPABASE_DASHBOARD_ANON_KEY` or `OPSBOT_SUPABASE_ANON_KEY`
  - `OPSBOT_SUPABASE_DASHBOARD_URL` (falls back to `OPSBOT_SUPABASE_URL`)

## 5) Vercel Ops Dashboard (Read-Only)
Process:
- `separate_projects/vercel-ops-dashboard/lib/isolation.ts`
- `separate_projects/vercel-ops-dashboard/lib/supabase.ts`

Secret keys required:
- `OPSBOT_SUPABASE_URL`
- `OPSBOT_SUPABASE_ANON_KEY`
- `OPSBOT_SUPABASE_PROJECT_REF`
- `OPSBOT_FORBIDDEN_PROJECT_HINT` (optional; defaults to `legacy_external_project`)

## Preflight Commands
Run these before enabling or re-enabling automations:

```bash
python3 scripts/automation_preflight.py --profile hourly --env-file data/research/account_onboarding.local.env
python3 scripts/automation_preflight.py --profile monthly --env-file data/research/account_onboarding.local.env
python3 scripts/automation_preflight.py --profile supabase_sync --secrets-file "$HOME/.codex/secrets/betting-bot-supabase.env"
```

If any command returns non-zero, treat it as a P0 blocker and fix keys/DNS before running automation.
