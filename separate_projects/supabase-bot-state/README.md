# Supabase Bot State (Separate Project)

This project defines a **new Supabase schema** for bot persistence and reporting.
It is intentionally separate from Zenith.

## What It Creates

Migration file:

- `supabase/migrations/20260401193000_init_bot_state.sql`

Core tables:

- `bot_ops.execution_journal`
- `bot_ops.execution_frontier_reports`
- `bot_ops.execution_frontier_report_buckets`
- `bot_ops.climate_availability_events`
- `bot_ops.overnight_runs`
- `bot_ops.pilot_scorecards`

Ingested scorecard payload types now include:

- `alpha_scoreboard`
- `autopilot_summary`
- `pilot_execution_evidence` (latest observer snapshot)
- `monthly_live_attempt_summary` (latest harness summary when present)

Dashboard views:

- `bot_ops.v_latest_overnight_run`
- `bot_ops.v_frontier_recent`
- `bot_ops.v_pilot_scorecards_recent`
- `bot_ops.v_climate_activity_24h`

## Apply Migration

Use Supabase SQL editor or CLI against your **new** project:

1. Open the migration SQL.
2. Run it against the new project database.
3. Confirm tables exist under schema `bot_ops`.

## Backfill / Ongoing Ingestion

Use the ingestion tool to load local `outputs/` artifacts into Supabase.

```bash
cd "/Users/dankerbadge/Documents/Betting Bot"
source separate_projects/supabase-bot-state/.env.example  # replace with real values
python3 separate_projects/supabase-bot-state/scripts/ingest_outputs_to_supabase.py \
  --outputs-dir outputs \
  --max-frontier-reports 200 \
  --max-execution-events 5000 \
  --max-climate-events 5000
```

Dry run:

```bash
python3 separate_projects/supabase-bot-state/scripts/ingest_outputs_to_supabase.py --dry-run
```

## Credential Separation (Required)

Use two credential sets from the new Supabase project:

- Ingest credential: `OPSBOT_SUPABASE_SERVICE_ROLE_KEY` for upserts only in ingestion workflows.
- Dashboard credential: `OPSBOT_SUPABASE_ANON_KEY` for read-only Vercel runtime access.

Never expose the service-role key to Vercel.

Automation keys required for `scripts/paper_live_db_sync.sh`:

- `OPSBOT_SUPABASE_URL`
- `OPSBOT_SUPABASE_SERVICE_ROLE_KEY`
- `OPSBOT_SUPABASE_PROJECT_REF`

Template file:

- `scripts/paper_live_supabase_keys.env.example`

Expected automation secret file path (default):

- `~/.codex/secrets/betting-bot-supabase.env`

## Acceptance Gate

Run the full acceptance checks after migration and before trusting the stack:

```bash
python3 separate_projects/supabase-bot-state/scripts/acceptance_gate.py \
  --outputs-dir outputs \
  --output-json outputs/supabase_acceptance_summary.json
```

What this verifies:

- isolation guardrails (non-Zenith target)
- schema objects and view readability
- dashboard credential write probe is blocked
- ingest idempotency by running ingestion twice
- freshness snapshot for overnight/frontier/scorecard/climate plus balance heartbeat age

Useful options:

- `--skip-idempotency` to run only permissions/schema/freshness checks
- `--strict-freshness` to fail if freshness fields are missing
- `--skip-write-probe` if you need a read-only dry acceptance pass

## Isolation Safety

The ingestion script fails fast if `OPSBOT_SUPABASE_URL` or `OPSBOT_SUPABASE_PROJECT_REF` contains `zenith` (or your configured forbidden hint).
