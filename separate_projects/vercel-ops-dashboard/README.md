# Opsbot Read-Only Dashboard (Vercel)

This is a **separate Vercel project** that reads from the separate Supabase project.
It does not run execution loops and does not perform writes.

## Isolation Rules

- Use only `OPSBOT_*` env vars.
- Never reuse any Zenith project URL/ref/key.
- App startup fails if URL/ref contains the forbidden hint (`zenith` by default).

## Local Run

```bash
cd "/Users/dankerbadge/Documents/Betting Bot/separate_projects/vercel-ops-dashboard"
cp .env.example .env.local
npm install
npm run dev
```

## Vercel Deploy (New Project)

1. Create a new Vercel project for this folder.
2. Set env vars from `.env.example` with your **new** Supabase project values.
3. Deploy.

## Read Paths

The dashboard reads from these Supabase objects in schema `bot_ops`:

- `v_latest_overnight_run`
- `v_frontier_recent`
- `v_climate_activity_24h`
- `v_pilot_scorecards_recent`

## Freshness Surface

The home page explicitly displays freshness for:

- latest overnight run timestamp
- latest frontier timestamp (plus frontier artifact age)
- latest pilot scorecard timestamp
- latest climate observation timestamp
- balance heartbeat age from overnight payload
