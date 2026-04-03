# Separation Guardrails (Required)

These guardrails are mandatory for this workspace:

1. Use a brand-new Supabase project and a brand-new Vercel project.
2. Do not reuse any Zenith URL, project ref, API key, database, or Vercel project.
3. Keep credentials namespaced to `OPSBOT_*` only.
4. Keep this dashboard read-only (no insert/update/delete code paths).

## Naming Convention

Use non-Zenith names such as:

- Supabase project: `opsbot-state-prod`
- Vercel project: `opsbot-dashboard-prod`

## Environment Guardrails

- `OPSBOT_SUPABASE_URL` must point to the new Supabase project.
- `OPSBOT_SUPABASE_PROJECT_REF` must match the new project ref.
- `OPSBOT_FORBIDDEN_PROJECT_HINT` defaults to `zenith`.

The dashboard app will fail fast at startup if the project ref or URL contains the forbidden hint.

## Isolation Checklist

1. Create a new Supabase project (do not clone Zenith).
2. Apply the migration in `supabase-bot-state/supabase/migrations/`.
3. Generate new keys only for the new project.
4. Create a new Vercel project and point it to `vercel-ops-dashboard/`.
5. Set `OPSBOT_*` env vars in Vercel.
6. Deploy and verify the dashboard can only read data.

## Data Flow

- Core bot execution stays local/VM/container.
- Ingestion script publishes artifacts to Supabase.
- Dashboard reads from Supabase views and tables.
