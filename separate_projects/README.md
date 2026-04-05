# Separate Projects (Non-Zenith)

This folder intentionally holds **new, isolated projects** for persistence and operator UI.

- `supabase-bot-state/`: Supabase schema + ingestion tooling for bot state/history.
- `vercel-ops-dashboard/`: Vercel-ready, read-only dashboard that queries Supabase.

Both projects are designed to remain separate from any Zenith resources.
Read [SEPARATION_GUARDRAILS.md](/Users/dankerbadge/Documents/Betting%20Bot/separate_projects/SEPARATION_GUARDRAILS.md) before provisioning.

Recommended validation command after provisioning:

```bash
python3 /Users/dankerbadge/Documents/Betting\ Bot/separate_projects/supabase-bot-state/scripts/acceptance_gate.py \
  --outputs-dir /Users/dankerbadge/Documents/Betting\ Bot/outputs \
  --output-json /Users/dankerbadge/Documents/Betting\ Bot/outputs/supabase_acceptance_summary.json
```
