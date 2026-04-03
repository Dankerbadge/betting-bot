-- Separate project migration for bot state + dashboard reads.
-- This schema is intentionally non-Zenith.

create schema if not exists bot_ops;

create table if not exists bot_ops.execution_journal (
    id bigint generated always as identity primary key,
    external_event_key text not null unique,
    event_id bigint,
    run_id text not null,
    captured_at_utc timestamptz not null,
    event_type text not null,
    market_ticker text,
    event_family text,
    side text,
    limit_price_dollars double precision,
    contracts_fp double precision,
    client_order_id text,
    exchange_order_id text,
    parent_order_id text,
    best_yes_bid_dollars double precision,
    best_yes_ask_dollars double precision,
    best_no_bid_dollars double precision,
    best_no_ask_dollars double precision,
    spread_dollars double precision,
    visible_depth_contracts double precision,
    queue_position_contracts double precision,
    signal_score double precision,
    signal_age_seconds double precision,
    time_to_close_seconds double precision,
    latency_ms double precision,
    websocket_lag_ms double precision,
    api_latency_ms double precision,
    fee_dollars double precision,
    maker_fee_dollars double precision,
    taker_fee_dollars double precision,
    realized_pnl_dollars double precision,
    markout_10s_dollars double precision,
    markout_60s_dollars double precision,
    markout_300s_dollars double precision,
    result text,
    status text,
    payload_json jsonb not null default '{}'::jsonb,
    source_file text,
    ingested_at timestamptz not null default now()
);

create index if not exists execution_journal_captured_at_idx
    on bot_ops.execution_journal (captured_at_utc desc);
create index if not exists execution_journal_market_ticker_idx
    on bot_ops.execution_journal (market_ticker, captured_at_utc desc);
create index if not exists execution_journal_run_id_idx
    on bot_ops.execution_journal (run_id, captured_at_utc desc);

create table if not exists bot_ops.execution_frontier_reports (
    id bigint generated always as identity primary key,
    run_id text not null unique,
    captured_at timestamptz not null,
    status text not null,
    submitted_orders integer,
    filled_orders integer,
    full_filled_orders integer,
    fill_samples_with_markout integer,
    trusted_bucket_count integer,
    untrusted_bucket_count integer,
    frontier_artifact_age_seconds double precision,
    frontier_selection_mode text,
    recommendations jsonb not null default '[]'::jsonb,
    source_strategy_counts jsonb not null default '{}'::jsonb,
    payload_json jsonb not null,
    source_file text,
    ingested_at timestamptz not null default now()
);

create index if not exists execution_frontier_reports_captured_at_idx
    on bot_ops.execution_frontier_reports (captured_at desc);

create table if not exists bot_ops.execution_frontier_report_buckets (
    id bigint generated always as identity primary key,
    frontier_run_id text not null references bot_ops.execution_frontier_reports(run_id) on delete cascade,
    bucket text not null,
    orders_submitted integer,
    fill_rate double precision,
    full_fill_rate double precision,
    median_time_to_fill_seconds double precision,
    p90_time_to_fill_seconds double precision,
    markout_10s_side_adjusted double precision,
    markout_60s_side_adjusted double precision,
    markout_300s_side_adjusted double precision,
    markout_10s_samples integer,
    markout_60s_samples integer,
    markout_300s_samples integer,
    markout_horizons_trusted boolean,
    markout_horizons_untrusted_reason text,
    fee_spread_cancel_leakage_dollars_per_order double precision,
    expected_net_edge_after_costs_per_contract double precision,
    break_even_edge_per_contract double precision,
    payload_json jsonb not null default '{}'::jsonb,
    ingested_at timestamptz not null default now(),
    unique (frontier_run_id, bucket)
);

create index if not exists execution_frontier_report_buckets_run_idx
    on bot_ops.execution_frontier_report_buckets (frontier_run_id);

create table if not exists bot_ops.climate_availability_events (
    id bigint generated always as identity primary key,
    external_event_key text not null unique,
    observation_id bigint,
    run_id text not null,
    observed_at_utc timestamptz not null,
    market_ticker text not null,
    contract_family text,
    strip_key text,
    event_type text,
    yes_bid_dollars double precision,
    yes_ask_dollars double precision,
    no_bid_dollars double precision,
    no_ask_dollars double precision,
    spread_dollars double precision,
    has_quotes boolean,
    has_orderable_side boolean,
    non_endpoint_quote boolean,
    endpoint_only boolean,
    two_sided_book boolean,
    wakeup_transition boolean,
    public_trade_event boolean,
    public_trade_contracts double precision,
    updated_at_utc timestamptz,
    payload_json jsonb not null default '{}'::jsonb,
    source_file text,
    ingested_at timestamptz not null default now()
);

create index if not exists climate_availability_events_observed_at_idx
    on bot_ops.climate_availability_events (observed_at_utc desc);
create index if not exists climate_availability_events_ticker_idx
    on bot_ops.climate_availability_events (market_ticker, observed_at_utc desc);
create index if not exists climate_availability_events_family_idx
    on bot_ops.climate_availability_events (contract_family, observed_at_utc desc);

create table if not exists bot_ops.overnight_runs (
    id bigint generated always as identity primary key,
    run_id text not null unique,
    run_started_at_utc timestamptz,
    run_finished_at_utc timestamptz,
    run_stamp_utc text,
    overall_status text,
    mode text,
    pipeline_ready boolean,
    live_ready boolean,
    frontier_trusted_bucket_count integer,
    frontier_untrusted_bucket_count integer,
    daily_weather_market_availability_regime text,
    daily_weather_market_availability_regime_reason text,
    climate_rows_total integer,
    climate_tradable_positive_rows integer,
    climate_hot_positive_rows integer,
    climate_router_pilot_status text,
    climate_router_pilot_expected_value_dollars double precision,
    climate_router_pilot_total_risk_dollars double precision,
    climate_router_pilot_promoted_rows integer,
    climate_router_pilot_attempted_orders integer,
    climate_router_pilot_filled_orders integer,
    climate_router_pilot_realized_pnl_dollars double precision,
    router_vs_planner_gap_status text,
    router_tradable_not_planned_count integer,
    payload_json jsonb not null,
    source_file text,
    ingested_at timestamptz not null default now()
);

create index if not exists overnight_runs_finished_idx
    on bot_ops.overnight_runs (run_finished_at_utc desc);

create table if not exists bot_ops.pilot_scorecards (
    id bigint generated always as identity primary key,
    scorecard_key text not null unique,
    captured_at timestamptz not null,
    scorecard_type text not null,
    status text,
    headline_metric_name text,
    headline_metric_value double precision,
    headline_metric_unit text,
    payload_json jsonb not null,
    source_file text,
    ingested_at timestamptz not null default now()
);

create index if not exists pilot_scorecards_captured_at_idx
    on bot_ops.pilot_scorecards (captured_at desc);
create index if not exists pilot_scorecards_type_idx
    on bot_ops.pilot_scorecards (scorecard_type, captured_at desc);

create or replace view bot_ops.v_latest_overnight_run as
select *
from bot_ops.overnight_runs
order by coalesce(run_finished_at_utc, run_started_at_utc, ingested_at) desc
limit 1;

create or replace view bot_ops.v_frontier_recent as
select
    run_id,
    captured_at,
    status,
    submitted_orders,
    filled_orders,
    full_filled_orders,
    trusted_bucket_count,
    untrusted_bucket_count,
    frontier_artifact_age_seconds,
    frontier_selection_mode
from bot_ops.execution_frontier_reports
order by captured_at desc
limit 200;

create or replace view bot_ops.v_pilot_scorecards_recent as
select
    scorecard_key,
    captured_at,
    scorecard_type,
    status,
    headline_metric_name,
    headline_metric_value,
    headline_metric_unit,
    source_file
from bot_ops.pilot_scorecards
order by captured_at desc
limit 200;

create or replace view bot_ops.v_climate_activity_24h as
select
    contract_family,
    event_type,
    count(*)::bigint as observations,
    count(*) filter (where has_orderable_side is true)::bigint as orderable_observations,
    max(observed_at_utc) as latest_observed_at_utc
from bot_ops.climate_availability_events
where observed_at_utc >= now() - interval '24 hours'
group by contract_family, event_type
order by observations desc, contract_family, event_type;

alter table bot_ops.execution_journal enable row level security;
alter table bot_ops.execution_frontier_reports enable row level security;
alter table bot_ops.execution_frontier_report_buckets enable row level security;
alter table bot_ops.climate_availability_events enable row level security;
alter table bot_ops.overnight_runs enable row level security;
alter table bot_ops.pilot_scorecards enable row level security;

drop policy if exists execution_journal_read on bot_ops.execution_journal;
create policy execution_journal_read
    on bot_ops.execution_journal
    for select
    to anon, authenticated
    using (true);

drop policy if exists execution_frontier_reports_read on bot_ops.execution_frontier_reports;
create policy execution_frontier_reports_read
    on bot_ops.execution_frontier_reports
    for select
    to anon, authenticated
    using (true);

drop policy if exists execution_frontier_report_buckets_read on bot_ops.execution_frontier_report_buckets;
create policy execution_frontier_report_buckets_read
    on bot_ops.execution_frontier_report_buckets
    for select
    to anon, authenticated
    using (true);

drop policy if exists climate_availability_events_read on bot_ops.climate_availability_events;
create policy climate_availability_events_read
    on bot_ops.climate_availability_events
    for select
    to anon, authenticated
    using (true);

drop policy if exists overnight_runs_read on bot_ops.overnight_runs;
create policy overnight_runs_read
    on bot_ops.overnight_runs
    for select
    to anon, authenticated
    using (true);

drop policy if exists pilot_scorecards_read on bot_ops.pilot_scorecards;
create policy pilot_scorecards_read
    on bot_ops.pilot_scorecards
    for select
    to anon, authenticated
    using (true);

grant usage on schema bot_ops to anon, authenticated, service_role;
grant select on all tables in schema bot_ops to anon, authenticated;
grant select on all sequences in schema bot_ops to anon, authenticated;
grant all privileges on all tables in schema bot_ops to service_role;
grant all privileges on all sequences in schema bot_ops to service_role;

alter default privileges in schema bot_ops
    grant select on tables to anon, authenticated;
alter default privileges in schema bot_ops
    grant all on tables to service_role;
alter default privileges in schema bot_ops
    grant all on sequences to service_role;
