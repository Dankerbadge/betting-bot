import { createOpsbotSupabaseClient } from "@/lib/supabase";

export type OvernightRun = {
  run_id: string;
  run_started_at_utc: string | null;
  run_finished_at_utc: string | null;
  ingested_at?: string | null;
  overall_status: string | null;
  mode: string | null;
  pipeline_ready: boolean | null;
  live_ready: boolean | null;
  frontier_trusted_bucket_count: number | null;
  frontier_untrusted_bucket_count: number | null;
  daily_weather_market_availability_regime: string | null;
  daily_weather_market_availability_regime_reason: string | null;
  climate_rows_total: number | null;
  climate_tradable_positive_rows: number | null;
  climate_hot_positive_rows: number | null;
  climate_router_pilot_status: string | null;
  climate_router_pilot_expected_value_dollars: number | null;
  climate_router_pilot_total_risk_dollars: number | null;
  climate_router_pilot_promoted_rows: number | null;
  climate_router_pilot_attempted_orders: number | null;
  climate_router_pilot_filled_orders: number | null;
  climate_router_pilot_realized_pnl_dollars: number | null;
  router_vs_planner_gap_status: string | null;
  router_tradable_not_planned_count: number | null;
  payload_json: Record<string, unknown> | null;
};

export type FrontierRow = {
  run_id: string;
  captured_at: string;
  status: string;
  submitted_orders: number | null;
  filled_orders: number | null;
  full_filled_orders: number | null;
  trusted_bucket_count: number | null;
  untrusted_bucket_count: number | null;
  frontier_artifact_age_seconds: number | null;
  frontier_selection_mode: string | null;
};

export type ClimateActivity = {
  contract_family: string | null;
  event_type: string | null;
  observations: number;
  orderable_observations: number;
  latest_observed_at_utc: string | null;
};

export type ScorecardRow = {
  scorecard_key: string;
  captured_at: string;
  scorecard_type: string;
  status: string | null;
  headline_metric_name: string | null;
  headline_metric_value: number | null;
  headline_metric_unit: string | null;
  source_file: string | null;
};

export type PaperLiveScorecardRow = {
  scorecard_key: string;
  captured_at: string;
  scorecard_type: string;
  status: string | null;
  headline_metric_name: string | null;
  headline_metric_value: number | null;
  headline_metric_unit: string | null;
  payload_json: Record<string, unknown> | null;
  source_file: string | null;
};

export type DashboardSnapshot = {
  overnight: OvernightRun | null;
  overnightPrevious: OvernightRun | null;
  frontierRecent: FrontierRow[];
  climateActivity24h: ClimateActivity[];
  scorecardsRecent: ScorecardRow[];
  paperLiveFamilyScorecards: PaperLiveScorecardRow[];
  paperLiveTickerScorecards: PaperLiveScorecardRow[];
  latestFrontierCapturedAtUtc: string | null;
  latestPilotScorecardCapturedAtUtc: string | null;
  latestClimateObservedAtUtc: string | null;
};

function throwIfError(scope: string, error: { message: string } | null): void {
  if (error) {
    throw new Error(`${scope}: ${error.message}`);
  }
}

export async function loadDashboardSnapshot(): Promise<DashboardSnapshot> {
  const supabase = createOpsbotSupabaseClient();

  const overnightQuery = supabase
    .schema("bot_ops")
    .from("v_latest_overnight_run")
    .select("*")
    .limit(1)
    .maybeSingle<OvernightRun>();

  const overnightRecentQuery = supabase
    .schema("bot_ops")
    .from("overnight_runs")
    .select("*")
    .order("ingested_at", { ascending: false })
    .limit(2)
    .returns<OvernightRun[]>();

  const frontierQuery = supabase
    .schema("bot_ops")
    .from("v_frontier_recent")
    .select("*")
    .limit(12)
    .returns<FrontierRow[]>();

  const climateQuery = supabase
    .schema("bot_ops")
    .from("v_climate_activity_24h")
    .select("*")
    .limit(12)
    .returns<ClimateActivity[]>();

  const scorecardQuery = supabase
    .schema("bot_ops")
    .from("pilot_scorecards")
    .select(
      "scorecard_key,captured_at,scorecard_type,status,headline_metric_name,headline_metric_value,headline_metric_unit,source_file"
    )
    .in("scorecard_type", ["alpha_scoreboard", "autopilot_summary"])
    .order("captured_at", { ascending: false })
    .limit(8)
    .returns<ScorecardRow[]>();

  const paperLiveFamilyScorecardQuery = supabase
    .schema("bot_ops")
    .from("pilot_scorecards")
    .select(
      "scorecard_key,captured_at,scorecard_type,status,headline_metric_name,headline_metric_value,headline_metric_unit,payload_json,source_file"
    )
    .eq("scorecard_type", "paper_live_family")
    .order("captured_at", { ascending: false })
    .limit(12)
    .returns<PaperLiveScorecardRow[]>();

  const paperLiveTickerScorecardQuery = supabase
    .schema("bot_ops")
    .from("pilot_scorecards")
    .select(
      "scorecard_key,captured_at,scorecard_type,status,headline_metric_name,headline_metric_value,headline_metric_unit,payload_json,source_file"
    )
    .eq("scorecard_type", "paper_live_ticker")
    .order("captured_at", { ascending: false })
    .limit(24)
    .returns<PaperLiveScorecardRow[]>();

  const latestClimateQuery = supabase
    .schema("bot_ops")
    .from("climate_availability_events")
    .select("observed_at_utc")
    .order("observed_at_utc", { ascending: false })
    .limit(1)
    .returns<Array<{ observed_at_utc: string }>>();

  const [
    overnightResult,
    overnightRecentResult,
    frontierResult,
    climateResult,
    scorecardResult,
    paperLiveFamilyScorecardResult,
    paperLiveTickerScorecardResult,
    latestClimateResult,
  ] =
    await Promise.all([
    overnightQuery,
    overnightRecentQuery,
    frontierQuery,
    climateQuery,
    scorecardQuery,
    paperLiveFamilyScorecardQuery,
    paperLiveTickerScorecardQuery,
    latestClimateQuery,
  ]);

  throwIfError("v_latest_overnight_run", overnightResult.error);
  throwIfError("overnight_runs", overnightRecentResult.error);
  throwIfError("v_frontier_recent", frontierResult.error);
  throwIfError("v_climate_activity_24h", climateResult.error);
  throwIfError("pilot_scorecards", scorecardResult.error);
  throwIfError("pilot_scorecards paper_live_family", paperLiveFamilyScorecardResult.error);
  throwIfError("pilot_scorecards paper_live_ticker", paperLiveTickerScorecardResult.error);
  throwIfError("climate_availability_events", latestClimateResult.error);

  const overnightRecent = overnightRecentResult.data ?? [];
  const frontierRecent = frontierResult.data ?? [];
  const scorecardsRecent = scorecardResult.data ?? [];
  const paperLiveFamilyScorecards = paperLiveFamilyScorecardResult.data ?? [];
  const paperLiveTickerScorecards = paperLiveTickerScorecardResult.data ?? [];

  return {
    overnight: overnightResult.data,
    overnightPrevious: overnightRecent.length > 1 ? overnightRecent[1] : null,
    frontierRecent,
    climateActivity24h: climateResult.data ?? [],
    scorecardsRecent,
    paperLiveFamilyScorecards,
    paperLiveTickerScorecards,
    latestFrontierCapturedAtUtc: frontierRecent[0]?.captured_at ?? null,
    latestPilotScorecardCapturedAtUtc: scorecardsRecent[0]?.captured_at ?? null,
    latestClimateObservedAtUtc: latestClimateResult.data?.[0]?.observed_at_utc ?? null,
  };
}
