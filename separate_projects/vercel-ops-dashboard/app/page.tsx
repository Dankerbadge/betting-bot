import {
  loadDashboardSnapshot,
  type ClimateActivity,
  type OvernightRun,
  type PaperLiveScorecardRow,
} from "@/lib/data";
import { readDashboardEnv } from "@/lib/isolation";

export const dynamic = "force-dynamic";
const UI_REVISION = "2026-04-02T20:45:00Z";

type Tone = "ok" | "warn" | "bad";

type FreshnessItem = {
  label: string;
  timestamp: string | null;
  ageSeconds: number | null;
  tone: Tone;
  severity: "fresh" | "aging" | "stale" | "unknown";
  blocking: boolean;
  note: string;
};

type Blocker = {
  title: string;
  category: "policy" | "market structure" | "data freshness" | "account state" | "unknown";
  detail: string;
  action: string;
  tone: Tone;
  blocking: boolean;
};

type ClimateFamilyRollup = {
  family: string;
  observations: number;
  orderable: number;
  watchOnly: number;
  eventTypes: number;
};

type FamilyMarketHealth = {
  family: string;
  state: "active" | "watch" | "dead";
  orderableObservations24h: number;
  nonEndpointObservations24h: number;
  wakeups24h: number;
  tradableRowsCurrentRun: number;
  minutesOrderable24h: number;
};

type StripSummary = {
  stripId: string;
  city: string | null;
  date: string | null;
  currentState: "active" | "watch" | "dead";
  topModeledBucket: "modeled_positive" | "negative_or_neutral";
  topPricedBucket: "tradable_positive" | "priced_watch_only" | "watch_only" | "dead";
  topPricedEv: number | null;
  stripNonEndpointQuoteCount: number;
  stripTradableBucketCount: number;
  wakeups24h: number;
  minutesOrderable24h: number | null;
  recommendedAction: "pilot" | "watch" | "ignore";
};

type PaperLiveScorecardSummary = {
  scorecardKey: string;
  capturedAt: string;
  family: string;
  ticker: string;
  attempts: number;
  fills: number;
  fillRate: number | null;
  fillTimeMeanSeconds: number | null;
  markout10sMeanDollars: number | null;
  markout60sMeanDollars: number | null;
  markout300sMeanDollars: number | null;
  realizedSettlementPnlDollars: number | null;
  expectedVsRealizedDeltaDollars: number | null;
  openPositionCount: number;
  closeSettlementCount: number;
};

function formatNumber(value: number | null | undefined, digits = 0): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }
  return value.toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatMoney(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }
  return value.toLocaleString("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatMoneyDelta(value: number | null | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }
  const sign = value > 0 ? "+" : "";
  return `${sign}${formatMoney(value)}`;
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "--";
  }
  const asPercent = Math.abs(value) <= 1.5 ? value * 100 : value;
  return `${formatNumber(asPercent, digits)}%`;
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value || !value.trim()) {
    return "--";
  }
  return value;
}

function formatAge(ageSeconds: number | null): string {
  if (ageSeconds === null || !Number.isFinite(ageSeconds) || ageSeconds < 0) {
    return "--";
  }
  if (ageSeconds < 60) {
    return `${formatNumber(ageSeconds, 0)}s`;
  }
  const minutes = Math.floor(ageSeconds / 60);
  if (minutes < 60) {
    return `${minutes}m`;
  }
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  if (hours < 48) {
    return `${hours}h ${remainingMinutes}m`;
  }
  const days = Math.floor(hours / 24);
  const remainingHours = hours % 24;
  return `${days}d ${remainingHours}h`;
}

function readPayloadNumber(payload: Record<string, unknown> | null | undefined, key: string): number | null {
  if (!payload) {
    return null;
  }
  const value = payload[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function readPayloadString(payload: Record<string, unknown> | null | undefined, key: string): string | null {
  if (!payload) {
    return null;
  }
  const value = payload[key];
  if (typeof value === "string" && value.trim().length > 0) {
    return value.trim();
  }
  return null;
}

function readPayloadBoolean(payload: Record<string, unknown> | null | undefined, key: string): boolean | null {
  if (!payload) {
    return null;
  }
  const value = payload[key];
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "number") {
    if (value === 1) {
      return true;
    }
    if (value === 0) {
      return false;
    }
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) {
      return true;
    }
    if (["0", "false", "no", "off"].includes(normalized)) {
      return false;
    }
  }
  return null;
}

function readPayloadStringArray(payload: Record<string, unknown> | null | undefined, key: string): string[] {
  if (!payload) {
    return [];
  }
  const value = payload[key];
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
    .filter((entry) => entry.length > 0);
}

function readPayloadObject(
  payload: Record<string, unknown> | null | undefined,
  key: string
): Record<string, unknown> | null {
  if (!payload) {
    return null;
  }
  const value = payload[key];
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function readPayloadCountMap(payload: Record<string, unknown> | null | undefined, key: string): Record<string, number> {
  if (!payload) {
    return {};
  }
  const value = payload[key];
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }

  const result: Record<string, number> = {};
  for (const [entryKey, entryValue] of Object.entries(value as Record<string, unknown>)) {
    if (typeof entryValue === "number" && Number.isFinite(entryValue)) {
      result[entryKey] = entryValue;
    } else if (typeof entryValue === "string") {
      const parsed = Number(entryValue);
      if (Number.isFinite(parsed)) {
        result[entryKey] = parsed;
      }
    }
  }
  return result;
}

function dominantReason(counts: Record<string, number>): string | null {
  let bestKey: string | null = null;
  let bestValue = -1;
  for (const [key, value] of Object.entries(counts)) {
    if (value > bestValue) {
      bestValue = value;
      bestKey = key;
    }
  }
  return bestKey;
}

function humanizeToken(token: string | null | undefined): string {
  if (!token || token.trim().length === 0) {
    return "--";
  }
  return token
    .replace(/^blocked_/, "")
    .replace(/^pilot_/, "")
    .replace(/_/g, " ")
    .trim();
}

function formatSizingBasisLabel(sizingBasis: string | null | undefined): string {
  const raw = (sizingBasis ?? "").trim();
  if (!raw) {
    return "--";
  }
  if (raw.toLowerCase().startsWith("shadow_")) {
    const bankroll = Number(raw.slice("shadow_".length));
    if (Number.isFinite(bankroll) && bankroll > 0) {
      return `Shadow bankroll (${formatMoney(bankroll)})`;
    }
    return "Shadow bankroll";
  }
  return humanizeToken(raw);
}

function formatExecutionBasisLabel(executionBasis: string | null | undefined): string {
  const raw = (executionBasis ?? "").trim();
  if (!raw) {
    return "--";
  }
  if (raw.toLowerCase() === "live_actual_balance") {
    return "Actual live balance";
  }
  if (raw.toLowerCase() === "paper_live_balance") {
    return "Paper-live balance";
  }
  return humanizeToken(raw);
}

function readPayloadObjectArray(payload: Record<string, unknown> | null | undefined, key: string): Array<Record<string, unknown>> {
  if (!payload) {
    return [];
  }
  const value = payload[key];
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter(
    (entry): entry is Record<string, unknown> =>
      typeof entry === "object" && entry !== null && !Array.isArray(entry)
  );
}

function parsePaperLiveScorecard(row: PaperLiveScorecardRow): PaperLiveScorecardSummary | null {
  const payload =
    row.payload_json && typeof row.payload_json === "object" && !Array.isArray(row.payload_json)
      ? row.payload_json
      : null;
  if (!payload) {
    return null;
  }

  const family = readPayloadString(payload, "family") ?? "--";
  const ticker = readPayloadString(payload, "ticker") ?? "--";
  const attempts = readPayloadNumber(payload, "attempts") ?? 0;
  const fills = readPayloadNumber(payload, "fills") ?? 0;
  const openPositionCount = readPayloadNumber(payload, "open_position_count") ?? 0;
  const closeSettlementCount = readPayloadNumber(payload, "close_settlement_count") ?? 0;

  return {
    scorecardKey: row.scorecard_key,
    capturedAt: row.captured_at,
    family,
    ticker,
    attempts,
    fills,
    fillRate: readPayloadNumber(payload, "fill_rate"),
    fillTimeMeanSeconds: readPayloadNumber(payload, "fill_time_mean_seconds"),
    markout10sMeanDollars: readPayloadNumber(payload, "markout_10s_mean_dollars"),
    markout60sMeanDollars: readPayloadNumber(payload, "markout_60s_mean_dollars"),
    markout300sMeanDollars: readPayloadNumber(payload, "markout_300s_mean_dollars"),
    realizedSettlementPnlDollars: readPayloadNumber(payload, "realized_settlement_pnl_dollars"),
    expectedVsRealizedDeltaDollars: readPayloadNumber(payload, "expected_vs_realized_delta_dollars"),
    openPositionCount,
    closeSettlementCount,
  };
}

function weightedMean(
  rows: PaperLiveScorecardSummary[],
  valueKey:
    | "markout10sMeanDollars"
    | "markout60sMeanDollars"
    | "markout300sMeanDollars"
    | "fillTimeMeanSeconds",
  weightKey: "fills" | "openPositionCount" | "closeSettlementCount"
): number | null {
  let weightTotal = 0;
  let weightedTotal = 0;

  for (const row of rows) {
    const value = row[valueKey];
    if (typeof value !== "number" || Number.isNaN(value)) {
      continue;
    }
    const rawWeight = row[weightKey];
    const weight = Math.max(1, Number.isFinite(rawWeight) ? rawWeight : 0);
    weightedTotal += value * weight;
    weightTotal += weight;
  }

  if (weightTotal <= 0) {
    return null;
  }
  return weightedTotal / weightTotal;
}

function uniqueLatestRowsByKey(
  rows: PaperLiveScorecardSummary[],
  keySelector: (row: PaperLiveScorecardSummary) => string
): PaperLiveScorecardSummary[] {
  const ordered = rows
    .slice()
    .sort((a, b) => Date.parse(b.capturedAt || "") - Date.parse(a.capturedAt || ""));
  const seen = new Set<string>();
  const uniqueRows: PaperLiveScorecardSummary[] = [];
  for (const row of ordered) {
    const key = keySelector(row).trim().toLowerCase();
    if (!key || seen.has(key)) {
      continue;
    }
    seen.add(key);
    uniqueRows.push(row);
  }
  return uniqueRows;
}

function readObjectString(obj: Record<string, unknown>, key: string): string | null {
  const value = obj[key];
  if (typeof value === "string" && value.trim().length > 0) {
    return value.trim();
  }
  return null;
}

function readObjectNumber(obj: Record<string, unknown>, key: string): number | null {
  const value = obj[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function inferCityFromTicker(ticker: string | null): string | null {
  if (!ticker) {
    return null;
  }
  const rainOrTempMatch = ticker.match(/^KX(?:RAIN|TEMP)([A-Z]{3,6})-/);
  if (rainOrTempMatch?.[1]) {
    return rainOrTempMatch[1];
  }
  return null;
}

function stateTone(state: "active" | "watch" | "dead"): Tone {
  if (state === "active") {
    return "ok";
  }
  if (state === "watch") {
    return "warn";
  }
  return "bad";
}

function lower(value: string | null | undefined): string {
  return String(value ?? "").toLowerCase();
}

function statusTone(status: string | null | undefined): Tone {
  const normalized = lower(status);
  if (["ok", "ready", "pass", "green", "healthy", "live"].some((token) => normalized.includes(token))) {
    return "ok";
  }
  if (["degraded", "warn", "hold", "insufficient", "dry", "stale"].some((token) => normalized.includes(token))) {
    return "warn";
  }
  if (normalized.length === 0 || normalized === "unknown") {
    return "warn";
  }
  return "bad";
}

function ageSecondsFromTimestamp(timestamp: string | null | undefined, nowMs: number): number | null {
  if (!timestamp || !timestamp.trim()) {
    return null;
  }
  const parsed = Date.parse(timestamp);
  if (Number.isNaN(parsed)) {
    return null;
  }
  const ageSeconds = Math.max(0, (nowMs - parsed) / 1000);
  return Number.isFinite(ageSeconds) ? ageSeconds : null;
}

function buildFreshnessItem(
  label: string,
  timestamp: string | null,
  nowMs: number,
  freshUnderSeconds: number,
  staleAfterSeconds: number,
  blockedWhenStale: boolean,
  note: string
): FreshnessItem {
  const ageSeconds = ageSecondsFromTimestamp(timestamp, nowMs);
  if (ageSeconds === null) {
    return {
      label,
      timestamp,
      ageSeconds,
      tone: "bad",
      severity: "unknown",
      blocking: blockedWhenStale,
      note,
    };
  }

  if (ageSeconds <= freshUnderSeconds) {
    return {
      label,
      timestamp,
      ageSeconds,
      tone: "ok",
      severity: "fresh",
      blocking: false,
      note,
    };
  }

  if (ageSeconds <= staleAfterSeconds) {
    return {
      label,
      timestamp,
      ageSeconds,
      tone: "warn",
      severity: "aging",
      blocking: false,
      note,
    };
  }

  return {
    label,
    timestamp,
    ageSeconds,
    tone: "bad",
    severity: "stale",
    blocking: blockedWhenStale,
    note,
  };
}

function buildBalanceFreshness(balanceHeartbeatAgeSeconds: number | null): FreshnessItem {
  if (balanceHeartbeatAgeSeconds === null || !Number.isFinite(balanceHeartbeatAgeSeconds)) {
    return {
      label: "Balance",
      timestamp: null,
      ageSeconds: null,
      tone: "bad",
      severity: "unknown",
      blocking: true,
      note: "No balance heartbeat found in overnight payload",
    };
  }

  if (balanceHeartbeatAgeSeconds <= 1800) {
    return {
      label: "Balance",
      timestamp: null,
      ageSeconds: balanceHeartbeatAgeSeconds,
      tone: "ok",
      severity: "fresh",
      blocking: false,
      note: "Balance heartbeat from overnight payload",
    };
  }

  if (balanceHeartbeatAgeSeconds <= 7200) {
    return {
      label: "Balance",
      timestamp: null,
      ageSeconds: balanceHeartbeatAgeSeconds,
      tone: "warn",
      severity: "aging",
      blocking: false,
      note: "Balance heartbeat from overnight payload",
    };
  }

  return {
    label: "Balance",
    timestamp: null,
    ageSeconds: balanceHeartbeatAgeSeconds,
    tone: "bad",
    severity: "stale",
    blocking: true,
    note: "Balance heartbeat from overnight payload",
  };
}

function buildClimateRollup(rows: ClimateActivity[]): ClimateFamilyRollup[] {
  const byFamily = new Map<
    string,
    {
      observations: number;
      orderable: number;
      eventTypes: Set<string>;
    }
  >();

  for (const row of rows) {
    const family = row.contract_family && row.contract_family.trim().length > 0 ? row.contract_family.trim() : "unknown";
    const current = byFamily.get(family) ?? {
      observations: 0,
      orderable: 0,
      eventTypes: new Set<string>(),
    };

    current.observations += row.observations;
    current.orderable += row.orderable_observations;
    if (row.event_type && row.event_type.trim().length > 0) {
      current.eventTypes.add(row.event_type.trim());
    }
    byFamily.set(family, current);
  }

  return Array.from(byFamily.entries())
    .map(([family, value]) => ({
      family,
      observations: value.observations,
      orderable: value.orderable,
      watchOnly: Math.max(0, value.observations - value.orderable),
      eventTypes: value.eventTypes.size,
    }))
    .sort((a, b) => {
      if (b.orderable !== a.orderable) {
        return b.orderable - a.orderable;
      }
      return b.observations - a.observations;
    });
}

function buildBlockers(params: {
  overnight: OvernightRun | null;
  frontierStatus: string | null;
  balanceHeartbeatAgeSeconds: number | null;
  freshness: FreshnessItem[];
}): { primary: Blocker; secondary: Blocker[] } {
  const { overnight, frontierStatus, balanceHeartbeatAgeSeconds, freshness } = params;

  if (overnight?.live_ready) {
    return {
      primary: {
        title: "No blocker",
        category: "policy",
        detail: "Live readiness is true for the latest overnight run.",
        action: "Monitor fills and frontier health.",
        tone: "ok",
        blocking: false,
      },
      secondary: [],
    };
  }

  const candidates: Blocker[] = [];

  if (overnight?.pipeline_ready === false) {
    candidates.push({
      title: "Pipeline not ready",
      category: "data freshness",
      detail: "Core pipeline readiness is false in the latest overnight artifact.",
      action: "Inspect overnight pipeline stage logs and rerun failed stages.",
      tone: "bad",
      blocking: true,
    });
  }

  if (lower(frontierStatus).includes("insufficient")) {
    candidates.push({
      title: "Frontier insufficient data",
      category: "market structure",
      detail: "Recent frontier runs are reporting insufficient_data, so routing confidence is low.",
      action: "Wait for more valid fill/markout samples or loosen minimum bucket requirements.",
      tone: "bad",
      blocking: true,
    });
  }

  const regime = lower(overnight?.daily_weather_market_availability_regime);
  const regimeReason = overnight?.daily_weather_market_availability_regime_reason;
  if (regime.includes("dead") || regime.includes("endpoint")) {
    candidates.push({
      title: "Daily weather lane mostly endpoint-only",
      category: "market structure",
      detail:
        regimeReason && regimeReason.length > 0
          ? regimeReason
          : "Daily weather markets are not consistently quoting orderable sides.",
      action: "Prioritize monthly anomaly strips while daily weather remains watch-only.",
      tone: "warn",
      blocking: true,
    });
  }

  if (balanceHeartbeatAgeSeconds === null || balanceHeartbeatAgeSeconds > 7200) {
    candidates.push({
      title: "Balance freshness is stale",
      category: "account state",
      detail:
        balanceHeartbeatAgeSeconds === null
          ? "No balance heartbeat age is present in the overnight payload."
          : `Balance heartbeat is ${formatAge(balanceHeartbeatAgeSeconds)} old.`,
      action: "Refresh account balance snapshot before any live order path is enabled.",
      tone: "bad",
      blocking: true,
    });
  }

  if (lower(overnight?.router_vs_planner_gap_status).includes("gap") &&
    (overnight?.router_tradable_not_planned_count ?? 0) > 0) {
    candidates.push({
      title: "Router/planner gap detected",
      category: "policy",
      detail: `${formatNumber(overnight?.router_tradable_not_planned_count)} tradable rows were not planned.`,
      action: "Review planner eligibility filters against router tradable outputs.",
      tone: "warn",
      blocking: true,
    });
  }

  const staleFreshness = freshness.filter((item) => item.tone === "bad" && item.blocking);
  if (staleFreshness.length > 0) {
    candidates.push({
      title: "Critical freshness gaps",
      category: "data freshness",
      detail: staleFreshness.map((item) => item.label).join(", "),
      action: "Re-run ingest and confirm stale feeds update before trusting the dashboard state.",
      tone: "bad",
      blocking: true,
    });
  }

  if (candidates.length === 0) {
    candidates.push({
      title: "Live readiness is false",
      category: "unknown",
      detail: "No explicit blocker was found in the surfaced fields, but live_ready is still false.",
      action: "Inspect payload_json blocker reasons and execution journal for the last run.",
      tone: "warn",
      blocking: true,
    });
  }

  return {
    primary: candidates[0],
    secondary: candidates.slice(1, 4),
  };
}

function liveStateLabel(overnight: OvernightRun | null): { label: string; tone: Tone } {
  const mode = lower(overnight?.mode);
  if (mode.includes("dry")) {
    return { label: "Dry-run", tone: "warn" };
  }
  if (overnight?.live_ready) {
    return { label: "Ready", tone: "ok" };
  }
  return { label: "Blocked", tone: "bad" };
}

function computeDelta(current: number | null | undefined, previous: number | null | undefined): number | null {
  if (typeof current !== "number" || Number.isNaN(current)) {
    return null;
  }
  if (typeof previous !== "number" || Number.isNaN(previous)) {
    return null;
  }
  return current - previous;
}

function buildSparklinePoints(values: number[], width = 220, height = 56, padding = 4): string | null {
  const finiteValues = values.filter((value) => Number.isFinite(value));
  if (finiteValues.length < 2) {
    return null;
  }
  const minValue = Math.min(...finiteValues);
  const maxValue = Math.max(...finiteValues);
  const range = Math.max(1e-9, maxValue - minValue);
  const stepX = (width - padding * 2) / Math.max(1, finiteValues.length - 1);
  return finiteValues
    .map((value, idx) => {
      const x = padding + idx * stepX;
      const normalized = (value - minValue) / range;
      const y = height - padding - normalized * (height - padding * 2);
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    })
    .join(" ");
}

export default async function Page() {
  const env = readDashboardEnv();
  const snapshot = await loadDashboardSnapshot();
  const overnight = snapshot.overnight;
  const previousOvernight = snapshot.overnightPrevious;

  const overnightPayload =
    overnight?.payload_json && typeof overnight.payload_json === "object" ? overnight.payload_json : null;
  const balanceHeartbeatPayload = readPayloadObject(overnightPayload, "balance_heartbeat");

  const balanceHeartbeatAgeSeconds = readPayloadNumber(overnightPayload, "balance_heartbeat_age_seconds");
  const frontierArtifactAgeSeconds = readPayloadNumber(overnightPayload, "frontier_artifact_age_seconds");
  const latestOvernightRunAtUtc = overnight?.run_finished_at_utc ?? overnight?.run_started_at_utc ?? null;
  const liveBalanceDollars =
    readPayloadNumber(overnightPayload, "actual_live_balance_dollars") ??
    readObjectNumber(balanceHeartbeatPayload ?? {}, "balance_dollars");
  const shadowBankrollEnabled = readPayloadBoolean(overnightPayload, "shadow_bankroll_enabled");
  const shadowTheoreticalValueDollars = readPayloadNumber(overnightPayload, "shadow_theoretical_value_dollars");
  const shadowBankrollStartDollars = readPayloadNumber(overnightPayload, "shadow_bankroll_start_dollars");
  const shadowTheoreticalDrawdownPct = readPayloadNumber(overnightPayload, "shadow_theoretical_drawdown_pct");
  const shadowTheoreticalUnrealizedEvDollars = readPayloadNumber(
    overnightPayload,
    "shadow_theoretical_unrealized_ev_dollars"
  );
  const shadowExpectedValueDollars = readPayloadNumber(overnightPayload, "shadow_expected_value_dollars");
  const shadowAllocatorTotalRiskDollars = readPayloadNumber(overnightPayload, "shadow_allocator_total_risk_dollars");
  const shadowAllocatorSelectedRows = readPayloadNumber(overnightPayload, "shadow_allocator_selected_rows");
  const shadowBankrollStatus = readPayloadString(overnightPayload, "shadow_bankroll_status");
  const sizingBasis =
    readPayloadString(overnightPayload, "sizing_basis") ??
    (shadowBankrollStartDollars === null ? null : `shadow_${String(shadowBankrollStartDollars)}`);
  const executionBasis = readPayloadString(overnightPayload, "execution_basis") ?? "live_actual_balance";
  const paperLiveEnabled = readPayloadBoolean(overnightPayload, "paper_live_enabled");
  const paperLiveStatus = readPayloadString(overnightPayload, "paper_live_status");
  const paperLiveExecutionBasis = readPayloadString(overnightPayload, "paper_live_execution_basis") ?? "paper_live_balance";
  const paperLiveBalanceStartDollars = readPayloadNumber(overnightPayload, "paper_live_balance_start_dollars");
  const paperLiveBalanceCurrentDollars = readPayloadNumber(overnightPayload, "paper_live_balance_current_dollars");
  const paperLiveRealizedTradePnlDollars = readPayloadNumber(overnightPayload, "paper_live_realized_trade_pnl_dollars");
  const paperLiveMarkToMarketPnlDollars = readPayloadNumber(overnightPayload, "paper_live_mark_to_market_pnl_dollars");
  const paperLiveDrawdownPct = readPayloadNumber(overnightPayload, "paper_live_drawdown_pct");
  const paperLiveOrderAttempts = readPayloadNumber(overnightPayload, "paper_live_order_attempts");
  const paperLiveOrdersResting = readPayloadNumber(overnightPayload, "paper_live_orders_resting");
  const paperLiveOrdersFilled = readPayloadNumber(overnightPayload, "paper_live_orders_filled");
  const paperLiveOrdersCanceled = readPayloadNumber(overnightPayload, "paper_live_orders_canceled");
  const paperLivePositionsOpenCount = readPayloadNumber(overnightPayload, "paper_live_positions_open_count");
  const paperLivePositionsClosedCount = readPayloadNumber(overnightPayload, "paper_live_positions_closed_count");
  const paperLiveSelectedTickers = readPayloadStringArray(overnightPayload, "paper_live_selected_tickers");
  const paperLivePrimaryTicker = paperLiveSelectedTickers[0] ?? null;
  const paperLiveEquityCurveRows = readPayloadObjectArray(overnightPayload, "paper_live_equity_curve");
  const paperLiveEquitySeries = paperLiveEquityCurveRows
    .map((row) => readObjectNumber(row, "equity_dollars"))
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const paperLiveEquitySeriesTail = paperLiveEquitySeries.slice(-24);
  const paperLiveEquitySparklinePoints = buildSparklinePoints(paperLiveEquitySeriesTail);
  const paperLiveEquityDelta =
    paperLiveEquitySeriesTail.length >= 2
      ? paperLiveEquitySeriesTail[paperLiveEquitySeriesTail.length - 1] - paperLiveEquitySeriesTail[0]
      : null;
  const shadowStrategyEquityDollars = readPayloadNumber(overnightPayload, "shadow_strategy_equity_dollars");
  const shadowStrategyDrawdownPct = readPayloadNumber(overnightPayload, "shadow_strategy_drawdown_pct");
  const shadowMarkToModelPnlDollars = readPayloadNumber(overnightPayload, "shadow_mark_to_model_pnl_dollars");
  const shadowRealizedTradePnlDollars = readPayloadNumber(overnightPayload, "shadow_realized_trade_pnl_dollars");
  const shadowPositionsOpen = readPayloadObjectArray(overnightPayload, "shadow_positions_open");
  const shadowPositionsClosed = readPayloadObjectArray(overnightPayload, "shadow_positions_closed");
  const shadowPositionsOpenCount =
    readPayloadNumber(overnightPayload, "shadow_positions_open_count") ?? shadowPositionsOpen.length;
  const shadowPositionsClosedCount =
    readPayloadNumber(overnightPayload, "shadow_positions_closed_count") ?? shadowPositionsClosed.length;
  const shadowEquityCurveRows = readPayloadObjectArray(overnightPayload, "shadow_equity_curve");
  const shadowEquitySeries = shadowEquityCurveRows
    .map((row) => readObjectNumber(row, "equity_dollars"))
    .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
  const shadowEquitySeriesTail = shadowEquitySeries.slice(-24);
  const shadowEquitySparklinePoints = buildSparklinePoints(shadowEquitySeriesTail);
  const shadowEquityDelta =
    shadowEquitySeriesTail.length >= 2
      ? shadowEquitySeriesTail[shadowEquitySeriesTail.length - 1] - shadowEquitySeriesTail[0]
      : null;
  const shadowEntryPrice = readPayloadNumber(overnightPayload, "shadow_entry_price");
  const shadowEntryTime = readPayloadString(overnightPayload, "shadow_entry_time");
  const shadowSide = readPayloadString(overnightPayload, "shadow_side");
  const shadowContracts = readPayloadNumber(overnightPayload, "shadow_contracts");
  const shadowNotionalRiskDollars = readPayloadNumber(overnightPayload, "shadow_notional_risk_dollars");
  const shadowMarkPrice = readPayloadNumber(overnightPayload, "shadow_mark_price");
  const shadowStrategyAccountingVersion = readPayloadNumber(overnightPayload, "shadow_strategy_accounting_version");

  const nowMs = Date.now();
  const freshness: FreshnessItem[] = [
    buildFreshnessItem(
      "Overnight",
      latestOvernightRunAtUtc,
      nowMs,
      30 * 3600,
      40 * 3600,
      true,
      "Latest overnight run timestamp"
    ),
    buildFreshnessItem(
      "Frontier",
      snapshot.latestFrontierCapturedAtUtc,
      nowMs,
      30 * 3600,
      40 * 3600,
      true,
      "Latest frontier report timestamp"
    ),
    buildFreshnessItem(
      "Scorecard",
      snapshot.latestPilotScorecardCapturedAtUtc,
      nowMs,
      8 * 3600,
      20 * 3600,
      true,
      "Latest pilot scorecard timestamp; stale scorecards can skew realized-vs-expected comparisons"
    ),
    buildFreshnessItem(
      "Climate",
      snapshot.latestClimateObservedAtUtc,
      nowMs,
      4 * 3600,
      12 * 3600,
      false,
      "Latest climate observation timestamp"
    ),
    buildBalanceFreshness(balanceHeartbeatAgeSeconds),
  ];

  const blockingFreshnessCount = freshness.filter((item) => item.tone === "bad" && item.blocking).length;
  const freshnessStatusLabel =
    blockingFreshnessCount === 0 ? "Healthy" : `${blockingFreshnessCount} blocking freshness issue${blockingFreshnessCount === 1 ? "" : "s"}`;

  const frontierLatest = snapshot.frontierRecent[0] ?? null;
  const economicBlockers = buildBlockers({
    overnight,
    frontierStatus: frontierLatest?.status ?? null,
    balanceHeartbeatAgeSeconds,
    freshness,
  });

  const climateRollup = buildClimateRollup(snapshot.climateActivity24h);
  const topTradableFamilies = climateRollup.filter((row) => row.orderable > 0).slice(0, 5);
  const topWatchOnlyFamilies = climateRollup.filter((row) => row.orderable === 0 && row.observations > 0).slice(0, 5);

  const totalObservations = climateRollup.reduce((sum, row) => sum + row.observations, 0);
  const totalOrderable = climateRollup.reduce((sum, row) => sum + row.orderable, 0);
  const totalWatchOnly = Math.max(0, totalObservations - totalOrderable);

  const modeledPositiveRowsCurrentRun =
    readPayloadNumber(overnightPayload, "climate_theoretical_positive_rows") ??
    overnight?.climate_rows_total ??
    overnight?.climate_tradable_positive_rows ??
    null;

  const deadEndpointFamilyCount = climateRollup.filter((row) => row.orderable === 0 && row.observations > 0).length;
  const topActiveFamily =
    topTradableFamilies[0]?.family ??
    climateRollup[0]?.family ??
    readPayloadString(overnightPayload, "top_climate_family") ??
    "--";

  const liveState = liveStateLabel(overnight);
  const runMode = overnight?.mode ?? "--";
  const isDryRunMode = lower(runMode).includes("dry");
  const modeBlocker = isDryRunMode
    ? {
        title: "Run mode is dry-run",
        detail: "This run is operating in research_dry_run_only, so order attempts stay blocked by policy.",
        tone: "warn" as Tone,
      }
    : null;
  const liveEligibility = overnight?.live_ready ? "Eligible" : "Not eligible";
  const liveEligibilityTone: Tone = overnight?.live_ready ? "ok" : "bad";

  const pilotExpectedValue = overnight?.climate_router_pilot_expected_value_dollars ?? null;
  const pilotTotalRisk = overnight?.climate_router_pilot_total_risk_dollars ?? null;
  const pilotRealizedPnl = overnight?.climate_router_pilot_realized_pnl_dollars ?? null;
  const expectedVsRealizedDelta =
    typeof pilotExpectedValue === "number" && typeof pilotRealizedPnl === "number"
      ? pilotRealizedPnl - pilotExpectedValue
      : null;

  const pilotFunnel = [
    { label: "Considered", value: overnight?.climate_rows_total ?? null },
    { label: "Promoted", value: overnight?.climate_router_pilot_promoted_rows ?? null },
    { label: "Execute Considered", value: overnight?.climate_tradable_positive_rows ?? null },
    { label: "Attempted", value: overnight?.climate_router_pilot_attempted_orders ?? null },
    { label: "Filled", value: overnight?.climate_router_pilot_filled_orders ?? null },
  ];

  const funnelBase =
    typeof pilotFunnel[0].value === "number" && pilotFunnel[0].value > 0
      ? pilotFunnel[0].value
      : null;
  const pilotSelectedTickers = readPayloadStringArray(overnightPayload, "climate_router_pilot_selected_tickers");
  const pilotSelectedTicker = pilotSelectedTickers[0] ?? null;
  const postPromotionBlockedReasons = readPayloadCountMap(
    overnightPayload,
    "climate_router_pilot_blocked_post_promotion_reason_counts"
  );
  const pilotBlockedReasons = readPayloadCountMap(overnightPayload, "climate_router_pilot_blocked_reason_counts");
  const dominantPostPromotionReason = dominantReason(postPromotionBlockedReasons);
  const dominantPilotBlockedReason = dominantReason(pilotBlockedReasons);
  const executeConsideredRows =
    readPayloadNumber(overnightPayload, "climate_router_pilot_execute_considered_rows") ??
    overnight?.climate_tradable_positive_rows ??
    0;
  const attemptedOrders = overnight?.climate_router_pilot_attempted_orders ?? 0;
  const dominantAttemptBlockerToken =
    attemptedOrders > 0
      ? "none"
      : dominantPostPromotionReason ??
        (isDryRunMode ? "blocked_research_dry_run_only" : dominantPilotBlockedReason ?? "not_classified");
  const dominantAttemptBlocker = humanizeToken(dominantAttemptBlockerToken);
  const wouldAttemptLiveIfEnabled =
    attemptedOrders > 0 ||
    (executeConsideredRows > 0 &&
      (isDryRunMode || lower(dominantAttemptBlockerToken).includes("dry_run") || pilotSelectedTickers.length > 0));

  const scorecardFreshness = freshness.find((item) => item.label === "Scorecard") ?? null;
  const balanceFreshness = freshness.find((item) => item.label === "Balance") ?? null;
  const operationalAlerts: Array<{ title: string; tone: Tone; detail: string }> = [];
  if (balanceFreshness && balanceFreshness.tone === "bad") {
    operationalAlerts.push({
      title: "Balance heartbeat missing",
      tone: "bad",
      detail: "Live orders should remain blocked until balance freshness is restored.",
    });
  }
  if (scorecardFreshness && scorecardFreshness.tone !== "ok") {
    operationalAlerts.push({
      title: "Pilot scorecard is stale",
      tone: scorecardFreshness.tone,
      detail: "Realized-vs-expected tracking may be outdated for the latest run.",
    });
  }
  if (modeBlocker) {
    operationalAlerts.push({
      title: "Run mode limits live execution",
      tone: "warn",
      detail: "Dry-run mode overrides economic readiness and prevents submission attempts.",
    });
  }

  const climateTopWakingStrips = readPayloadObjectArray(overnightPayload, "climate_top_waking_strips");
  const climateTopTradableCandidates = readPayloadObjectArray(overnightPayload, "climate_top_tradable_candidates");
  const climateShadowTopAllocations = readPayloadObjectArray(overnightPayload, "climate_router_shadow_plan_top_allocations");

  const tradableRowsByFamily = new Map<string, number>();
  for (const candidate of climateTopTradableCandidates) {
    const family = readObjectString(candidate, "contract_family") ?? "unknown";
    tradableRowsByFamily.set(family, (tradableRowsByFamily.get(family) ?? 0) + 1);
  }

  const routedEvByFamily = new Map<string, number>();
  for (const allocation of climateShadowTopAllocations) {
    const family = readObjectString(allocation, "contract_family") ?? "unknown";
    const ev = readObjectNumber(allocation, "expected_value_dollars") ?? 0;
    routedEvByFamily.set(family, (routedEvByFamily.get(family) ?? 0) + ev);
  }

  const stripPricedEvByKey = new Map<string, number>();
  for (const allocation of climateShadowTopAllocations) {
    const stripKey = readObjectString(allocation, "strip_key");
    if (!stripKey) {
      continue;
    }
    const ev = readObjectNumber(allocation, "expected_value_dollars");
    if (ev === null) {
      continue;
    }
    const prior = stripPricedEvByKey.get(stripKey);
    if (prior === undefined || ev > prior) {
      stripPricedEvByKey.set(stripKey, ev);
    }
  }

  const familyHealthMap = new Map<
    string,
    {
      orderableObservations24h: number;
      nonEndpointObservations24h: number;
      wakeups24h: number;
      tradableRowsCurrentRun: number;
      minutesOrderable24h: number;
    }
  >();

  for (const strip of climateTopWakingStrips) {
    const stripKey = readObjectString(strip, "strip_key");
    const keyFamily = stripKey?.split("|")[0] ?? null;
    const family = readObjectString(strip, "top_contract_family") ?? keyFamily ?? "unknown";
    const current = familyHealthMap.get(family) ?? {
      orderableObservations24h: 0,
      nonEndpointObservations24h: 0,
      wakeups24h: 0,
      tradableRowsCurrentRun: 0,
      minutesOrderable24h: 0,
    };
    current.orderableObservations24h += readObjectNumber(strip, "strip_orderable_observations") ?? 0;
    current.nonEndpointObservations24h += readObjectNumber(strip, "strip_non_endpoint_observations") ?? 0;
    current.wakeups24h += readObjectNumber(strip, "strip_wakeup_count") ?? 0;
    current.minutesOrderable24h += readObjectNumber(strip, "strip_avg_minutes_orderable") ?? 0;
    familyHealthMap.set(family, current);
  }

  const allFamilies = new Set<string>([
    ...climateRollup.map((row) => row.family),
    ...Array.from(familyHealthMap.keys()),
    ...Array.from(tradableRowsByFamily.keys()),
    ...Array.from(routedEvByFamily.keys()),
  ]);

  const whereMarketIsAlive: FamilyMarketHealth[] = Array.from(allFamilies)
    .map((family) => {
      const base = familyHealthMap.get(family) ?? {
        orderableObservations24h: 0,
        nonEndpointObservations24h: 0,
        wakeups24h: 0,
        tradableRowsCurrentRun: 0,
        minutesOrderable24h: 0,
      };
      const rollup = climateRollup.find((row) => row.family === family);
      if (rollup && base.orderableObservations24h === 0) {
        base.orderableObservations24h = rollup.orderable;
      }
      const tradableRowsCurrentRun = tradableRowsByFamily.get(family) ?? 0;
      const state: "active" | "watch" | "dead" =
        tradableRowsCurrentRun > 0 || base.minutesOrderable24h >= 30 || base.orderableObservations24h >= 10
          ? "active"
          : base.orderableObservations24h > 0 || base.nonEndpointObservations24h > 0 || base.wakeups24h > 0
            ? "watch"
            : "dead";
      return {
        family,
        state,
        orderableObservations24h: base.orderableObservations24h,
        nonEndpointObservations24h: base.nonEndpointObservations24h,
        wakeups24h: base.wakeups24h,
        tradableRowsCurrentRun,
        minutesOrderable24h: base.minutesOrderable24h,
      };
    })
    .sort((a, b) => {
      const stateRank = { active: 2, watch: 1, dead: 0 };
      if (stateRank[b.state] !== stateRank[a.state]) {
        return stateRank[b.state] - stateRank[a.state];
      }
      if (b.orderableObservations24h !== a.orderableObservations24h) {
        return b.orderableObservations24h - a.orderableObservations24h;
      }
      return b.minutesOrderable24h - a.minutesOrderable24h;
    });

  const topFamilyByRoutedEv =
    Array.from(routedEvByFamily.entries()).sort((a, b) => b[1] - a[1])[0]?.[0] ?? topActiveFamily;
  const topFamilyByMinutesOrderable =
    whereMarketIsAlive.slice().sort((a, b) => b.minutesOrderable24h - a.minutesOrderable24h)[0]?.family ?? topActiveFamily;

  const topStripSummaries: StripSummary[] = climateTopWakingStrips
    .map((strip) => {
      const stripId = readObjectString(strip, "strip_key") ?? "--";
      const stripParts = stripId.split("|");
      const topMarketTicker = readObjectString(strip, "top_market_ticker") ?? stripParts[1] ?? null;
      const date = stripParts[2] ?? null;
      const stripNonEndpointQuoteCount = readObjectNumber(strip, "strip_non_endpoint_observations") ?? 0;
      const stripOrderableObservations = readObjectNumber(strip, "strip_orderable_observations") ?? 0;
      const wakeups24h = readObjectNumber(strip, "strip_wakeup_count") ?? 0;
      const minutesOrderable24h = readObjectNumber(strip, "strip_avg_minutes_orderable");
      const topTheoreticalEdge = readObjectNumber(strip, "top_theoretical_edge_net");
      const tradableRate = readObjectNumber(strip, "strip_tradable_positive_rate") ?? 0;
      const pricedWatchRate = readObjectNumber(strip, "strip_priced_watch_only_rate") ?? 0;
      const topModeledBucket: "modeled_positive" | "negative_or_neutral" =
        (topTheoreticalEdge ?? 0) > 0 ? "modeled_positive" : "negative_or_neutral";
      const topPricedBucket: "tradable_positive" | "priced_watch_only" | "watch_only" | "dead" =
        tradableRate > 0 || stripOrderableObservations > 0
          ? "tradable_positive"
          : pricedWatchRate > 0
            ? "priced_watch_only"
            : stripNonEndpointQuoteCount > 0
              ? "watch_only"
              : "dead";
      const currentState: "active" | "watch" | "dead" =
        topPricedBucket === "tradable_positive"
          ? "active"
          : topPricedBucket === "dead"
            ? "dead"
            : "watch";
      const topPricedEv = stripPricedEvByKey.get(stripId) ?? (topPricedBucket === "dead" ? null : topTheoreticalEdge);
      const stripTradableBucketCount =
        currentState === "active" ? (readObjectNumber(strip, "strip_row_count") ?? 1) : 0;
      const recommendedAction: "pilot" | "watch" | "ignore" =
        currentState === "active" && (topPricedEv ?? 0) > 0
          ? "pilot"
          : currentState === "dead"
            ? "ignore"
            : "watch";
      return {
        stripId,
        city: inferCityFromTicker(topMarketTicker),
        date,
        currentState,
        topModeledBucket,
        topPricedBucket,
        topPricedEv,
        stripNonEndpointQuoteCount,
        stripTradableBucketCount,
        wakeups24h,
        minutesOrderable24h,
        recommendedAction,
      };
    })
    .sort((a, b) => {
      const actionRank = { pilot: 2, watch: 1, ignore: 0 };
      if (actionRank[b.recommendedAction] !== actionRank[a.recommendedAction]) {
        return actionRank[b.recommendedAction] - actionRank[a.recommendedAction];
      }
      const evA = a.topPricedEv ?? Number.NEGATIVE_INFINITY;
      const evB = b.topPricedEv ?? Number.NEGATIVE_INFINITY;
      if (evB !== evA) {
        return evB - evA;
      }
      return (b.minutesOrderable24h ?? 0) - (a.minutesOrderable24h ?? 0);
    })
    .slice(0, 5);

  const paperLiveFamilyScorecards = uniqueLatestRowsByKey(
    snapshot.paperLiveFamilyScorecards
      .map(parsePaperLiveScorecard)
      .filter((row): row is PaperLiveScorecardSummary => row !== null),
    (row) => row.family
  )
    .sort((a, b) => {
      if (b.attempts !== a.attempts) {
        return b.attempts - a.attempts;
      }
      if (b.fills !== a.fills) {
        return b.fills - a.fills;
      }
      return (b.markout300sMeanDollars ?? Number.NEGATIVE_INFINITY) - (a.markout300sMeanDollars ?? Number.NEGATIVE_INFINITY);
    })
    .slice(0, 5);

  const paperLiveTickerScorecards = uniqueLatestRowsByKey(
    snapshot.paperLiveTickerScorecards
      .map(parsePaperLiveScorecard)
      .filter((row): row is PaperLiveScorecardSummary => row !== null),
    (row) => row.ticker
  )
    .sort((a, b) => {
      if (b.attempts !== a.attempts) {
        return b.attempts - a.attempts;
      }
      if (b.fills !== a.fills) {
        return b.fills - a.fills;
      }
      return (b.markout300sMeanDollars ?? Number.NEGATIVE_INFINITY) - (a.markout300sMeanDollars ?? Number.NEGATIVE_INFINITY);
    })
    .slice(0, 5);

  const paperLiveAttemptsSample = paperLiveFamilyScorecards.reduce((sum, row) => sum + row.attempts, 0);
  const paperLiveFillsSample = paperLiveFamilyScorecards.reduce((sum, row) => sum + row.fills, 0);
  const paperLiveLowSample = paperLiveAttemptsSample < 5 || paperLiveFillsSample < 5;
  const paperLiveLowSampleLabel = `Low sample: directional only (attempts=${formatNumber(
    paperLiveAttemptsSample
  )}, fills=${formatNumber(paperLiveFillsSample)})`;

  const paperLiveOpenRows = paperLiveFamilyScorecards.filter((row) => row.openPositionCount > 0);
  const paperLiveSettledRows = paperLiveFamilyScorecards.filter((row) => row.closeSettlementCount > 0);
  const paperLiveOpenCount = paperLiveFamilyScorecards.reduce((sum, row) => sum + row.openPositionCount, 0);
  const paperLiveSettledCount = paperLiveFamilyScorecards.reduce((sum, row) => sum + row.closeSettlementCount, 0);

  const paperLiveOpenMarkout10s = weightedMean(paperLiveOpenRows, "markout10sMeanDollars", "openPositionCount");
  const paperLiveOpenMarkout60s = weightedMean(paperLiveOpenRows, "markout60sMeanDollars", "openPositionCount");
  const paperLiveOpenMarkout300s = weightedMean(paperLiveOpenRows, "markout300sMeanDollars", "openPositionCount");
  const paperLiveSettledFillTime = weightedMean(paperLiveSettledRows, "fillTimeMeanSeconds", "closeSettlementCount");
  const paperLiveSettledRealizedPnl = paperLiveSettledRows.reduce(
    (sum, row) => sum + (row.realizedSettlementPnlDollars ?? 0),
    0
  );
  const paperLiveSettledExpectedVsRealized = paperLiveSettledRows.reduce(
    (sum, row) => sum + (row.expectedVsRealizedDeltaDollars ?? 0),
    0
  );
  const paperLiveSettledRealizedPnlDisplay = paperLiveSettledCount > 0 ? paperLiveSettledRealizedPnl : null;
  const paperLiveSettledExpectedVsRealizedDisplay =
    paperLiveSettledCount > 0 ? paperLiveSettledExpectedVsRealized : null;
  const latestSelectedTickerSet = new Set<string>(
    [...paperLiveSelectedTickers, pilotSelectedTicker ?? ""]
      .map((ticker) => ticker.trim().toLowerCase())
      .filter((ticker) => ticker.length > 0)
  );

  const deltaExpectedValue = computeDelta(
    overnight?.climate_router_pilot_expected_value_dollars,
    previousOvernight?.climate_router_pilot_expected_value_dollars
  );
  const deltaRisk = computeDelta(
    overnight?.climate_router_pilot_total_risk_dollars,
    previousOvernight?.climate_router_pilot_total_risk_dollars
  );
  const deltaTradable = computeDelta(overnight?.climate_tradable_positive_rows, previousOvernight?.climate_tradable_positive_rows);

  const previousLiveReady =
    typeof previousOvernight?.live_ready === "boolean" ? (previousOvernight.live_ready ? "Ready" : "Blocked") : "--";
  const currentLiveReady = typeof overnight?.live_ready === "boolean" ? (overnight.live_ready ? "Ready" : "Blocked") : "--";
  const liveStateDelta = previousLiveReady === "--" ? "No prior run" : `${previousLiveReady} -> ${currentLiveReady}`;

  const frontierWindow = snapshot.frontierRecent.slice(0, 8);
  const frontierWindowSubmitted = frontierWindow.reduce((sum, row) => sum + (row.submitted_orders ?? 0), 0);
  const frontierWindowFilled = frontierWindow.reduce((sum, row) => sum + (row.filled_orders ?? 0), 0);
  const frontierWindowInsufficient = frontierWindow.filter((row) => lower(row.status).includes("insufficient")).length;
  const frontierFillRate =
    frontierWindowSubmitted > 0 ? (frontierWindowFilled / frontierWindowSubmitted) * 100 : null;

  const frontierTrendTone: Tone =
    frontierWindowInsufficient === 0 ? "ok" : frontierWindowInsufficient <= 2 ? "warn" : "bad";

  return (
    <main>
      <div className="dashboard-shell">
        <section className="topline">
          <div>
            <h1>Opsbot Trading Operations</h1>
            <p className="subtle">
              Separate project ref: <span className="code">{env.projectRef}</span>
            </p>
            <p className="subtle">
              Run: <span className="code">{overnight?.run_id ?? "--"}</span> | Mode: <span className="code">{overnight?.mode ?? "--"}</span>
            </p>
            <p className="subtle">
              UI revision: <span className="code">{UI_REVISION}</span>
            </p>
          </div>
          <div className="pill-row">
            <span className={`status-pill ${liveState.tone}`}>Live State: {liveState.label}</span>
            <span className={`status-pill ${statusTone(overnight?.overall_status)}`}>
              Overnight: {overnight?.overall_status ?? "unknown"}
            </span>
          </div>
        </section>

        <section className="decision-grid">
          <article className="card card-emphasis">
            <h2>Run Mode</h2>
            <div className="metric metric-text code">{runMode}</div>
            <div className="subtle">Mode blocker: {modeBlocker ? "Yes" : "No"}</div>
          </article>

          <article className="card card-emphasis">
            <h2>Live Eligibility</h2>
            <div className={`metric tone-${liveEligibilityTone}`}>{liveEligibility}</div>
            <div className="subtle">Pipeline ready: {overnight?.pipeline_ready ? "Yes" : "No"}</div>
            <div className="subtle">Live state: {liveState.label}</div>
          </article>

          <article className="card card-emphasis">
            <h2>Top Economic Blocker</h2>
            <div className="metric metric-text">{economicBlockers.primary.title}</div>
            <div className="subtle">
              Category: <span className="code">{economicBlockers.primary.category}</span>
            </div>
          </article>

          <article className="card card-emphasis">
            <h2>Top Active Family</h2>
            <div className="metric metric-text">{topActiveFamily}</div>
            <div className="subtle">
              Tradable observations (24h): {formatNumber(topTradableFamilies[0]?.orderable ?? 0)}
            </div>
          </article>

          <article className="card card-emphasis">
            <h2>Pilot EV / Risk</h2>
            <div className="metric">{formatMoney(pilotExpectedValue)}</div>
            <div className="subtle">Risk: {formatMoney(pilotTotalRisk)}</div>
          </article>

          <article className="card card-emphasis">
            <h2>Live Balance + Paper-Live + Shadow View</h2>
            <div className="metric">{formatMoney(liveBalanceDollars)}</div>
            <div className="subtle">Live balance</div>
            <div className="subtle">
              Recommendation sizing basis: <span className="code">{formatSizingBasisLabel(sizingBasis)}</span>
            </div>
            <div className="subtle">
              Execution sizing basis: <span className="code">{formatExecutionBasisLabel(executionBasis)}</span>
            </div>
            <div className="subtle" style={{ marginTop: "0.45rem" }}>
              <strong>Paper-live simulation account</strong>
            </div>
            <div className="subtle">
              Paper-live basis: <span className="code">{formatExecutionBasisLabel(paperLiveExecutionBasis)}</span>
            </div>
            <div className="subtle">
              Balance: {formatMoney(paperLiveBalanceCurrentDollars)}
              {paperLiveBalanceStartDollars === null ? "" : ` | Start: ${formatMoney(paperLiveBalanceStartDollars)}`}
            </div>
            <div className="subtle">
              Realized / Marked PnL: {formatMoney(paperLiveRealizedTradePnlDollars)} / {formatMoney(paperLiveMarkToMarketPnlDollars)}
            </div>
            <div className="subtle">
              Drawdown: {paperLiveDrawdownPct === null ? "--" : `${formatNumber(paperLiveDrawdownPct, 2)}%`}
            </div>
            <div className="subtle">
              Attempts / Resting / Filled / Canceled: {formatNumber(paperLiveOrderAttempts)} / {formatNumber(paperLiveOrdersResting)} /{" "}
              {formatNumber(paperLiveOrdersFilled)} / {formatNumber(paperLiveOrdersCanceled)}
            </div>
            <div className="subtle">
              Open / Closed positions: {formatNumber(paperLivePositionsOpenCount)} / {formatNumber(paperLivePositionsClosedCount)}
              {paperLivePrimaryTicker ? ` | Primary ticker: ${paperLivePrimaryTicker}` : ""}
            </div>
            <div className="subtle">
              Paper-live lane:{" "}
              <span className={`status-pill ${paperLiveEnabled === false ? "bad" : "ok"}`}>
                {paperLiveEnabled === false ? "disabled" : "enabled"}
              </span>
              {paperLiveStatus ? <span className="code"> {paperLiveStatus}</span> : null}
            </div>
            {paperLiveEquitySparklinePoints ? (
              <div style={{ marginTop: "0.45rem" }}>
                <svg
                  viewBox="0 0 220 56"
                  role="img"
                  aria-label="Paper-live strategy equity curve"
                  style={{ width: "100%", height: "56px" }}
                >
                  <polyline fill="none" stroke="currentColor" strokeWidth="2" points={paperLiveEquitySparklinePoints} />
                </svg>
                <div className="subtle">
                  Paper-live equity curve ({formatNumber(paperLiveEquitySeriesTail.length)} points)
                  {paperLiveEquityDelta === null ? "" : ` | Delta: ${formatMoneyDelta(paperLiveEquityDelta)}`}
                </div>
              </div>
            ) : (
              <div className="subtle">Paper-live equity curve: --</div>
            )}
            <div className="subtle">
              <strong>Shadow strategy accounting</strong>
            </div>
            <div className="subtle">
              Strategy equity: {formatMoney(shadowStrategyEquityDollars)}
            </div>
            <div className="subtle">
              Strategy drawdown: {shadowStrategyDrawdownPct === null ? "--" : `${formatNumber(shadowStrategyDrawdownPct, 2)}%`}
            </div>
            <div className="subtle">Marked PnL: {formatMoney(shadowMarkToModelPnlDollars)}</div>
            <div className="subtle">Realized trade PnL: {formatMoney(shadowRealizedTradePnlDollars)}</div>
            <div className="subtle">
              Open / Closed positions: {formatNumber(shadowPositionsOpenCount)} / {formatNumber(shadowPositionsClosedCount)}
            </div>
            <div className="subtle">
              Primary open position:{" "}
              {shadowSide
                ? `${shadowSide.toUpperCase()} ${formatNumber(shadowContracts)} @ ${formatMoney(shadowEntryPrice)}`
                : "--"}
              {shadowNotionalRiskDollars === null ? "" : ` | Risk: ${formatMoney(shadowNotionalRiskDollars)}`}
              {shadowMarkPrice === null ? "" : ` | Mark: ${formatMoney(shadowMarkPrice)}`}
            </div>
            <div className="subtle">
              Entry time: <span className="code">{formatTimestamp(shadowEntryTime)}</span>
              {shadowStrategyAccountingVersion === null ? "" : ` | Accounting v${formatNumber(shadowStrategyAccountingVersion)}`}
            </div>
            {shadowEquitySparklinePoints ? (
              <div style={{ marginTop: "0.45rem" }}>
                <svg
                  viewBox="0 0 220 56"
                  role="img"
                  aria-label="Shadow strategy equity curve"
                  style={{ width: "100%", height: "56px" }}
                >
                  <polyline fill="none" stroke="currentColor" strokeWidth="2" points={shadowEquitySparklinePoints} />
                </svg>
                <div className="subtle">
                  Equity curve ({formatNumber(shadowEquitySeriesTail.length)} points)
                  {shadowEquityDelta === null ? "" : ` | Delta: ${formatMoneyDelta(shadowEquityDelta)}`}
                </div>
              </div>
            ) : (
              <div className="subtle">Equity curve: --</div>
            )}
            <div className="subtle" style={{ marginTop: "0.45rem" }}>
              <strong>Theoretical overlay</strong>
            </div>
            <div className="subtle">
              Theoretical value: {formatMoney(shadowTheoreticalValueDollars)}
            </div>
            <div className="subtle">
              Theoretical unrealized EV: {formatMoney(shadowTheoreticalUnrealizedEvDollars)}
            </div>
            <div className="subtle">
              Theoretical drawdown:{" "}
              {shadowTheoreticalDrawdownPct === null ? "--" : `${formatNumber(shadowTheoreticalDrawdownPct, 2)}%`}
            </div>
            <div className="subtle">
              Expected EV / Allocator risk: {formatMoney(shadowExpectedValueDollars)} / {formatMoney(shadowAllocatorTotalRiskDollars)}
            </div>
            <div className="subtle">
              Allocator selected rows: {formatNumber(shadowAllocatorSelectedRows)}
              {shadowBankrollStartDollars === null ? "" : ` | Start: ${formatMoney(shadowBankrollStartDollars)}`}
            </div>
            <div className="subtle">
              Shadow lane:{" "}
              <span className={`status-pill ${shadowBankrollEnabled === false ? "bad" : "ok"}`}>
                {shadowBankrollEnabled === false ? "disabled" : "enabled"}
              </span>
              {shadowBankrollStatus ? <span className="code"> {shadowBankrollStatus}</span> : null}
            </div>
          </article>

          <article className="card card-emphasis card-alert">
            <h2>Operational Alerts</h2>
            {operationalAlerts.length === 0 ? (
              <div className="subtle">No critical operator alerts.</div>
            ) : (
              <ul className="plain-list alert-list">
                {operationalAlerts.slice(0, 3).map((alert) => (
                  <li key={alert.title}>
                    <span className={`status-pill ${alert.tone}`}>{alert.title}</span>
                    <span className="subtle">{alert.detail}</span>
                  </li>
                ))}
              </ul>
            )}
          </article>

          <article className="card card-emphasis">
            <h2>What Changed Since Last Run?</h2>
            <div className="delta-row">
              <span>EV</span>
              <span className="code">{formatMoneyDelta(deltaExpectedValue)}</span>
            </div>
            <div className="delta-row">
              <span>Risk</span>
              <span className="code">{formatMoneyDelta(deltaRisk)}</span>
            </div>
            <div className="delta-row">
              <span>Tradable Rows</span>
              <span className="code">
                {deltaTradable === null ? "--" : `${deltaTradable > 0 ? "+" : ""}${formatNumber(deltaTradable)}`}
              </span>
            </div>
            <div className="subtle">Live state: {liveStateDelta}</div>
          </article>
        </section>

        <section className="card">
          <h2>Freshness Status</h2>
          <div className="subtle">{freshnessStatusLabel}</div>
          <div className="freshness-grid">
            {freshness.map((item) => (
              <article key={item.label} className={`freshness-item ${item.tone}`}>
                <div className="freshness-head">
                  <span>{item.label}</span>
                  <span className={`status-pill ${item.tone}`}>{item.severity}</span>
                </div>
                <div className="freshness-age">{formatAge(item.ageSeconds)}</div>
                <div className="subtle code">{item.timestamp ? formatTimestamp(item.timestamp) : "payload-derived"}</div>
                <div className="subtle">{item.note}</div>
                {item.label === "Scorecard" && item.tone !== "ok" ? (
                  <div className="subtle">Action: refresh scorecards before trusting realized-vs-expected comparisons.</div>
                ) : null}
                {item.label === "Balance" && item.tone === "bad" ? (
                  <div className="subtle">Action: refresh balance heartbeat before enabling any live order path.</div>
                ) : null}
              </article>
            ))}
          </div>
        </section>

        <section className="panel-grid">
          <article className="card">
            <h2>Why No Live Order?</h2>
            {modeBlocker ? (
              <>
                <div className={`status-pill ${modeBlocker.tone}`}>Mode blocker: {modeBlocker.title}</div>
                <p className="detail-text">{modeBlocker.detail}</p>
              </>
            ) : null}
            <div className={`status-pill ${economicBlockers.primary.tone}`}>
              Economic blocker: {economicBlockers.primary.title}
            </div>
            <p className="detail-text">{economicBlockers.primary.detail}</p>
            <div className="subtle">
              Category: <span className="code">{economicBlockers.primary.category}</span>
            </div>
            <div className="subtle">
              Recommended action: <span>{economicBlockers.primary.action}</span>
            </div>
            {economicBlockers.secondary.length > 0 ? (
              <>
                <h3>Secondary Blockers</h3>
                <ul className="plain-list">
                  {economicBlockers.secondary.map((blocker) => (
                    <li key={`${blocker.category}-${blocker.title}`}>
                      <span className={`status-pill ${blocker.tone}`}>{blocker.category}</span>
                      <span>{blocker.title}</span>
                    </li>
                  ))}
                </ul>
              </>
            ) : null}
          </article>

          <article className="card">
            <h2>Climate Opportunity Board</h2>
            <div className="mini-metric-grid">
              <div>
                <div className="mini-label">Modeled Positive (Current Run)</div>
                <div className="mini-value">{formatNumber(modeledPositiveRowsCurrentRun)}</div>
              </div>
              <div>
                <div className="mini-label">Priced Watch-Only (24h Obs)</div>
                <div className="mini-value">{formatNumber(totalWatchOnly)}</div>
              </div>
              <div>
                <div className="mini-label">Tradable Observations (24h)</div>
                <div className="mini-value">{formatNumber(totalOrderable)}</div>
              </div>
              <div>
                <div className="mini-label">Dead / Endpoint Families (24h)</div>
                <div className="mini-value">{formatNumber(deadEndpointFamilyCount)}</div>
              </div>
            </div>
            <div className="subtle">
              Daily weather regime: <span className="code">{overnight?.daily_weather_market_availability_regime ?? "--"}</span>
            </div>
            <div className="subtle">
              Observations sampled (24h): <span className="code">{formatNumber(totalObservations)}</span>
            </div>
            <div className="table-scroll compact-table">
              <table>
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Tradable</th>
                    <th>Watch-Only</th>
                    <th>Observed</th>
                  </tr>
                </thead>
                <tbody>
                  {climateRollup.length === 0 ? (
                    <tr>
                      <td colSpan={4}>No climate family activity available.</td>
                    </tr>
                  ) : (
                    climateRollup.slice(0, 4).map((row) => (
                      <tr key={row.family}>
                        <td>{row.family}</td>
                        <td>{formatNumber(row.orderable)}</td>
                        <td>{formatNumber(row.watchOnly)}</td>
                        <td>{formatNumber(row.observations)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </article>

          <article className="card">
            <h2>Pilot Funnel</h2>
            <div className="funnel-list">
              {pilotFunnel.map((step) => {
                const ratio =
                  funnelBase && typeof step.value === "number" && step.value >= 0
                    ? Math.max(0, Math.min(100, (step.value / funnelBase) * 100))
                    : 0;
                return (
                  <div key={step.label} className="funnel-row">
                    <div className="funnel-head">
                      <span>{step.label}</span>
                      <span className="code">{formatNumber(step.value)}</span>
                    </div>
                    <div className="funnel-track">
                      <div className="funnel-fill" style={{ width: `${ratio}%` }} />
                    </div>
                  </div>
                );
              })}
            </div>
            <div className="attempt-context">
              <div className="subtle">
                Dominant attempt blocker: <span className="code">{dominantAttemptBlocker}</span>
              </div>
              <div className="subtle">
                Would attempt live if enabled:{" "}
                <span className={`status-pill ${wouldAttemptLiveIfEnabled ? "ok" : "warn"}`}>
                  {wouldAttemptLiveIfEnabled ? "yes" : "no"}
                </span>
              </div>
              <div className="subtle">
                Selected ticker: <span className="code">{pilotSelectedTicker ?? "--"}</span>
              </div>
            </div>
            <div className="mini-metric-grid mini-metric-grid-tight">
              <div>
                <div className="mini-label">Expected EV</div>
                <div className="mini-value">{formatMoney(pilotExpectedValue)}</div>
              </div>
              <div>
                <div className="mini-label">Expected Risk</div>
                <div className="mini-value">{formatMoney(pilotTotalRisk)}</div>
              </div>
              <div>
                <div className="mini-label">Realized PnL</div>
                <div className="mini-value">{formatMoney(pilotRealizedPnl)}</div>
              </div>
              <div>
                <div className="mini-label">Expected vs Realized</div>
                <div className="mini-value">{formatMoneyDelta(expectedVsRealizedDelta)}</div>
              </div>
            </div>
          </article>
        </section>

        <section className="panel-grid panel-grid-two">
          <section className="table-shell">
            <h2>Where Market Is Alive</h2>
            <div className="summary-inline">
              <span className="subtle">
                Top family by routed EV: <span className="code">{topFamilyByRoutedEv}</span>
              </span>
              <span className="subtle">
                Top family by minutes orderable: <span className="code">{topFamilyByMinutesOrderable}</span>
              </span>
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>State</th>
                    <th>Orderable Obs 24h</th>
                    <th>Non-Endpoint Obs 24h</th>
                    <th>Wakeups 24h</th>
                    <th>Tradable Rows Run</th>
                  </tr>
                </thead>
                <tbody>
                  {whereMarketIsAlive.length === 0 ? (
                    <tr>
                      <td colSpan={6}>No family activity rows available.</td>
                    </tr>
                  ) : (
                    whereMarketIsAlive.map((row) => (
                      <tr key={`alive-${row.family}`}>
                        <td>{row.family}</td>
                        <td>
                          <span className={`status-pill ${stateTone(row.state)}`}>{row.state}</span>
                        </td>
                        <td>{formatNumber(row.orderableObservations24h)}</td>
                        <td>{formatNumber(row.nonEndpointObservations24h)}</td>
                        <td>{formatNumber(row.wakeups24h)}</td>
                        <td>{formatNumber(row.tradableRowsCurrentRun)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="table-shell">
            <h2>Top Strip Summaries</h2>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Strip Id</th>
                    <th>City</th>
                    <th>Date</th>
                    <th>State</th>
                    <th>Top Modeled</th>
                    <th>Top Priced</th>
                    <th>Top Priced EV</th>
                    <th>Non-Endpoint Quotes</th>
                    <th>Tradable Buckets</th>
                    <th>Wakeups 24h</th>
                    <th>Minutes Orderable 24h</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {topStripSummaries.length === 0 ? (
                    <tr>
                      <td colSpan={12}>No strip summaries available.</td>
                    </tr>
                  ) : (
                    topStripSummaries.map((strip) => (
                      <tr key={strip.stripId}>
                        <td className="code">{strip.stripId}</td>
                        <td>{strip.city ?? "--"}</td>
                        <td className="code">{strip.date ?? "--"}</td>
                        <td>
                          <span className={`status-pill ${stateTone(strip.currentState)}`}>{strip.currentState}</span>
                        </td>
                        <td className="code">{strip.topModeledBucket}</td>
                        <td className="code">{strip.topPricedBucket}</td>
                        <td>{formatMoney(strip.topPricedEv)}</td>
                        <td>{formatNumber(strip.stripNonEndpointQuoteCount)}</td>
                        <td>{formatNumber(strip.stripTradableBucketCount)}</td>
                        <td>{formatNumber(strip.wakeups24h)}</td>
                        <td>{strip.minutesOrderable24h === null ? "--" : formatNumber(strip.minutesOrderable24h, 1)}</td>
                        <td>
                          <span
                            className={`status-pill ${
                              strip.recommendedAction === "pilot"
                                ? "ok"
                                : strip.recommendedAction === "watch"
                                  ? "warn"
                                  : "bad"
                            }`}
                          >
                            {strip.recommendedAction}
                          </span>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </section>

        <section className="panel-grid panel-grid-two">
          <section className="table-shell">
            <h2>Paper-Live Fill Quality (Family)</h2>
            <div className="summary-inline">
              <span className={`status-pill ${paperLiveLowSample ? "warn" : "ok"}`}>
                {paperLiveLowSample ? paperLiveLowSampleLabel : "Sample size: sufficient"}
              </span>
              <span className="subtle">
                Open-position quality: 10s/60s/300s ={" "}
                <span className="code">
                  {formatMoneyDelta(paperLiveOpenMarkout10s)} / {formatMoneyDelta(paperLiveOpenMarkout60s)} /{" "}
                  {formatMoneyDelta(paperLiveOpenMarkout300s)}
                </span>
              </span>
              <span className="subtle">
                Settled realized quality:{" "}
                <span className="code">
                  Realized {formatMoneyDelta(paperLiveSettledRealizedPnlDisplay)} | Delta{" "}
                  {formatMoneyDelta(paperLiveSettledExpectedVsRealizedDisplay)}
                </span>
              </span>
            </div>
            <div className="summary-inline">
              <span className="subtle">
                Open positions: <span className="code">{formatNumber(paperLiveOpenCount)}</span>
              </span>
              <span className="subtle">
                Settled positions: <span className="code">{formatNumber(paperLiveSettledCount)}</span>
              </span>
              <span className="subtle">
                {paperLiveSettledCount === 0
                  ? "No settled rows yet; expected-vs-realized deltas are provisional."
                  : `Settled fill-time mean: ${paperLiveSettledFillTime === null ? "--" : `${formatNumber(
                      paperLiveSettledFillTime,
                      1
                    )}s`}`}
              </span>
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Attempts</th>
                    <th>Fills</th>
                    <th>Fill Rate</th>
                    <th>Fill Time Mean</th>
                    <th>Markout 10s</th>
                    <th>Markout 60s</th>
                    <th>Markout 300s</th>
                    <th>Realized Settle PnL</th>
                    <th>Expected vs Realized</th>
                    <th>Open</th>
                    <th>Settled</th>
                  </tr>
                </thead>
                <tbody>
                  {paperLiveFamilyScorecards.length === 0 ? (
                    <tr>
                      <td colSpan={12}>No paper-live family scorecards available yet.</td>
                    </tr>
                  ) : (
                    paperLiveFamilyScorecards.map((row) => (
                      <tr key={row.scorecardKey}>
                        <td>{row.family}</td>
                        <td>{formatNumber(row.attempts)}</td>
                        <td>{formatNumber(row.fills)}</td>
                        <td>{formatPercent(row.fillRate)}</td>
                        <td>{row.fillTimeMeanSeconds === null ? "--" : `${formatNumber(row.fillTimeMeanSeconds, 1)}s`}</td>
                        <td>{formatMoneyDelta(row.markout10sMeanDollars)}</td>
                        <td>{formatMoneyDelta(row.markout60sMeanDollars)}</td>
                        <td>{formatMoneyDelta(row.markout300sMeanDollars)}</td>
                        <td>{row.closeSettlementCount > 0 ? formatMoneyDelta(row.realizedSettlementPnlDollars) : "--"}</td>
                        <td>{row.closeSettlementCount > 0 ? formatMoneyDelta(row.expectedVsRealizedDeltaDollars) : "--"}</td>
                        <td>{formatNumber(row.openPositionCount)}</td>
                        <td>{formatNumber(row.closeSettlementCount)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="table-shell">
            <h2>Paper-Live Fill Quality (Ticker)</h2>
            <div className="summary-inline">
              <span className="subtle">
                Latest selected ticker badge marks the current pilot context from the latest overnight run.
              </span>
            </div>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Ticker</th>
                    <th>Family</th>
                    <th>Attempts</th>
                    <th>Fills</th>
                    <th>Fill Rate</th>
                    <th>Markout 10s</th>
                    <th>Markout 60s</th>
                    <th>Markout 300s</th>
                    <th>Expected vs Realized</th>
                  </tr>
                </thead>
                <tbody>
                  {paperLiveTickerScorecards.length === 0 ? (
                    <tr>
                      <td colSpan={9}>No paper-live ticker scorecards available yet.</td>
                    </tr>
                  ) : (
                    paperLiveTickerScorecards.map((row) => {
                      const isLatestSelected = latestSelectedTickerSet.has(row.ticker.toLowerCase());
                      return (
                        <tr key={row.scorecardKey}>
                          <td className="code">
                            {row.ticker}
                            {isLatestSelected ? (
                              <span style={{ marginLeft: "0.45rem" }} className="status-pill ok">
                                latest selected
                              </span>
                            ) : null}
                          </td>
                          <td>{row.family}</td>
                          <td>{formatNumber(row.attempts)}</td>
                          <td>{formatNumber(row.fills)}</td>
                          <td>{formatPercent(row.fillRate)}</td>
                          <td>{formatMoneyDelta(row.markout10sMeanDollars)}</td>
                          <td>{formatMoneyDelta(row.markout60sMeanDollars)}</td>
                          <td>{formatMoneyDelta(row.markout300sMeanDollars)}</td>
                          <td>{row.closeSettlementCount > 0 ? formatMoneyDelta(row.expectedVsRealizedDeltaDollars) : "--"}</td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </section>

        <section className="panel-grid panel-grid-two">
          <section className="table-shell">
            <h2>Top Tradable Candidates</h2>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Tradable</th>
                    <th>Watch-Only</th>
                    <th>Observed</th>
                    <th>Event Types</th>
                  </tr>
                </thead>
                <tbody>
                  {topTradableFamilies.length === 0 ? (
                    <tr>
                      <td colSpan={5}>No tradable families detected in the last 24h.</td>
                    </tr>
                  ) : (
                    topTradableFamilies.map((row) => (
                      <tr key={`tradable-${row.family}`}>
                        <td>{row.family}</td>
                        <td>{formatNumber(row.orderable)}</td>
                        <td>{formatNumber(row.watchOnly)}</td>
                        <td>{formatNumber(row.observations)}</td>
                        <td>{formatNumber(row.eventTypes)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="table-shell">
            <h2>Top Watch-Only Candidates</h2>
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Family</th>
                    <th>Watch-Only</th>
                    <th>Tradable</th>
                    <th>Observed</th>
                    <th>Event Types</th>
                  </tr>
                </thead>
                <tbody>
                  {topWatchOnlyFamilies.length === 0 ? (
                    <tr>
                      <td colSpan={5}>No watch-only families detected in the last 24h.</td>
                    </tr>
                  ) : (
                    topWatchOnlyFamilies.map((row) => (
                      <tr key={`watch-only-${row.family}`}>
                        <td>{row.family}</td>
                        <td>{formatNumber(row.watchOnly)}</td>
                        <td>{formatNumber(row.orderable)}</td>
                        <td>{formatNumber(row.observations)}</td>
                        <td>{formatNumber(row.eventTypes)}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </section>

        <section className="card-grid">
          <article className="card">
            <h2>Frontier Latest Status</h2>
            <div className="metric metric-text">{frontierLatest?.status ?? "--"}</div>
            <div className="subtle">
              Trusted / Untrusted: {formatNumber(frontierLatest?.trusted_bucket_count)} / {formatNumber(frontierLatest?.untrusted_bucket_count)}
            </div>
          </article>

          <article className="card">
            <h2>Frontier Trend (Last {frontierWindow.length})</h2>
            <div className="metric">{formatNumber(frontierWindowInsufficient)}</div>
            <div className="subtle">insufficient_data statuses in recent window</div>
          </article>

          <article className="card">
            <h2>Submitted / Filled (Window)</h2>
            <div className="metric">
              {formatNumber(frontierWindowSubmitted)} / {formatNumber(frontierWindowFilled)}
            </div>
            <div className="subtle">
              Fill rate: {frontierFillRate === null ? "--" : `${formatNumber(frontierFillRate, 1)}%`}
            </div>
          </article>

          <article className="card">
            <h2>Frontier Artifact Age</h2>
            <div className="metric">{frontierArtifactAgeSeconds === null ? "--" : formatAge(frontierArtifactAgeSeconds)}</div>
            <div className={`subtle tone-${frontierTrendTone}`}>Trend tone: {frontierTrendTone}</div>
          </article>
        </section>

        <details className="table-shell collapsible-shell">
          <summary>Raw Frontier Reports (Diagnostic)</summary>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Captured At (UTC)</th>
                  <th>Status</th>
                  <th>Submitted</th>
                  <th>Filled</th>
                  <th>Trusted / Untrusted</th>
                  <th>Selection Mode</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.frontierRecent.length === 0 ? (
                  <tr>
                    <td colSpan={6}>No frontier rows available.</td>
                  </tr>
                ) : (
                  snapshot.frontierRecent.map((row) => (
                    <tr key={row.run_id}>
                      <td className="code">{row.captured_at}</td>
                      <td>{row.status}</td>
                      <td>{formatNumber(row.submitted_orders)}</td>
                      <td>{formatNumber(row.filled_orders)}</td>
                      <td>
                        {formatNumber(row.trusted_bucket_count)} / {formatNumber(row.untrusted_bucket_count)}
                      </td>
                      <td className="code">{row.frontier_selection_mode ?? "--"}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </details>

        <details className="table-shell collapsible-shell">
          <summary>Raw Climate Activity (24h, Diagnostic)</summary>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Contract Family</th>
                  <th>Event Type</th>
                  <th>Observations</th>
                  <th>Orderable</th>
                  <th>Latest Observed (UTC)</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.climateActivity24h.length === 0 ? (
                  <tr>
                    <td colSpan={5}>No climate activity rows available.</td>
                  </tr>
                ) : (
                  snapshot.climateActivity24h.map((row) => (
                    <tr key={`${row.contract_family ?? "unknown"}-${row.event_type ?? "none"}`}>
                      <td>{row.contract_family ?? "--"}</td>
                      <td>{row.event_type ?? "--"}</td>
                      <td>{formatNumber(row.observations)}</td>
                      <td>{formatNumber(row.orderable_observations)}</td>
                      <td className="code">{row.latest_observed_at_utc ?? "--"}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </details>

        <details className="table-shell collapsible-shell">
          <summary>Raw Pilot Scorecards (Diagnostic)</summary>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Captured At (UTC)</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Headline Metric</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.scorecardsRecent.length === 0 ? (
                  <tr>
                    <td colSpan={5}>No scorecards available.</td>
                  </tr>
                ) : (
                  snapshot.scorecardsRecent.map((row) => (
                    <tr key={row.scorecard_key}>
                      <td className="code">{row.captured_at}</td>
                      <td>{row.scorecard_type}</td>
                      <td>{row.status ?? "--"}</td>
                      <td>{row.headline_metric_name ?? "--"}</td>
                      <td>
                        {typeof row.headline_metric_value === "number"
                          ? `${formatNumber(row.headline_metric_value, 2)} ${row.headline_metric_unit ?? ""}`
                          : "--"}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </details>
      </div>
    </main>
  );
}
