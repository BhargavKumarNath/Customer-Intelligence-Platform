import "server-only";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type {
  AbGridCell,
  AffinityFile,
  ArtifactMeta,
  DailyKpi,
  DataManifest,
  ModelMetrics,
  SegmentSummary,
  WeeklyRetentionRow,
} from "./types";

// Small artifacts are read from disk at build time and inlined into the static
// HTML. The two big files (segments.json ~2.3 MB, propensity.json ~2.4 MB) and
// events_trimmed.parquet are never read here: they load client-side, only on the
// routes that need them (see src/lib/client-data.ts).
const DATA_DIR = join(process.cwd(), "public", "data", "current");

const cache = new Map<string, unknown>();

function read<T>(name: string): T {
  const hit = cache.get(name);
  if (hit) return hit as T;
  let raw: string;
  try {
    raw = readFileSync(join(DATA_DIR, name), "utf8");
  } catch {
    throw new Error(
      `[data] ${name} not found under public/data/current. Run \`node scripts/sync-data.mjs\` ` +
        `(it needs ../dist/data/<sha>/ from python scripts/build_static_artifacts.py).`,
    );
  }
  const parsed = JSON.parse(raw) as T;
  cache.set(name, parsed);
  return parsed;
}

export const getManifest = (): DataManifest => read<DataManifest>("manifest.json");
export const getMeta = (): ArtifactMeta => read<ArtifactMeta>("meta.json");
export const getMetrics = (): ModelMetrics => read<ModelMetrics>("metrics.json");
export const getKpis = (): DailyKpi[] => read<DailyKpi[]>("kpis.json");
export const getRetention = (): WeeklyRetentionRow[] => read<WeeklyRetentionRow[]>("retention.json");
export const getRfmSummary = (): SegmentSummary[] => read<SegmentSummary[]>("rfm_summary.json");
export const getAffinity = (): AffinityFile => read<AffinityFile>("affinity.json");
export const getAbGrid = (): AbGridCell[] => read<AbGridCell[]>("ab_grid.json");

/** Derived headline figures used across routes. Computed once, never hardcoded. */
export function getExecutiveSummary() {
  const kpis = getKpis();
  const revenue = kpis.reduce((a, k) => a + k.daily_revenue, 0);
  const orders = kpis.reduce((a, k) => a + k.purchases, 0);
  const views = kpis.reduce((a, k) => a + k.views, 0);
  const carts = kpis.reduce((a, k) => a + k.carts, 0);
  const sessions = kpis.reduce((a, k) => a + k.daily_sessions, 0);
  const events = kpis.reduce((a, k) => a + k.daily_events, 0);
  const peakDau = kpis.reduce((a, k) => Math.max(a, k.dau), 0);
  return {
    revenue,
    orders,
    views,
    carts,
    sessions,
    events,
    peakDau,
    aov: orders > 0 ? revenue / orders : 0,
    // Session-level conversion proxy: purchase events over total sessions.
    sessionConversion: sessions > 0 ? orders / sessions : 0,
    viewToCart: views > 0 ? carts / views : 0,
    cartToOrder: carts > 0 ? orders / carts : 0,
    days: kpis.length,
  };
}

/** Cohort-weighted week-0 to week-1 retention across every cohort that has a week 1. */
export function getWeekOneRetention(): number {
  const rows = getRetention();
  const week0 = new Map(rows.filter((r) => r.weeks_since_first === 0).map((r) => [r.cohort_week, r.cohort_size]));
  let base = 0;
  let kept = 0;
  for (const r of rows) {
    if (r.weeks_since_first !== 1) continue;
    const size = week0.get(r.cohort_week);
    if (size == null) continue;
    base += size;
    kept += r.active_users;
  }
  return base > 0 ? kept / base : NaN;
}
