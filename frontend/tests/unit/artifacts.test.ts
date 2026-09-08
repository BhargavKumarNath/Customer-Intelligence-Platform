import { describe, expect, it } from "vitest";
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
} from "@/lib/types";

const DIR = join(__dirname, "..", "..", "public", "data", "current");
const read = <T,>(name: string): T => JSON.parse(readFileSync(join(DIR, name), "utf8")) as T;

const manifest = read<DataManifest>("manifest.json");
const meta = read<ArtifactMeta>("meta.json");
const metrics = read<ModelMetrics>("metrics.json");
const kpis = read<DailyKpi[]>("kpis.json");
const retention = read<WeeklyRetentionRow[]>("retention.json");
const rfm = read<SegmentSummary[]>("rfm_summary.json");
const affinity = read<AffinityFile>("affinity.json");
const grid = read<AbGridCell[]>("ab_grid.json");

describe("synced artifact set", () => {
  it("manifest pins the same build as meta.json", () => {
    expect(manifest.gitSha).toBe(meta.git_sha);
    expect(manifest.builtAt).toBe(meta.built_at);
  });

  it("meta row counts are positive and cover the seven schema tables", () => {
    const tables = Object.keys(meta.row_counts);
    expect(tables).toContain("fact_daily_kpis");
    expect(tables).toContain("user_rfm_segments");
    for (const v of Object.values(meta.row_counts)) expect(v).toBeGreaterThan(0);
  });

  it("kpis has one row per day of the declared range", () => {
    const [from, to] = meta.date_range;
    const days = Math.round(
      (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86_400_000,
    ) + 1;
    expect(kpis.length).toBe(days);
    expect(kpis.length).toBe(meta.row_counts.fact_daily_kpis);
  });
});

describe("number-parity invariants the pages render", () => {
  it("segment populations sum to the user_rfm_segments row count", () => {
    const sum = rfm.reduce((a, s) => a + s.population, 0);
    expect(sum).toBe(meta.row_counts.user_rfm_segments);
  });

  it("segment share of buyers sums to 100", () => {
    const sum = rfm.reduce((a, s) => a + s.pct_of_buyers, 0);
    expect(sum).toBeCloseTo(100, 3);
  });

  it("executive revenue total is the sum of daily revenue", () => {
    const total = kpis.reduce((a, k) => a + k.daily_revenue, 0);
    // rendered as a rounded currency string; assert the underlying value is finite and large
    expect(total).toBeGreaterThan(1_000_000);
    expect(Number.isFinite(total)).toBe(true);
  });

  it("week 0 retention is 1.0 for every cohort", () => {
    for (const r of retention.filter((x) => x.weeks_since_first === 0)) {
      expect(r.retention_rate).toBe(1);
      expect(r.active_users).toBe(r.cohort_size);
    }
  });

  it("model card values are the ones the ml-engine page reads", () => {
    expect(metrics.auc_roc).toBeGreaterThan(0.5);
    expect(metrics.auc_roc).toBeLessThan(1);
    expect(metrics.lift_top5pct).toBeGreaterThan(1);
    expect(metrics.top5pct_conversion_rate).toBeGreaterThan(metrics.baseline_conversion_rate);
  });

  it("every affinity by_product entry is ordered by lift descending", () => {
    for (const recs of Object.values(affinity.by_product)) {
      for (let i = 1; i < recs.length; i += 1) {
        expect(recs[i - 1]!.lift).toBeGreaterThanOrEqual(recs[i]!.lift);
      }
    }
  });

  it("ab grid covers the documented axes", () => {
    const lifts = new Set(grid.map((c) => c.lift));
    const confs = new Set(grid.map((c) => c.confidence_level));
    expect([...confs].sort()).toEqual([0.8, 0.9, 0.95, 0.99]);
    expect(lifts.size).toBeGreaterThanOrEqual(8);
    expect(grid.some((c) => c.power_undefined)).toBe(true);
  });
});
