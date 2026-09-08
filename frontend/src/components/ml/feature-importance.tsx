"use client";

import { HorizontalBars } from "@/components/charts/bar-series";
import { percent } from "@/lib/format";

const LABELS: Record<string, string> = {
  oct_events: "Total October events",
  active_span_days: "Active span in days",
  oct_carts: "Cart adds",
  oct_views: "Product views",
  recency_oct: "Days since last October event",
  oct_sessions: "Session count",
  oct_removes: "Cart removals",
};

export function FeatureImportanceChart({ gain }: { gain: Record<string, number> }) {
  const total = Object.values(gain).reduce((a, b) => a + b, 0) || 1;
  const data = Object.entries(gain)
    .sort((a, b) => b[1] - a[1])
    .map(([key, value], i) => ({
      label: LABELS[key] ?? key,
      value: value / total,
      colorIndex: i === 0 ? 0 : 5,
      meta: `${percent(value / total, 1)} of total gain`,
    }));

  return <HorizontalBars data={data} valueFormat={(v) => percent(v, 1)} barSize={20} />;
}
