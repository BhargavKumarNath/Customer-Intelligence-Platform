"use client";

import { useMemo, useState } from "react";
import type { DailyKpi } from "@/lib/types";
import { TimeSeries } from "@/components/charts/time-series";
import { HorizontalBars } from "@/components/charts/bar-series";
import { SegmentedControl } from "@/components/ui/controls";
import { compactNumber, currency, integer, monthDay } from "@/lib/format";

type MetricKey = "revenue" | "dau" | "sessions" | "orders";
const METRIC_KEYS: MetricKey[] = ["revenue", "dau", "sessions", "orders"];

const METRICS: Record<
  MetricKey,
  { label: string; pick: (k: DailyKpi) => number; fmt: (v: number) => string; kind: "area" | "line" }
> = {
  revenue: { label: "Revenue", pick: (k) => k.daily_revenue, fmt: (v) => currency(v, { compact: true }), kind: "area" },
  dau: { label: "Active users", pick: (k) => k.dau, fmt: (v) => compactNumber(v), kind: "line" },
  sessions: { label: "Sessions", pick: (k) => k.daily_sessions, fmt: (v) => compactNumber(v), kind: "line" },
  orders: { label: "Orders", pick: (k) => k.purchases, fmt: (v) => integer(v), kind: "line" },
};

export function KpiTrends({ kpis }: { kpis: DailyKpi[] }) {
  const [metric, setMetric] = useState<MetricKey>("revenue");
  const spec = METRICS[metric];
  const data = useMemo(
    () => kpis.map((k) => ({ date: k.date, value: spec.pick(k) })),
    [kpis, spec],
  );
  const peak = useMemo(() => data.reduce((m, d) => (d.value > m.value ? d : m), data[0]!), [data]);

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <SegmentedControl
          ariaLabel="Choose a metric"
          value={metric}
          onChange={setMetric}
          options={METRIC_KEYS.map((k) => ({ value: k, label: METRICS[k].label }))}
        />
        <p className="tnum text-2xs text-ink-faint">
          peak {spec.fmt(peak.value)} on {monthDay(peak.date)}
        </p>
      </div>
      <TimeSeries
        data={data}
        xKey="date"
        series={[{ key: "value", label: spec.label, kind: spec.kind, colorIndex: metric === "revenue" ? 0 : 1 }]}
        xTickFormat={(v) => monthDay(String(v))}
        yTickFormat={spec.fmt}
        valueFormat={(v) => (metric === "revenue" ? currency(v) : integer(v))}
        tooltipTitleFormat={(v) => monthDay(String(v))}
        height={320}
      />
    </div>
  );
}

const WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

export function WeekdayRhythm({ kpis }: { kpis: DailyKpi[] }) {
  const rows = useMemo(() => {
    const sum = new Array(7).fill(0);
    const count = new Array(7).fill(0);
    for (const k of kpis) {
      const day = new Date(`${k.date}T00:00:00Z`).getUTCDay();
      sum[day] += k.daily_revenue;
      count[day] += 1;
    }
    return WEEKDAYS.map((label, i) => ({
      label,
      value: count[i] ? sum[i] / count[i] : 0,
      colorIndex: 2,
    }));
  }, [kpis]);

  return <HorizontalBars data={rows} valueFormat={(v) => currency(v)} barSize={16} />;
}
