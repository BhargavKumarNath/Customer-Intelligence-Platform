"use client";

import { Bar, BarChart, Cell, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import type { HistogramBin } from "@/lib/client-data";
import { useChartColors } from "@/lib/palette";
import { AXIS, ChartCanvas, ChartTooltip } from "./chart-kit";

export function Histogram({
  bins,
  height = 260,
  xFormat = (v) => v.toFixed(2),
  markers = [],
  highlightFrom,
}: {
  bins: HistogramBin[];
  height?: number;
  xFormat?: (v: number) => string;
  markers?: { value: number; label: string }[];
  highlightFrom?: number;
}) {
  const colors = useChartColors();
  const data = bins.map((b) => ({ ...b, mid: (b.x0 + b.x1) / 2 }));

  return (
    <ChartCanvas height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 10, bottom: 4, left: 4 }} barCategoryGap={1}>
          <XAxis
            dataKey="x0"
            {...AXIS}
            tickFormatter={(v) => xFormat(v as number)}
            interval="preserveStartEnd"
            minTickGap={40}
          />
          <YAxis
            {...AXIS}
            width={48}
            tickFormatter={(v) => new Intl.NumberFormat("en-US", { notation: "compact" }).format(v as number)}
          />
          <Tooltip
            cursor={{ fill: "hsl(var(--surface-sunken))" }}
            content={({ active, payload }) => {
              const d = payload?.[0]?.payload as (HistogramBin & { mid: number }) | undefined;
              return (
                <ChartTooltip
                  active={active}
                  title={d ? `${xFormat(d.x0)} to ${xFormat(d.x1)}` : ""}
                  rows={d ? [{ label: "users", value: new Intl.NumberFormat("en-US").format(d.count) }] : []}
                />
              );
            }}
          />
          {markers.map((m) => (
            <ReferenceLine
              key={m.label}
              x={m.value}
              stroke="hsl(var(--accent))"
              strokeDasharray="4 3"
              label={{ value: m.label, position: "top", fontSize: 10, fill: "hsl(var(--accent-ink))" }}
            />
          ))}
          <Bar dataKey="count" isAnimationActive={false}>
            {data.map((d, i) => (
              <Cell
                key={i}
                fill={
                  highlightFrom != null && d.x0 >= highlightFrom
                    ? colors.accent
                    : colors["series-6"]
                }
                fillOpacity={highlightFrom != null && d.x0 >= highlightFrom ? 0.9 : 0.55}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartCanvas>
  );
}

/** Two overlaid normal-ish curves for the A/B page (conversion-rate sampling distributions). */
export function DensityPair({
  control,
  treatment,
  height = 260,
  xFormat = (v) => `${(v * 100).toFixed(1)}%`,
}: {
  control: { x: number; y: number }[];
  treatment: { x: number; y: number }[];
  height?: number;
  xFormat?: (v: number) => string;
}) {
  const colors = useChartColors();
  const merged = control.map((c, i) => ({
    x: c.x,
    control: c.y,
    treatment: treatment[i]?.y ?? 0,
  }));

  return (
    <ChartCanvas height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={merged} margin={{ top: 8, right: 10, bottom: 4, left: 4 }} barCategoryGap={0} barGap={0}>
          <XAxis dataKey="x" {...AXIS} tickFormatter={(v) => xFormat(v as number)} minTickGap={44} />
          <YAxis {...AXIS} width={10} tick={false} axisLine={false} />
          <Tooltip
            cursor={{ fill: "hsl(var(--surface-sunken))" }}
            content={({ active, payload, label }) => (
              <ChartTooltip
                active={active}
                title={xFormat(label as number)}
                rows={[
                  { label: "control density", value: (payload?.[0]?.value as number)?.toFixed(1) ?? "0", swatch: colors["series-6"] },
                  { label: "treatment density", value: (payload?.[1]?.value as number)?.toFixed(1) ?? "0", swatch: colors.accent },
                ]}
              />
            )}
          />
          <Bar dataKey="control" fill={colors["series-6"]} fillOpacity={0.4} isAnimationActive={false} />
          <Bar dataKey="treatment" fill={colors.accent} fillOpacity={0.45} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </ChartCanvas>
  );
}
