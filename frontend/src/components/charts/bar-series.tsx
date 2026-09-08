"use client";

import {
  Bar,
  BarChart,
  Cell,
  LabelList,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useChartColors } from "@/lib/palette";
import { AXIS, ChartCanvas, ChartTooltip } from "./chart-kit";

export interface BarDatum {
  label: string;
  value: number;
  colorIndex?: number;
  meta?: string;
}

export function HorizontalBars({
  data,
  height,
  valueFormat = (v) => String(v),
  showValueLabels = true,
  barSize = 18,
  accentAll = false,
}: {
  data: BarDatum[];
  height?: number;
  valueFormat?: (v: number) => string;
  showValueLabels?: boolean;
  barSize?: number;
  accentAll?: boolean;
}) {
  const colors = useChartColors();
  const h = height ?? Math.max(120, data.length * (barSize + 16) + 24);

  return (
    <ChartCanvas height={h}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} layout="vertical" margin={{ top: 4, right: 56, bottom: 4, left: 4 }}>
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="label"
            width={148}
            {...AXIS}
            tick={{ fontSize: 12, fill: "hsl(var(--ink-muted))" }}
          />
          <Tooltip
            cursor={{ fill: "hsl(var(--surface-sunken))" }}
            content={({ active, payload }) => {
              const d = payload?.[0]?.payload as BarDatum | undefined;
              return (
                <ChartTooltip
                  active={active}
                  rows={
                    d
                      ? [
                          { label: d.label, value: valueFormat(d.value) },
                          ...(d.meta ? [{ label: "detail", value: d.meta }] : []),
                        ]
                      : []
                  }
                />
              );
            }}
          />
          <Bar dataKey="value" barSize={barSize} radius={[0, 2, 2, 0]} isAnimationActive={false}>
            {data.map((d, i) => (
              <Cell
                key={i}
                fill={
                  accentAll
                    ? colors.accent
                    : colors.series[d.colorIndex ?? i % 6] ?? colors["series-1"]
                }
              />
            ))}
            {showValueLabels && (
              <LabelList
                dataKey="value"
                position="right"
                formatter={(v: number) => valueFormat(v)}
                className="tnum"
                fill="hsl(var(--ink-faint))"
                fontSize={11}
              />
            )}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartCanvas>
  );
}

export function VerticalBars({
  data,
  height = 260,
  valueFormat = (v) => String(v),
  xTickFormat = (v) => v,
}: {
  data: BarDatum[];
  height?: number;
  valueFormat?: (v: number) => string;
  xTickFormat?: (v: string) => string;
}) {
  const colors = useChartColors();
  return (
    <ChartCanvas height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, bottom: 4, left: 4 }}>
          <XAxis dataKey="label" {...AXIS} tickFormatter={xTickFormat} interval={0} />
          <YAxis {...AXIS} width={48} tickFormatter={(v) => valueFormat(v as number)} />
          <Tooltip
            cursor={{ fill: "hsl(var(--surface-sunken))" }}
            content={({ active, payload, label }) => (
              <ChartTooltip
                active={active}
                title={String(label)}
                rows={(payload ?? []).map((p) => ({
                  label: "count",
                  value: valueFormat(p.value as number),
                }))}
              />
            )}
          />
          <Bar dataKey="value" radius={[2, 2, 0, 0]} isAnimationActive={false} maxBarSize={56}>
            {data.map((d, i) => (
              <Cell key={i} fill={colors.series[d.colorIndex ?? 0] ?? colors["series-2"]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </ChartCanvas>
  );
}
