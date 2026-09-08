"use client";

import {
  Area,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useChartColors } from "@/lib/palette";
import { AXIS, CHART_MARGIN, ChartCanvas, ChartTooltip, GRID } from "./chart-kit";

export interface TimeSeriesDatum {
  [key: string]: string | number;
}

export interface SeriesSpec {
  key: string;
  label: string;
  kind?: "line" | "area";
  colorIndex?: number;
}

export function TimeSeries({
  data,
  xKey,
  series,
  height = 300,
  xTickFormat = (v) => String(v),
  yTickFormat = (v) => String(v),
  valueFormat = (v) => String(v),
  tooltipTitleFormat,
}: {
  data: TimeSeriesDatum[];
  xKey: string;
  series: SeriesSpec[];
  height?: number;
  xTickFormat?: (v: string | number) => string;
  yTickFormat?: (v: number) => string;
  valueFormat?: (v: number) => string;
  tooltipTitleFormat?: (v: string | number) => string;
}) {
  const colors = useChartColors();

  return (
    <ChartCanvas height={height}>
      <ResponsiveContainer width="100%" height="100%">
        <ComposedChart data={data} margin={CHART_MARGIN}>
          <defs>
            {series.map((s, i) => {
              const c = colors.series[s.colorIndex ?? i] ?? colors["series-1"];
              return (
                <linearGradient key={s.key} id={`grad-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={c} stopOpacity={0.22} />
                  <stop offset="100%" stopColor={c} stopOpacity={0.02} />
                </linearGradient>
              );
            })}
          </defs>
          <CartesianGrid {...GRID} />
          <XAxis
            dataKey={xKey}
            {...AXIS}
            tickFormatter={(v) => xTickFormat(v)}
            minTickGap={28}
          />
          <YAxis {...AXIS} width={54} tickFormatter={(v) => yTickFormat(v as number)} />
          <Tooltip
            cursor={{ stroke: "hsl(var(--rule-strong))", strokeWidth: 1 }}
            content={({ active, payload, label }) => (
              <ChartTooltip
                active={active}
                title={tooltipTitleFormat ? tooltipTitleFormat(label as string) : String(label)}
                rows={(payload ?? []).map((p) => ({
                  label: series.find((s) => s.key === p.dataKey)?.label ?? String(p.dataKey),
                  value: valueFormat(p.value as number),
                  swatch: p.stroke as string,
                }))}
              />
            )}
          />
          {series.map((s, i) => {
            const c = colors.series[s.colorIndex ?? i] ?? colors["series-1"];
            if (s.kind === "area") {
              return (
                <Area
                  key={s.key}
                  type="monotone"
                  dataKey={s.key}
                  stroke={c}
                  strokeWidth={1.75}
                  fill={`url(#grad-${s.key})`}
                  dot={false}
                  activeDot={{ r: 3, strokeWidth: 0 }}
                  isAnimationActive={false}
                />
              );
            }
            return (
              <Line
                key={s.key}
                type="monotone"
                dataKey={s.key}
                stroke={c}
                strokeWidth={1.75}
                dot={false}
                activeDot={{ r: 3, strokeWidth: 0 }}
                isAnimationActive={false}
              />
            );
          })}
        </ComposedChart>
      </ResponsiveContainer>
    </ChartCanvas>
  );
}
