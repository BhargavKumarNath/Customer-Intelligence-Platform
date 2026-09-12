"use client";

import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";
import { useChartColors } from "@/lib/palette";
import { AXIS, CHART_MARGIN, ChartCanvas, ChartTooltip, GRID, Legend } from "./chart-kit";

export interface ScatterPoint {
  x: number;
  y: number;
  z: number;
  group: string;
  label?: string;
}

/** RFM landscape: recency on X, spend on Y (log), point size by frequency. */
export function GroupedScatter({
  points,
  groups,
  xLabel,
  yLabel,
  xFormat = (v) => String(v),
  yFormat = (v) => String(v),
  height = 420,
  yScale = "linear",
}: {
  points: ScatterPoint[];
  groups: string[];
  xLabel: string;
  yLabel: string;
  xFormat?: (v: number) => string;
  yFormat?: (v: number) => string;
  height?: number;
  yScale?: "linear" | "log";
}) {
  const colors = useChartColors();
  const byGroup = groups.map((g, i) => ({
    group: g,
    color: colors.series[i % 6] ?? colors["series-1"],
    data: points.filter((p) => p.group === g),
  }));

  return (
    <div>
      {/* Recharts gives every individual scatter point its own role="img" with
          no name (hundreds of them here), which axe flags as missing
          accessible names. There's no meaningful per-point description for a
          scatter plot this dense, so hide the SVG internals from assistive
          tech and describe the chart as a whole instead. */}
      <div role="img" aria-label={`Scatter plot of ${yLabel} by ${xLabel}, grouped by segment`}>
        <div aria-hidden="true">
          <ChartCanvas height={height}>
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={CHART_MARGIN}>
                <CartesianGrid {...GRID} vertical />
                <XAxis
                  type="number"
                  dataKey="x"
                  name={xLabel}
                  {...AXIS}
                  tickFormatter={(v) => xFormat(v as number)}
                  label={{
                    value: xLabel,
                    position: "insideBottom",
                    offset: -2,
                    fontSize: 11,
                    fill: "hsl(var(--ink-faint))",
                  }}
                />
                <YAxis
                  type="number"
                  dataKey="y"
                  name={yLabel}
                  scale={yScale}
                  domain={yScale === "log" ? ["auto", "auto"] : undefined}
                  allowDataOverflow={yScale === "log"}
                  {...AXIS}
                  width={58}
                  tickFormatter={(v) => yFormat(v as number)}
                />
                <ZAxis type="number" dataKey="z" range={[12, 220]} />
                <Tooltip
                  cursor={{ strokeDasharray: "3 3", stroke: "hsl(var(--rule-strong))" }}
                  content={({ active, payload }) => {
                    const p = payload?.[0]?.payload as ScatterPoint | undefined;
                    return (
                      <ChartTooltip
                        active={active}
                        title={p?.group}
                        rows={
                          p
                            ? [
                                { label: xLabel, value: xFormat(p.x) },
                                { label: yLabel, value: yFormat(p.y) },
                                { label: "frequency", value: String(p.z) },
                              ]
                            : []
                        }
                      />
                    );
                  }}
                />
                {byGroup.map((g) => (
                  <Scatter
                    key={g.group}
                    name={g.group}
                    data={g.data}
                    fill={g.color}
                    fillOpacity={0.55}
                    stroke={g.color}
                    strokeOpacity={0.9}
                    isAnimationActive={false}
                  />
                ))}
              </ScatterChart>
            </ResponsiveContainer>
          </ChartCanvas>
        </div>
      </div>
      <div className="mt-3">
        <Legend items={byGroup.map((g) => ({ label: g.group, color: g.color }))} />
      </div>
    </div>
  );
}
