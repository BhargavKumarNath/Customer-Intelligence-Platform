"use client";

import { useId } from "react";

/** Inline trend mark for a Figure. Pure SVG, no library. */
export function Sparkline({
  values,
  width = 120,
  height = 32,
  strokeClass = "stroke-accent",
  fillFrom = true,
}: {
  values: number[];
  width?: number;
  height?: number;
  strokeClass?: string;
  fillFrom?: boolean;
}) {
  const id = useId();
  if (values.length < 2) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const span = max - min || 1;
  const step = width / (values.length - 1);
  const pts = values.map((v, i) => [i * step, height - ((v - min) / span) * (height - 4) - 2] as const);
  const line = pts.map(([x, y], i) => `${i === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const area = `${line} L${width} ${height} L0 ${height} Z`;

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`} aria-hidden className="overflow-visible">
      {fillFrom && (
        <>
          <defs>
            <linearGradient id={`spark-${id}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="currentColor" stopOpacity={0.18} />
              <stop offset="100%" stopColor="currentColor" stopOpacity={0} />
            </linearGradient>
          </defs>
          <path d={area} fill={`url(#spark-${id})`} className={strokeClass.replace("stroke-", "text-")} />
        </>
      )}
      <path d={line} fill="none" strokeWidth={1.5} className={strokeClass} strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}
