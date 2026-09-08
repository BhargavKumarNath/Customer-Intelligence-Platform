"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

/** Shared axis + grid styling knobs so every chart reads the same way. */
export const AXIS = {
  stroke: "hsl(var(--rule-strong))",
  tickLine: false as const,
  axisLine: { stroke: "hsl(var(--rule))" },
  tick: { fontSize: 11 },
};

export const GRID = {
  stroke: "hsl(var(--rule))",
  strokeDasharray: "0",
  vertical: false as const,
};

export const CHART_MARGIN = { top: 8, right: 12, bottom: 4, left: 4 };

interface TooltipRow {
  label: string;
  value: ReactNode;
  swatch?: string;
}

export function ChartTooltip({
  title,
  rows,
  active,
}: {
  title?: ReactNode;
  rows: TooltipRow[];
  active?: boolean;
}) {
  if (!active) return null;
  return (
    <div className="rounded-md border border-rule bg-surface px-3 py-2 shadow-lg">
      {title != null && (
        <p className="mb-1 border-b border-rule pb-1 text-2xs uppercase tracking-wider text-ink-faint">
          {title}
        </p>
      )}
      <ul className="space-y-0.5">
        {rows.map((row, i) => (
          <li key={i} className="flex items-center justify-between gap-4 text-xs">
            <span className="flex items-center gap-1.5 text-ink-muted">
              {row.swatch && (
                <span aria-hidden className="h-2 w-2 rounded-[2px]" style={{ background: row.swatch }} />
              )}
              {row.label}
            </span>
            <span className="tnum text-ink">{row.value}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function ChartCanvas({
  height = 300,
  children,
  className,
}: {
  height?: number;
  children: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn("w-full", className)} style={{ height }}>
      {children}
    </div>
  );
}

export function Legend({ items }: { items: { label: string; color: string }[] }) {
  return (
    <ul className="flex flex-wrap gap-x-4 gap-y-1.5">
      {items.map((it) => (
        <li key={it.label} className="flex items-center gap-1.5 text-2xs text-ink-muted">
          <span aria-hidden className="h-2 w-2 rounded-[2px]" style={{ background: it.color }} />
          {it.label}
        </li>
      ))}
    </ul>
  );
}
