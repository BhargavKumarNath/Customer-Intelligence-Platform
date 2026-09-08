"use client";

import { useMemo, useState } from "react";
import type { WeeklyRetentionRow } from "@/lib/types";
import { percent, shortDate } from "@/lib/format";
import { cn } from "@/lib/cn";

/**
 * Retention cohort grid. Hand-built rather than a chart library: it is a table of
 * coloured cells, and a real <table> keeps it legible to screen readers and
 * copy-paste. Colour encodes retention rate on a single-hue ramp.
 */
export function CohortHeatmap({ rows }: { rows: WeeklyRetentionRow[] }) {
  const [hover, setHover] = useState<{ cohort: string; week: number } | null>(null);

  const { cohorts, weeks, grid, sizes, maxWeek } = useMemo(() => {
    const cohortSet = Array.from(new Set(rows.map((r) => r.cohort_week))).sort();
    const maxW = rows.reduce((m, r) => Math.max(m, r.weeks_since_first), 0);
    const weekList = Array.from({ length: maxW + 1 }, (_, i) => i);
    const g = new Map<string, number>();
    const sz = new Map<string, number>();
    for (const r of rows) {
      g.set(`${r.cohort_week}:${r.weeks_since_first}`, r.retention_rate);
      if (r.weeks_since_first === 0) sz.set(r.cohort_week, r.cohort_size);
    }
    return { cohorts: cohortSet, weeks: weekList, grid: g, sizes: sz, maxWeek: maxW };
  }, [rows]);

  const rampFor = (rate: number) => {
    // 0 -> paper, 1 -> deep teal-slate. Skip week 0 which is always 1.0.
    const t = Math.min(1, Math.max(0, rate));
    const light = 96 - t * 58;
    const sat = 12 + t * 26;
    return `hsl(190 ${sat}% ${light}%)`;
  };

  return (
    <div className="overflow-x-auto">
      <table className="border-separate border-spacing-1 text-2xs">
        <thead>
          <tr>
            <th scope="col" className="px-2 py-1 text-left font-medium text-ink-faint">
              Cohort start
            </th>
            <th scope="col" className="px-2 py-1 text-right font-medium text-ink-faint">
              Users
            </th>
            {weeks.map((w) => (
              <th key={w} scope="col" className="px-1 py-1 text-center font-medium text-ink-faint">
                w{w}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {cohorts.map((cohort) => (
            <tr key={cohort}>
              <th scope="row" className="whitespace-nowrap px-2 py-1 text-left font-normal text-ink-muted">
                {shortDate(cohort)}
              </th>
              <td className="tnum px-2 py-1 text-right text-ink-faint">
                {new Intl.NumberFormat("en-US").format(sizes.get(cohort) ?? 0)}
              </td>
              {weeks.map((w) => {
                const rate = grid.get(`${cohort}:${w}`);
                const isHover = hover?.cohort === cohort && hover?.week === w;
                if (rate == null) {
                  return <td key={w} className="h-7 w-10 rounded-[3px] bg-surface-sunken/40" aria-hidden />;
                }
                return (
                  <td
                    key={w}
                    onMouseEnter={() => setHover({ cohort, week: w })}
                    onMouseLeave={() => setHover(null)}
                    className={cn(
                      "tnum h-7 w-10 rounded-[3px] text-center align-middle transition-transform",
                      isHover && "scale-[1.12] ring-1 ring-ink/30",
                    )}
                    style={{
                      background: rampFor(rate),
                      color: rate > 0.55 ? "hsl(40 30% 96%)" : "hsl(30 9% 22%)",
                    }}
                    title={`${shortDate(cohort)} cohort, week ${w}: ${percent(rate)}`}
                  >
                    {percent(rate, 0)}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-3 flex items-center gap-2 text-2xs text-ink-faint">
        <span>Lower</span>
        <span className="flex">
          {Array.from({ length: 9 }, (_, i) => (
            <span key={i} className="h-2.5 w-4" style={{ background: rampFor(i / 8) }} />
          ))}
        </span>
        <span>Higher retention</span>
        <span className="ml-2">Grid runs to week {maxWeek}.</span>
      </p>
    </div>
  );
}
