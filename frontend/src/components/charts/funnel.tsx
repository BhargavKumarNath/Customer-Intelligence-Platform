"use client";

import { motion, useReducedMotion } from "framer-motion";
import { integer, percent } from "@/lib/format";

export interface FunnelStage {
  label: string;
  value: number;
  note?: string;
}

/**
 * Session funnel as stacked horizontal bars. Each stage shows its width relative
 * to the first stage, plus step-to-step conversion.
 */
export function Funnel({ stages }: { stages: FunnelStage[] }) {
  const reduce = useReducedMotion();
  const top = stages[0]?.value ?? 1;

  return (
    <ol className="space-y-3">
      {stages.map((stage, i) => {
        const share = stage.value / top;
        const prev = i > 0 ? stages[i - 1]!.value : null;
        const stepConv = prev ? stage.value / prev : null;
        return (
          <li key={stage.label}>
            <div className="flex items-baseline justify-between text-sm">
              <span className="text-ink">{stage.label}</span>
              <span className="tnum text-ink-muted">
                {integer(stage.value)}
                <span className="ml-2 text-2xs text-ink-faint">{percent(share, 1)} of entry</span>
              </span>
            </div>
            <div className="mt-1.5 h-8 w-full overflow-hidden rounded-sm bg-surface-sunken">
              <motion.div
                className="h-full rounded-sm"
                style={{
                  background: `linear-gradient(90deg, hsl(var(--series-2)), hsl(var(--series-2) / 0.72))`,
                }}
                initial={{ width: reduce ? `${share * 100}%` : 0 }}
                whileInView={{ width: `${share * 100}%` }}
                viewport={{ once: true }}
                transition={{ duration: reduce ? 0 : 0.7, ease: [0.22, 1, 0.36, 1], delay: i * 0.06 }}
              />
            </div>
            {stepConv != null && (
              <p className="mt-1 text-2xs text-ink-faint">
                {percent(stepConv, 1)} continue from {stages[i - 1]!.label.toLowerCase()}
                {stage.note ? ` · ${stage.note}` : ""}
              </p>
            )}
          </li>
        );
      })}
    </ol>
  );
}
