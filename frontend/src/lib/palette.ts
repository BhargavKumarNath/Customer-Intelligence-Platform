"use client";

import { useEffect, useState } from "react";

/**
 * Recharts needs concrete colour strings. The design tokens live as HSL triples
 * in CSS variables (globals.css), so this reads them off the document and keeps
 * them in sync with the active theme. Fallbacks match the light palette for SSR.
 */

const TOKENS = [
  "series-1",
  "series-2",
  "series-3",
  "series-4",
  "series-5",
  "series-6",
  "accent",
  "positive",
  "caution",
  "critical",
  "ink",
  "ink-muted",
  "ink-faint",
  "rule",
  "rule-strong",
  "surface",
] as const;

type Token = (typeof TOKENS)[number];

const FALLBACK: Record<Token, string> = {
  "series-1": "hsl(18 72% 45%)",
  "series-2": "hsl(190 34% 32%)",
  "series-3": "hsl(38 58% 46%)",
  "series-4": "hsl(348 30% 47%)",
  "series-5": "hsl(112 20% 40%)",
  "series-6": "hsl(232 26% 50%)",
  accent: "hsl(18 72% 40%)",
  positive: "hsl(145 30% 32%)",
  caution: "hsl(36 60% 40%)",
  critical: "hsl(8 55% 44%)",
  ink: "hsl(30 8% 12%)",
  "ink-muted": "hsl(35 6% 40%)",
  "ink-faint": "hsl(35 8% 55%)",
  rule: "hsl(38 18% 84%)",
  "rule-strong": "hsl(34 14% 72%)",
  surface: "hsl(40 40% 99%)",
};

export type ChartColors = Record<Token, string> & { series: string[] };

function readColors(): ChartColors {
  if (typeof window === "undefined") {
    return { ...FALLBACK, series: SERIES_KEYS.map((k) => FALLBACK[k]) };
  }
  const style = getComputedStyle(document.documentElement);
  const out = {} as Record<Token, string>;
  for (const token of TOKENS) {
    const raw = style.getPropertyValue(`--${token}`).trim();
    out[token] = raw ? `hsl(${raw})` : FALLBACK[token];
  }
  return { ...out, series: SERIES_KEYS.map((k) => out[k]) };
}

const SERIES_KEYS: Token[] = ["series-1", "series-2", "series-3", "series-4", "series-5", "series-6"];

export function useChartColors(): ChartColors {
  const [colors, setColors] = useState<ChartColors>(() => readColors());
  useEffect(() => {
    const update = () => setColors(readColors());
    update();
    const observer = new MutationObserver(update);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    const media = window.matchMedia("(prefers-color-scheme: dark)");
    media.addEventListener("change", update);
    return () => {
      observer.disconnect();
      media.removeEventListener("change", update);
    };
  }, []);
  return colors;
}

/** Stable colour for a named RFM segment, independent of array order. */
const SEGMENT_ORDER = [
  "Champions",
  "Loyal Customers",
  "Promising",
  "Regular",
  "At Risk",
  "Lost",
];

export function segmentColorIndex(segment: string): number {
  const i = SEGMENT_ORDER.indexOf(segment);
  return i === -1 ? 5 : i;
}
