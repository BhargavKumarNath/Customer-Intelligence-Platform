"use client";

import type { PropensityFile, SegmentsFile } from "./types";

/**
 * Lazy client fetch for the two large artifacts. Keeping these out of the server
 * loader is deliberate: propensity.json and segments.json must not enter the
 * bundle graph of "/" or "/overview" (deployment_stages.md Phase 5). They are
 * requested only from /ml-engine and /user-intelligence.
 */

const BASE = "/data/current";

async function fetchJson<T>(name: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(`${BASE}/${name}`, { signal, cache: "force-cache" });
  if (!res.ok) throw new Error(`Failed to load ${name} (${res.status})`);
  return (await res.json()) as T;
}

export const fetchPropensity = (signal?: AbortSignal) =>
  fetchJson<PropensityFile>("propensity.json", signal);

export const fetchSegments = (signal?: AbortSignal) =>
  fetchJson<SegmentsFile>("segments.json", signal);

export interface HistogramBin {
  x0: number;
  x1: number;
  count: number;
}

export function histogram(values: number[], bins: number, min = 0, max = 1): HistogramBin[] {
  const width = (max - min) / bins;
  const out: HistogramBin[] = Array.from({ length: bins }, (_, i) => ({
    x0: min + i * width,
    x1: min + (i + 1) * width,
    count: 0,
  }));
  for (const v of values) {
    if (v < min || v > max) continue;
    let idx = Math.floor((v - min) / width);
    if (idx >= bins) idx = bins - 1;
    out[idx]!.count += 1;
  }
  return out;
}

export interface DecileRow {
  decile: number;
  users: number;
  meanScore: number;
  minScore: number;
}

/** Split scores into ten equal-count buckets, decile 1 = highest scores. */
export function scoreDeciles(scores: number[]): DecileRow[] {
  const sorted = [...scores].sort((a, b) => b - a);
  const n = sorted.length;
  const rows: DecileRow[] = [];
  for (let d = 0; d < 10; d += 1) {
    const start = Math.floor((d * n) / 10);
    const end = Math.floor(((d + 1) * n) / 10);
    const slice = sorted.slice(start, end);
    const sum = slice.reduce((a, b) => a + b, 0);
    rows.push({
      decile: d + 1,
      users: slice.length,
      meanScore: slice.length ? sum / slice.length : 0,
      minScore: slice.length ? slice[slice.length - 1]! : 0,
    });
  }
  return rows;
}

export function quantile(sortedAsc: number[], q: number): number {
  if (sortedAsc.length === 0) return NaN;
  const pos = (sortedAsc.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  const lo = sortedAsc[base]!;
  const hi = sortedAsc[Math.min(base + 1, sortedAsc.length - 1)]!;
  return lo + rest * (hi - lo);
}
