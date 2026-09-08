/** Display formatting. Keep every rendered number traceable to a source value. */

const NBSP = " ";

export function compactNumber(value: number, digits = 1): string {
  const abs = Math.abs(value);
  if (abs < 1000) return String(Math.round(value));
  const units = [
    { v: 1e9, s: "B" },
    { v: 1e6, s: "M" },
    { v: 1e3, s: "K" },
  ];
  for (const { v, s } of units) {
    if (abs >= v) {
      const n = value / v;
      const rounded = n >= 100 ? Math.round(n) : Number(n.toFixed(digits));
      return `${rounded}${s}`;
    }
  }
  return String(value);
}

export function integer(value: number): string {
  return new Intl.NumberFormat("en-US").format(Math.round(value));
}

export function decimal(value: number, digits = 2): string {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  }).format(value);
}

export function currency(value: number, opts: { compact?: boolean } = {}): string {
  if (opts.compact) return `$${compactNumber(value)}`;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(value);
}

export function currencyPrecise(value: number): string {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

/** 0.0803 -> "8.0%" */
export function percent(value: number, digits = 1): string {
  return `${(value * 100).toFixed(digits)}%`;
}

/** already-scaled percent: 21.038 -> "21.0%" */
export function percentPoints(value: number, digits = 1): string {
  return `${value.toFixed(digits)}%`;
}

export function signedPercent(value: number, digits = 1): string {
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return `${sign}${(value * 100).toFixed(digits)}%`;
}

export function multiple(value: number, digits = 1): string {
  return `${value.toFixed(digits)}${NBSP}x`;
}

export function shortDate(iso: string): string {
  const d = new Date(iso.length <= 10 ? `${iso}T00:00:00Z` : iso);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

export function monthDay(iso: string): string {
  return shortDate(iso);
}

export function fullDate(iso: string): string {
  const d = new Date(iso.length <= 10 ? `${iso}T00:00:00Z` : iso);
  return d.toLocaleDateString("en-US", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  });
}

export function dateRangeLabel([from, to]: [string, string]): string {
  const a = new Date(`${from}T00:00:00Z`);
  const b = new Date(`${to}T00:00:00Z`);
  const days = Math.round((b.getTime() - a.getTime()) / 86_400_000) + 1;
  return `${fullDate(from)} to ${fullDate(to)} (${days} days)`;
}

export function pValueLabel(p: number): string {
  if (p < 0.0001) return "< 0.0001";
  return p.toFixed(4);
}

/** Trim a git sha for display without losing its identity. */
export function shortSha(sha: string): string {
  return sha.length > 12 ? sha.slice(0, 12) : sha;
}
