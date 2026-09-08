import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

/* ------------------------------------------------------------------ Page header */

export function PageHeader({
  index,
  kicker,
  title,
  lede,
  meta,
}: {
  index?: string;
  kicker: string;
  title: string;
  lede?: ReactNode;
  meta?: ReactNode;
}) {
  return (
    <header className="max-w-prose">
      <div className="flex items-center gap-3">
        {index && <span className="tnum text-sm text-accent-ink">{index}</span>}
        <span className="kicker">{kicker}</span>
      </div>
      <h1 className="mt-4 text-balance text-4xl leading-[1.08] sm:text-5xl">{title}</h1>
      {lede && <p className="mt-5 text-lg leading-relaxed text-ink-muted">{lede}</p>}
      {meta && <div className="mt-6 flex flex-wrap items-center gap-x-6 gap-y-2 text-2xs text-ink-faint">{meta}</div>}
    </header>
  );
}

/* --------------------------------------------------------------------- Section */

export function Section({
  index,
  title,
  description,
  aside,
  children,
  className,
}: {
  index: string;
  title: string;
  description?: ReactNode;
  aside?: ReactNode;
  children: ReactNode;
  className?: string;
}) {
  return (
    <section className={cn("border-t border-rule pt-8", className)}>
      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
        <div className="max-w-prose">
          <div className="flex items-baseline gap-3">
            <span className="tnum text-xs text-ink-faint">{index}</span>
            <h2 className="text-2xl">{title}</h2>
          </div>
          {description && <p className="mt-3 text-sm leading-relaxed text-ink-muted">{description}</p>}
        </div>
        {aside && <div className="shrink-0 text-sm text-ink-muted">{aside}</div>}
      </div>
      <div className="mt-7">{children}</div>
    </section>
  );
}

/* ----------------------------------------------------------------------- Panel */

export function Panel({
  title,
  subtitle,
  toolbar,
  footnote,
  children,
  className,
  bodyClassName,
}: {
  title?: ReactNode;
  subtitle?: ReactNode;
  toolbar?: ReactNode;
  footnote?: ReactNode;
  children: ReactNode;
  className?: string;
  bodyClassName?: string;
}) {
  return (
    <div className={cn("panel flex flex-col", className)}>
      {(title || toolbar) && (
        <div className="flex items-start justify-between gap-4 border-b border-rule px-5 py-3.5">
          <div>
            {title && <h3 className="text-sm font-medium text-ink">{title}</h3>}
            {subtitle && <p className="mt-0.5 text-2xs text-ink-faint">{subtitle}</p>}
          </div>
          {toolbar && <div className="shrink-0">{toolbar}</div>}
        </div>
      )}
      <div className={cn("flex-1 p-5", bodyClassName)}>{children}</div>
      {footnote && (
        <div className="border-t border-rule px-5 py-2.5 text-2xs leading-relaxed text-ink-faint">
          {footnote}
        </div>
      )}
    </div>
  );
}

/* ---------------------------------------------------------------------- Figure */

export function Figure({
  label,
  value,
  sub,
  delta,
  tone = "neutral",
  className,
}: {
  label: ReactNode;
  value: ReactNode;
  sub?: ReactNode;
  delta?: { value: string; direction: "up" | "down" | "flat" };
  tone?: "neutral" | "accent";
  className?: string;
}) {
  return (
    <div className={cn("flex flex-col gap-1.5", className)}>
      <span className="kicker">{label}</span>
      <span
        className={cn(
          "tnum text-3xl leading-none tracking-tight sm:text-[2.1rem]",
          tone === "accent" ? "text-accent-ink" : "text-ink",
        )}
      >
        {value}
      </span>
      {(sub || delta) && (
        <span className="flex items-center gap-2 text-2xs text-ink-faint">
          {delta && (
            <span
              className={cn(
                "tnum inline-flex items-center gap-0.5",
                delta.direction === "up" && "text-positive",
                delta.direction === "down" && "text-critical",
              )}
            >
              {delta.direction === "up" ? "▲" : delta.direction === "down" ? "▼" : "•"} {delta.value}
            </span>
          )}
          {sub && <span>{sub}</span>}
        </span>
      )}
    </div>
  );
}

export function FigureGrid({ children, cols = 4 }: { children: ReactNode; cols?: 2 | 3 | 4 }) {
  return (
    <div
      className={cn(
        "grid gap-px overflow-hidden rounded-md border border-rule bg-rule",
        cols === 2 && "sm:grid-cols-2",
        cols === 3 && "sm:grid-cols-2 lg:grid-cols-3",
        cols === 4 && "sm:grid-cols-2 lg:grid-cols-4",
      )}
    >
      {children}
    </div>
  );
}

export function FigureCell({ children }: { children: ReactNode }) {
  return <div className="bg-surface p-5">{children}</div>;
}

/* ------------------------------------------------------------------------- Tag */

export function Tag({
  children,
  tone = "neutral",
}: {
  children: ReactNode;
  tone?: "neutral" | "accent" | "positive" | "caution" | "critical";
}) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-sm border px-1.5 py-0.5 text-2xs font-medium",
        tone === "neutral" && "border-rule text-ink-muted",
        tone === "accent" && "border-accent/40 bg-accent-soft text-accent-ink",
        tone === "positive" && "border-positive/30 text-positive",
        tone === "caution" && "border-caution/30 text-caution",
        tone === "critical" && "border-critical/30 text-critical",
      )}
    >
      {children}
    </span>
  );
}

/* --------------------------------------------------------------------- Callout */

const TONE_RING: Record<string, string> = {
  finding: "border-l-accent",
  positive: "border-l-positive",
  caution: "border-l-caution",
  critical: "border-l-critical",
};

export function Callout({
  tone = "finding",
  label,
  title,
  children,
  action,
}: {
  tone?: "finding" | "positive" | "caution" | "critical";
  label?: string;
  title: ReactNode;
  children: ReactNode;
  action?: ReactNode;
}) {
  return (
    <div className={cn("panel border-l-2 p-5", TONE_RING[tone])}>
      {label && <span className="kicker">{label}</span>}
      <h3 className="mt-1.5 font-display text-lg leading-snug text-ink">{title}</h3>
      <div className="mt-2 text-sm leading-relaxed text-ink-muted">{children}</div>
      {action && <div className="mt-3">{action}</div>}
    </div>
  );
}

/* -------------------------------------------------------------- Inline metric  */

export function Stat({ children }: { children: ReactNode }) {
  return <span className="tnum font-medium text-ink">{children}</span>;
}
