"use client";

import * as RSlider from "@radix-ui/react-slider";
import * as RSelect from "@radix-ui/react-select";
import * as RTabs from "@radix-ui/react-tabs";
import * as RTooltip from "@radix-ui/react-tooltip";
import { CheckIcon, ChevronDownIcon } from "@/components/icons";
import { cn } from "@/lib/cn";

/* ---------------------------------------------------------------------- Slider */

export function Slider({
  label,
  value,
  min,
  max,
  step,
  onChange,
  display,
  hint,
}: {
  label: string;
  value: number;
  min: number;
  max: number;
  step: number;
  onChange: (v: number) => void;
  display: string;
  hint?: string;
}) {
  return (
    <div>
      <div className="flex items-baseline justify-between">
        <label className="text-sm text-ink">{label}</label>
        <span className="tnum text-sm text-accent-ink">{display}</span>
      </div>
      <RSlider.Root
        className="relative mt-3 flex h-5 w-full touch-none select-none items-center"
        value={[value]}
        min={min}
        max={max}
        step={step}
        onValueChange={([v]) => onChange(v ?? min)}
        aria-label={label}
      >
        <RSlider.Track className="relative h-[3px] w-full grow rounded-full bg-rule">
          <RSlider.Range className="absolute h-full rounded-full bg-accent" />
        </RSlider.Track>
        <RSlider.Thumb
          className="block h-4 w-4 rounded-full border border-accent bg-surface shadow-sm outline-none transition-transform focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-surface data-[state=active]:scale-110"
        />
      </RSlider.Root>
      {hint && <p className="mt-2 text-2xs text-ink-faint">{hint}</p>}
    </div>
  );
}

/* ---------------------------------------------------------------------- Select */

export function Select<T extends string>({
  label,
  value,
  onChange,
  options,
  className,
}: {
  label: string;
  value: T;
  onChange: (v: T) => void;
  options: { value: T; label: string }[];
  className?: string;
}) {
  return (
    <div className={className}>
      <label className="mb-1.5 block text-2xs uppercase tracking-wider text-ink-faint">{label}</label>
      <RSelect.Root value={value} onValueChange={(v) => onChange(v as T)}>
        <RSelect.Trigger
          className="inline-flex w-full items-center justify-between gap-2 rounded-md border border-rule bg-surface px-3 py-2 text-sm text-ink outline-none transition-colors hover:border-rule-strong focus-visible:ring-2 focus-visible:ring-accent"
          aria-label={label}
        >
          <RSelect.Value />
          <RSelect.Icon>
            <ChevronDownIcon size={14} className="text-ink-faint" />
          </RSelect.Icon>
        </RSelect.Trigger>
        <RSelect.Portal>
          <RSelect.Content
            position="popper"
            sideOffset={4}
            className="z-50 max-h-64 min-w-[--radix-select-trigger-width] overflow-hidden rounded-md border border-rule bg-surface shadow-lg"
          >
            <RSelect.Viewport className="p-1">
              {options.map((opt) => (
                <RSelect.Item
                  key={opt.value}
                  value={opt.value}
                  className="relative flex cursor-pointer select-none items-center rounded-sm px-2 py-1.5 pl-7 text-sm text-ink-muted outline-none data-[highlighted]:bg-accent-soft data-[highlighted]:text-accent-ink"
                >
                  <RSelect.ItemIndicator className="absolute left-1.5 inline-flex items-center">
                    <CheckIcon size={13} />
                  </RSelect.ItemIndicator>
                  <RSelect.ItemText>{opt.label}</RSelect.ItemText>
                </RSelect.Item>
              ))}
            </RSelect.Viewport>
          </RSelect.Content>
        </RSelect.Portal>
      </RSelect.Root>
    </div>
  );
}

/* ------------------------------------------------------------------------ Tabs */

export function Tabs({
  tabs,
  value,
  onChange,
  children,
}: {
  tabs: { value: string; label: string }[];
  value: string;
  onChange: (v: string) => void;
  children: React.ReactNode;
}) {
  return (
    <RTabs.Root value={value} onValueChange={onChange}>
      <RTabs.List className="flex gap-1 border-b border-rule">
        {tabs.map((t) => (
          <RTabs.Trigger
            key={t.value}
            value={t.value}
            className={cn(
              "-mb-px border-b-2 border-transparent px-3 py-2 text-sm text-ink-faint outline-none transition-colors",
              "hover:text-ink data-[state=active]:border-accent data-[state=active]:text-ink focus-visible:ring-2 focus-visible:ring-accent",
            )}
          >
            {t.label}
          </RTabs.Trigger>
        ))}
      </RTabs.List>
      {children}
    </RTabs.Root>
  );
}

export const TabPanel = RTabs.Content;

/* --------------------------------------------------------------------- Tooltip */

export function InfoTip({ children, label }: { children: React.ReactNode; label: string }) {
  return (
    <RTooltip.Provider delayDuration={150}>
      <RTooltip.Root>
        <RTooltip.Trigger asChild>{children}</RTooltip.Trigger>
        <RTooltip.Portal>
          <RTooltip.Content
            sideOffset={6}
            className="z-50 max-w-[16rem] rounded-md border border-rule bg-surface px-2.5 py-1.5 text-2xs leading-relaxed text-ink-muted shadow-md"
          >
            {label}
            <RTooltip.Arrow className="fill-[hsl(var(--rule))]" />
          </RTooltip.Content>
        </RTooltip.Portal>
      </RTooltip.Root>
    </RTooltip.Provider>
  );
}

/* ------------------------------------------------------------------ Toggle set */

export function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
  ariaLabel,
}: {
  options: { value: T; label: string }[];
  value: T;
  onChange: (v: T) => void;
  ariaLabel: string;
}) {
  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className="inline-flex rounded-md border border-rule bg-surface-sunken p-0.5"
    >
      {options.map((opt) => (
        <button
          key={opt.value}
          type="button"
          role="radio"
          aria-checked={value === opt.value}
          onClick={() => onChange(opt.value)}
          className={cn(
            "rounded-[4px] px-2.5 py-1 text-xs transition-colors",
            value === opt.value ? "bg-surface text-ink shadow-sm" : "text-ink-faint hover:text-ink",
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
