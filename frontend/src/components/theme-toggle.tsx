"use client";

import { useEffect, useState } from "react";
import { useTheme } from "./theme-provider";
import { MoonIcon, SunIcon } from "./icons";
import { cn } from "@/lib/cn";

export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const { theme, toggle } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  if (compact) {
    return (
      <button
        type="button"
        onClick={toggle}
        aria-label={mounted ? `Switch to ${theme === "dark" ? "light" : "dark"} theme` : "Toggle theme"}
        className="flex h-9 w-9 items-center justify-center rounded-md border border-rule text-ink-muted transition-colors hover:text-ink"
      >
        {mounted && theme === "dark" ? <SunIcon size={16} /> : <MoonIcon size={16} />}
      </button>
    );
  }

  return (
    <button
      type="button"
      onClick={toggle}
      className="group flex w-full items-center justify-between rounded-md border border-rule px-3 py-2 text-xs text-ink-muted transition-colors hover:border-rule-strong hover:text-ink"
      aria-label={mounted ? `Switch to ${theme === "dark" ? "light" : "dark"} theme` : "Toggle theme"}
    >
      <span>{mounted ? (theme === "dark" ? "Dark" : "Light") : "Theme"}</span>
      <span className="flex items-center gap-1.5">
        <SunIcon size={14} className={cn("transition-opacity", mounted && theme === "dark" ? "opacity-30" : "opacity-100")} />
        <span aria-hidden className="h-3 w-px bg-rule" />
        <MoonIcon size={14} className={cn("transition-opacity", mounted && theme === "dark" ? "opacity-100" : "opacity-30")} />
      </span>
    </button>
  );
}
