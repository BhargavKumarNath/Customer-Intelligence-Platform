"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";

type Theme = "light" | "dark";
type Preference = Theme | "system";

interface ThemeContextValue {
  theme: Theme;
  preference: Preference;
  setPreference: (p: Preference) => void;
  toggle: () => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);
const STORAGE_KEY = "cip-theme";

function systemTheme(): Theme {
  if (typeof window === "undefined") return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [preference, setPreferenceState] = useState<Preference>("system");
  const [theme, setTheme] = useState<Theme>("light");

  useEffect(() => {
    const stored = (localStorage.getItem(STORAGE_KEY) as Preference | null) ?? "system";
    setPreferenceState(stored);
  }, []);

  useEffect(() => {
    const resolved = preference === "system" ? systemTheme() : preference;
    setTheme(resolved);
    document.documentElement.classList.toggle("dark", resolved === "dark");

    if (preference === "system") {
      const media = window.matchMedia("(prefers-color-scheme: dark)");
      const onChange = () => {
        const next = media.matches ? "dark" : "light";
        setTheme(next);
        document.documentElement.classList.toggle("dark", next === "dark");
      };
      media.addEventListener("change", onChange);
      return () => media.removeEventListener("change", onChange);
    }
  }, [preference]);

  const setPreference = useCallback((p: Preference) => {
    setPreferenceState(p);
    try {
      if (p === "system") localStorage.removeItem(STORAGE_KEY);
      else localStorage.setItem(STORAGE_KEY, p);
    } catch {
      /* private mode: fall back to in-memory only */
    }
  }, []);

  const toggle = useCallback(() => {
    setPreference(theme === "dark" ? "light" : "dark");
  }, [theme, setPreference]);

  const value = useMemo(
    () => ({ theme, preference, setPreference, toggle }),
    [theme, preference, setPreference, toggle],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
