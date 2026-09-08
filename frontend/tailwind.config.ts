import type { Config } from "tailwindcss";

/**
 * The palette is a warm editorial system (paper / ink / a single rust accent),
 * chosen to read as an analytical instrument rather than a SaaS dashboard.
 * Every colour resolves to a CSS variable defined in src/app/globals.css so the
 * light and dark themes stay in one place.
 */
const withVar = (name: string) => `hsl(var(--${name}) / <alpha-value>)`;

const config: Config = {
  darkMode: "class",
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        paper: withVar("paper"),
        surface: withVar("surface"),
        "surface-sunken": withVar("surface-sunken"),
        ink: withVar("ink"),
        "ink-muted": withVar("ink-muted"),
        "ink-faint": withVar("ink-faint"),
        rule: withVar("rule"),
        "rule-strong": withVar("rule-strong"),
        accent: withVar("accent"),
        "accent-soft": withVar("accent-soft"),
        "accent-ink": withVar("accent-ink"),
        positive: withVar("positive"),
        caution: withVar("caution"),
        critical: withVar("critical"),
        "series-1": withVar("series-1"),
        "series-2": withVar("series-2"),
        "series-3": withVar("series-3"),
        "series-4": withVar("series-4"),
        "series-5": withVar("series-5"),
        "series-6": withVar("series-6"),
      },
      fontFamily: {
        display: ["var(--font-display)", "Georgia", "serif"],
        sans: ["var(--font-sans)", "system-ui", "sans-serif"],
        mono: ["var(--font-mono)", "ui-monospace", "monospace"],
      },
      fontSize: {
        "2xs": ["0.6875rem", { lineHeight: "1rem", letterSpacing: "0.04em" }],
      },
      borderRadius: {
        sm: "3px",
        DEFAULT: "4px",
        md: "6px",
        lg: "9px",
      },
      maxWidth: {
        prose: "68ch",
        shell: "1320px",
      },
      transitionTimingFunction: {
        "out-quint": "cubic-bezier(0.22, 1, 0.36, 1)",
      },
      keyframes: {
        "fade-rise": {
          from: { opacity: "0", transform: "translateY(8px)" },
          to: { opacity: "1", transform: "translateY(0)" },
        },
        "draw-in": {
          from: { "stroke-dashoffset": "1" },
          to: { "stroke-dashoffset": "0" },
        },
        shimmer: {
          "100%": { transform: "translateX(100%)" },
        },
      },
      animation: {
        "fade-rise": "fade-rise 0.5s var(--ease-out-quint) both",
        shimmer: "shimmer 1.6s infinite",
      },
    },
  },
  plugins: [],
};

export default config;
