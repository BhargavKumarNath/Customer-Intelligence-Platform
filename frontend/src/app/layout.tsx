import type { Metadata, Viewport } from "next";
import { Fraunces, Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { Providers } from "@/components/providers";
import { AppShell } from "@/components/app-shell";
import { getManifest, getMeta } from "@/lib/data";

const display = Fraunces({
  subsets: ["latin"],
  axes: ["opsz", "SOFT"],
  variable: "--font-display",
  display: "swap",
});

const sans = Inter({
  subsets: ["latin"],
  variable: "--font-sans",
  display: "swap",
});

const mono = JetBrains_Mono({
  subsets: ["latin"],
  weight: ["400", "500"],
  variable: "--font-mono",
  display: "swap",
});

export const metadata: Metadata = {
  title: {
    default: "Customer Intelligence Platform",
    template: "%s | Customer Intelligence Platform",
  },
  description:
    "A behavioral analytics read of an e-commerce event log: segments, retention, purchase propensity, and a reactivation experiment, all from precomputed data.",
  robots: { index: true, follow: true },
};

export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f4f1ea" },
    { media: "(prefers-color-scheme: dark)", color: "#141310" },
  ],
};

const THEME_SCRIPT = `
(function(){
  try {
    var stored = localStorage.getItem('cip-theme');
    var system = window.matchMedia('(prefers-color-scheme: dark)').matches;
    var dark = stored ? stored === 'dark' : system;
    document.documentElement.classList.toggle('dark', dark);
  } catch (e) {}
})();
`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const meta = getMeta();
  const manifest = getManifest();

  return (
    <html lang="en" className={`${display.variable} ${sans.variable} ${mono.variable}`} suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: THEME_SCRIPT }} />
      </head>
      <body className="min-h-screen font-sans antialiased">
        <a
          href="#main"
          className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-50 focus:panel focus:px-3 focus:py-2 focus:text-sm"
        >
          Skip to content
        </a>
        <Providers>
          <AppShell
            build={{
              gitSha: meta.git_sha,
              builtAt: meta.built_at,
              datasetRows: meta.dataset_rows,
              dateRange: meta.date_range,
              syncedAt: manifest.syncedAt,
            }}
          >
            {children}
          </AppShell>
        </Providers>
      </body>
    </html>
  );
}
