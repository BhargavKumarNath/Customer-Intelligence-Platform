"use client";

import { useEffect } from "react";

/**
 * Phase 8 monitoring hooks. Both integrations are dormant by default (no
 * secrets configured locally or in CI) and only activate once the owner sets
 * the corresponding NEXT_PUBLIC_* build-time env var — same "wired but
 * dormant until configured" pattern as the Phase 6 Cloudflare Pages deploy
 * jobs. Neither call ships in the initial route bundle: Sentry loads via a
 * dynamic import behind a token check, and the CF beacon is a plain
 * `<script defer>` fetched by the browser outside Next's JS bundle, so the
 * Phase 4 first-load JS budget is unaffected when they're off (the default)
 * and only the Sentry chunk is fetched when a DSN is actually set.
 */

const SENTRY_DSN = process.env.NEXT_PUBLIC_SENTRY_DSN?.trim();
const CF_BEACON_TOKEN = process.env.NEXT_PUBLIC_CF_BEACON_TOKEN?.trim();

export function ErrorTracking({ release }: { release: string }) {
  useEffect(() => {
    if (!SENTRY_DSN) return;
    let cancelled = false;
    import("@sentry/browser").then((Sentry) => {
      if (cancelled) return;
      Sentry.init({
        dsn: SENTRY_DSN,
        release,
        // Static site, no user input: sample lightly, capture errors only.
        tracesSampleRate: 0,
        replaysSessionSampleRate: 0,
        replaysOnErrorSampleRate: 0,
      });
    });
    return () => {
      cancelled = true;
    };
  }, [release]);

  return null;
}

export function WebAnalytics() {
  if (!CF_BEACON_TOKEN) return null;
  return (
    <script
      defer
      src="https://static.cloudflareinsights.com/beacon.min.js"
      data-cf-beacon={JSON.stringify({ token: CF_BEACON_TOKEN })}
    />
  );
}
