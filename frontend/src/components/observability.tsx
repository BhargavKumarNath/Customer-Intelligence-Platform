"use client";

import { useEffect } from "react";
import { Analytics } from "@vercel/analytics/react";

/**
 * Phase 8 monitoring hooks. Sentry is dormant by default (no secret configured
 * locally or in CI) and only activates once the owner sets
 * NEXT_PUBLIC_SENTRY_DSN at build time — same "wired but dormant until
 * configured" pattern as the Phase 6 deploy jobs. It loads via a dynamic
 * import behind a DSN check, so the Phase 4 first-load JS budget is
 * unaffected when it's off (the default) and only the Sentry chunk is
 * fetched when a DSN is actually set.
 *
 * Vercel Web Analytics (`WebAnalytics` below) needs no manually-set env var —
 * `NEXT_PUBLIC_VERCEL_ENV` is populated automatically (re-exposed from
 * Vercel's own build-time `VERCEL_ENV` in next.config.mjs) on every build that
 * actually runs on Vercel. `<Analytics/>` unconditionally injects a
 * `<script src="/_vercel/insights/script.js">`, which only resolves on
 * Vercel's platform — everywhere else (local dev, CI's static build) it 404s,
 * so `WebAnalytics` is gated on that var to keep it truly dormant off-Vercel.
 * Web Analytics itself still only reports if the owner also turns the
 * feature on for the project in the Vercel dashboard; in production it
 * loads/reports same-origin (`/_vercel/insights/*`), so it needs no CSP entry.
 */

const SENTRY_DSN = process.env.NEXT_PUBLIC_SENTRY_DSN?.trim();
const ON_VERCEL = Boolean(process.env.NEXT_PUBLIC_VERCEL_ENV);

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
  if (!ON_VERCEL) return null;
  return <Analytics />;
}
