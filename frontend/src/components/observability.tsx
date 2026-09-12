"use client";

import { useEffect, useState } from "react";
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
 * Vercel Web Analytics (`WebAnalytics` below) can't be gated by a build-time
 * env var here: this project's `frontend-ci.yml` builds the static export in
 * CI (or locally) and ships the *already-built* `out/` via `vercel deploy`,
 * so Vercel's own build servers — the only place `VERCEL_ENV` gets set — never
 * run for this app. `<Analytics/>` unconditionally injects a
 * `<script src="/_vercel/insights/script.js">`, which only resolves once
 * Vercel's edge is actually serving the page — local dev and CI's static
 * build both 404 on it. So the check has to happen at runtime, in the
 * browser, against whatever is *actually serving this exact page load*: a
 * same-origin HEAD request to the current URL (always 200 — the page itself
 * just loaded) whose `Server` response header Vercel's edge always sets to
 * "Vercel", on any domain (custom or *.vercel.app). Web Analytics still only
 * reports once the owner also turns the feature on for the project in the
 * Vercel dashboard; in production it loads/reports same-origin
 * (`/_vercel/insights/*`), so it needs no CSP entry.
 */

const SENTRY_DSN = process.env.NEXT_PUBLIC_SENTRY_DSN?.trim();

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
  const [onVercel, setOnVercel] = useState(false);

  useEffect(() => {
    let cancelled = false;
    fetch(window.location.href, { method: "HEAD" })
      .then((res) => {
        if (!cancelled && res.headers.get("server") === "Vercel") setOnVercel(true);
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, []);

  if (!onVercel) return null;
  return <Analytics />;
}
