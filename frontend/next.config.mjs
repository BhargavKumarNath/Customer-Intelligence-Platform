/**
 * Static export. There is no server: every route is prerendered to HTML at build
 * time from the pinned Phase 2 data artifacts, and DuckDB-WASM does the ad-hoc
 * work in the browser. See deployment_stages.md Phase 4.
 */

// DuckDB-WASM ships ~35 MB wasm modules. Cloudflare Pages caps a single asset at
// 25 MiB, so the runtime is loaded from jsDelivr (the library's own default host)
// rather than self-hosted. The CSP below is widened just enough for that one CDN.
const DUCKDB_CDN = "https://cdn.jsdelivr.net";

// Phase 8 monitoring: both dormant unless the owner sets the matching
// NEXT_PUBLIC_* var at build time (src/components/observability.tsx), but the
// CSP has to allow their hosts either way since it's baked in at build time,
// before it's known whether a given deploy has them configured.
const CF_BEACON = "https://static.cloudflareinsights.com";
const SENTRY_INGEST = "https://*.ingest.sentry.io https://*.ingest.us.sentry.io";

const devScript = process.env.NODE_ENV === "development" ? " 'unsafe-eval'" : "";
// 'unsafe-inline' for script-src is unavoidable with `output: export`: Next emits
// the RSC hydration bootstrap as inline <script> tags whose content changes every
// build, so they cannot be hash-pinned in a static _headers file and there is no
// server to issue per-request nonces. This is an accepted trade on a static site
// with no user input, no cookies, and no injection surface (see the review, 14.2).
const csp = [
  "default-src 'self'",
  `script-src 'self' 'unsafe-inline' 'wasm-unsafe-eval'${devScript} ${DUCKDB_CDN} ${CF_BEACON}`,
  "style-src 'self' 'unsafe-inline'",
  "img-src 'self' data:",
  "font-src 'self' data:",
  `connect-src 'self' ${DUCKDB_CDN} ${CF_BEACON} ${SENTRY_INGEST}`,
  `worker-src 'self' blob: ${DUCKDB_CDN}`,
  "object-src 'none'",
  "base-uri 'self'",
  "form-action 'self'",
  "frame-ancestors 'none'",
].join("; ");

const securityHeaders = [
  { key: "Content-Security-Policy", value: csp },
  { key: "Strict-Transport-Security", value: "max-age=63072000; includeSubDomains; preload" },
  { key: "X-Content-Type-Options", value: "nosniff" },
  { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
  { key: "Permissions-Policy", value: "camera=(), microphone=(), geolocation=(), browsing-topics=()" },
];

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "export",
  reactStrictMode: true,
  trailingSlash: true,
  images: { unoptimized: true },
};

// `headers()` is inert under `output: export`; the real headers ship via
// public/_headers (Cloudflare Pages). Only wire it up for `next dev` so local
// development mirrors the deployed CSP, and the export stays warning-free.
if (process.env.NODE_ENV === "development") {
  nextConfig.headers = async () => [{ source: "/:path*", headers: securityHeaders }];
}

export default nextConfig;
