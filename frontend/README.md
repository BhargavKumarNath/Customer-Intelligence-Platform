# Frontend

Static Next.js (App Router) site that renders the Phase 2 precomputed artifacts. No
server on the request path. DuckDB-WASM does the ad-hoc work in the browser.

## Run it

```bash
# 1. build the data artifacts (from the repo root, dev env installed)
python scripts/build_static_artifacts.py --out dist/data --git-sha dev

# 2. install and run
cd frontend
pnpm install
pnpm dev          # predev copies dist/data/dev -> public/data/current
```

### Data pin

The `predev` / `prebuild` hooks run `scripts/fetch-data.mjs` then
`scripts/sync-data.mjs`. The artifact set is pinned in this order:

1. `DATA_SHA` env var (CI passes the precompute job's output sha)
2. `frontend/data.lock` `.sha` (the committed pin, tracked in git)
3. the sole sub-directory of `../dist/data/` when there is exactly one

`fetch-data.mjs` pulls the pinned set from `DATA_BASE_URL/<sha>/` (the published
R2 / Release store) when it is not already on disk; with no `DATA_BASE_URL` it is
a no-op and you build the set locally (step 1 above). `sync-data.mjs` then copies
it to `public/data/current/` and writes `manifest.json`. A missing or ambiguous
pin is a hard error; there is no fallback to a stale set.

## Checks

```bash
pnpm lint          # eslint (next/core-web-vitals + next/typescript)
pnpm typecheck     # tsc --noEmit, strict
pnpm test          # vitest: A/B parity, formatters, artifact invariants
pnpm build         # next build, output: export -> out/
pnpm exec playwright test          # route smoke + number parity + theme
E2E_DUCKDB=1 pnpm exec playwright test   # also exercises the in-browser SQL engine
pnpm gen:ab-fixture                 # regenerate the A/B parity fixture from Python
```

## Layout

```
src/app/                one route per Streamlit page, ordered as a narrative
src/components/ui/      primitives: PageHeader, Section, Panel, Figure, Callout, DataTable, states
src/components/charts/  Recharts wrappers + hand-built cohort heatmap and funnel
src/lib/data.ts         server-only loader for the small artifacts (build-time inline)
src/lib/client-data.ts  lazy client fetch for segments.json / propensity.json
src/lib/stats.ts        TypeScript port of ABTestEngine.analyze_experiment
src/lib/duckdb.ts       DuckDB-WASM client with a row cap and query timeout
```

## Design

Warm editorial palette (paper, ink, a single rust accent) defined once as CSS
variables in `src/app/globals.css`; light and dark redefine the same roles.
Fraunces for display, Inter for UI, JetBrains Mono for figures. Motion is
`framer-motion`, gated by `prefers-reduced-motion`.

## Deploy

`output: 'export'` produces `out/`. Ship it to Vercel. `public/vercel.json`
carries the CSP (it allows `cdn.jsdelivr.net` for the DuckDB-WASM runtime,
loaded from there rather than self-hosted), HSTS, and the cache rules: HTML gets
a short shared TTL with `stale-while-revalidate`, hashed assets and
`/data/current/*` are `immutable`.

`.github/workflows/frontend-ci.yml` builds and tests `out/`, then (once the owner
sets the `VERCEL_PROJECT_ID` repo variable plus `VERCEL_TOKEN` / `VERCEL_ORG_ID`
secrets) deploys a preview per PR and production on `main` via the `vercel` CLI,
followed by a post-deploy smoke against `PROD_URL`. Vercel's own Git integration
is the alternative: leave the variable unset and the CI deploy jobs stay skipped.
Data is published by `.github/workflows/precompute.yml`. See `deployment_stages.md`
Phases 4, 5, and 6.
