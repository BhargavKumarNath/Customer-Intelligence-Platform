// Shared pin resolution for the two data scripts (fetch-data, sync-data).
//
// The frontend never invents numbers: every page renders one pinned Phase 2
// artifact set, identified by the git sha of the `src/` commit that produced it.
// The pin is resolved in this order:
//
//   1. DATA_SHA env var          - CI passes the precompute job's output sha
//   2. frontend/data.lock .sha   - the committed pin, for reproducible local
//                                  and preview builds decoupled from HEAD
//   3. the sole sub-directory of ../dist/data/  - convenience for a fresh
//      local `build_static_artifacts.py` run that produced exactly one set
//
// Anything ambiguous (2+ candidate dirs, none of the above) is a hard error.
// There is no silent fallback to a stale set.

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
export const FRONTEND_ROOT = resolve(here, "..");
export const DIST_ROOT = resolve(FRONTEND_ROOT, "..", "dist", "data");
export const LOCK_PATH = join(FRONTEND_ROOT, "data.lock");
export const PUBLIC_DATA_DIR = join(FRONTEND_ROOT, "public", "data", "current");

export const REQUIRED = [
  "meta.json",
  "metrics.json",
  "kpis.json",
  "retention.json",
  "rfm_summary.json",
  "affinity.json",
  "ab_grid.json",
  "segments.json",
  "propensity.json",
  "events_trimmed.parquet",
];

export function readLock() {
  if (!existsSync(LOCK_PATH)) return null;
  try {
    const lock = JSON.parse(readFileSync(LOCK_PATH, "utf8"));
    return typeof lock?.sha === "string" && lock.sha.trim() ? lock : null;
  } catch (err) {
    throw new Error(`data.lock is present but not valid JSON: ${err.message}`);
  }
}

/**
 * @returns {{ sha: string, source: "env" | "lock" | "dist" }}
 */
export function resolvePin() {
  const fromEnv = process.env.DATA_SHA?.trim();
  if (fromEnv) return { sha: fromEnv, source: "env" };

  const lock = readLock();
  if (lock) return { sha: lock.sha.trim(), source: "lock" };

  if (!existsSync(DIST_ROOT)) {
    throw new Error(
      `no pin available: DATA_SHA unset, no data.lock, and no dist/data dir at ${DIST_ROOT}`,
    );
  }
  const dirs = readdirSync(DIST_ROOT, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);
  if (dirs.length === 1) return { sha: dirs[0], source: "dist" };
  if (dirs.length === 0) {
    throw new Error(`dist/data is empty at ${DIST_ROOT} and no DATA_SHA / data.lock pin is set`);
  }
  throw new Error(
    `dist/data has ${dirs.length} artifact sets (${dirs.join(", ")}); set DATA_SHA or data.lock to pick one`,
  );
}

export function missingFiles(dir) {
  return REQUIRED.filter((f) => !existsSync(join(dir, f)));
}

export function hint() {
  return [
    "Build the artifacts locally:",
    "  python scripts/build_static_artifacts.py --out dist/data --git-sha <sha>",
    "or pull a published set by setting DATA_BASE_URL (see scripts/fetch-data.mjs),",
    "then re-run with DATA_SHA=<sha> (or pin it in frontend/data.lock).",
  ].join("\n");
}
