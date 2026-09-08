// Copy one pinned Phase 2 artifact set (../dist/data/<sha>/) into
// public/data/current/ and write a manifest the app reads at build time.
//
// Pin resolution order:
//   1. DATA_SHA env var (CI passes the precompute-data job's output sha)
//   2. the sole sub-directory of ../dist/data/ when there is exactly one
//
// A missing / ambiguous pin is a hard error. There is no silent fallback to a
// stale set: the build must stop rather than ship yesterday's numbers.

import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, copyFileSync, writeFileSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(here, "..");
const distRoot = resolve(frontendRoot, "..", "dist", "data");
const outDir = join(frontendRoot, "public", "data", "current");

const REQUIRED = [
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

function die(msg) {
  console.error(`\n[sync-data] ${msg}\n`);
  console.error("Build the artifacts first:");
  console.error("  python scripts/build_static_artifacts.py --out dist/data --git-sha <sha>");
  console.error("then re-run with DATA_SHA=<sha> (or leave it unset if dist/data has one set).\n");
  process.exit(1);
}

function resolveSha() {
  const fromEnv = process.env.DATA_SHA?.trim();
  if (fromEnv) return fromEnv;
  if (!existsSync(distRoot)) die(`no dist/data directory at ${distRoot}`);
  const dirs = readdirSync(distRoot, { withFileTypes: true })
    .filter((d) => d.isDirectory())
    .map((d) => d.name);
  if (dirs.length === 1) return dirs[0];
  if (dirs.length === 0) die(`dist/data is empty at ${distRoot}`);
  die(`dist/data has ${dirs.length} artifact sets (${dirs.join(", ")}); set DATA_SHA to pick one`);
}

const sha = resolveSha();
const srcDir = join(distRoot, sha);
if (!existsSync(srcDir)) die(`pinned data set not found: dist/data/${sha}`);

const missing = REQUIRED.filter((f) => !existsSync(join(srcDir, f)));
if (missing.length) die(`dist/data/${sha} is incomplete, missing: ${missing.join(", ")}`);

rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

const files = {};
for (const name of REQUIRED) {
  const from = join(srcDir, name);
  copyFileSync(from, join(outDir, name));
  files[name] = statSync(from).size;
}

const meta = JSON.parse(readFileSync(join(srcDir, "meta.json"), "utf8"));
const manifest = {
  sha,
  gitSha: meta.git_sha,
  builtAt: meta.built_at,
  syncedAt: new Date().toISOString(),
  datasetRows: meta.dataset_rows,
  files,
};
writeFileSync(join(outDir, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");

const total = Object.values(files).reduce((a, b) => a + b, 0);
console.log(`[sync-data] pinned ${sha} (built ${meta.built_at})`);
console.log(`[sync-data] ${REQUIRED.length} files, ${(total / 1024 / 1024).toFixed(1)} MB -> public/data/current/`);
