// Copy one pinned Phase 2 artifact set (../dist/data/<sha>/) into
// public/data/current/ and write a manifest the app reads at build time.
//
// The pin (DATA_SHA env -> frontend/data.lock -> sole dist/data sub-dir) is
// resolved by scripts/data-pin.mjs. fetch-data.mjs runs first in the
// predev/prebuild chain and can pull the set from a published store; this
// script assumes it is already on disk and fails hard if it is not. There is
// no silent fallback to a stale set: the build stops rather than ship
// yesterday's numbers.

import { copyFileSync, mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { DIST_ROOT, PUBLIC_DATA_DIR, REQUIRED, hint, missingFiles, resolvePin } from "./data-pin.mjs";

function die(msg) {
  console.error(`\n[sync-data] ${msg}\n`);
  console.error(hint());
  console.error("");
  process.exit(1);
}

let pin;
try {
  pin = resolvePin();
} catch (err) {
  die(err.message);
}

const srcDir = join(DIST_ROOT, pin.sha);
if (!existsSync(srcDir)) die(`pinned data set not found: dist/data/${pin.sha} (pin source: ${pin.source})`);

const missing = missingFiles(srcDir);
if (missing.length) die(`dist/data/${pin.sha} is incomplete, missing: ${missing.join(", ")}`);

rmSync(PUBLIC_DATA_DIR, { recursive: true, force: true });
mkdirSync(PUBLIC_DATA_DIR, { recursive: true });

const files = {};
for (const name of REQUIRED) {
  const from = join(srcDir, name);
  copyFileSync(from, join(PUBLIC_DATA_DIR, name));
  files[name] = statSync(from).size;
}

const meta = JSON.parse(readFileSync(join(srcDir, "meta.json"), "utf8"));
const manifest = {
  sha: pin.sha,
  pinSource: pin.source,
  gitSha: meta.git_sha,
  builtAt: meta.built_at,
  syncedAt: new Date().toISOString(),
  datasetRows: meta.dataset_rows,
  files,
};
writeFileSync(join(PUBLIC_DATA_DIR, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n");

const total = Object.values(files).reduce((a, b) => a + b, 0);
console.log(`[sync-data] pinned ${pin.sha} (source: ${pin.source}, built ${meta.built_at}, git_sha ${meta.git_sha})`);
console.log(`[sync-data] ${REQUIRED.length} files, ${(total / 1024 / 1024).toFixed(1)} MB -> public/data/current/`);
