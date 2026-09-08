// Pull one published Phase 2 artifact set into ../dist/data/<sha>/ when it is
// not already on disk.
//
// The precompute job publishes dist/data/<sha>/ to a durable store keyed by sha
// (Cloudflare R2 or a GitHub Release asset). Point DATA_BASE_URL at that store
// and this script fetches the pinned set before sync-data.mjs copies it into
// public/data/current/.
//
//   DATA_BASE_URL=https://data.example.com/cip   -> fetches <base>/<sha>/<file>
//
// Behaviour:
//   - set already complete on disk         -> no-op (idempotent, offline-safe)
//   - set missing + DATA_BASE_URL set       -> download the 10 files
//   - set missing + DATA_BASE_URL unset     -> exit 0 with a note; sync-data.mjs
//     then produces the real (actionable) error. This keeps `pnpm build` working
//     unchanged for a purely local workflow.

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { DIST_ROOT, REQUIRED, hint, missingFiles, resolvePin } from "./data-pin.mjs";

const baseUrl = process.env.DATA_BASE_URL?.trim().replace(/\/+$/, "");

let pin;
try {
  pin = resolvePin();
} catch (err) {
  if (!baseUrl) {
    console.log(`[fetch-data] ${err.message}`);
    console.log("[fetch-data] no DATA_BASE_URL set; leaving it to sync-data.mjs to report.");
    process.exit(0);
  }
  // With a base URL we still need a sha. Only DATA_SHA / data.lock can give one
  // here (dist/data is empty), so re-raise with the hint.
  console.error(`\n[fetch-data] ${err.message}\n\n${hint()}\n`);
  process.exit(1);
}

const targetDir = join(DIST_ROOT, pin.sha);
const missing = missingFiles(targetDir);

if (missing.length === 0) {
  console.log(`[fetch-data] ${pin.sha} already present (pin source: ${pin.source}); nothing to fetch.`);
  process.exit(0);
}

if (!baseUrl) {
  console.log(
    `[fetch-data] ${pin.sha} is missing ${missing.length} file(s) and DATA_BASE_URL is not set.`,
  );
  console.log(`[fetch-data] ${hint()}`);
  process.exit(0);
}

console.log(`[fetch-data] pin ${pin.sha} (source: ${pin.source}); fetching ${missing.length} file(s) from ${baseUrl}/${pin.sha}/`);
mkdirSync(targetDir, { recursive: true });

for (const name of missing) {
  const url = `${baseUrl}/${pin.sha}/${name}`;
  let res;
  try {
    res = await fetch(url);
  } catch (err) {
    console.error(`\n[fetch-data] GET ${url} failed: ${err.cause?.message ?? err.message}\n`);
    console.error("DATA_BASE_URL is unreachable. Check the host, or build the set locally.");
    process.exit(1);
  }
  if (!res.ok) {
    console.error(`\n[fetch-data] GET ${url} -> ${res.status} ${res.statusText}\n`);
    console.error("The pinned sha is not in the store, or DATA_BASE_URL is wrong.");
    process.exit(1);
  }
  const buf = Buffer.from(await res.arrayBuffer());
  writeFileSync(join(targetDir, name), buf);
  console.log(`[fetch-data]   ${name}  ${(buf.byteLength / 1024).toFixed(0)} KiB`);
}

const stillMissing = missingFiles(targetDir);
if (stillMissing.length) {
  console.error(`\n[fetch-data] incomplete after fetch, still missing: ${stillMissing.join(", ")}\n`);
  process.exit(1);
}
console.log(`[fetch-data] ${REQUIRED.length} files in dist/data/${pin.sha}/`);
