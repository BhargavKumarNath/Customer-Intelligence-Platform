import { defineConfig, devices } from "@playwright/test";

const PORT = 4180;

// The Phase 8 synthetic monitor (tests/e2e/synthetic.spec.ts) reruns against a
// live deploy on a schedule: .github/workflows/synthetic.yml sets this to the
// PROD_URL repo variable. When it's unset (local dev, PR CI), the suite runs
// against the local static build like everything else in tests/e2e, and no
// local server is spun up when a remote target is set.
const SYNTHETIC_BASE_URL = process.env.SYNTHETIC_BASE_URL?.replace(/\/+$/, "");

export default defineConfig({
  testDir: "./tests/e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [["github"], ["list"]] : "list",
  use: {
    baseURL: SYNTHETIC_BASE_URL ?? `http://localhost:${PORT}`,
    trace: "on-first-retry",
  },
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"] } },
    { name: "mobile", use: { ...devices["Pixel 7"] } },
  ],
  webServer: SYNTHETIC_BASE_URL
    ? undefined
    : {
        command: "node scripts/serve-out.mjs",
        port: PORT,
        reuseExistingServer: !process.env.CI,
        timeout: 30_000,
      },
});
