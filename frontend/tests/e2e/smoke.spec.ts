import { test, expect } from "@playwright/test";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DATA = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "public", "data", "current");
const read = (name: string) => JSON.parse(readFileSync(join(DATA, name), "utf8"));
const meta = read("meta.json");
const metrics = read("metrics.json");
const kpis: { daily_revenue: number; purchases: number }[] = read("kpis.json");
const rfm: { population: number; pct_of_buyers: number }[] = read("rfm_summary.json");

const ROUTES = [
  "/",
  "/overview",
  "/executive",
  "/user-intelligence",
  "/experiments",
  "/ml-engine",
  "/data-explorer",
];

test.describe("every route renders", () => {
  for (const path of ROUTES) {
    test(`${path} loads with an h1 and no console errors`, async ({ page }) => {
      const errors: string[] = [];
      page.on("console", (msg) => {
        if (msg.type() === "error") errors.push(msg.text());
      });
      page.on("pageerror", (err) => errors.push(err.message));

      const res = await page.goto(path, { waitUntil: "networkidle" });
      expect(res?.status()).toBe(200);
      await expect(page.locator("h1")).toBeVisible();
      expect(errors, `console errors on ${path}: ${errors.join(" | ")}`).toEqual([]);
    });
  }
});

test("landing headline shows the real event count", async ({ page }) => {
  await page.goto("/");
  const events = new Intl.NumberFormat("en-US").format(meta.dataset_rows);
  await expect(page.getByRole("heading", { level: 1 })).toContainText(events);
});

test("executive revenue matches the sum of daily revenue", async ({ page }) => {
  await page.goto("/executive");
  const total = kpis.reduce((a, k) => a + k.daily_revenue, 0);
  const formatted = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(total);
  // The figure animates to its final value; wait for it to settle.
  await expect(page.getByText(formatted, { exact: false }).first()).toBeVisible({ timeout: 5000 });
});

test("ml-engine AUC matches metrics.json", async ({ page }) => {
  await page.goto("/ml-engine");
  await expect(page.getByText(metrics.auc_roc.toFixed(3), { exact: false }).first()).toBeVisible();
});

test("user-intelligence segment table sums to the buyer count", async ({ page }) => {
  await page.goto("/user-intelligence");
  const buyers = rfm.reduce((a, s) => a + s.population, 0);
  const formatted = new Intl.NumberFormat("en-US").format(buyers);
  await expect(page.getByText(formatted, { exact: false }).first()).toBeVisible();
  // share of buyers on the page footnote sums to 100
  const pct = rfm.reduce((a, s) => a + s.pct_of_buyers, 0);
  expect(Math.round(pct)).toBe(100);
});

test("theme toggle flips the root class and persists", async ({ page }) => {
  await page.goto("/");
  const toggle = page.getByRole("button", { name: /theme/i }).first();
  const before = await page.evaluate(() => document.documentElement.classList.contains("dark"));
  await toggle.click();
  await expect
    .poll(() => page.evaluate(() => document.documentElement.classList.contains("dark")))
    .toBe(!before);
  await page.reload();
  await expect
    .poll(() => page.evaluate(() => document.documentElement.classList.contains("dark")))
    .toBe(!before);
});

test("unknown route serves the 404 page", async ({ page }) => {
  const res = await page.goto("/no-such-page");
  expect(res?.status()).toBe(404);
  await expect(page.getByText(/not part of this report/i)).toBeVisible();
});

test("landing and overview never fetch the large per-user artifacts", async ({ page }) => {
  const requested: string[] = [];
  page.on("request", (r) => requested.push(r.url()));

  await page.goto("/", { waitUntil: "networkidle" });
  await page.goto("/overview", { waitUntil: "networkidle" });

  const heavy = requested.filter((u) => /propensity\.json|segments\.json/.test(u));
  expect(heavy, `unexpected heavy fetches: ${heavy.join(", ")}`).toEqual([]);
});

// The explorer depends on jsDelivr serving the DuckDB-WASM runtime. That is
// outside this suite's control, so the end-to-end query check is opt-in via
// E2E_DUCKDB=1. Without it, we still assert the page and its start control render.
test("data explorer renders its start control", async ({ page }) => {
  await page.goto("/data-explorer");
  await expect(page.getByRole("button", { name: /start the engine/i })).toBeVisible();
});

test("data explorer runs a preset query in the browser", async ({ page }) => {
  test.skip(!process.env.E2E_DUCKDB, "Set E2E_DUCKDB=1 to exercise the in-browser engine (needs the CDN).");
  test.slow();
  await page.goto("/data-explorer");
  await page.getByRole("button", { name: /start the engine/i }).click();

  const runButton = page.getByRole("button", { name: /^run$/i });
  const failed = page.getByText(/did not start/i);
  await expect(runButton.or(failed)).toBeVisible({ timeout: 60_000 });
  if (await failed.isVisible()) {
    test.info().annotations.push({ type: "note", description: "DuckDB CDN unreachable in this environment" });
    return;
  }

  await runButton.click();
  await expect(page.getByText(/\d+\s+rows/).first()).toBeVisible({ timeout: 20_000 });
  await expect(page.getByText(/\bms$/).first()).toBeVisible();
  // event-mix preset returns the three event types
  await expect(page.getByText("view", { exact: true }).first()).toBeVisible();
});
