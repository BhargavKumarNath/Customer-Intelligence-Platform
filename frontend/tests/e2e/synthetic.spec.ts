import { test, expect } from "@playwright/test";

/**
 * Phase 8 synthetic monitor (deployment_stages.md): loads /executive headless
 * and asserts a known KPI renders. Runs on a schedule (.github/workflows/synthetic.yml)
 * against the live site via SYNTHETIC_BASE_URL / playwright.config.ts, and also
 * locally against the static build like the rest of tests/e2e, so a regression
 * that would trip the live monitor fails the PR first.
 *
 * The expected revenue figure is fetched from the target's own
 * /data/current/kpis.json rather than a local file, so this test needs
 * nothing but the deployed site itself (no Python pipeline, no repo checkout
 * of build artifacts) — it is an independent "does the page still reflect its
 * own published data" check, not a re-derivation of the numbers (that parity
 * is already gated by tests/precompute and the frontend-ci `check` job).
 */
test("synthetic: /executive is up and renders the known revenue KPI", async ({ page, request }) => {
  const kpiRes = await request.get("/data/current/kpis.json");
  expect(kpiRes.ok(), "GET /data/current/kpis.json").toBeTruthy();
  const kpis: { daily_revenue: number }[] = await kpiRes.json();
  const total = kpis.reduce((a, k) => a + k.daily_revenue, 0);
  const formatted = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(total);

  const res = await page.goto("/executive", { waitUntil: "networkidle" });
  expect(res?.status(), "GET /executive status").toBe(200);
  await expect(page.getByText(formatted, { exact: false }).first()).toBeVisible({ timeout: 10_000 });
});
