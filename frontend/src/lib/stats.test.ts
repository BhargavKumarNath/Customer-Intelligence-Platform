import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  analyze,
  conversionsFromRate,
  ibeta,
  noncentralTCdf,
  normalCdf,
  normalInv,
  studentTCdf,
  studentTInv,
} from "./stats";
import type { AbGridCell } from "./types";

/* --------------------------------------------------------- special functions */

describe("special functions", () => {
  it("normalCdf matches known quantiles", () => {
    expect(normalCdf(0)).toBeCloseTo(0.5, 12);
    expect(normalCdf(1.959963984540054)).toBeCloseTo(0.975, 10);
    expect(normalCdf(-2.5758293035489)).toBeCloseTo(0.005, 9);
  });

  it("normalInv inverts normalCdf", () => {
    for (const p of [0.001, 0.025, 0.1, 0.5, 0.8, 0.975, 0.999]) {
      expect(normalCdf(normalInv(p))).toBeCloseTo(p, 9);
    }
  });

  it("ibeta hits reference values", () => {
    // I_0.5(2,3) = 0.6875 exactly
    expect(ibeta(0.5, 2, 3)).toBeCloseTo(0.6875, 10);
    expect(ibeta(0, 2, 3)).toBe(0);
    expect(ibeta(1, 2, 3)).toBe(1);
  });

  it("studentTCdf and studentTInv round-trip", () => {
    for (const df of [3, 10, 30, 120, 5000]) {
      for (const p of [0.01, 0.25, 0.5, 0.9, 0.975, 0.995]) {
        const t = studentTInv(p, df);
        expect(studentTCdf(t, df)).toBeCloseTo(p, 8);
      }
    }
  });

  it("studentTInv approaches the normal quantile for large df", () => {
    expect(studentTInv(0.975, 1e6)).toBeCloseTo(1.959963984540054, 4);
  });

  it("noncentralTCdf reduces to central t when ncp = 0", () => {
    expect(noncentralTCdf(1.5, 12, 0)).toBeCloseTo(studentTCdf(1.5, 12), 12);
  });

  it("noncentralTCdf matches scipy.stats.nct.cdf reference points", () => {
    // Reference: scipy.stats.nct.cdf(x, df, nc)
    expect(noncentralTCdf(2, 10, 1.5)).toBeCloseTo(0.6591540724, 8);
    expect(noncentralTCdf(0, 20, 2)).toBeCloseTo(0.0227501319, 8);
    expect(noncentralTCdf(-1, 8, 0.5)).toBeCloseTo(0.0767230931, 8);
  });
});

/* --------------------------------------------------- parity with the Python engine */

interface FixtureCase {
  input: {
    controlN: number;
    controlConversions: number;
    treatmentN: number;
    treatmentConversions: number;
    confidenceLevel: number;
  };
  expected: {
    relativeLift: number;
    pValue: number;
    isSignificant: boolean;
    ci95Lower: number;
    ci95Upper: number;
    power: number | null;
    powerDefined: boolean;
  };
}

const fixtures = JSON.parse(
  readFileSync(join(__dirname, "__fixtures__", "ab_cases.json"), "utf8"),
) as FixtureCase[];

describe("analyze() parity with ABTestEngine.analyze_experiment", () => {
  it("has fixture cases", () => {
    expect(fixtures.length).toBeGreaterThanOrEqual(10);
  });

  for (const c of fixtures) {
    const { controlConversions: ck, treatmentConversions: tk } = c.input;
    it(`case c=${ck} t=${tk} conf=${c.input.confidenceLevel}`, () => {
      const r = analyze(c.input);
      expect(r.relativeLift).toBeCloseTo(c.expected.relativeLift, 9);
      expect(Math.abs(r.pValue - c.expected.pValue)).toBeLessThan(1e-6);
      expect(Math.abs(r.ci95[0] - c.expected.ci95Lower)).toBeLessThan(1e-6);
      expect(Math.abs(r.ci95[1] - c.expected.ci95Upper)).toBeLessThan(1e-6);
      expect(r.isSignificant).toBe(c.expected.isSignificant);
      expect(r.powerDefined).toBe(c.expected.powerDefined);
      if (c.expected.powerDefined && c.expected.power != null) {
        expect(Math.abs(r.power - c.expected.power)).toBeLessThan(1e-6);
      }
    });
  }
});

/* ---------------------------------------------- consistency with the shipped A/B grid */

const grid = JSON.parse(
  readFileSync(join(__dirname, "..", "..", "public", "data", "current", "ab_grid.json"), "utf8"),
) as AbGridCell[];

describe("analyze() reproduces every ab_grid.json cell", () => {
  it("grid is non-empty", () => {
    expect(grid.length).toBeGreaterThan(100);
  });

  it("each cell's stored statistics match a recompute within 1e-6", () => {
    let checked = 0;
    for (const cell of grid) {
      const r = analyze({
        controlN: cell.control_visitors,
        treatmentN: cell.treatment_visitors,
        controlConversions: conversionsFromRate(cell.control_visitors, cell.control_conversion_rate),
        treatmentConversions: conversionsFromRate(
          cell.treatment_visitors,
          cell.treatment_conversion_rate,
        ),
        confidenceLevel: cell.confidence_level,
      });
      expect(Math.abs(r.pValue - cell.p_value)).toBeLessThan(1e-6);
      expect(Math.abs(r.ci95[0] - cell.ci_95_lower)).toBeLessThan(1e-6);
      expect(Math.abs(r.ci95[1] - cell.ci_95_upper)).toBeLessThan(1e-6);
      expect(Math.abs(r.relativeLift - cell.relative_lift)).toBeLessThan(1e-6);
      if (!cell.power_undefined && cell.statistical_power != null) {
        // Only assert where the Python side considered power well-defined.
        if (r.powerDefined) {
          expect(Math.abs(r.power - cell.statistical_power)).toBeLessThan(1e-6);
        }
      }
      checked += 1;
    }
    expect(checked).toBe(grid.length);
  });
});
