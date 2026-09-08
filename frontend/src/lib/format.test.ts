import { describe, expect, it } from "vitest";
import {
  compactNumber,
  currency,
  dateRangeLabel,
  percent,
  percentPoints,
  pValueLabel,
  shortSha,
} from "./format";

describe("format", () => {
  it("compactNumber", () => {
    expect(compactNumber(950)).toBe("950");
    expect(compactNumber(1645912)).toBe("1.6M");
    expect(compactNumber(75915)).toBe("75.9K");
    expect(compactNumber(7467390)).toBe("7.5M");
  });

  it("currency", () => {
    expect(currency(7467390)).toBe("$7,467,390");
    expect(currency(1234, { compact: true })).toBe("$1.2K");
  });

  it("percent variants", () => {
    expect(percent(0.0803)).toBe("8.0%");
    expect(percent(0.338, 0)).toBe("34%");
    expect(percentPoints(21.038)).toBe("21.0%");
  });

  it("pValueLabel clamps tiny values", () => {
    expect(pValueLabel(0.00001)).toBe("< 0.0001");
    expect(pValueLabel(0.0342)).toBe("0.0342");
  });

  it("shortSha keeps identity", () => {
    expect(shortSha("1956349")).toBe("1956349");
    expect(shortSha("0123456789abcdef")).toBe("0123456789ab");
  });

  it("dateRangeLabel counts inclusive days", () => {
    expect(dateRangeLabel(["2019-10-01", "2019-11-30"])).toContain("(61 days)");
  });
});
