"use client";

import { useMemo, useState } from "react";
import type { AbGridCell } from "@/lib/types";
import { analyze, conversionsFromRate } from "@/lib/stats";
import { Slider, Select } from "@/components/ui/controls";
import { Panel, Callout, Tag } from "@/components/ui/primitives";
import { DensityPair } from "@/components/charts/distribution";
import { integer, percent, pValueLabel } from "@/lib/format";

const GRID_LIFTS = [0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0, 2.0, 5.0];
const CONFIDENCE_OPTIONS = [
  { value: "0.8", label: "80%" },
  { value: "0.9", label: "90%" },
  { value: "0.95", label: "95%" },
  { value: "0.99", label: "99%" },
];
const BUSINESS_LIFT_BAR = 0.1;

function normalCurve(mean: number, sd: number, xs: number[]) {
  return xs.map((x) => ({
    x,
    y: sd > 0 ? Math.exp(-0.5 * ((x - mean) / sd) ** 2) / (sd * Math.sqrt(2 * Math.PI)) : 0,
  }));
}

export function AbCalculator({
  segments,
  grid,
  defaultSampleSize,
}: {
  segments: string[];
  grid: AbGridCell[];
  defaultSampleSize: number;
}) {
  const [segment, setSegment] = useState(segments[0] ?? "At Risk");
  const [sampleSize, setSampleSize] = useState(defaultSampleSize);
  const [baseline, setBaseline] = useState(0.12);
  const [lift, setLift] = useState(0.15);
  const [confidence, setConfidence] = useState("0.95");

  const conf = Number(confidence);

  const result = useMemo(() => {
    const controlConversions = Math.round(sampleSize * baseline);
    const treatmentConversions = Math.round(sampleSize * baseline * (1 + lift));
    return analyze({
      controlN: sampleSize,
      treatmentN: sampleSize,
      controlConversions,
      treatmentConversions,
      confidenceLevel: conf,
    });
  }, [sampleSize, baseline, lift, conf]);

  const density = useMemo(() => {
    const seC = Math.sqrt((result.controlRate * (1 - result.controlRate)) / sampleSize);
    const seT = Math.sqrt((result.treatmentRate * (1 - result.treatmentRate)) / sampleSize);
    const lo = Math.max(0, Math.min(result.controlRate, result.treatmentRate) - 4 * Math.max(seC, seT));
    const hi = Math.max(result.controlRate, result.treatmentRate) + 4 * Math.max(seC, seT);
    const xs = Array.from({ length: 80 }, (_, i) => lo + ((hi - lo) * i) / 79);
    return { control: normalCurve(result.controlRate, seC, xs), treatment: normalCurve(result.treatmentRate, seT, xs) };
  }, [result, sampleSize]);

  const gridCheck = useMemo(() => {
    const nearestLift = GRID_LIFTS.reduce((a, b) => (Math.abs(b - lift) < Math.abs(a - lift) ? b : a), GRID_LIFTS[0]!);
    const cell = grid.find(
      (c) => c.segment === segment && c.lift === nearestLift && Math.abs(c.confidence_level - conf) < 1e-9,
    );
    if (!cell) return null;
    const recomputed = analyze({
      controlN: cell.control_visitors,
      treatmentN: cell.treatment_visitors,
      controlConversions: conversionsFromRate(cell.control_visitors, cell.control_conversion_rate),
      treatmentConversions: conversionsFromRate(cell.treatment_visitors, cell.treatment_conversion_rate),
      confidenceLevel: cell.confidence_level,
    });
    const rows = [
      { label: "Relative lift", stored: cell.relative_lift, got: recomputed.relativeLift, fmt: (v: number) => percent(v, 2) },
      { label: "p-value", stored: cell.p_value, got: recomputed.pValue, fmt: pValueLabel },
      { label: "CI lower", stored: cell.ci_95_lower, got: recomputed.ci95[0], fmt: (v: number) => v.toFixed(5) },
      { label: "CI upper", stored: cell.ci_95_upper, got: recomputed.ci95[1], fmt: (v: number) => v.toFixed(5) },
      {
        label: "Power",
        stored: cell.power_undefined ? null : cell.statistical_power,
        got: recomputed.powerDefined ? recomputed.power : null,
        fmt: (v: number | null) => (v == null ? "undefined" : v.toFixed(5)),
      },
    ];
    const maxDelta = Math.max(
      ...rows.map((r) => (r.stored == null || r.got == null ? (r.stored == null && r.got == null ? 0 : 1) : Math.abs(r.stored - r.got))),
    );
    return { cell, nearestLift, rows, matches: maxDelta < 1e-6 };
  }, [grid, segment, lift, conf]);

  const verdict = (() => {
    if (!result.isSignificant)
      return {
        tone: "caution" as const,
        title: "Inconclusive at this size",
        body: `The ${percent(result.relativeLift, 1)} observed difference is within noise at ${percent(conf, 0)} confidence (p ${pValueLabel(result.pValue)}). Increase traffic or run longer.`,
      };
    if (result.relativeLift < BUSINESS_LIFT_BAR)
      return {
        tone: "finding" as const,
        title: "Significant but below the action bar",
        body: `The lift is real (p ${pValueLabel(result.pValue)}) but ${percent(result.relativeLift, 1)} is under the ${percent(BUSINESS_LIFT_BAR, 0)} threshold this team uses to justify a rollout.`,
      };
    return {
      tone: "positive" as const,
      title: "Ship candidate",
      body: `Significant at ${percent(conf, 0)} (p ${pValueLabel(result.pValue)}) and ${percent(result.relativeLift, 1)} clears the ${percent(BUSINESS_LIFT_BAR, 0)} action bar. Confirm the guardrail metrics before rollout.`,
    };
  })();

  return (
    <div className="grid gap-6 lg:grid-cols-[20rem_minmax(0,1fr)]">
      <div className="panel h-fit space-y-6 p-5">
        <Select
          label="Segment"
          value={segment}
          onChange={setSegment}
          options={segments.map((s) => ({ value: s, label: s }))}
        />
        <Slider
          label="Users per arm"
          value={sampleSize}
          min={500}
          max={12000}
          step={250}
          onChange={setSampleSize}
          display={integer(sampleSize)}
        />
        <Slider
          label="Baseline conversion"
          value={baseline}
          min={0.02}
          max={0.3}
          step={0.005}
          onChange={setBaseline}
          display={percent(baseline, 1)}
        />
        <Slider
          label="Expected relative lift"
          value={lift}
          min={0}
          max={0.6}
          step={0.01}
          onChange={setLift}
          display={percent(lift, 0)}
        />
        <Select label="Confidence" value={confidence} onChange={setConfidence} options={CONFIDENCE_OPTIONS} />
        <p className="text-2xs leading-relaxed text-ink-faint">
          Outcomes are the expected counts at these rates, so the reads are deterministic. The
          synthetic-draw path in the Python engine is not reproduced here.
        </p>
      </div>

      <div className="space-y-6">
        <div className="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-rule bg-rule sm:grid-cols-4">
          {[
            { k: "Control", v: percent(result.controlRate, 2) },
            { k: "Treatment", v: percent(result.treatmentRate, 2) },
            { k: "Observed lift", v: percent(result.relativeLift, 1) },
            { k: "p-value", v: pValueLabel(result.pValue) },
          ].map((c) => (
            <div key={c.k} className="bg-surface p-4">
              <span className="kicker">{c.k}</span>
              <p className="tnum mt-1.5 text-xl text-ink">{c.v}</p>
            </div>
          ))}
        </div>

        <Callout tone={verdict.tone} label={`Verdict at ${percent(conf, 0)} confidence`} title={verdict.title}>
          {verdict.body}
          <div className="mt-3 flex flex-wrap gap-2">
            <Tag tone={result.isSignificant ? "positive" : "caution"}>
              {result.isSignificant ? "significant" : "not significant"}
            </Tag>
            <Tag tone={result.powerDefined && result.power >= 0.8 ? "positive" : "caution"}>
              power {result.powerDefined ? result.power.toFixed(2) : "undefined"}
            </Tag>
            <Tag>
              95% CI on absolute diff [{result.ci95[0].toFixed(4)}, {result.ci95[1].toFixed(4)}]
            </Tag>
          </div>
        </Callout>

        <Panel title="Sampling distributions" subtitle="Conversion rate for each arm, normal approximation">
          <DensityPair control={density.control} treatment={density.treatment} height={240} />
          <p className="mt-2 text-2xs text-ink-faint">
            Separation between the curves is what a significant result looks like. Drop the sample
            size and watch them merge.
          </p>
        </Panel>

        {gridCheck && (
          <Panel
            title="Grid check"
            subtitle={`Nearest precomputed cell: ${segment}, lift ${percent(gridCheck.nearestLift, 0)}, ${percent(conf, 0)}`}
            toolbar={
              <Tag tone={gridCheck.matches ? "positive" : "critical"}>
                {gridCheck.matches ? "matches within 1e-6" : "mismatch"}
              </Tag>
            }
          >
            <table className="w-full text-sm">
              <thead className="text-2xs uppercase tracking-wider text-ink-faint">
                <tr>
                  <th className="py-1.5 text-left font-medium">Statistic</th>
                  <th className="py-1.5 text-right font-medium">Stored in grid</th>
                  <th className="py-1.5 text-right font-medium">This calculator</th>
                </tr>
              </thead>
              <tbody>
                {gridCheck.rows.map((r) => (
                  <tr key={r.label} className="border-t border-rule/70">
                    <td className="py-1.5 text-ink-muted">{r.label}</td>
                    <td className="tnum py-1.5 text-right text-ink-muted">
                      {r.fmt(r.stored as number & (number | null))}
                    </td>
                    <td className="tnum py-1.5 text-right text-ink">
                      {r.fmt(r.got as number & (number | null))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="mt-3 text-2xs leading-relaxed text-ink-faint">
              The calculator is fed this cell&apos;s exact visitor and conversion counts, then its
              Welch t-test, delta-method interval, and power are compared to what the Python engine
              wrote. Agreement here is the parity check.
            </p>
          </Panel>
        )}

      </div>
    </div>
  );
}
