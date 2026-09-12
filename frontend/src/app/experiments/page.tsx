import type { Metadata } from "next";
import { getAbGrid, getRfmSummary } from "@/lib/data";
import { PageHeader, Panel, Section, Callout } from "@/components/ui/primitives";
import { AbCalculator } from "@/components/experiments/ab-calculator";
import { GridExplorer } from "@/components/experiments/grid-explorer";
import { currency, integer } from "@/lib/format";

export const metadata: Metadata = {
  title: "Experiment lab",
  description:
    "An A/B calculator for the reactivation test: Welch t-test, delta-method interval, and post-hoc power, checked live against a precomputed grid.",
};

export default function ExperimentsPage() {
  const grid = getAbGrid();
  const rfm = getRfmSummary();
  const segments = Array.from(new Set(grid.map((c) => c.segment))).sort();

  const atRisk = rfm.find((s) => s.segment === "At Risk");
  const defaultSize = atRisk ? Math.min(12000, Math.max(1000, Math.round(atRisk.population / 2 / 250) * 250)) : 2500;

  return (
    <div className="space-y-14">
      <PageHeader
        index="04"
        kicker="How to decide"
        title="Experiment lab"
        lede={
          <>
            The reactivation idea from the segment analysis needs a test before it ships. This
            calculator runs the same statistics as the platform&apos;s Python engine, so you can size
            the experiment and read a result the way the pipeline would.
          </>
        }
        meta={
          <>
            <span>{grid.length} precomputed cells</span>
            <span>{segments.length} segments</span>
            {atRisk && <span>At Risk holds {currency(atRisk.total_monetary)}</span>}
          </>
        }
      />

      <Callout
        tone="finding"
        label="The question"
        level={2}
        title={
          atRisk
            ? `Would a reactivation offer move the ${integer(atRisk.population)} At Risk buyers?`
            : "Would a reactivation offer move the At Risk buyers?"
        }
      >
        These buyers used to purchase often and have gone quiet. A coupon might bring them back, or it
        might just discount buyers who would have returned anyway. An A/B test is how you tell the
        difference. Size it so it can actually detect the lift you would act on.
      </Callout>

      <Section
        index="01"
        title="Calculator"
        description="Set the design parameters. The read updates live and is cross-checked against the grid."
      >
        <AbCalculator segments={segments} grid={grid} defaultSampleSize={defaultSize} />
      </Section>

      <Section
        index="02"
        title="Precomputed grid"
        description="The A/B engine run ahead of time across design lift and confidence, per segment."
      >
        <Panel bodyClassName="p-4">
          <GridExplorer grid={grid} segments={segments} />
        </Panel>
      </Section>

      <Section index="03" title="Method">
        <div className="max-w-prose space-y-3 text-sm leading-relaxed text-ink-muted">
          <p>
            <strong className="font-medium text-ink">Significance.</strong> A two-sided Welch t-test
            on the two arms&apos; per-user conversion outcomes. Welch does not assume equal variance,
            which matters when the arms convert at different rates.
          </p>
          <p>
            <strong className="font-medium text-ink">Interval.</strong> A delta-method 95 percent
            interval on the absolute difference in conversion rate, using the normal quantile.
          </p>
          <p>
            <strong className="font-medium text-ink">Power.</strong> Post-hoc power for the observed
            effect size, computed the way statsmodels&apos; two-sample t-power does, with a
            noncentral t distribution.
          </p>
          <p>
            <strong className="font-medium text-ink">Action bar.</strong> The verdict treats a
            significant result under a 10 percent relative lift as real but not worth a rollout, which
            mirrors the bar-raiser rule the platform documents.
          </p>
        </div>
      </Section>
    </div>
  );
}
